"""LiteLLM Provider — 通过 LiteLLM 统一适配 100+ LLM Provider。

参考 nanobot/providers/litellm_provider.py 设计。
消息使用 OpenAI 通用格式，LiteLLM 内部处理各 Provider 的格式转换。
支持异步流式调用，通过 emitter 实时推送 delta 事件。
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog

from sahara_runtime.llm.base import LLMProvider
from sahara_runtime.llm.types import LLMResponse, ToolCallRequest

if TYPE_CHECKING:
    from sahara_runtime.events.emitter import RunEmitter

logger = structlog.get_logger(__name__)


class LiteLLMProvider(LLMProvider):
    """LiteLLM 统一适配 Provider。

    通过 model string 路由到不同的后端:
    - "anthropic/claude-sonnet-4-5" → Anthropic
    - "deepseek/deepseek-chat" → DeepSeek
    - "openai/gpt-4o" → OpenAI
    - "openai/kimi-k2.5" → Kimi (via OpenAI-compatible endpoint)

    加模型只需改 model string + 配置对应的 API key，零代码改动。
    """

    async def chat(
        self,
        *,
        api_key: str,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        emitter: RunEmitter,
        max_tokens: int = 8192,
        api_base: str | None = None,
    ) -> LLMResponse:
        from litellm import acompletion

        full_messages: list[dict[str, Any]] = [
            {"role": "system", "content": system},
            *messages,
        ]

        effective_api_base = api_base
        if not effective_api_base and "deepseek" in model:
            effective_api_base = "https://api.deepseek.com"

        kwargs: dict[str, Any] = {
            "model": model,
            "messages": full_messages,
            "max_tokens": max(1, max_tokens),
            "stream": True,
            "api_key": api_key,
        }
        if effective_api_base:
            kwargs["api_base"] = effective_api_base
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        logger.debug(
            "litellm_chat_request",
            model=model,
            api_base=effective_api_base or "(provider default)",
            tool_count=len(tools) if tools else 0,
            message_count=len(full_messages),
        )

        text_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_calls_accum: dict[int, dict[str, Any]] = {}
        usage_info: dict[str, int] = {}
        finish_reason = "stop"

        response = await acompletion(**kwargs)
        async for chunk in response:
            choice = chunk.choices[0] if chunk.choices else None
            if choice is None:
                continue

            delta = choice.delta

            if delta.content:
                text_parts.append(delta.content)
                await emitter.emit_delta(delta.content)

            reasoning = getattr(delta, "reasoning_content", None)
            if reasoning:
                reasoning_parts.append(reasoning)
                await emitter.emit_thinking(reasoning)

            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    idx = tc_delta.index
                    if idx not in tool_calls_accum:
                        tool_calls_accum[idx] = {
                            "id": getattr(tc_delta, "id", None) or "",
                            "name": "",
                            "arguments": "",
                        }
                    entry = tool_calls_accum[idx]
                    if tc_delta.id:
                        entry["id"] = tc_delta.id
                    func = getattr(tc_delta, "function", None)
                    if func:
                        if func.name:
                            entry["name"] = func.name
                        if func.arguments:
                            entry["arguments"] += func.arguments

            if choice.finish_reason:
                finish_reason = choice.finish_reason

            if hasattr(chunk, "usage") and chunk.usage:
                usage_info = {
                    "input_tokens": getattr(chunk.usage, "prompt_tokens", 0),
                    "output_tokens": getattr(chunk.usage, "completion_tokens", 0),
                }

        tool_calls: list[ToolCallRequest] = []
        for _idx in sorted(tool_calls_accum):
            entry = tool_calls_accum[_idx]
            args_str = entry["arguments"]
            try:
                arguments = json.loads(args_str) if args_str else {}
            except (json.JSONDecodeError, ValueError):
                arguments = {"raw": args_str}
            tool_calls.append(ToolCallRequest(
                id=entry["id"],
                name=entry["name"],
                arguments=arguments,
            ))

        return LLMResponse(
            content="".join(text_parts) or None,
            tool_calls=tool_calls,
            finish_reason=finish_reason,
            usage=usage_info,
            reasoning_content="".join(reasoning_parts) or None,
        )
