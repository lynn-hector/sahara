"""DeepSeek LLM Provider — DeepSeek API 流式调用实现。

DeepSeek API 兼容 OpenAI 接口，使用 openai AsyncClient 调用。
针对 DeepSeek-R1 的 reasoning_content（思维链）做了专门处理，
通过 emitter 实时推送推理过程。

支持模型:
    - deepseek-chat     (V3, 通用对话)
    - deepseek-reasoner (R1, 深度推理, 返回 reasoning_content)
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

DEEPSEEK_API_BASE = "https://api.deepseek.com"


class DeepSeekProvider(LLMProvider):
    """DeepSeek API 流式 Provider。

    基于 OpenAI 兼容接口，通过 openai.AsyncOpenAI 调用。
    按 api_key 缓存客户端实例，减少 TCP 连接开销。

    DeepSeek-R1 特殊处理:
    - 流式 chunk 中的 reasoning_content 通过 emitter.emit_thinking 推送
    - 最终 reasoning_content 聚合到 LLMResponse.reasoning_content 字段
    - R1 不支持 tool_calls，检测到 reasoner 模型时自动跳过 tools 参数
    """

    def __init__(self) -> None:
        self._client_cache: dict[str, Any] = {}

    def _get_or_create_client(
        self, api_key: str, api_base: str | None = None,
    ) -> Any:
        base_url = api_base or DEEPSEEK_API_BASE
        cache_key = f"{api_key}@{base_url}"
        if cache_key not in self._client_cache:
            import openai
            self._client_cache[cache_key] = openai.AsyncOpenAI(
                api_key=api_key,
                base_url=base_url,
            )
        return self._client_cache[cache_key]

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
        client = self._get_or_create_client(api_key, api_base)

        full_messages: list[dict[str, Any]] = [
            {"role": "system", "content": system},
            *messages,
        ]

        is_reasoner = "reasoner" in model

        kwargs: dict[str, Any] = {
            "model": model,
            "messages": full_messages,
            "max_tokens": max(1, max_tokens),
            "stream": True,
        }

        if tools and not is_reasoner:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        text_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_calls_accum: dict[int, dict[str, Any]] = {}
        usage_info: dict[str, int] = {}
        finish_reason = "stop"

        stream = await client.chat.completions.create(**kwargs)
        async for chunk in stream:
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
                            "id": tc_delta.id or "",
                            "name": "",
                            "arguments": "",
                        }
                    entry = tool_calls_accum[idx]
                    if tc_delta.id:
                        entry["id"] = tc_delta.id
                    if tc_delta.function:
                        if tc_delta.function.name:
                            entry["name"] = tc_delta.function.name
                        if tc_delta.function.arguments:
                            entry["arguments"] += tc_delta.function.arguments

            if choice.finish_reason:
                finish_reason = choice.finish_reason

            if hasattr(chunk, "usage") and chunk.usage:
                usage_info = {
                    "input_tokens": chunk.usage.prompt_tokens or 0,
                    "output_tokens": chunk.usage.completion_tokens or 0,
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
