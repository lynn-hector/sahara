"""Anthropic LLM Provider — Anthropic Messages API 流式调用实现。

接收 OpenAI 通用格式消息，内部转换为 Anthropic 原生格式后调用 SDK。
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


def _convert_to_anthropic(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """将 OpenAI 通用格式消息列表转换为 Anthropic Messages API 格式。

    转换规则:
    - assistant + tool_calls → content blocks [text, tool_use, ...]
    - role:"tool" → 合并为 role:"user" content [tool_result, ...]
    - user/assistant 纯文本 → 保持不变
    """
    result: list[dict[str, Any]] = []

    i = 0
    while i < len(messages):
        msg = messages[i]
        role = msg.get("role", "")

        if role == "assistant":
            content_blocks: list[dict[str, Any]] = []
            text = msg.get("content")
            if text:
                content_blocks.append({"type": "text", "text": text})

            tool_calls = msg.get("tool_calls")
            if tool_calls:
                for tc in tool_calls:
                    func = tc.get("function", {})
                    args = func.get("arguments", "{}")
                    if isinstance(args, str):
                        try:
                            args = json.loads(args)
                        except (json.JSONDecodeError, ValueError):
                            args = {"raw": args}
                    content_blocks.append({
                        "type": "tool_use",
                        "id": tc.get("id", ""),
                        "name": func.get("name", ""),
                        "input": args,
                    })

            if content_blocks:
                result.append({"role": "assistant", "content": content_blocks})
            else:
                result.append({"role": "assistant", "content": msg.get("content") or ""})
            i += 1

        elif role == "tool":
            tool_results: list[dict[str, Any]] = []
            while i < len(messages) and messages[i].get("role") == "tool":
                tmsg = messages[i]
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tmsg.get("tool_call_id", ""),
                    "content": tmsg.get("content", ""),
                })
                i += 1
            result.append({"role": "user", "content": tool_results})

        else:
            result.append({"role": role, "content": msg.get("content", "")})
            i += 1

    return result


def _convert_tools_to_anthropic(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """将 OpenAI 格式的 tools 列表转换为 Anthropic 格式。

    OpenAI:    {"type": "function", "function": {"name": ..., "description": ..., "parameters": ...}}
    Anthropic: {"name": ..., "description": ..., "input_schema": ...}
    """
    result: list[dict[str, Any]] = []
    for tool in tools:
        func = tool.get("function", {})
        result.append({
            "name": func.get("name", ""),
            "description": func.get("description", ""),
            "input_schema": func.get("parameters", {}),
        })
    return result


class AnthropicProvider(LLMProvider):
    """Anthropic Messages API 流式 Provider。

    接收 OpenAI 通用格式消息，内部通过 _convert_to_anthropic() 转换后调用 SDK。
    通过 api_key 管理 AsyncAnthropic 客户端实例（缓存复用，减少 TCP 握手开销）。

    处理以下 SSE 事件类型:
        - message_start:        提取 input_tokens
        - content_block_start:  检测 tool_use block, 初始化 tool 状态
        - content_block_delta:  累积 text / tool JSON / thinking 内容
        - content_block_stop:   解析 tool JSON -> tool_calls 列表
        - message_delta:        提取 output_tokens
    """

    def __init__(self) -> None:
        self._client_cache: dict[str, Any] = {}

    def _get_or_create_client(self, api_key: str) -> Any:
        """根据 api_key 获取或创建缓存的 AsyncAnthropic 客户端。"""
        if api_key not in self._client_cache:
            import anthropic
            self._client_cache[api_key] = anthropic.AsyncAnthropic(api_key=api_key)
        return self._client_cache[api_key]

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
        client = self._get_or_create_client(api_key)
        anthropic_messages = _convert_to_anthropic(messages)

        kwargs: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": anthropic_messages,
        }
        if tools:
            kwargs["tools"] = _convert_tools_to_anthropic(tools)

        text_parts: list[str] = []
        thinking_parts: list[str] = []
        tool_calls: list[ToolCallRequest] = []
        current_tool_id: str | None = None
        current_tool_name: str | None = None
        current_tool_json = ""
        usage_info: dict[str, int] = {}

        async with client.messages.stream(**kwargs) as stream:
            async for event in stream:
                event_type = event.type

                if event_type == "content_block_start":
                    block = event.content_block
                    if block.type == "tool_use":
                        current_tool_id = block.id
                        current_tool_name = block.name
                        current_tool_json = ""

                elif event_type == "content_block_delta":
                    delta = event.delta
                    if delta.type == "text_delta":
                        text_parts.append(delta.text)
                        await emitter.emit_delta(delta.text)
                    elif delta.type == "input_json_delta":
                        if current_tool_id is not None:
                            current_tool_json += delta.partial_json
                    elif delta.type == "thinking_delta":
                        thinking_parts.append(delta.thinking)
                        await emitter.emit_thinking(delta.thinking)

                elif event_type == "content_block_stop":
                    if current_tool_id is not None:
                        try:
                            arguments = json.loads(current_tool_json) if current_tool_json else {}
                        except json.JSONDecodeError:
                            arguments = {"raw": current_tool_json}
                        tool_calls.append(ToolCallRequest(
                            id=current_tool_id,
                            name=current_tool_name or "",
                            arguments=arguments,
                        ))
                        current_tool_id = None
                        current_tool_name = None
                        current_tool_json = ""

                elif event_type == "message_delta":
                    if hasattr(event, "usage") and event.usage:
                        usage_info["output_tokens"] = getattr(
                            event.usage, "output_tokens", 0
                        )

                elif event_type == "message_start":
                    if hasattr(event, "message") and hasattr(event.message, "usage"):
                        usage_info["input_tokens"] = getattr(
                            event.message.usage, "input_tokens", 0
                        )

        return LLMResponse(
            content="".join(text_parts) or None,
            tool_calls=tool_calls,
            finish_reason="tool_use" if tool_calls else "end_turn",
            usage=usage_info,
            thinking="".join(thinking_parts) or None,
        )
