"""LLM 调用的统一数据类型。

参考 nanobot/providers/base.py 的 ToolCallRequest + LLMResponse 设计。
消息交换统一使用 OpenAI 格式，各 Provider 内部自行转换为原生 API 格式。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ToolCallRequest:
    """LLM 返回的工具调用请求。"""

    id: str
    name: str
    arguments: dict[str, Any]

    def to_openai_tool_call(self) -> dict[str, Any]:
        """序列化为 OpenAI 格式的 tool_call payload (通用交换格式)。"""
        return {
            "id": self.id,
            "type": "function",
            "function": {
                "name": self.name,
                "arguments": json.dumps(self.arguments, ensure_ascii=False),
            },
        }


@dataclass
class LLMResponse:
    """LLM 单次调用的结构化返回。

    流式推送 (delta/thinking) 在 Provider.chat() 内部通过 emitter 实时完成，
    此处仅保存最终聚合结果。
    """

    content: str | None
    tool_calls: list[ToolCallRequest] = field(default_factory=list)
    finish_reason: str = "stop"
    usage: dict[str, int] = field(default_factory=dict)
    thinking: str | None = None
    reasoning_content: str | None = None

    @property
    def has_tool_calls(self) -> bool:
        return len(self.tool_calls) > 0

    @property
    def input_tokens(self) -> int:
        return self.usage.get("input_tokens", 0)

    @property
    def output_tokens(self) -> int:
        return self.usage.get("output_tokens", 0)
