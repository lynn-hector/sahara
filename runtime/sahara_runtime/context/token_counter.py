"""TokenCounter — 估算消息列表的 token 数量。

Phase 1: 基于字符数的粗估 (4 chars ≈ 1 token)。
Phase 2: 接入 tiktoken 精确计数。
"""

from __future__ import annotations

import json
from typing import Any

CHARS_PER_TOKEN = 4


class TokenCounter:
    """估算 token 数量。"""

    def count_text(self, text: str) -> int:
        return max(1, len(text) // CHARS_PER_TOKEN)

    def count_message(self, message: dict[str, Any]) -> int:
        """估算单条 Anthropic 消息的 token 数。"""
        overhead = 4  # role + framing
        content = message.get("content", "")

        if isinstance(content, str):
            return overhead + self.count_text(content)

        if isinstance(content, list):
            total = overhead
            for block in content:
                if isinstance(block, dict):
                    if block.get("type") == "text":
                        total += self.count_text(block.get("text", ""))
                    elif block.get("type") == "tool_use":
                        total += self.count_text(json.dumps(block.get("input", {}))) + 10
                    elif block.get("type") == "tool_result":
                        total += self.count_text(str(block.get("content", ""))) + 5
                    else:
                        total += self.count_text(json.dumps(block))
                else:
                    total += self.count_text(str(block))
            return total

        return overhead + self.count_text(str(content))

    def count_messages(self, messages: list[dict[str, Any]]) -> int:
        return sum(self.count_message(m) for m in messages)

    def count_system(self, system: str) -> int:
        return self.count_text(system) + 4
