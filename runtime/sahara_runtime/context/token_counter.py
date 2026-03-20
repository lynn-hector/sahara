"""TokenCounter — 估算消息列表的 token 数量。

支持 OpenAI 通用格式消息:
- assistant: content + tool_calls
- tool: tool_call_id + name + content
- user/system: content (str 或 list)
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
        """估算单条 OpenAI 格式消息的 token 数。"""
        overhead = 4  # role + framing
        content = message.get("content", "")

        if isinstance(content, str):
            tokens = overhead + self.count_text(content)
        elif isinstance(content, list):
            tokens = overhead
            for block in content:
                if isinstance(block, dict):
                    text = block.get("text", "")
                    if text:
                        tokens += self.count_text(text)
                    else:
                        tokens += self.count_text(json.dumps(block))
                else:
                    tokens += self.count_text(str(block))
        else:
            tokens = overhead + self.count_text(str(content))

        tool_calls = message.get("tool_calls")
        if tool_calls:
            tokens += self.count_text(json.dumps(tool_calls)) + 10

        if message.get("tool_call_id"):
            tokens += 5

        if message.get("name"):
            tokens += self.count_text(message["name"])

        return tokens

    def count_messages(self, messages: list[dict[str, Any]]) -> int:
        return sum(self.count_message(m) for m in messages)

    def count_system(self, system: str) -> int:
        return self.count_text(system) + 4
