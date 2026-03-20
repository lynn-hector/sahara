"""Mock LLM Provider — 无 API key 时的降级路径。

回显用户输入并模拟流式推送，用于开发和测试。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from sahara_runtime.llm.base import LLMProvider
from sahara_runtime.llm.types import LLMResponse

if TYPE_CHECKING:
    from sahara_runtime.events.emitter import RunEmitter


class MockProvider(LLMProvider):
    """Mock LLM — 回显用户输入，模拟 delta 推送。"""

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
        user_text = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, str):
                    user_text = content
                break

        response = f'Hello from Sahara Runtime! You said: "{user_text}"'

        chunks = [response[i: i + 10] for i in range(0, len(response), 10)]
        for chunk in chunks:
            await emitter.emit_delta(chunk)
            await asyncio.sleep(0.05)

        return LLMResponse(
            content=response,
            tool_calls=[],
            finish_reason="end_turn",
            usage={"input_tokens": 10, "output_tokens": len(response) // 4},
        )
