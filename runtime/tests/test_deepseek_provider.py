"""DeepSeekProvider 单元测试 — 流式 text / reasoning / tool_calls 解析。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sahara_runtime.llm.deepseek_provider import DeepSeekProvider, DEEPSEEK_API_BASE
from sahara_runtime.llm.types import LLMResponse


class FakeEmitter:
    def __init__(self):
        self.deltas: list[str] = []
        self.thinkings: list[str] = []

    async def emit_delta(self, text: str, stream: str = "assistant") -> None:
        self.deltas.append(text)

    async def emit_thinking(self, text: str) -> None:
        self.thinkings.append(text)


@dataclass
class FakeDelta:
    content: str | None = None
    reasoning_content: str | None = None
    tool_calls: list | None = None


@dataclass
class FakeChoice:
    delta: FakeDelta
    finish_reason: str | None = None


@dataclass
class FakeUsage:
    prompt_tokens: int = 10
    completion_tokens: int = 20


@dataclass
class FakeChunk:
    choices: list[FakeChoice]
    usage: FakeUsage | None = None


async def _fake_stream(chunks):
    for c in chunks:
        yield c


class TestDeepSeekProviderInit:
    def test_client_cache_created(self):
        provider = DeepSeekProvider()
        assert provider._client_cache == {}

    def test_api_base_constant(self):
        assert DEEPSEEK_API_BASE == "https://api.deepseek.com"


class TestDeepSeekProviderChat:
    @pytest.mark.asyncio
    async def test_text_streaming(self):
        provider = DeepSeekProvider()
        emitter = FakeEmitter()

        chunks = [
            FakeChunk(choices=[FakeChoice(delta=FakeDelta(content="Hello "))]),
            FakeChunk(choices=[FakeChoice(delta=FakeDelta(content="world!"))]),
            FakeChunk(
                choices=[FakeChoice(delta=FakeDelta(), finish_reason="stop")],
                usage=FakeUsage(prompt_tokens=5, completion_tokens=2),
            ),
        ]

        mock_client = MagicMock()
        mock_client.chat.completions.create = AsyncMock(
            return_value=_fake_stream(chunks),
        )
        provider._client_cache["test_key@https://api.deepseek.com"] = mock_client

        response = await provider.chat(
            api_key="test_key",
            model="deepseek-chat",
            system="You are helpful.",
            messages=[{"role": "user", "content": "hi"}],
            tools=None,
            emitter=emitter,
        )

        assert response.content == "Hello world!"
        assert response.has_tool_calls is False
        assert response.finish_reason == "stop"
        assert emitter.deltas == ["Hello ", "world!"]

    @pytest.mark.asyncio
    async def test_reasoning_content_r1(self):
        provider = DeepSeekProvider()
        emitter = FakeEmitter()

        chunks = [
            FakeChunk(choices=[FakeChoice(
                delta=FakeDelta(reasoning_content="Let me think..."),
            )]),
            FakeChunk(choices=[FakeChoice(
                delta=FakeDelta(reasoning_content=" step by step."),
            )]),
            FakeChunk(choices=[FakeChoice(
                delta=FakeDelta(content="The answer is 42."),
            )]),
            FakeChunk(choices=[FakeChoice(
                delta=FakeDelta(), finish_reason="stop",
            )]),
        ]

        mock_client = MagicMock()
        mock_client.chat.completions.create = AsyncMock(
            return_value=_fake_stream(chunks),
        )
        provider._client_cache["key@https://api.deepseek.com"] = mock_client

        response = await provider.chat(
            api_key="key",
            model="deepseek-reasoner",
            system="",
            messages=[{"role": "user", "content": "what is 6*7?"}],
            tools=None,
            emitter=emitter,
        )

        assert response.content == "The answer is 42."
        assert response.reasoning_content == "Let me think... step by step."
        assert emitter.thinkings == ["Let me think...", " step by step."]
        assert emitter.deltas == ["The answer is 42."]

    @pytest.mark.asyncio
    async def test_reasoner_skips_tools(self):
        """deepseek-reasoner 模型不支持 tools，应自动跳过。"""
        provider = DeepSeekProvider()
        emitter = FakeEmitter()

        chunks = [
            FakeChunk(choices=[FakeChoice(
                delta=FakeDelta(content="ok"), finish_reason="stop",
            )]),
        ]

        mock_client = MagicMock()
        mock_client.chat.completions.create = AsyncMock(
            return_value=_fake_stream(chunks),
        )
        provider._client_cache["key@https://api.deepseek.com"] = mock_client

        tools = [{"type": "function", "function": {"name": "exec"}}]
        await provider.chat(
            api_key="key",
            model="deepseek-reasoner",
            system="",
            messages=[{"role": "user", "content": "hi"}],
            tools=tools,
            emitter=emitter,
        )

        call_kwargs = mock_client.chat.completions.create.call_args[1]
        assert "tools" not in call_kwargs

    @pytest.mark.asyncio
    async def test_tool_calls_streaming(self):
        provider = DeepSeekProvider()
        emitter = FakeEmitter()

        @dataclass
        class FakeFunction:
            name: str | None = None
            arguments: str | None = None

        @dataclass
        class FakeToolCallDelta:
            index: int = 0
            id: str | None = None
            function: FakeFunction | None = None

        chunks = [
            FakeChunk(choices=[FakeChoice(delta=FakeDelta(
                content="Let me run that.",
            ))]),
            FakeChunk(choices=[FakeChoice(delta=FakeDelta(
                tool_calls=[FakeToolCallDelta(
                    index=0, id="call_1",
                    function=FakeFunction(name="exec", arguments='{"co'),
                )],
            ))]),
            FakeChunk(choices=[FakeChoice(delta=FakeDelta(
                tool_calls=[FakeToolCallDelta(
                    index=0,
                    function=FakeFunction(arguments='mmand":"ls"}'),
                )],
            ))]),
            FakeChunk(choices=[FakeChoice(
                delta=FakeDelta(), finish_reason="tool_calls",
            )]),
        ]

        mock_client = MagicMock()
        mock_client.chat.completions.create = AsyncMock(
            return_value=_fake_stream(chunks),
        )
        provider._client_cache["key@https://api.deepseek.com"] = mock_client

        response = await provider.chat(
            api_key="key",
            model="deepseek-chat",
            system="",
            messages=[{"role": "user", "content": "run ls"}],
            tools=[{"type": "function", "function": {"name": "exec"}}],
            emitter=emitter,
        )

        assert response.content == "Let me run that."
        assert response.has_tool_calls is True
        assert len(response.tool_calls) == 1
        tc = response.tool_calls[0]
        assert tc.id == "call_1"
        assert tc.name == "exec"
        assert tc.arguments == {"command": "ls"}

    @pytest.mark.asyncio
    async def test_custom_api_base(self):
        provider = DeepSeekProvider()
        emitter = FakeEmitter()

        chunks = [
            FakeChunk(choices=[FakeChoice(
                delta=FakeDelta(content="ok"), finish_reason="stop",
            )]),
        ]

        mock_client = MagicMock()
        mock_client.chat.completions.create = AsyncMock(
            return_value=_fake_stream(chunks),
        )
        custom_base = "https://my-proxy.example.com"
        provider._client_cache[f"key@{custom_base}"] = mock_client

        response = await provider.chat(
            api_key="key",
            model="deepseek-chat",
            system="",
            messages=[],
            tools=None,
            emitter=emitter,
            api_base=custom_base,
        )

        assert response.content == "ok"
