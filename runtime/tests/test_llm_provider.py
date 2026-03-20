"""LLMProvider 单元测试 — MockProvider + chat_with_retry 重试逻辑。"""

from dataclasses import dataclass

import pytest

from sahara_runtime.llm.base import LLMProvider
from sahara_runtime.llm.mock_provider import MockProvider
from sahara_runtime.llm.types import LLMResponse


class FakeEmitter:
    def __init__(self):
        self.deltas: list[str] = []
        self.thinkings: list[str] = []

    async def emit_delta(self, text: str, stream: str = "assistant") -> None:
        self.deltas.append(text)

    async def emit_thinking(self, text: str) -> None:
        self.thinkings.append(text)


# ── MockProvider 测试 ──────────────────────────────────


class TestMockProvider:
    @pytest.mark.asyncio
    async def test_echoes_user_message(self):
        provider = MockProvider()
        emitter = FakeEmitter()
        response = await provider.chat(
            api_key="",
            model="mock",
            system="You are helpful",
            messages=[{"role": "user", "content": "ping"}],
            tools=None,
            emitter=emitter,
        )
        assert "ping" in response.content
        assert response.has_tool_calls is False
        assert response.finish_reason == "end_turn"
        assert len(emitter.deltas) > 0

    @pytest.mark.asyncio
    async def test_returns_usage(self):
        provider = MockProvider()
        emitter = FakeEmitter()
        response = await provider.chat(
            api_key="", model="mock", system="",
            messages=[{"role": "user", "content": "hi"}],
            tools=None, emitter=emitter,
        )
        assert response.input_tokens > 0
        assert response.output_tokens > 0

    @pytest.mark.asyncio
    async def test_empty_messages_fallback(self):
        provider = MockProvider()
        emitter = FakeEmitter()
        response = await provider.chat(
            api_key="", model="mock", system="",
            messages=[], tools=None, emitter=emitter,
        )
        assert response.content is not None
        assert '""' in response.content


# ── chat_with_retry 测试 ──────────────────────────────


class FailThenSucceedProvider(LLMProvider):
    """前 N 次调用抛异常，之后成功。"""

    def __init__(self, fail_count: int, exc: Exception):
        self._fail_count = fail_count
        self._exc = exc
        self._call_count = 0

    async def chat(
        self, *, api_key, model, system, messages, tools, emitter,
        max_tokens=8192, api_base=None,
    ):
        self._call_count += 1
        if self._call_count <= self._fail_count:
            raise self._exc
        return LLMResponse(content="ok", usage={"input_tokens": 10, "output_tokens": 5})


@dataclass
class FakeModelConfig:
    provider: str = "anthropic"
    model_id: str = "test-model"
    max_tokens: int = 1024
    max_context_tokens: int = 4096
    max_iterations: int = 5
    api_base: str = ""


class FakeModelRouter:
    def __init__(self):
        self.success_calls = 0
        self.error_calls = 0
        self.release_calls = 0

    def get_api_key_for_session(self, session_key, config):
        return "fake_api_key"

    def report_key_success(self, session_key):
        self.success_calls += 1

    def report_key_error(self, session_key, status_code):
        self.error_calls += 1

    def release_session(self, session_key):
        self.release_calls += 1


class TestChatWithRetry:
    @pytest.mark.asyncio
    async def test_success_on_first_try(self):
        provider = FailThenSucceedProvider(fail_count=0, exc=Exception("nope"))
        router = FakeModelRouter()
        emitter = FakeEmitter()
        import sahara_runtime.llm.base as mod
        original = mod.RETRY_BACKOFF_BASE
        mod.RETRY_BACKOFF_BASE = 0.01
        try:
            response = await provider.chat_with_retry(
                model_router=router,
                session_key="s1",
                model_config=FakeModelConfig(),
                system="sys",
                messages=[],
                tools=None,
                emitter=emitter,
            )
            assert response.content == "ok"
            assert router.success_calls == 1
            assert router.error_calls == 0
        finally:
            mod.RETRY_BACKOFF_BASE = original

    @pytest.mark.asyncio
    async def test_retries_on_transient_error(self):
        exc = Exception("server error")
        exc.status_code = 429
        provider = FailThenSucceedProvider(fail_count=1, exc=exc)
        router = FakeModelRouter()
        emitter = FakeEmitter()
        import sahara_runtime.llm.base as mod
        original = mod.RETRY_BACKOFF_BASE
        mod.RETRY_BACKOFF_BASE = 0.01
        try:
            response = await provider.chat_with_retry(
                model_router=router,
                session_key="s2",
                model_config=FakeModelConfig(),
                system="sys",
                messages=[],
                tools=None,
                emitter=emitter,
            )
            assert response.content == "ok"
            assert router.release_calls == 1
            assert router.error_calls == 1
        finally:
            mod.RETRY_BACKOFF_BASE = original

    @pytest.mark.asyncio
    async def test_raises_on_non_retryable(self):
        exc = Exception("bad request")
        exc.status_code = 400
        provider = FailThenSucceedProvider(fail_count=5, exc=exc)
        router = FakeModelRouter()
        emitter = FakeEmitter()
        with pytest.raises(Exception, match="bad request"):
            await provider.chat_with_retry(
                model_router=router,
                session_key="s3",
                model_config=FakeModelConfig(),
                system="sys",
                messages=[],
                tools=None,
                emitter=emitter,
            )

    @pytest.mark.asyncio
    async def test_exhausts_retries(self):
        exc = Exception("overloaded")
        exc.status_code = 529
        provider = FailThenSucceedProvider(fail_count=10, exc=exc)
        router = FakeModelRouter()
        emitter = FakeEmitter()
        import sahara_runtime.llm.base as mod
        original = mod.RETRY_BACKOFF_BASE
        mod.RETRY_BACKOFF_BASE = 0.01
        try:
            with pytest.raises(Exception, match="overloaded"):
                await provider.chat_with_retry(
                    model_router=router,
                    session_key="s4",
                    model_config=FakeModelConfig(),
                    system="sys",
                    messages=[],
                    tools=None,
                    emitter=emitter,
                )
        finally:
            mod.RETRY_BACKOFF_BASE = original
