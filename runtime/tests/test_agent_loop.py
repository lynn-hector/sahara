"""AgentLoop 单元测试 — 核心循环逻辑、Hook 触发、消息构建。"""

import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from sahara_runtime.agent_loop import AgentLoop, run_agent_loop
from sahara_runtime.context.context_builder import (
    ContextBuilder,
    _maybe_convert_legacy_anthropic,
    build_assistant_message,
)
from sahara_runtime.context.context_manager import ContextManager
from sahara_runtime.context.token_counter import TokenCounter
from sahara_runtime.hooks.runner import HookContext, HookName, HookRunner
from sahara_runtime.llm.mock_provider import MockProvider
from sahara_runtime.llm.types import LLMResponse, ToolCallRequest
from sahara_runtime.memory.session_store import SessionMessage

# ── build_assistant_message 测试 ─────────────────────


class TestBuildAssistantMessage:
    def test_text_only(self):
        msg = build_assistant_message("Hello")
        assert msg == {"role": "assistant", "content": "Hello"}

    def test_with_tool_calls(self):
        tc = [{"id": "tc1", "type": "function", "function": {"name": "exec", "arguments": "{}"}}]
        msg = build_assistant_message("Check", tool_calls=tc)
        assert msg["tool_calls"] == tc
        assert msg["content"] == "Check"

    def test_with_thinking(self):
        msg = build_assistant_message("result", thinking="hmm")
        assert msg["thinking"] == "hmm"

    def test_none_content(self):
        msg = build_assistant_message(None)
        assert msg["content"] is None


# ── ContextBuilder.add_assistant_message 测试 ────────


class TestAddAssistantMessage:
    def _builder(self):
        return ContextBuilder(TokenCounter())

    def test_text_only(self):
        cb = self._builder()
        msgs: list[dict] = []
        resp = LLMResponse(content="Hello")
        cb.add_assistant_message(msgs, resp)
        assert len(msgs) == 1
        assert msgs[0]["role"] == "assistant"
        assert msgs[0]["content"] == "Hello"
        assert "tool_calls" not in msgs[0]

    def test_with_tool_calls(self):
        cb = self._builder()
        msgs: list[dict] = []
        tc = ToolCallRequest(id="tc1", name="exec", arguments={"command": "ls"})
        resp = LLMResponse(content="Let me check", tool_calls=[tc])
        cb.add_assistant_message(msgs, resp)
        assert msgs[0]["content"] == "Let me check"
        assert len(msgs[0]["tool_calls"]) == 1
        tc_dict = msgs[0]["tool_calls"][0]
        assert tc_dict["id"] == "tc1"
        assert tc_dict["type"] == "function"
        assert tc_dict["function"]["name"] == "exec"
        assert json.loads(tc_dict["function"]["arguments"]) == {"command": "ls"}

    def test_tool_calls_only_no_text(self):
        cb = self._builder()
        msgs: list[dict] = []
        tc = ToolCallRequest(id="tc2", name="read", arguments={"path": "/a"})
        resp = LLMResponse(content=None, tool_calls=[tc])
        cb.add_assistant_message(msgs, resp)
        assert msgs[0]["content"] is None
        assert len(msgs[0]["tool_calls"]) == 1


# ── session_msg_to_dict 测试 ──────────────────────────


class TestSessionMsgToDict:
    def test_plain_text(self):
        msg = SessionMessage(role="user", content="hello")
        result = ContextBuilder.session_msg_to_dict(msg)
        assert result == {"role": "user", "content": "hello"}

    def test_with_tool_calls_in_extra(self):
        tc = [{"id": "tc1", "type": "function", "function": {"name": "exec", "arguments": "{}"}}]
        msg = SessionMessage(role="assistant", content="check", extra={"tool_calls": tc})
        result = ContextBuilder.session_msg_to_dict(msg)
        assert result["tool_calls"] == tc
        assert result["content"] == "check"

    def test_tool_message_from_extra(self):
        msg = SessionMessage(
            role="tool", content="ok",
            extra={"tool_call_id": "tc1", "name": "exec"},
        )
        result = ContextBuilder.session_msg_to_dict(msg)
        assert result["role"] == "tool"
        assert result["tool_call_id"] == "tc1"
        assert result["name"] == "exec"

    def test_legacy_anthropic_assistant_converted(self):
        blocks = [
            {"type": "text", "text": "Let me check"},
            {"type": "tool_use", "id": "tc1", "name": "exec", "input": {"command": "ls"}},
        ]
        msg = SessionMessage(role="assistant", content=json.dumps(blocks))
        result = ContextBuilder.session_msg_to_dict(msg)
        assert result["role"] == "assistant"
        assert result["content"] == "Let me check"
        assert len(result["tool_calls"]) == 1
        assert result["tool_calls"][0]["function"]["name"] == "exec"

    def test_invalid_json_stays_string(self):
        msg = SessionMessage(role="user", content="[not valid json")
        result = ContextBuilder.session_msg_to_dict(msg)
        assert result["content"] == "[not valid json"


# ── _maybe_convert_legacy_anthropic 测试 ─────────────


class TestLegacyAnthropicConversion:
    def test_converts_assistant_tool_use(self):
        blocks = [
            {"type": "text", "text": "checking"},
            {"type": "tool_use", "id": "tc1", "name": "exec", "input": {"cmd": "ls"}},
        ]
        msg = {"role": "assistant", "content": json.dumps(blocks)}
        result = _maybe_convert_legacy_anthropic(msg)
        assert result["content"] == "checking"
        assert len(result["tool_calls"]) == 1
        assert result["tool_calls"][0]["function"]["name"] == "exec"

    def test_plain_text_not_modified(self):
        msg = {"role": "user", "content": "hello"}
        result = _maybe_convert_legacy_anthropic(msg)
        assert result == {"role": "user", "content": "hello"}

    def test_non_json_not_modified(self):
        msg = {"role": "assistant", "content": "just text"}
        result = _maybe_convert_legacy_anthropic(msg)
        assert result == {"role": "assistant", "content": "just text"}


# ── AgentLoop 集成测试（使用 MockProvider）────────────


class FakeEmitter:
    def __init__(self):
        self.events: list[tuple] = []

    async def emit_run_start(self, **kw):
        self.events.append(("run_start", kw))

    async def emit_run_complete(self, **kw):
        self.events.append(("run_complete", kw))

    async def emit_run_error(self, msg, **kw):
        self.events.append(("run_error", msg))

    async def emit_abort(self, reason, **kw):
        self.events.append(("abort", reason))

    async def emit_delta(self, text, stream="assistant"):
        self.events.append(("delta", text))

    async def emit_thinking(self, text):
        self.events.append(("thinking", text))

    async def emit_usage(self, **kw):
        self.events.append(("usage", kw))

    async def emit_tool_start(self, *args):
        self.events.append(("tool_start",))

    async def emit_tool_result(self, *args):
        self.events.append(("tool_result",))

    async def emit_tool_confirm_required(self, **kw):
        self.events.append(("tool_confirm",))


class FakeEmitterFactory:
    def __init__(self):
        self.last_emitter = None

    def for_run(self, run_id, session_key, task_id):
        self.last_emitter = FakeEmitter()
        return self.last_emitter


@dataclass
class FakeModelConfig:
    provider: str = "mock"
    model_id: str = "mock-model"
    max_tokens: int = 1024
    max_context_tokens: int = 4096
    max_iterations: int = 5
    api_base: str = ""


class FakeModelRouter:
    def resolve(self, session_key, model_override=""):
        return FakeModelConfig()

    def get_api_key_for_session(self, session_key, config):
        return "fake_api_key"

    def report_key_success(self, session_key):
        pass

    def report_key_error(self, session_key, status_code):
        pass

    def release_session(self, session_key):
        pass


class FakeToolRegistry:
    def get_definitions(self):
        return []

    def names(self):
        return []


class FakeToolExecutor:
    async def execute(self, **kw):
        return {"content": "tool ok"}


@dataclass
class FakeContainer:
    emitter_factory: Any = None
    llm_provider: Any = None
    tool_executor: Any = None
    tool_registry: Any = None
    model_router: Any = None
    context_manager: Any = None
    hook_runner: Any = None
    session_store: Any = None
    skills_manager: Any = None
    sandbox_manager: Any = None
    workspace: Any = None


def make_container(**overrides) -> FakeContainer:
    defaults = dict(
        emitter_factory=FakeEmitterFactory(),
        llm_provider=MockProvider(),
        tool_executor=FakeToolExecutor(),
        tool_registry=FakeToolRegistry(),
        model_router=FakeModelRouter(),
        context_manager=ContextManager(),
        hook_runner=None,
        session_store=None,
        skills_manager=None,
    )
    defaults.update(overrides)
    return FakeContainer(**defaults)


class TestAgentLoopRun:
    @pytest.mark.asyncio
    async def test_basic_run_completes(self):
        container = make_container()
        completed = []
        loop = AgentLoop(
            task_id="t1",
            run_id="r1",
            session_key="s1",
            user_message="hi",
            agent_id="agent-1",
            metadata={},
            container=container,
            on_complete=lambda tid: completed.append(tid),
        )
        await loop.run()

        assert "t1" in completed
        emitter = container.emitter_factory.last_emitter
        event_types = [e[0] for e in emitter.events]
        assert "run_start" in event_types
        assert "delta" in event_types
        assert "usage" in event_types
        assert "run_complete" in event_types

    @pytest.mark.asyncio
    async def test_on_complete_called_on_error(self):
        container = make_container()
        container.model_router = MagicMock()
        container.model_router.resolve.side_effect = RuntimeError("boom")
        completed = []
        loop = AgentLoop(
            task_id="t2",
            run_id="r2",
            session_key="s2",
            user_message="hi",
            agent_id="agent-2",
            metadata={},
            container=container,
            on_complete=lambda tid: completed.append(tid),
        )
        await loop.run()
        assert "t2" in completed

    @pytest.mark.asyncio
    async def test_hooks_are_fired(self):
        runner = HookRunner()
        fired_hooks: list[str] = []

        async def capture_hook(ctx: HookContext) -> HookContext:
            fired_hooks.append(ctx.hook_name.value)
            return ctx

        from sahara_runtime.hooks.runner import HookRegistration
        for name in (HookName.ON_RUN_START, HookName.ON_RUN_COMPLETE,
                     HookName.PRE_LLM_CALL, HookName.POST_LLM_CALL):
            runner.register(HookRegistration(name=name, callback=capture_hook, priority=1))

        container = make_container(hook_runner=runner)
        loop = AgentLoop(
            task_id="t3",
            run_id="r3",
            session_key="s3",
            user_message="test hooks",
            agent_id="agent-3",
            metadata={},
            container=container,
        )
        await loop.run()

        assert "on_run_start" in fired_hooks
        assert "pre_llm_call" in fired_hooks
        assert "post_llm_call" in fired_hooks
        assert "on_run_complete" in fired_hooks


class TestRunAgentLoopCompat:
    @pytest.mark.asyncio
    async def test_compat_entry_point(self):
        container = make_container()
        completed = []
        await run_agent_loop(
            task_id="compat-1",
            run_id="r-compat",
            session_key="s-compat",
            user_message="hello compat",
            agent_id="agent-compat",
            metadata={},
            container=container,
            on_complete=lambda tid: completed.append(tid),
        )
        assert "compat-1" in completed
