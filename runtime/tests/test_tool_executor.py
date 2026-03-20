"""ToolExecutor 单元测试。"""

import asyncio
import json

import pytest

from sahara_runtime.tools.executor import ToolExecutor
from sahara_runtime.tools.registry import ToolDef, ToolRegistry


class FakeEmitter:
    """Mock RunEmitter for tests."""

    def __init__(self):
        self.events = []

    async def emit_tool_start(self, tool_call_id, tool_name, input_json):
        self.events.append(("tool_start", tool_call_id, tool_name))

    async def emit_tool_result(self, tool_call_id, tool_name, success, output, duration_ms):
        self.events.append(("tool_result", tool_call_id, tool_name, success, output[:100]))


async def echo_tool(text: str) -> str:
    return f"echo: {text}"


async def slow_tool(seconds: int = 10) -> str:
    await asyncio.sleep(seconds)
    return "done"


async def failing_tool() -> str:
    raise RuntimeError("tool exploded")


@pytest.fixture
def registry():
    reg = ToolRegistry()
    reg.register(ToolDef(
        name="echo",
        description="echo tool",
        input_schema={"type": "object", "properties": {"text": {"type": "string"}}},
        func=echo_tool,
    ))
    reg.register(ToolDef(
        name="slow",
        description="slow tool",
        input_schema={"type": "object", "properties": {"seconds": {"type": "integer"}}},
        func=slow_tool,
    ))
    reg.register(ToolDef(
        name="fail",
        description="failing tool",
        input_schema={"type": "object"},
        func=failing_tool,
    ))
    return reg


@pytest.fixture
def executor(registry):
    return ToolExecutor(registry)


class TestToolExecutor:
    @pytest.mark.asyncio
    async def test_success(self, executor):
        emitter = FakeEmitter()
        result = await executor.execute(
            tool_call_id="tc1",
            tool_name="echo",
            input_json=json.dumps({"text": "hello"}),
            emitter=emitter,
        )
        assert result["content"] == "echo: hello"
        assert "is_error" not in result
        assert len(emitter.events) == 2
        assert emitter.events[0][0] == "tool_start"
        assert emitter.events[1][3] is True  # success=True

    @pytest.mark.asyncio
    async def test_unknown_tool(self, executor):
        emitter = FakeEmitter()
        result = await executor.execute(
            tool_call_id="tc2",
            tool_name="nonexistent",
            input_json="{}",
            emitter=emitter,
        )
        assert result["is_error"]
        assert "unknown tool" in result["content"]

    @pytest.mark.asyncio
    async def test_invalid_json(self, executor):
        emitter = FakeEmitter()
        result = await executor.execute(
            tool_call_id="tc3",
            tool_name="echo",
            input_json="{bad json}",
            emitter=emitter,
        )
        assert result["is_error"]
        assert "invalid tool input JSON" in result["content"]

    @pytest.mark.asyncio
    async def test_tool_exception(self, executor):
        emitter = FakeEmitter()
        result = await executor.execute(
            tool_call_id="tc4",
            tool_name="fail",
            input_json="{}",
            emitter=emitter,
        )
        assert result["is_error"]
        assert "tool exploded" in result["content"]

    @pytest.mark.asyncio
    async def test_tool_timeout(self, executor):
        import sahara_runtime.tools.executor as mod
        original = mod.TOOL_TIMEOUT
        mod.TOOL_TIMEOUT = 0.1  # 100ms
        try:
            emitter = FakeEmitter()
            result = await executor.execute(
                tool_call_id="tc5",
                tool_name="slow",
                input_json=json.dumps({"seconds": 10}),
                emitter=emitter,
            )
            assert result["is_error"]
            assert "timed out" in result["content"]
        finally:
            mod.TOOL_TIMEOUT = original


class TestToolRegistry:
    def test_register_and_get(self):
        reg = ToolRegistry()
        td = ToolDef(name="test", description="d", input_schema={}, func=echo_tool)
        reg.register(td)
        assert reg.get("test") is td
        assert reg.get("nope") is None

    def test_list_schemas(self):
        reg = ToolRegistry()
        reg.register(ToolDef(name="b", description="B", input_schema={"b": 1}, func=echo_tool, tier=1))
        reg.register(ToolDef(name="a", description="A", input_schema={"a": 1}, func=echo_tool, tier=0))
        schemas = reg.list_schemas()
        assert len(schemas) == 2
        assert schemas[0]["type"] == "function"
        assert schemas[0]["function"]["name"] == "a"  # tier 0 first
        assert schemas[0]["function"]["parameters"] == {"a": 1}
        assert schemas[1]["function"]["name"] == "b"
        assert schemas[1]["function"]["parameters"] == {"b": 1}

    def test_names(self):
        reg = ToolRegistry()
        reg.register(ToolDef(name="x", description="X", input_schema={}, func=echo_tool))
        assert reg.names() == ["x"]

    def test_sandboxed_flag(self):
        reg = ToolRegistry()
        td = ToolDef(name="sb", description="d", input_schema={}, func=echo_tool, sandboxed=True)
        reg.register(td)
        assert reg.get("sb").sandboxed is True
