"""ToolExecutor + ToolRegistry 单元测试。"""

import asyncio
import json
from typing import Any

import pytest

from sahara_runtime.tools.base import Tool
from sahara_runtime.tools.executor import ToolExecutor
from sahara_runtime.tools.registry import ToolRegistry


class FakeEmitter:
    def __init__(self):
        self.events = []

    async def emit_tool_start(self, tool_call_id, tool_name, input_json):
        self.events.append(("tool_start", tool_call_id, tool_name))

    async def emit_tool_result(self, tool_call_id, tool_name, success, output, duration_ms):
        self.events.append(("tool_result", tool_call_id, tool_name, success, output[:100]))


class EchoTool(Tool):
    @property
    def name(self) -> str:
        return "echo"

    @property
    def description(self) -> str:
        return "echo tool"

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        }

    async def execute(self, text: str, **kw: Any) -> str:
        return f"echo: {text}"


class SlowTool(Tool):
    @property
    def name(self) -> str:
        return "slow"

    @property
    def description(self) -> str:
        return "slow tool"

    @property
    def parameters(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"seconds": {"type": "integer"}}}

    async def execute(self, seconds: int = 10, **kw: Any) -> str:
        await asyncio.sleep(seconds)
        return "done"


class FailTool(Tool):
    @property
    def name(self) -> str:
        return "fail"

    @property
    def description(self) -> str:
        return "failing tool"

    @property
    def parameters(self) -> dict[str, Any]:
        return {"type": "object"}

    async def execute(self, **kw: Any) -> str:
        raise RuntimeError("tool exploded")


@pytest.fixture
def registry():
    reg = ToolRegistry()
    reg.register(EchoTool())
    reg.register(SlowTool())
    reg.register(FailTool())
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
        assert emitter.events[1][3] is True

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
        assert "not found" in result["content"].lower()

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
        mod.TOOL_TIMEOUT = 0.1
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
        tool = EchoTool()
        reg.register(tool)
        assert reg.get("echo") is tool
        assert reg.get("nope") is None

    def test_get_definitions(self):
        reg = ToolRegistry()
        reg.register(EchoTool())
        schemas = reg.get_definitions()
        assert len(schemas) == 1
        assert schemas[0]["type"] == "function"
        assert schemas[0]["function"]["name"] == "echo"
        assert "text" in schemas[0]["function"]["parameters"]["properties"]

    def test_names(self):
        reg = ToolRegistry()
        reg.register(EchoTool())
        assert reg.names() == ["echo"]

    @pytest.mark.asyncio
    async def test_execute_success(self):
        reg = ToolRegistry()
        reg.register(EchoTool())
        result = await reg.execute("echo", {"text": "hi"})
        assert result["content"] == "echo: hi"
        assert "is_error" not in result

    @pytest.mark.asyncio
    async def test_execute_not_found(self):
        reg = ToolRegistry()
        result = await reg.execute("nope", {})
        assert result["is_error"]
        assert "not found" in result["content"].lower()

    @pytest.mark.asyncio
    async def test_execute_validation_error(self):
        reg = ToolRegistry()
        reg.register(EchoTool())
        result = await reg.execute("echo", {})
        assert result["is_error"]
        assert "missing required" in result["content"].lower()
