"""LLM 数据类型单元测试 — ToolCallRequest, LLMResponse。"""

import json

from sahara_runtime.llm.types import LLMResponse, ToolCallRequest


class TestToolCallRequest:
    def test_basic_fields(self):
        tc = ToolCallRequest(id="tc_1", name="exec", arguments={"command": "ls"})
        assert tc.id == "tc_1"
        assert tc.name == "exec"
        assert tc.arguments == {"command": "ls"}

    def test_to_openai_tool_call(self):
        tc = ToolCallRequest(id="tc_2", name="read", arguments={"path": "/tmp/a.txt"})
        result = tc.to_openai_tool_call()
        assert result["id"] == "tc_2"
        assert result["type"] == "function"
        assert result["function"]["name"] == "read"
        assert json.loads(result["function"]["arguments"]) == {"path": "/tmp/a.txt"}

    def test_empty_arguments(self):
        tc = ToolCallRequest(id="tc_3", name="noop", arguments={})
        result = tc.to_openai_tool_call()
        assert json.loads(result["function"]["arguments"]) == {}

    def test_unicode_arguments(self):
        tc = ToolCallRequest(id="tc_4", name="exec", arguments={"text": "你好"})
        result = tc.to_openai_tool_call()
        assert "你好" in result["function"]["arguments"]


class TestLLMResponse:
    def test_text_only(self):
        r = LLMResponse(content="Hello world")
        assert r.content == "Hello world"
        assert r.has_tool_calls is False
        assert r.tool_calls == []
        assert r.finish_reason == "stop"

    def test_with_tool_calls(self):
        tc = ToolCallRequest(id="tc_1", name="exec", arguments={"command": "ls"})
        r = LLMResponse(content="Let me check", tool_calls=[tc])
        assert r.has_tool_calls is True
        assert len(r.tool_calls) == 1
        assert r.tool_calls[0].name == "exec"

    def test_none_content(self):
        r = LLMResponse(content=None)
        assert r.content is None
        assert r.has_tool_calls is False

    def test_usage_properties(self):
        r = LLMResponse(
            content="ok",
            usage={"input_tokens": 100, "output_tokens": 50},
        )
        assert r.input_tokens == 100
        assert r.output_tokens == 50

    def test_usage_defaults_to_zero(self):
        r = LLMResponse(content="ok", usage={})
        assert r.input_tokens == 0
        assert r.output_tokens == 0

    def test_empty_usage(self):
        r = LLMResponse(content="ok")
        assert r.input_tokens == 0
        assert r.output_tokens == 0

    def test_thinking(self):
        r = LLMResponse(content="result", thinking="Let me think...")
        assert r.thinking == "Let me think..."

    def test_reasoning_content(self):
        r = LLMResponse(content="result", reasoning_content="Step by step...")
        assert r.reasoning_content == "Step by step..."

    def test_finish_reason(self):
        r = LLMResponse(content=None, finish_reason="tool_use")
        assert r.finish_reason == "tool_use"
