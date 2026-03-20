"""SessionStore 单元测试 — _find_legal_start / SessionMessage 序列化。"""

import json

from sahara_runtime.memory.session_store import SessionMessage, _find_legal_start


class TestFindLegalStartOpenAI:
    """OpenAI 通用格式的 _find_legal_start 测试。"""

    def test_no_tool_messages(self):
        msgs = [
            SessionMessage(role="user", content="hello"),
            SessionMessage(role="assistant", content="hi"),
        ]
        assert _find_legal_start(msgs) == 0

    def test_matched_tool_calls_openai(self):
        tc = [{"id": "tc1", "type": "function", "function": {"name": "exec", "arguments": "{}"}}]
        msgs = [
            SessionMessage(role="user", content="run ls"),
            SessionMessage(role="assistant", content="check", extra={"tool_calls": tc}),
            SessionMessage(role="tool", content="ok", extra={"tool_call_id": "tc1"}),
            SessionMessage(role="assistant", content="Done"),
        ]
        assert _find_legal_start(msgs) == 0

    def test_orphan_tool_result_openai(self):
        msgs = [
            SessionMessage(role="tool", content="result", extra={"tool_call_id": "tc_orphan"}),
            SessionMessage(role="user", content="hello"),
            SessionMessage(role="assistant", content="hi"),
        ]
        assert _find_legal_start(msgs) == 1

    def test_multiple_tool_calls_openai(self):
        tc = [
            {"id": "tc1", "type": "function", "function": {"name": "exec", "arguments": "{}"}},
            {"id": "tc2", "type": "function", "function": {"name": "read", "arguments": "{}"}},
        ]
        msgs = [
            SessionMessage(role="assistant", content=None, extra={"tool_calls": tc}),
            SessionMessage(role="tool", content="ok1", extra={"tool_call_id": "tc1"}),
            SessionMessage(role="tool", content="ok2", extra={"tool_call_id": "tc2"}),
            SessionMessage(role="assistant", content="Done"),
        ]
        assert _find_legal_start(msgs) == 0

    def test_empty_messages(self):
        assert _find_legal_start([]) == 0

    def test_plain_text_not_affected(self):
        msgs = [
            SessionMessage(role="user", content="[this is not json"),
            SessionMessage(role="assistant", content="ok"),
        ]
        assert _find_legal_start(msgs) == 0


class TestFindLegalStartLegacy:
    """旧 Anthropic 格式向后兼容测试。"""

    def test_matched_tool_calls_legacy(self):
        assistant_content = json.dumps([
            {"type": "text", "text": "Let me check"},
            {"type": "tool_use", "id": "tc1", "name": "exec", "input": {}},
        ])
        tool_result_content = json.dumps([
            {"type": "tool_result", "tool_use_id": "tc1", "content": "ok"},
        ])
        msgs = [
            SessionMessage(role="user", content="run ls"),
            SessionMessage(role="assistant", content=assistant_content),
            SessionMessage(role="user", content=tool_result_content),
            SessionMessage(role="assistant", content="Done"),
        ]
        assert _find_legal_start(msgs) == 0

    def test_orphan_tool_result_legacy(self):
        tool_result_content = json.dumps([
            {"type": "tool_result", "tool_use_id": "tc_orphan", "content": "result"},
        ])
        msgs = [
            SessionMessage(role="user", content=tool_result_content),
            SessionMessage(role="user", content="hello"),
            SessionMessage(role="assistant", content="hi"),
        ]
        assert _find_legal_start(msgs) == 1

    def test_list_content_directly_legacy(self):
        assistant_blocks = [
            {"type": "tool_use", "id": "tc1", "name": "read", "input": {"path": "/a"}},
        ]
        result_blocks = [
            {"type": "tool_result", "tool_use_id": "tc1", "content": "file data"},
        ]
        msgs = [
            SessionMessage(role="assistant", content=assistant_blocks),
            SessionMessage(role="user", content=result_blocks),
        ]
        assert _find_legal_start(msgs) == 0


class TestSessionMessage:
    def test_default_fields(self):
        msg = SessionMessage(role="user")
        assert msg.content == ""
        assert msg.seq == 0
        assert msg.timestamp_ms == 0
        assert msg.extra == {}

    def test_extra_tool_calls(self):
        tc = [{"id": "tc1", "type": "function", "function": {"name": "exec", "arguments": "{}"}}]
        msg = SessionMessage(role="assistant", extra={"tool_calls": tc})
        assert msg.extra["tool_calls"] == tc

    def test_extra_tool_call_id(self):
        msg = SessionMessage(role="tool", extra={"tool_call_id": "tc1", "name": "exec"})
        assert msg.extra["tool_call_id"] == "tc1"
        assert msg.extra["name"] == "exec"

    def test_string_content(self):
        msg = SessionMessage(role="user", content="hello")
        assert msg.content == "hello"
