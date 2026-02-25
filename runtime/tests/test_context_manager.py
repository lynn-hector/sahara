"""ContextManager + TokenCounter 单元测试。"""

import pytest

from sahara_runtime.context.manager import ContextManager
from sahara_runtime.context.token_counter import TokenCounter


@pytest.fixture
def counter():
    return TokenCounter()


@pytest.fixture
def mgr(counter):
    return ContextManager(counter)


class TestTokenCounter:
    def test_count_text(self, counter):
        assert counter.count_text("") == 1
        assert counter.count_text("hello world!") == 3  # 12 chars / 4

    def test_count_message_str(self, counter):
        msg = {"role": "user", "content": "hello"}
        tokens = counter.count_message(msg)
        assert tokens >= 5  # overhead + text

    def test_count_message_tool_use(self, counter):
        msg = {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "I'll help."},
                {"type": "tool_use", "id": "t1", "name": "exec", "input": {"command": "ls"}},
            ],
        }
        tokens = counter.count_message(msg)
        assert tokens > 10

    def test_count_messages(self, counter):
        msgs = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"},
        ]
        total = counter.count_messages(msgs)
        assert total > 0
        assert total == counter.count_message(msgs[0]) + counter.count_message(msgs[1])

    def test_count_system(self, counter):
        tokens = counter.count_system("You are a helpful assistant.")
        assert tokens > 4


class TestContextManager:
    def test_fit_within_budget(self, mgr):
        msgs = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"},
        ]
        result = mgr.fit_messages(msgs, max_context_tokens=100_000)
        assert len(result) == 2

    def test_eviction_removes_oldest(self, mgr):
        msgs = [{"role": "user", "content": f"message {i}" * 100} for i in range(20)]
        result = mgr.fit_messages(msgs, max_context_tokens=500)
        assert len(result) < len(msgs)
        assert len(result) >= 2

    def test_preserves_minimum_messages(self, mgr):
        msgs = [
            {"role": "user", "content": "x" * 10000},
            {"role": "assistant", "content": "y" * 10000},
        ]
        result = mgr.fit_messages(msgs, max_context_tokens=200)
        assert len(result) >= 1

    def test_zero_budget_returns_last(self, mgr):
        msgs = [
            {"role": "user", "content": "a"},
            {"role": "assistant", "content": "b"},
            {"role": "user", "content": "c"},
        ]
        result = mgr.fit_messages(msgs, max_context_tokens=10, system_tokens=10)
        assert len(result) <= 2

    def test_system_tokens_deducted(self, mgr):
        msgs = [{"role": "user", "content": "hi " * 50} for _ in range(10)]
        result_no_sys = mgr.fit_messages(msgs, max_context_tokens=300, system_tokens=0)
        result_with_sys = mgr.fit_messages(msgs, max_context_tokens=300, system_tokens=200)
        assert len(result_with_sys) <= len(result_no_sys)
