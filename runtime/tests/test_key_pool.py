"""KeyPool 单元测试。"""

import time

import pytest

from sahara_runtime.model_router.key_pool import CIRCUIT_BREAK_DURATION, KeyPool, KeyState


@pytest.fixture
def pool():
    kp = KeyPool()
    kp.add_keys("anthropic", ["key-a", "key-b", "key-c"])
    return kp


class TestKeyPool:
    def test_round_robin(self, pool):
        k1 = pool.get_key("anthropic")
        k2 = pool.get_key("anthropic")
        k3 = pool.get_key("anthropic")
        k4 = pool.get_key("anthropic")
        assert k1 == "key-a"
        assert k2 == "key-b"
        assert k3 == "key-c"
        assert k4 == "key-a"

    def test_empty_provider(self):
        kp = KeyPool()
        assert kp.get_key("openai") is None

    def test_circuit_break_on_401(self, pool):
        pool.report_error("anthropic", "key-a", 401)
        k = pool.get_key("anthropic")
        assert k != "key-a"

    def test_circuit_break_on_403(self, pool):
        pool.report_error("anthropic", "key-b", 403)
        keys = [pool.get_key("anthropic") for _ in range(6)]
        assert "key-b" not in keys

    def test_429_does_not_circuit_break(self, pool):
        pool.report_error("anthropic", "key-a", 429)
        k = pool.get_key("anthropic")
        # 429 should NOT trigger circuit break, key-a should still be available
        assert k is not None

    def test_success_clears_circuit(self, pool):
        pool.report_error("anthropic", "key-a", 401)
        assert pool.get_key("anthropic") != "key-a"

        pool.report_success("anthropic", "key-a")
        keys = {pool.get_key("anthropic") for _ in range(10)}
        assert "key-a" in keys

    def test_all_keys_broken_returns_none(self, pool):
        pool.report_error("anthropic", "key-a", 401)
        pool.report_error("anthropic", "key-b", 401)
        pool.report_error("anthropic", "key-c", 401)
        assert pool.get_key("anthropic") is None

    def test_call_count_increments(self, pool):
        pool.get_key("anthropic")
        pool.get_key("anthropic")
        states = pool._keys["anthropic"]
        total_calls = sum(s.call_count for s in states)
        assert total_calls == 2

    def test_multiple_providers(self):
        kp = KeyPool()
        kp.add_keys("anthropic", ["ant-1"])
        kp.add_keys("openai", ["oai-1", "oai-2"])

        assert kp.get_key("anthropic") == "ant-1"
        assert kp.get_key("openai") in ("oai-1", "oai-2")

    def test_add_keys_replaces(self, pool):
        pool.add_keys("anthropic", ["new-key"])
        k = pool.get_key("anthropic")
        assert k == "new-key"
