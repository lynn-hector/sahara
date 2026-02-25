"""Fallback 逻辑单元测试 — _is_retryable / _extract_status_code。"""

import pytest

from sahara_runtime.agent_loop import _extract_status_code, _is_retryable


class TestIsRetryable:
    def test_429_retryable(self):
        assert _is_retryable(429, Exception()) is True

    def test_5xx_retryable(self):
        for code in (500, 502, 503, 529):
            assert _is_retryable(code, Exception()) is True

    def test_401_403_retryable_via_key_rotation(self):
        assert _is_retryable(401, Exception()) is True
        assert _is_retryable(403, Exception()) is True

    def test_400_not_retryable(self):
        assert _is_retryable(400, Exception()) is False

    def test_404_not_retryable(self):
        assert _is_retryable(404, Exception()) is False

    def test_timeout_exception_retryable(self):
        assert _is_retryable(0, TimeoutError("timed out")) is True

    def test_connection_error_retryable(self):
        assert _is_retryable(0, ConnectionError("refused")) is True

    def test_generic_exception_not_retryable(self):
        assert _is_retryable(0, ValueError("bad value")) is False


class TestExtractStatusCode:
    def test_no_status_code(self):
        assert _extract_status_code(ValueError("x")) == 0

    def test_with_status_code_attr(self):
        exc = Exception("test")
        exc.status_code = 429
        assert _extract_status_code(exc) == 429
