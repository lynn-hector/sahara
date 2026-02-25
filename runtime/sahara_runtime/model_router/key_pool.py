"""API Key 池管理: 轮换 + 熔断。"""

from __future__ import annotations

import time
from dataclasses import dataclass

import structlog

logger = structlog.get_logger(__name__)

CIRCUIT_BREAK_DURATION = 300  # 5 minutes


@dataclass
class KeyState:
    key: str
    provider: str
    call_count: int = 0
    error_count: int = 0
    broken_until: float = 0.0


class KeyPool:
    """API Key 池: 轮换选择 + 自动熔断。"""

    def __init__(self) -> None:
        self._keys: dict[str, list[KeyState]] = {}  # provider → [KeyState]
        self._idx: dict[str, int] = {}  # provider → round-robin index

    def add_keys(self, provider: str, keys: list[str]) -> None:
        states = [KeyState(key=k, provider=provider) for k in keys if k]
        if states:
            self._keys[provider] = states
            self._idx[provider] = 0

    def get_key(self, provider: str) -> str | None:
        """轮换选择一个可用 Key。"""
        states = self._keys.get(provider, [])
        if not states:
            return None

        now = time.time()
        n = len(states)
        start = self._idx.get(provider, 0)

        for i in range(n):
            idx = (start + i) % n
            ks = states[idx]
            if ks.broken_until > now:
                continue
            self._idx[provider] = (idx + 1) % n
            ks.call_count += 1
            return ks.key

        logger.error("all_keys_broken", provider=provider)
        return None

    def report_error(self, provider: str, key: str, status_code: int) -> None:
        """上报 Key 错误。401/403 触发熔断。"""
        for ks in self._keys.get(provider, []):
            if ks.key == key:
                ks.error_count += 1
                if status_code in (401, 403):
                    ks.broken_until = time.time() + CIRCUIT_BREAK_DURATION
                    logger.warning(
                        "key_circuit_broken",
                        provider=provider,
                        status_code=status_code,
                        broken_for=CIRCUIT_BREAK_DURATION,
                    )
                break

    def report_success(self, provider: str, key: str) -> None:
        for ks in self._keys.get(provider, []):
            if ks.key == key:
                ks.broken_until = 0.0
                break
