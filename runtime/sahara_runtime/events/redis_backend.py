"""Redis Streams 事件后端 — Phase 1 默认实现。"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from sahara_runtime.events.backend import EventBackend

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = structlog.get_logger(__name__)


class RedisStreamsBackend(EventBackend):
    def __init__(self, redis: Redis, maxlen: int = 5000) -> None:
        self._redis = redis
        self._maxlen = maxlen

    async def publish(self, topic: str, data: bytes) -> None:
        await self._redis.xadd(
            topic,
            {"data": data},
            maxlen=self._maxlen,
            approximate=True,
        )

    async def health_check(self) -> bool:
        try:
            return await self._redis.ping()
        except Exception:
            return False

    async def close(self) -> None:
        pass  # Redis connection lifecycle managed by Container

    @property
    def backend_name(self) -> str:
        return "redis_streams"
