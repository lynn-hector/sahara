"""SessionLock — Redis 分布式锁, 防止同一 session 并发执行。"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = structlog.get_logger(__name__)

DEFAULT_LOCK_TTL = 30  # seconds


class SessionLock:
    """基于 Redis SET NX EX 的分布式锁。"""

    def __init__(self, redis: Redis, ttl: int = DEFAULT_LOCK_TTL) -> None:
        self._redis = redis
        self._ttl = ttl
        self._owner = uuid.uuid4().hex

    def _key(self, session_key: str) -> str:
        return f"session_lock:{session_key}"

    async def acquire(self, session_key: str) -> bool:
        """尝试获取锁。返回 True 表示成功。"""
        result = await self._redis.set(
            self._key(session_key),
            self._owner,
            nx=True,
            ex=self._ttl,
        )
        acquired = result is not None
        if acquired:
            logger.debug("lock_acquired", session_key=session_key)
        return acquired

    async def release(self, session_key: str) -> bool:
        """释放锁 (仅锁持有者可以释放)。"""
        key = self._key(session_key)
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        result = await self._redis.eval(script, 1, key, self._owner)
        released = result == 1
        if released:
            logger.debug("lock_released", session_key=session_key)
        return released

    async def extend(self, session_key: str) -> bool:
        """续期锁 (仅锁持有者可以续期)。"""
        key = self._key(session_key)
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = await self._redis.eval(script, 1, key, self._owner, self._ttl)
        return result == 1
