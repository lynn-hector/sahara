"""SessionStore — Redis 热存储 + PostgreSQL 冷存储。

Redis: 最近 N 条消息 (ZSET by seq)
PostgreSQL: 完整历史
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = structlog.get_logger(__name__)

MAX_HOT_MESSAGES = 50
HOT_TTL = 3600 * 24  # 24 hours


@dataclass
class SessionMessage:
    role: str  # "user" | "assistant"
    content: Any
    seq: int = 0
    timestamp_ms: int = 0


class SessionStore:
    """Redis 热存储 + PG 冷存储 (Phase 1: Redis only)."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    def _key(self, session_key: str) -> str:
        return f"session:msgs:{session_key}"

    def _meta_key(self, session_key: str) -> str:
        return f"session:meta:{session_key}"

    async def append(self, session_key: str, message: SessionMessage) -> None:
        """追加一条消息到会话。"""
        key = self._key(session_key)
        data = json.dumps({
            "role": message.role,
            "content": message.content,
            "seq": message.seq,
            "timestamp_ms": message.timestamp_ms or int(time.time() * 1000),
        })
        await self._redis.zadd(key, {data: message.seq})
        await self._redis.expire(key, HOT_TTL)

        count = await self._redis.zcard(key)
        if count > MAX_HOT_MESSAGES * 2:
            await self._redis.zremrangebyrank(key, 0, count - MAX_HOT_MESSAGES - 1)

    async def load(self, session_key: str, limit: int = MAX_HOT_MESSAGES) -> list[SessionMessage]:
        """从 Redis 加载最近 N 条消息。"""
        key = self._key(session_key)
        raw = await self._redis.zrange(key, -limit, -1)

        messages = []
        for item in raw:
            data = json.loads(item)
            messages.append(
                SessionMessage(
                    role=data["role"],
                    content=data["content"],
                    seq=data.get("seq", 0),
                    timestamp_ms=data.get("timestamp_ms", 0),
                )
            )
        return messages

    async def set_meta(self, session_key: str, meta: dict[str, Any]) -> None:
        """存储 session 元数据。"""
        key = self._meta_key(session_key)
        await self._redis.hset(key, mapping={k: json.dumps(v) for k, v in meta.items()})
        await self._redis.expire(key, HOT_TTL)

    async def get_meta(self, session_key: str) -> dict[str, Any]:
        """获取 session 元数据。"""
        key = self._meta_key(session_key)
        raw = await self._redis.hgetall(key)
        return {k.decode(): json.loads(v) for k, v in raw.items()} if raw else {}

    async def clear(self, session_key: str) -> None:
        """清除 session 数据。"""
        await self._redis.delete(self._key(session_key), self._meta_key(session_key))
