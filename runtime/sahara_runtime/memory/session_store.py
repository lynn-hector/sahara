"""SessionStore — Redis 热存储 + PostgreSQL 冷存储。

Redis: 最近 N 条消息 (ZSET by seq)
PostgreSQL: 完整历史 (Phase 3)

消息使用 OpenAI 通用格式:
- assistant 消息: role, content, tool_calls (可选)
- tool 消息: role="tool", tool_call_id, name, content
- extra 字段存储 tool_calls / tool_call_id 等，序列化时写入 Redis
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = structlog.get_logger(__name__)

MAX_HOT_MESSAGES = 50
HOT_TTL = 3600 * 24  # 24 hours


@dataclass
class SessionMessage:
    """会话消息 (OpenAI 通用格式)。

    content: str — 纯文本消息内容
    extra: 存储 OpenAI 格式的额外字段:
        - tool_calls: assistant 消息中的工具调用列表
        - tool_call_id: role:"tool" 消息关联的 tool call ID
        - name: role:"tool" 消息关联的工具名称
        - thinking / reasoning_content: LLM 思考过程
    """

    role: str  # "user" | "assistant" | "tool"
    content: Any = ""
    seq: int = 0
    timestamp_ms: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


def _find_legal_start(messages: list[SessionMessage]) -> int:
    """找到第一个安全起始位置，确保每个 tool result 都有匹配的 assistant tool_call。

    参考 nanobot/session/manager.py 的 Session._find_legal_start。
    防止历史窗口截断导致孤立的 tool result 消息，引发 Provider 400 错误。

    支持两种格式:
    - OpenAI 格式: assistant.extra["tool_calls"], role="tool" + extra["tool_call_id"]
    - 旧 Anthropic 格式: content blocks 中的 tool_use / tool_result
    """
    declared: set[str] = set()
    start = 0

    for i, msg in enumerate(messages):
        if msg.role == "assistant":
            _collect_declared_tool_ids(msg, declared)

        elif msg.role == "tool":
            tid = (msg.extra or {}).get("tool_call_id")
            if tid and str(tid) not in declared:
                start = i + 1
                declared.clear()

        elif msg.role == "user":
            _check_legacy_tool_results(msg, declared, i, lambda new_start: None)
            orphan_start = _find_legacy_orphan(msg, declared, i)
            if orphan_start is not None:
                start = orphan_start
                declared.clear()

    return start


def _collect_declared_tool_ids(msg: SessionMessage, declared: set[str]) -> None:
    """从 assistant 消息中收集已声明的 tool call ID。"""
    extra = msg.extra or {}
    tool_calls = extra.get("tool_calls")
    if isinstance(tool_calls, list):
        for tc in tool_calls:
            if isinstance(tc, dict):
                tc_id = tc.get("id")
                if tc_id:
                    declared.add(str(tc_id))
        return

    content = msg.content
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            return
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                block_id = block.get("id")
                if block_id:
                    declared.add(str(block_id))


def _find_legacy_orphan(
    msg: SessionMessage, declared: set[str], idx: int,
) -> int | None:
    """检测旧 Anthropic 格式中的孤立 tool_result，返回新的 start 或 None。"""
    content = msg.content
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            return None
    if not isinstance(content, list):
        return None

    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_result":
            tid = block.get("tool_use_id")
            if tid and str(tid) not in declared:
                return idx + 1
    return None


def _check_legacy_tool_results(
    msg: SessionMessage,
    declared: set[str],
    idx: int,
    on_orphan: Any,
) -> None:
    """Placeholder for legacy tool result checking — handled by _find_legacy_orphan."""


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

        content = message.content
        if isinstance(content, (list, dict)):
            content = json.dumps(content, ensure_ascii=False)

        data = json.dumps({
            "role": message.role,
            "content": content,
            "seq": message.seq,
            "timestamp_ms": message.timestamp_ms or int(time.time() * 1000),
            "extra": message.extra or {},
        }, ensure_ascii=False)
        await self._redis.zadd(key, {data: message.seq})
        await self._redis.expire(key, HOT_TTL)

        count = await self._redis.zcard(key)
        if count > MAX_HOT_MESSAGES * 2:
            await self._redis.zremrangebyrank(key, 0, count - MAX_HOT_MESSAGES - 1)

    async def load(
        self, session_key: str, limit: int = MAX_HOT_MESSAGES,
    ) -> list[SessionMessage]:
        """从 Redis 加载最近 N 条消息。

        自动清理孤立的 tool_result 消息，防止 Provider 报错。
        """
        key = self._key(session_key)
        raw = await self._redis.zrange(key, -limit, -1)

        messages: list[SessionMessage] = []
        for item in raw:
            data = json.loads(item)
            messages.append(
                SessionMessage(
                    role=data["role"],
                    content=data["content"],
                    seq=data.get("seq", 0),
                    timestamp_ms=data.get("timestamp_ms", 0),
                    extra=data.get("extra", {}),
                )
            )

        legal_start = _find_legal_start(messages)
        if legal_start > 0:
            logger.debug(
                "session_trimmed_orphan_tool_results",
                session_key=session_key,
                trimmed=legal_start,
                remaining=len(messages) - legal_start,
            )
            messages = messages[legal_start:]

        return messages

    async def set_meta(self, session_key: str, meta: dict[str, Any]) -> None:
        """存储 session 元数据。"""
        key = self._meta_key(session_key)
        await self._redis.hset(
            key, mapping={k: json.dumps(v) for k, v in meta.items()},
        )
        await self._redis.expire(key, HOT_TTL)

    async def get_meta(self, session_key: str) -> dict[str, Any]:
        """获取 session 元数据。"""
        key = self._meta_key(session_key)
        raw = await self._redis.hgetall(key)
        return {k.decode(): json.loads(v) for k, v in raw.items()} if raw else {}

    async def clear(self, session_key: str) -> None:
        """清除 session 数据。"""
        await self._redis.delete(self._key(session_key), self._meta_key(session_key))
