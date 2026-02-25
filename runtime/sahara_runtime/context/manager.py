"""ContextManager — 管理对话上下文窗口, 防止超过模型 token 限制。

Phase 1 最小实现:
- Eviction: 当 token 超限时, 从最旧的消息开始移除 (保留 system + 最新 user)
- Emergency: 超限仍无法解决时, 截断最长的消息内容
"""

from __future__ import annotations

import json
from typing import Any

import structlog

from sahara_runtime.context.token_counter import TokenCounter

logger = structlog.get_logger(__name__)

CONTEXT_RESERVE = 8192  # 为输出预留的 token 数


class ContextManager:
    """管理对话上下文窗口。"""

    def __init__(self, token_counter: TokenCounter | None = None) -> None:
        self._counter = token_counter or TokenCounter()

    def fit_messages(
        self,
        messages: list[dict[str, Any]],
        *,
        max_context_tokens: int,
        system_tokens: int = 0,
    ) -> list[dict[str, Any]]:
        """确保 messages 总 token 数不超过上下文限制。

        策略:
        1. 计算当前总量
        2. 如果超限, 从最旧的消息开始移除 (eviction)
        3. 如果只剩最后一对 user/assistant 仍超限, 截断最长消息 (emergency)
        """
        budget = max_context_tokens - system_tokens - CONTEXT_RESERVE
        if budget <= 0:
            logger.warning("context_budget_exhausted", max_tokens=max_context_tokens)
            return messages[-2:] if len(messages) >= 2 else messages[-1:]

        total = self._counter.count_messages(messages)

        if total <= budget:
            return messages

        # ── Eviction: 从头部移除旧消息 ────────────────
        # 保留至少最后 2 条 (一对 user + assistant/tool_result)
        result = list(messages)
        while len(result) > 2 and self._counter.count_messages(result) > budget:
            removed = result.pop(0)
            logger.debug(
                "context_evict",
                removed_role=removed.get("role"),
                remaining=len(result),
            )

        total = self._counter.count_messages(result)
        if total <= budget:
            if len(result) < len(messages):
                logger.info(
                    "context_evicted",
                    original=len(messages),
                    remaining=len(result),
                    tokens=total,
                )
            return result

        # ── Emergency: 截断最长的消息 ─────────────────
        logger.warning(
            "context_emergency_truncate",
            tokens=total,
            budget=budget,
            messages=len(result),
        )
        result = self._truncate_longest(result, budget)
        return result

    def _truncate_longest(
        self,
        messages: list[dict[str, Any]],
        budget: int,
    ) -> list[dict[str, Any]]:
        """截断最长消息的内容, 直到满足 budget。"""
        result = [dict(m) for m in messages]

        for _ in range(5):
            total = self._counter.count_messages(result)
            if total <= budget:
                break

            longest_idx = -1
            longest_tokens = 0
            for i, msg in enumerate(result):
                t = self._counter.count_message(msg)
                if t > longest_tokens:
                    longest_tokens = t
                    longest_idx = i

            if longest_idx < 0:
                break

            msg = result[longest_idx]
            content = msg.get("content", "")

            if isinstance(content, str):
                keep_chars = max(100, (budget * 4) // len(result))
                msg["content"] = content[:keep_chars] + "\n... [truncated]"
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        for key in ("text", "content", "output"):
                            if key in block and isinstance(block[key], str) and len(block[key]) > 200:
                                keep = max(100, len(block[key]) // 3)
                                block[key] = block[key][:keep] + "\n... [truncated]"

        return result
