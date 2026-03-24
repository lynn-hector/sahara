"""ContextBuilder — 消息编排 + 上下文裁剪 + 格式转换。

从 agent_loop.py 散落函数和 context/manager.py 裁剪逻辑统一迁入。
参考 nanobot/agent/context.py 的消息组装方法设计。
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog

from sahara_runtime.context.token_counter import TokenCounter

if TYPE_CHECKING:
    from sahara_runtime.llm.types import LLMResponse
    from sahara_runtime.memory.session_store import SessionMessage

logger = structlog.get_logger(__name__)

CONTEXT_RESERVE = 8192


class ContextBuilder:
    """消息编排器 — 组装、追加、裁剪对话消息列表。"""

    def __init__(self, token_counter: TokenCounter) -> None:
        self._counter = token_counter

    # ── 消息组装 ─────────────────────────────────────

    @staticmethod
    def build_messages(
        history: list[dict[str, Any]],
        user_message: str,
    ) -> list[dict[str, Any]]:
        """组装完整的消息列表: [*history, user_msg]。"""
        return [*history, {"role": "user", "content": user_message}]

    @staticmethod
    def add_assistant_message(
        messages: list[dict[str, Any]],
        response: LLMResponse,
    ) -> list[dict[str, Any]]:
        """将 LLMResponse 转换为 OpenAI 格式 assistant message 并追加。"""
        tool_call_dicts = (
            [tc.to_openai_tool_call() for tc in response.tool_calls] or None
        )
        messages.append(build_assistant_message(
            response.content,
            tool_calls=tool_call_dicts,
            thinking=response.thinking,
            reasoning_content=response.reasoning_content,
        ))
        return messages

    @staticmethod
    def add_tool_result(
        messages: list[dict[str, Any]],
        tool_call_id: str,
        name: str,
        content: str,
    ) -> list[dict[str, Any]]:
        """追加 tool result 消息。"""
        messages.append({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "name": name,
            "content": content,
        })
        return messages

    # ── 上下文窗口管理 ───────────────────────────────

    def fit_context(
        self,
        messages: list[dict[str, Any]],
        *,
        max_context_tokens: int,
        system_prompt: str,
    ) -> list[dict[str, Any]]:
        """确保 messages 总 token 数不超过上下文限制。

        策略:
        1. 计算当前总量
        2. 如果超限, 从最旧的消息开始移除 (eviction)
        3. 如果只剩最后 2 条仍超限, 截断最长消息 (emergency)
        """
        system_tokens = self._counter.count_system(system_prompt)
        budget = max_context_tokens - system_tokens - CONTEXT_RESERVE
        if budget <= 0:
            logger.warning("context_budget_exhausted", max_tokens=max_context_tokens)
            return messages[-2:] if len(messages) >= 2 else messages[-1:]

        total = self._counter.count_messages(messages)
        if total <= budget:
            return messages

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

        logger.warning(
            "context_emergency_truncate",
            tokens=total, budget=budget, messages=len(result),
        )
        return self._truncate_longest(result, budget)

    def _truncate_longest(
        self,
        messages: list[dict[str, Any]],
        budget: int,
    ) -> list[dict[str, Any]]:
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
                            if (
                                key in block
                                and isinstance(block[key], str)
                                and len(block[key]) > 200
                            ):
                                keep = max(100, len(block[key]) // 3)
                                block[key] = block[key][:keep] + "\n... [truncated]"
        return result

    # ── Session 历史转换 ─────────────────────────────

    @staticmethod
    def session_msg_to_dict(msg: SessionMessage) -> dict[str, Any]:
        """将 SessionMessage 转换为 OpenAI 通用格式。

        兼容旧的 Anthropic 格式数据（content 为 JSON content blocks）。
        """
        result: dict[str, Any] = {"role": msg.role, "content": msg.content}

        extra = msg.extra or {}
        if extra.get("tool_calls"):
            result["tool_calls"] = extra["tool_calls"]
        if extra.get("tool_call_id"):
            result["tool_call_id"] = extra["tool_call_id"]
        if extra.get("name"):
            result["name"] = extra["name"]
        if extra.get("thinking"):
            result["thinking"] = extra["thinking"]
        if extra.get("reasoning_content"):
            result["reasoning_content"] = extra["reasoning_content"]

        if not extra:
            result = _maybe_convert_legacy_anthropic(result)

        return result


# ── 辅助函数 ──────────────────────────────────────────

def build_assistant_message(
    content: str | None,
    tool_calls: list[dict[str, Any]] | None = None,
    thinking: str | None = None,
    reasoning_content: str | None = None,
) -> dict[str, Any]:
    """构建 OpenAI 格式的 assistant message。"""
    msg: dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    if thinking is not None:
        msg["thinking"] = thinking
    if reasoning_content is not None:
        msg["reasoning_content"] = reasoning_content
    return msg


def _maybe_convert_legacy_anthropic(msg: dict[str, Any]) -> dict[str, Any]:
    """兼容旧 Anthropic 格式: 将 content blocks 转换为 OpenAI 格式。"""
    content = msg.get("content")
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            return msg
        if not isinstance(content, list):
            return msg

    if not isinstance(content, list):
        return msg

    if msg.get("role") == "assistant":
        text_parts: list[str] = []
        tool_calls: list[dict[str, Any]] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                tool_calls.append({
                    "id": block.get("id", ""),
                    "type": "function",
                    "function": {
                        "name": block.get("name", ""),
                        "arguments": json.dumps(
                            block.get("input", {}), ensure_ascii=False,
                        ),
                    },
                })
        result: dict[str, Any] = {
            "role": "assistant",
            "content": "".join(text_parts) or None,
        }
        if tool_calls:
            result["tool_calls"] = tool_calls
        return result

    return msg
