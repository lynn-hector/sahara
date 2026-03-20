"""LLMProvider 抽象基类 — 统一流式 LLM 调用接口。

参考 nanobot/providers/base.py 设计:
- chat() 使用 api_key 替代 client，Provider 内部管理 SDK 连接
- messages 统一使用 OpenAI 通用格式
- chat_with_retry() 内置重试 + Key 轮换逻辑
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import structlog

from sahara_runtime.llm.types import LLMResponse

if TYPE_CHECKING:
    from sahara_runtime.events.emitter import RunEmitter
    from sahara_runtime.model_router.router import ModelConfig, ModelRouter

logger = structlog.get_logger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 529})
AUTH_ERROR_CODES = frozenset({401, 403})


class LLMProvider(ABC):
    """LLM Provider 抽象基类。

    子类需实现 chat() 方法，完成具体的流式调用和事件推送。
    messages 统一使用 OpenAI 通用格式，各 Provider 内部自行转换为原生 API 格式。
    retry + key rotation 逻辑由基类的 chat_with_retry() 统一处理。
    """

    @abstractmethod
    async def chat(
        self,
        *,
        api_key: str,
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        emitter: RunEmitter,
        max_tokens: int = 8192,
        api_base: str | None = None,
    ) -> LLMResponse:
        """流式调用 LLM，通过 emitter 推送 delta/thinking 事件。

        Args:
            api_key: Provider 的 API key
            model: 模型标识符
            system: 系统提示词
            messages: OpenAI 通用格式的对话消息列表
            tools: 工具 schema 列表 (None 表示不启用工具)
            emitter: 事件发射器，用于实时推送流式内容
            max_tokens: 最大输出 token 数
            api_base: 自定义 API endpoint (可选)

        Returns:
            LLMResponse 包含聚合后的文本、工具调用和 usage 信息
        """

    async def chat_with_retry(
        self,
        *,
        model_router: ModelRouter,
        session_key: str,
        model_config: ModelConfig,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        emitter: RunEmitter,
    ) -> LLMResponse:
        """带自动重试和 Key 轮换的 LLM 调用。

        Fallback 策略:
        1. 首次调用: 从 ModelRouter 获取 api_key
        2. 429/5xx/连接错误: 等待 backoff -> 轮换 Key -> 重试
        3. 401/403: 熔断当前 Key -> 轮换 -> 重试
        4. 超过 MAX_RETRIES: 抛出最后一个异常
        """
        last_exc: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                api_key = model_router.get_api_key_for_session(
                    session_key, model_config,
                )
                response = await self.chat(
                    api_key=api_key,
                    model=model_config.model_id,
                    system=system,
                    messages=messages,
                    tools=tools,
                    emitter=emitter,
                    max_tokens=model_config.max_tokens,
                    api_base=model_config.api_base or None,
                )
                model_router.report_key_success(session_key)
                return response

            except Exception as exc:
                last_exc = exc
                status_code = _extract_status_code(exc)
                retryable = _is_retryable(status_code, exc)

                logger.warning(
                    "llm_call_failed",
                    attempt=attempt,
                    max_retries=MAX_RETRIES,
                    error_type=type(exc).__name__,
                    status_code=status_code,
                    retryable=retryable,
                    session_key=session_key,
                )

                if status_code:
                    model_router.report_key_error(session_key, status_code)

                if not retryable or attempt >= MAX_RETRIES:
                    raise

                model_router.release_session(session_key)

                backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                logger.info("llm_retry_backoff", seconds=backoff, attempt=attempt)
                await asyncio.sleep(backoff)

        raise last_exc  # type: ignore[misc]


def _extract_status_code(exc: Exception) -> int:
    """从 SDK 异常中提取 HTTP 状态码。"""
    try:
        import anthropic
        if isinstance(exc, anthropic.APIStatusError):
            return exc.status_code
    except ImportError:
        pass
    if hasattr(exc, "status_code"):
        return exc.status_code
    return 0


def _is_retryable(status_code: int, exc: Exception) -> bool:
    """判断 LLM 调用异常是否可通过重试/Key 轮换恢复。"""
    if status_code in RETRYABLE_STATUS_CODES:
        return True
    if status_code in AUTH_ERROR_CODES:
        return True
    exc_name = type(exc).__name__.lower()
    return "timeout" in exc_name or "connection" in exc_name
