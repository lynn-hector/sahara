"""
Dependencies 注入容器 — 管理所有 Runtime 子系统实例

参考: D4 §14 Dependencies 启动流程
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from sahara_runtime.config.settings import Settings
    from sahara_runtime.events.backend import EventBackend
    from sahara_runtime.events.emitter import EventEmitterFactory
    from sahara_runtime.model_router.router import ModelRouter
    from sahara_runtime.prompt.builder import PromptBuilder
    from sahara_runtime.tools.registry import ToolRegistry

logger = structlog.get_logger(__name__)


@dataclass
class Container:
    """依赖注入容器 — Runtime Worker 的核心对象图。"""

    settings: Settings

    # ── Phase 1 Sprint 1 ──────────────────────────────
    redis: Redis | None = None
    event_backend: EventBackend | None = None
    emitter_factory: EventEmitterFactory | None = None

    # ── Phase 1 Sprint 2 ──────────────────────────────
    llm_client: Any | None = None
    model_router: ModelRouter | None = None
    tool_registry: ToolRegistry | None = None
    prompt_builder: PromptBuilder | None = None

    # ── Phase 1 Sprint 3 ──────────────────────────────
    # pg_pool: asyncpg.Pool | None = None
    # session_store: SessionStore | None = None
    # sandbox_manager: SandboxManager | None = None

    async def startup(self) -> None:
        """启动所有子系统 (按依赖顺序)."""
        logger.info(
            "container starting",
            worker_id=self.settings.worker_id,
            max_tasks=self.settings.max_concurrent_tasks,
        )

        # 1. Redis
        from redis.asyncio import Redis as AsyncRedis

        self.redis = AsyncRedis.from_url(
            self.settings.redis_url,
            decode_responses=False,
        )
        await self.redis.ping()
        logger.info("redis connected", url=self.settings.redis_url)

        # 2. Event Backend + Factory
        from sahara_runtime.events.emitter import EventEmitterFactory
        from sahara_runtime.events.redis_backend import RedisStreamsBackend

        self.event_backend = RedisStreamsBackend(self.redis)
        self.emitter_factory = EventEmitterFactory(
            self.event_backend,
            self.settings.worker_id,
        )

        # 3. Model Router
        from sahara_runtime.model_router.router import ModelRouter

        self.model_router = ModelRouter(self.settings)

        # 4. LLM Client (if API key available)
        if self.settings.anthropic_api_key:
            try:
                import anthropic

                self.llm_client = anthropic.AsyncAnthropic(
                    api_key=self.settings.anthropic_api_key.split(",")[0].strip()
                )
                logger.info("anthropic client initialized")
            except Exception:
                logger.warning("failed to init anthropic client, using mock LLM")
        else:
            logger.info("no anthropic API key, using mock LLM")

        # 5. Tool Registry + Builtins
        from sahara_runtime.tools.builtins import register_builtins
        from sahara_runtime.tools.registry import ToolRegistry

        self.tool_registry = ToolRegistry()
        register_builtins(self.tool_registry)
        logger.info("tools registered", tools=self.tool_registry.names())

        # 6. Prompt Builder
        from sahara_runtime.prompt.builder import PromptBuilder

        self.prompt_builder = PromptBuilder()

        logger.info("container started")

    async def shutdown(self) -> None:
        """关闭所有子系统 (反向顺序)."""
        logger.info("container shutting down")

        if self.event_backend:
            await self.event_backend.close()

        if self.redis:
            await self.redis.close()

        logger.info("container stopped")
