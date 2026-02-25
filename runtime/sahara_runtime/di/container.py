"""
Dependencies 注入容器 — 管理所有 Runtime 子系统实例

参考: D4 §14 Dependencies 启动流程
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from sahara_runtime.config.settings import Settings

logger = structlog.get_logger(__name__)


@dataclass
class Container:
    """
    依赖注入容器 — Runtime Worker 的核心对象图。

    Phase 0: 仅 settings + 占位
    Phase 1: 添加 redis, pg, event_emitter, agent_loop, tool_registry, sandbox_manager 等
    Phase 3: 添加 plugin_registry
    """

    settings: Settings

    # ── Phase 1 子系统 (后续逐步添加) ──────────────────
    # redis: redis.asyncio.Redis | None = None
    # pg_pool: asyncpg.Pool | None = None
    # event_emitter: EventEmitter | None = None
    # agent_loop: AgentLoop | None = None
    # tool_registry: ToolRegistry | None = None
    # model_router: ModelRouter | None = None
    # sandbox_manager: SandboxManager | None = None
    # session_store: SessionStore | None = None
    # prompt_builder: PromptBuilder | None = None

    # ── Phase 2 子系统 ─────────────────────────────────
    # hook_runner: HookRunner | None = None
    # skill_loader: SkillLoader | None = None
    # context_manager: ContextManager | None = None

    # ── Phase 3 子系统 ─────────────────────────────────
    # plugin_registry: PluginRegistry | None = None  # ★ 预留

    async def startup(self) -> None:
        """启动所有子系统 (按依赖顺序)."""
        logger.info(
            "container starting",
            worker_id=self.settings.worker_id,
            max_tasks=self.settings.max_concurrent_tasks,
        )

        # TODO Phase 1:
        # 1. self.redis = await create_redis(self.settings.redis_url)
        # 2. self.pg_pool = await asyncpg.create_pool(self.settings.postgres_dsn)
        # 3. self.event_emitter = RedisStreamsBackend(self.redis)
        # 4. self.model_router = ModelRouter(self.settings)
        # 5. self.sandbox_manager = DockerSandboxManager(self.settings)
        # 6. self.tool_registry = ToolRegistry(...)
        # 7. self.prompt_builder = PromptBuilder(...)
        # 8. self.session_store = SessionStore(self.redis, self.pg_pool)
        # 9. self.agent_loop = AgentLoop(self)

        logger.info("container started (Phase 0: empty shell)")

    async def shutdown(self) -> None:
        """关闭所有子系统 (反向顺序)."""
        logger.info("container shutting down")

        # TODO Phase 1:
        # if self.sandbox_manager: await self.sandbox_manager.shutdown()
        # if self.pg_pool: await self.pg_pool.close()
        # if self.redis: await self.redis.close()

        logger.info("container stopped")
