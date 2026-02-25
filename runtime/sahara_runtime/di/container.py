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
    from sahara_runtime.context.manager import ContextManager
    from sahara_runtime.context.token_counter import TokenCounter
    from sahara_runtime.events.backend import EventBackend
    from sahara_runtime.events.emitter import EventEmitterFactory
    from sahara_runtime.hooks.runner import HookRunner
    from sahara_runtime.memory.session_store import SessionStore
    from sahara_runtime.model_router.router import ModelRouter
    from sahara_runtime.prompt.builder import PromptBuilder
    from sahara_runtime.sandbox.base import SandboxManager
    from sahara_runtime.skills.filter import SkillFilter
    from sahara_runtime.skills.loader import SkillLoader
    from sahara_runtime.tools.registry import ToolRegistry

logger = structlog.get_logger(__name__)


@dataclass
class Container:
    """依赖注入容器 — Runtime Worker 的核心对象图。"""

    settings: Settings

    # ── Infrastructure ────────────────────────────────
    redis: Redis | None = None
    event_backend: EventBackend | None = None
    emitter_factory: EventEmitterFactory | None = None

    # ── LLM / Model ──────────────────────────────────
    llm_client: Any | None = None
    model_router: ModelRouter | None = None

    # ── Tools & Prompt ────────────────────────────────
    tool_registry: ToolRegistry | None = None
    prompt_builder: PromptBuilder | None = None

    # ── Session & Context ─────────────────────────────
    session_store: SessionStore | None = None
    token_counter: TokenCounter | None = None
    context_manager: ContextManager | None = None

    # ── Sandbox ───────────────────────────────────────
    sandbox_manager: SandboxManager | None = None

    # ── Skills ────────────────────────────────────────
    skill_loader: SkillLoader | None = None
    skill_filter: SkillFilter | None = None

    # ── Hooks ─────────────────────────────────────────
    hook_runner: HookRunner | None = None

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

        # 3. Session Store
        from sahara_runtime.memory.session_store import SessionStore

        self.session_store = SessionStore(self.redis)
        logger.info("session_store initialized")

        # 4. Context Manager
        from sahara_runtime.context.manager import ContextManager
        from sahara_runtime.context.token_counter import TokenCounter

        self.token_counter = TokenCounter()
        self.context_manager = ContextManager(self.token_counter)

        # 5. Model Router
        from sahara_runtime.model_router.router import ModelRouter

        self.model_router = ModelRouter(self.settings)

        # 6. LLM Client (if API key available)
        if self.settings.anthropic_api_key:
            try:
                import anthropic

                self.llm_client = anthropic.AsyncAnthropic(
                    api_key=self.settings.anthropic_api_key.split(",")[0].strip()
                )
                logger.info("anthropic client initialized")
            except ImportError:
                logger.warning("anthropic package not installed, using mock LLM")
            except Exception:
                logger.exception("failed to init anthropic client, using mock LLM")
        else:
            logger.info("no anthropic API key, using mock LLM")

        # 7. Sandbox Manager — provider: "noop" | "docker" | "e2b"
        self.sandbox_manager = await self._create_sandbox_manager()
        await self.sandbox_manager.startup()
        logger.info("sandbox manager started", provider=self.settings.sandbox_provider)

        # 8. Tool Registry + Builtins
        from sahara_runtime.tools.builtins import register_builtins
        from sahara_runtime.tools.registry import ToolRegistry

        self.tool_registry = ToolRegistry()
        register_builtins(self.tool_registry)
        logger.info("tools registered", tools=self.tool_registry.names())

        # 9. Skill Loader + Filter
        from sahara_runtime.skills.filter import SkillFilter
        from sahara_runtime.skills.loader import SkillLoader

        self.skill_loader = SkillLoader(
            bundled_dir=self.settings.bundled_skills_dir,
            configured_dir=self.settings.configured_skills_dir,
            managed_dir=self.settings.managed_skills_dir,
        )
        disabled = set()
        if self.settings.disabled_skills:
            disabled = {s.strip() for s in self.settings.disabled_skills.split(",") if s.strip()}
        self.skill_filter = SkillFilter(disabled_skills=disabled)
        logger.info("skill_loader initialized")

        # 10. Hook Runner + Builtin Hooks
        from sahara_runtime.hooks.builtins import register_builtin_hooks
        from sahara_runtime.hooks.runner import HookRunner

        self.hook_runner = HookRunner()
        register_builtin_hooks(self.hook_runner)
        logger.info("hook_runner initialized", hooks=len(self.hook_runner.list_hooks()))

        # 11. Prompt Builder
        from sahara_runtime.prompt.builder import PromptBuilder

        self.prompt_builder = PromptBuilder()

        logger.info("container started")

    async def _create_sandbox_manager(self) -> SandboxManager:
        """根据 sandbox_provider 配置创建对应的 SandboxManager。"""
        from sahara_runtime.sandbox.noop_sandbox import NoopSandboxManager

        provider = self.settings.sandbox_provider if self.settings.sandbox_enabled else "noop"

        if provider == "e2b":
            if not self.settings.e2b_api_key:
                logger.warning("e2b_api_key not set, falling back to noop sandbox")
                return NoopSandboxManager()
            try:
                from sahara_runtime.sandbox.e2b_sandbox import E2BSandboxManager

                return E2BSandboxManager(
                    api_key=self.settings.e2b_api_key,
                    template=self.settings.e2b_template,
                    timeout=self.settings.e2b_timeout,
                    pool_size=self.settings.sandbox_pool_size,
                )
            except ImportError:
                logger.warning("e2b package not installed, falling back to noop sandbox")
                return NoopSandboxManager()

        if provider == "docker":
            try:
                from sahara_runtime.sandbox.docker_sandbox import DockerSandboxManager

                return DockerSandboxManager(
                    image=self.settings.sandbox_image,
                    pool_size=self.settings.sandbox_pool_size,
                )
            except Exception:
                logger.warning("docker sandbox unavailable, falling back to noop sandbox")
                return NoopSandboxManager()

        return NoopSandboxManager()

    async def shutdown(self) -> None:
        """关闭所有子系统 (反向顺序)."""
        logger.info("container shutting down")

        if self.sandbox_manager:
            try:
                await self.sandbox_manager.shutdown()
            except Exception:
                logger.exception("sandbox_manager shutdown error")

        if self.event_backend:
            await self.event_backend.close()

        if self.redis:
            await self.redis.close()

        logger.info("container stopped")
