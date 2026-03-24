"""Dependencies 注入容器 — 管理所有 Runtime 子系统实例

参考: D4 §14 Dependencies 启动流程

重构改进:
- llm_client 替换为 llm_provider (LLMProvider ABC)
- 新增 tool_executor 实例（复用而非每次任务新建）
- LLM Provider 根据配置自动选择 Anthropic / Mock
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from sahara_runtime.config.settings import Settings
    from sahara_runtime.context.context_manager import ContextManager
    from sahara_runtime.events.backend import EventBackend
    from sahara_runtime.events.emitter import EventEmitterFactory
    from sahara_runtime.hooks.runner import HookRunner
    from sahara_runtime.llm.base import LLMProvider
    from sahara_runtime.memory.session_store import SessionStore
    from sahara_runtime.model_router.router import ModelRouter
    from sahara_runtime.sandbox.base import SandboxManager
    from sahara_runtime.skills.manager import SkillsManager
    from sahara_runtime.tools.executor import ToolExecutor
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
    llm_provider: LLMProvider | None = None
    model_router: ModelRouter | None = None

    # ── Tools ─────────────────────────────────────────
    tool_registry: ToolRegistry | None = None
    tool_executor: ToolExecutor | None = None

    # ── Context & Session ────────────────────────────
    context_manager: ContextManager | None = None
    session_store: SessionStore | None = None

    # ── Workspace & Sandbox ──────────────────────────
    workspace: Path | None = None
    sandbox_manager: SandboxManager | None = None

    # ── Skills ────────────────────────────────────────
    skills_manager: SkillsManager | None = None

    # ── Hooks ─────────────────────────────────────────
    hook_runner: HookRunner | None = None

    async def startup(self) -> None:
        """启动所有子系统 (按依赖顺序)."""
        logger.info(
            "container starting",
            worker_id=self.settings.worker_id,
            max_tasks=self.settings.max_concurrent_tasks,
        )

        self._init_workspace()
        await self._init_redis()
        self._init_events()
        self._init_session_store()
        self._init_model_router()
        self._init_llm_provider()
        await self._init_sandbox()
        self._init_tools()
        self._init_skills()
        self._init_context()
        self._init_hooks()

        logger.info("container started")

    def _init_workspace(self) -> None:
        from sahara_runtime.tools.workspace import ensure_workspace

        self.workspace = ensure_workspace(self.settings.workspace_dir)

    async def _init_redis(self) -> None:
        from redis.asyncio import Redis as AsyncRedis

        self.redis = AsyncRedis.from_url(
            self.settings.redis_url,
            decode_responses=False,
        )
        await self.redis.ping()
        logger.info("redis connected", url=self.settings.redis_url)

    def _init_events(self) -> None:
        from sahara_runtime.events.emitter import EventEmitterFactory
        from sahara_runtime.events.redis_backend import RedisStreamsBackend

        self.event_backend = RedisStreamsBackend(self.redis)
        self.emitter_factory = EventEmitterFactory(
            self.event_backend,
            self.settings.worker_id,
        )

    def _init_session_store(self) -> None:
        from sahara_runtime.memory.session_store import SessionStore

        self.session_store = SessionStore(self.redis)
        logger.info("session_store initialized")

    def _init_model_router(self) -> None:
        from sahara_runtime.model_router.router import ModelRouter

        self.model_router = ModelRouter(self.settings)

    def _init_llm_provider(self) -> None:
        self.llm_provider = self._create_llm_provider()

    async def _init_sandbox(self) -> None:
        self.sandbox_manager = await self._create_sandbox_manager()
        await self.sandbox_manager.startup()
        logger.info("sandbox manager started", provider=self.settings.sandbox_provider)

    def _init_tools(self) -> None:
        from sahara_runtime.tools.builtins import register_builtins
        from sahara_runtime.tools.executor import ToolExecutor
        from sahara_runtime.tools.registry import ToolRegistry

        self.tool_registry = ToolRegistry()
        register_builtins(self.tool_registry, workspace=self.workspace)
        logger.info("tools registered", tools=self.tool_registry.names())

        real_sandbox = self.sandbox_manager if self.settings.sandbox_enabled else None
        self.tool_executor = ToolExecutor(
            self.tool_registry,
            sandbox_manager=real_sandbox,
        )

    def _init_skills(self) -> None:
        from sahara_runtime.skills.filter import SkillFilter
        from sahara_runtime.skills.loader import SkillLoader
        from sahara_runtime.skills.manager import SkillsManager

        skill_loader = SkillLoader(
            bundled_dir=self.settings.bundled_skills_dir,
            configured_dir=self.settings.configured_skills_dir,
            managed_dir=self.settings.managed_skills_dir,
        )
        skill_filter = SkillFilter(disabled_skills=self._build_disabled_skills())
        self.skills_manager = SkillsManager(
            loader=skill_loader,
            skill_filter=skill_filter,
        )
        logger.info("skills initialized")

    def _init_context(self) -> None:
        from sahara_runtime.context.context_manager import ContextManager

        self.context_manager = ContextManager(
            workspace=self.workspace,
            skills_manager=self.skills_manager,
            tool_registry=self.tool_registry,
        )

    def _init_hooks(self) -> None:
        from sahara_runtime.hooks.builtins import register_builtin_hooks
        from sahara_runtime.hooks.runner import HookRunner

        self.hook_runner = HookRunner()
        register_builtin_hooks(self.hook_runner)
        logger.info(
            "hook_runner initialized",
            hooks=len(self.hook_runner.list_hooks()),
        )

    def _build_disabled_skills(self) -> set[str]:
        if not self.settings.disabled_skills:
            return set()
        return {
            name.strip()
            for name in self.settings.disabled_skills.split(",")
            if name.strip()
        }

    def _create_llm_provider(self) -> LLMProvider:
        """根据配置选择 LLM Provider。

        settings.llm_provider:
        - "litellm"    → LiteLLMProvider (默认，支持 100+ 模型)
        - "anthropic"  → AnthropicProvider (Anthropic SDK 直连)
        - "deepseek"   → DeepSeekProvider (DeepSeek SDK 直连)
        - "mock"       → MockProvider (开发/测试降级)
        """
        provider_type = self.settings.llm_provider

        if provider_type == "litellm":
            try:
                from sahara_runtime.llm.litellm_provider import LiteLLMProvider
                logger.info("llm_provider initialized", provider="litellm")
                return LiteLLMProvider()
            except ImportError:
                logger.warning(
                    "litellm package not installed, falling back to anthropic provider",
                )
                provider_type = "anthropic"

        if provider_type == "deepseek":
            if self.settings.deepseek_api_key:
                from sahara_runtime.llm.deepseek_provider import DeepSeekProvider
                logger.info("llm_provider initialized", provider="deepseek")
                return DeepSeekProvider()
            logger.warning(
                "deepseek_api_key not set, falling back to mock provider",
            )

        if provider_type == "anthropic":
            if self.settings.anthropic_api_key:
                try:
                    from sahara_runtime.llm.anthropic_provider import AnthropicProvider
                    logger.info("llm_provider initialized", provider="anthropic")
                    return AnthropicProvider()
                except ImportError:
                    logger.warning(
                        "anthropic package not installed, using mock LLM provider",
                    )
                except Exception:
                    logger.exception(
                        "failed to init anthropic provider, using mock LLM provider",
                    )
            else:
                logger.warning(
                    "anthropic_api_key not set, falling back to mock provider",
                )

        from sahara_runtime.llm.mock_provider import MockProvider
        logger.info("llm_provider initialized", provider="mock")
        return MockProvider()

    async def _create_sandbox_manager(self) -> SandboxManager:
        """根据 sandbox_provider 配置创建对应的 SandboxManager。"""
        from sahara_runtime.sandbox.noop_sandbox import NoopSandboxManager

        provider = (
            self.settings.sandbox_provider
            if self.settings.sandbox_enabled
            else "noop"
        )

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
                logger.warning(
                    "e2b package not installed, falling back to noop sandbox",
                )
                return NoopSandboxManager()

        if provider == "docker":
            try:
                from sahara_runtime.sandbox.docker_sandbox import DockerSandboxManager

                return DockerSandboxManager(
                    image=self.settings.sandbox_image,
                    pool_size=self.settings.sandbox_pool_size,
                )
            except Exception:
                logger.warning(
                    "docker sandbox unavailable, falling back to noop sandbox",
                )
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
