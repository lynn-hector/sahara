"""ModelRouter — 模型解析、Session 亲和、Key 池集成。

核心职责:
    1. **模型解析** (resolve): 根据 model_override 或 Settings.default_model 确定 provider + model_id
    2. **Session 亲和** (get_client): 同一 session 内复用同一 SDK client / API key,
       减少 TCP 握手开销, 并保证 provider 侧的会话级限流窗口一致
    3. **Key 池集成** (KeyPool): 自动 round-robin 分配 API key, 结合熔断和健康恢复机制

调用链路::

    AgentLoop._call_with_fallback
        → ModelRouter.resolve(session_key)       → ModelConfig
        → ModelRouter.get_client(session_key, ...)→ AsyncAnthropic / AsyncOpenAI
        → on success: report_key_success
        → on failure: report_key_error → release_session → retry
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

from sahara_runtime.model_router.key_pool import KeyPool

if TYPE_CHECKING:
    from sahara_runtime.config.settings import Settings

logger = structlog.get_logger(__name__)


@dataclass
class ModelConfig:
    """Resolved model configuration for a single LLM call."""

    provider: str  # "anthropic" | "openai"
    model_id: str  # e.g. "claude-sonnet-4-20250514", "gpt-4o"
    max_tokens: int = 8192
    max_context_tokens: int = 200_000
    max_iterations: int = 20


@dataclass
class SessionBinding:
    """Per-session affinity: locks a session to a specific provider + key + client instance."""

    session_key: str
    provider: str
    model_id: str
    api_key: str
    client: Any  # AsyncAnthropic | AsyncOpenAI
    call_count: int = 0


class ModelRouter:
    """模型路由器 — 管理 Session 亲和绑定和 Key 池。

    Lifecycle:
        Container.startup → ModelRouter.__init__ → _init_keys (从 Settings 加载 API keys)
        ↓
        每次 LLM 调用:
            resolve() → get_client() → [调用 SDK] → report_key_success / report_key_error
        ↓
        Session 结束: release_session()
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._key_pool = KeyPool()
        self._bindings: dict[str, SessionBinding] = {}

        self._init_keys()

    def _init_keys(self) -> None:
        """从 Settings 中加载逗号分隔的 API keys 到 KeyPool。"""
        if self._settings.anthropic_api_key:
            keys = [k.strip() for k in self._settings.anthropic_api_key.split(",")]
            self._key_pool.add_keys("anthropic", keys)
        if self._settings.openai_api_key:
            keys = [k.strip() for k in self._settings.openai_api_key.split(",")]
            self._key_pool.add_keys("openai", keys)

    def resolve(self, session_key: str, model_override: str = "") -> ModelConfig:
        """根据 model_override 或默认模型推断 provider, 返回完整 ModelConfig。

        Provider 推断规则: model_id 以 gpt/o1/o3 开头 → openai, 否则 → anthropic。
        """
        model_id = model_override or self._settings.default_model

        provider = "anthropic"
        if model_id.startswith("gpt") or model_id.startswith("o1") or model_id.startswith("o3"):
            provider = "openai"

        return ModelConfig(
            provider=provider,
            model_id=model_id,
            max_iterations=self._settings.max_iterations,
        )

    def get_client(self, session_key: str, config: ModelConfig) -> Any:
        """获取 session 绑定的 SDK client, 首次调用时从 KeyPool 分配 key 并创建 client。

        如果已有绑定且 provider/model 一致, 直接复用 (Session 亲和)。
        否则释放旧绑定, 从 KeyPool 获取新 key, 创建新 client。

        Raises:
            RuntimeError: 无可用 API key (所有 key 被熔断或未配置)。
        """
        binding = self._bindings.get(session_key)
        if binding and binding.provider == config.provider and binding.model_id == config.model_id:
            binding.call_count += 1
            return binding.client

        api_key = self._key_pool.get_key(config.provider)
        if not api_key:
            raise RuntimeError(f"no available API key for provider {config.provider}")

        client = self._create_client(config.provider, api_key)
        self._bindings[session_key] = SessionBinding(
            session_key=session_key,
            provider=config.provider,
            model_id=config.model_id,
            api_key=api_key,
            client=client,
        )
        return client

    def get_api_key(self, session_key: str) -> str:
        """返回当前 session 绑定的 API key, 无绑定时返回空字符串。"""
        binding = self._bindings.get(session_key)
        return binding.api_key if binding else ""

    def report_key_error(self, session_key: str, status_code: int) -> None:
        """上报 API 错误, 触发 KeyPool 的熔断判断 (401/403 → 熔断, 429 → 仅记录)。"""
        binding = self._bindings.get(session_key)
        if binding:
            self._key_pool.report_error(binding.provider, binding.api_key, status_code)

    def report_key_success(self, session_key: str) -> None:
        """上报 API 成功, 清除 KeyPool 中该 key 的熔断状态。"""
        binding = self._bindings.get(session_key)
        if binding:
            self._key_pool.report_success(binding.provider, binding.api_key)

    def release_session(self, session_key: str) -> None:
        """释放 session 绑定, 下次 get_client 将重新从 KeyPool 分配 key。

        用于 fallback key 轮换: release → get_client → 自动获取下一个可用 key。
        """
        self._bindings.pop(session_key, None)

    @staticmethod
    def _create_client(provider: str, api_key: str) -> Any:
        """按 provider 类型创建对应的异步 SDK client (懒导入以避免未安装 SDK 时报错)。"""
        if provider == "anthropic":
            import anthropic

            return anthropic.AsyncAnthropic(api_key=api_key)
        elif provider == "openai":
            import openai

            return openai.AsyncOpenAI(api_key=api_key)
        else:
            raise ValueError(f"unknown provider: {provider}")
