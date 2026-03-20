"""ModelRouter — 模型解析、Session 亲和、Key 池集成。

核心职责:
    1. **模型解析** (resolve): 根据 model_override 或 Settings.default_model
       确定 provider + model_id
    2. **Session 亲和** (get_api_key_for_session): 同一 session 复用同一 API key,
       保证 provider 侧的会话级限流窗口一致
    3. **Key 池集成** (KeyPool): 自动 round-robin 分配 API key, 结合熔断和健康恢复机制

调用链路::

    LLMProvider.chat_with_retry
        → ModelRouter.resolve(session_key)                → ModelConfig
        → ModelRouter.get_api_key_for_session(session_key) → api_key
        → on success: report_key_success
        → on failure: report_key_error → release_session → retry
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from sahara_runtime.model_router.key_pool import KeyPool

if TYPE_CHECKING:
    from sahara_runtime.config.settings import Settings

logger = structlog.get_logger(__name__)


@dataclass
class ModelConfig:
    """Resolved model configuration for a single LLM call."""

    provider: str  # "anthropic" | "openai" | "deepseek"
    model_id: str  # e.g. "claude-sonnet-4-20250514", "deepseek-chat"
    max_tokens: int = 8192
    max_context_tokens: int = 200_000
    max_iterations: int = 20
    api_base: str = ""  # custom endpoint override (empty = provider default)


@dataclass
class SessionBinding:
    """Per-session affinity: locks a session to a specific provider + key."""

    session_key: str
    provider: str
    model_id: str
    api_key: str
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
        if self._settings.deepseek_api_key:
            keys = [k.strip() for k in self._settings.deepseek_api_key.split(",")]
            self._key_pool.add_keys("deepseek", keys)

    def resolve(self, session_key: str, model_override: str = "") -> ModelConfig:
        """根据 model_override 或默认模型推断 provider, 返回完整 ModelConfig。

        Provider 推断规则:
        - deepseek 开头 → deepseek
        - gpt/o1/o3 开头 → openai
        - 其他 → anthropic
        """
        model_id = model_override or self._settings.default_model

        provider = "anthropic"
        api_base = ""
        if model_id.startswith("deepseek"):
            provider = "deepseek"
            api_base = self._settings.deepseek_api_base
        elif model_id.startswith(("gpt", "o1", "o3")):
            provider = "openai"

        return ModelConfig(
            provider=provider,
            model_id=model_id,
            max_iterations=self._settings.max_iterations,
            api_base=api_base,
        )

    def get_api_key_for_session(self, session_key: str, config: ModelConfig) -> str:
        """获取 session 绑定的 API key, 首次调用时从 KeyPool 分配。

        如果已有绑定且 provider/model 一致, 直接复用 (Session 亲和)。
        否则释放旧绑定, 从 KeyPool 获取新 key。

        Raises:
            RuntimeError: 无可用 API key (所有 key 被熔断或未配置)。
        """
        binding = self._bindings.get(session_key)
        if binding and binding.provider == config.provider and binding.model_id == config.model_id:
            binding.call_count += 1
            return binding.api_key

        api_key = self._key_pool.get_key(config.provider)
        if not api_key:
            raise RuntimeError(f"no available API key for provider {config.provider}")

        self._bindings[session_key] = SessionBinding(
            session_key=session_key,
            provider=config.provider,
            model_id=config.model_id,
            api_key=api_key,
        )
        return api_key

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

    # SDK client 创建已移至各 LLMProvider 实现中，ModelRouter 只管理 API key 分配。
