"""ModelRouter — 模型解析、Session 亲和、Key 池集成。"""

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
    provider: str
    model_id: str
    max_tokens: int = 8192
    max_context_tokens: int = 200_000
    max_iterations: int = 20


@dataclass
class SessionBinding:
    """Session 级的模型+Key 绑定。"""

    session_key: str
    provider: str
    model_id: str
    api_key: str
    client: Any
    call_count: int = 0


class ModelRouter:
    """解析模型配置, 管理 Session 亲和绑定和 Key 池。"""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._key_pool = KeyPool()
        self._bindings: dict[str, SessionBinding] = {}

        self._init_keys()

    def _init_keys(self) -> None:
        if self._settings.anthropic_api_key:
            keys = [k.strip() for k in self._settings.anthropic_api_key.split(",")]
            self._key_pool.add_keys("anthropic", keys)
        if self._settings.openai_api_key:
            keys = [k.strip() for k in self._settings.openai_api_key.split(",")]
            self._key_pool.add_keys("openai", keys)

    def resolve(self, session_key: str, model_override: str = "") -> ModelConfig:
        """解析 session 的模型配置。"""
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
        """获取 session 绑定的 SDK client (复用或新建)。"""
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
        binding = self._bindings.get(session_key)
        return binding.api_key if binding else ""

    def report_key_error(self, session_key: str, status_code: int) -> None:
        binding = self._bindings.get(session_key)
        if binding:
            self._key_pool.report_error(binding.provider, binding.api_key, status_code)

    def report_key_success(self, session_key: str) -> None:
        binding = self._bindings.get(session_key)
        if binding:
            self._key_pool.report_success(binding.provider, binding.api_key)

    def release_session(self, session_key: str) -> None:
        self._bindings.pop(session_key, None)

    @staticmethod
    def _create_client(provider: str, api_key: str) -> Any:
        if provider == "anthropic":
            import anthropic

            return anthropic.AsyncAnthropic(api_key=api_key)
        elif provider == "openai":
            import openai

            return openai.AsyncOpenAI(api_key=api_key)
        else:
            raise ValueError(f"unknown provider: {provider}")
