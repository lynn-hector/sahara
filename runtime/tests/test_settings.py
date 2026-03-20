"""配置加载测试."""

import os

from sahara_runtime.config.settings import Settings


def test_default_settings(monkeypatch):
    """验证默认配置值（屏蔽 .env 和环境变量干扰）."""
    for key in list(os.environ):
        if key.startswith("SAHARA_"):
            monkeypatch.delenv(key, raising=False)

    settings = Settings(_env_file=None)
    assert settings.grpc_port == 50051
    assert settings.max_concurrent_tasks == 8
    assert settings.max_iterations == 20
    assert settings.log_level == "INFO"
