"""配置加载测试."""

from sahara_runtime.config.settings import Settings


def test_default_settings():
    """验证默认配置值."""
    settings = Settings()
    assert settings.grpc_port == 50051
    assert settings.max_concurrent_tasks == 8
    assert settings.max_iterations == 20
    assert settings.log_level == "INFO"
