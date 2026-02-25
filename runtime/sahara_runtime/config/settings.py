"""
配置管理 — 基于 pydantic-settings 从环境变量加载

参考: D4 §17 配置管理
"""

from __future__ import annotations

import os
import uuid

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Runtime Worker 配置."""

    # ── Worker 标识 ─────────────────────────────────────
    worker_id: str = f"worker-{uuid.uuid4().hex[:8]}"

    # ── gRPC ────────────────────────────────────────────
    grpc_port: int = 50051

    # ── 并发 ────────────────────────────────────────────
    max_concurrent_tasks: int = 8

    # ── Redis ───────────────────────────────────────────
    redis_url: str = "redis://localhost:6379"

    # ── PostgreSQL ──────────────────────────────────────
    postgres_dsn: str = "postgresql://sahara:sahara_dev@localhost:5432/sahara"

    # ── LLM ─────────────────────────────────────────────
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    default_model: str = "claude-sonnet-4-20250514"

    # ── Agent Loop ──────────────────────────────────────
    max_iterations: int = 20
    task_timeout_seconds: int = 300

    # ── Skills ───────────────────────────────────────────
    # defaults to <runtime_repo>/skills/ for bundled skills
    bundled_skills_dir: str = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "skills",
    )
    configured_skills_dir: str = ""
    managed_skills_dir: str = ""
    disabled_skills: str = ""  # comma-separated skill names

    # ── 沙箱 ────────────────────────────────────────────
    sandbox_enabled: bool = False
    sandbox_provider: str = "noop"  # "noop" | "docker" | "e2b"
    sandbox_pool_size: int = 2
    sandbox_image: str = "sahara-sandbox:latest"  # docker provider
    e2b_api_key: str = ""  # e2b provider
    e2b_template: str = "base"  # e2b provider
    e2b_timeout: int = 300  # e2b sandbox TTL (seconds)

    # ── 可观测性 ──────────────────────────────────────────
    metrics_port: int = 9090  # Prometheus /metrics HTTP, 0 to disable

    # ── 日志 ────────────────────────────────────────────
    log_level: str = "INFO"
    log_format: str = "json"  # "json" | "console"

    model_config = {"env_prefix": "SAHARA_", "env_file": ".env", "extra": "ignore"}
