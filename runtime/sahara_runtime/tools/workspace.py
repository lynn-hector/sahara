"""Workspace 管理 — 初始化 + 路径解析 re-export。"""

from __future__ import annotations

from pathlib import Path

import structlog

from sahara_runtime.tools.builtins.fs_base import resolve_path

logger = structlog.get_logger(__name__)

__all__ = ["ensure_workspace", "resolve_path"]


def ensure_workspace(workspace_dir: str) -> Path:
    """确保 workspace 目录存在，返回解析后的绝对路径。"""
    ws = Path(workspace_dir).expanduser().resolve()
    ws.mkdir(parents=True, exist_ok=True)
    logger.info("workspace initialized", path=str(ws))
    return ws
