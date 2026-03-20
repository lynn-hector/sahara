"""Workspace 管理 — 路径解析与初始化。

参考 nanobot/agent/tools/filesystem.py 的 _resolve_path 设计：
- 相对路径以 workspace 为基准解析
- 绝对路径直接使用
- ~ 路径通过 expanduser 展开
"""

from __future__ import annotations

from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


def resolve_path(path: str, workspace: Path) -> Path:
    """将工具收到的路径解析为绝对路径。

    规则:
        - 相对路径 (e.g. "hello.py") → workspace / hello.py
        - 绝对路径 (e.g. "/etc/hosts") → 直接使用
        - ~ 路径 (e.g. "~/docs") → expanduser 后使用
    """
    p = Path(path).expanduser()
    if not p.is_absolute():
        p = workspace / p
    return p.resolve()


def ensure_workspace(workspace_dir: str) -> Path:
    """确保 workspace 目录存在，返回解析后的绝对路径。

    在 Container.startup() 中调用，早于工具注册。
    """
    ws = Path(workspace_dir).expanduser().resolve()
    ws.mkdir(parents=True, exist_ok=True)
    logger.info("workspace initialized", path=str(ws))
    return ws
