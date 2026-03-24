"""文件系统工具共享基类 — 路径解析 + 目录限制。

参考 nanobot/agent/tools/filesystem.py 的 _FsTool + _resolve_path 设计。
"""

from __future__ import annotations

from pathlib import Path

from sahara_runtime.tools.base import Tool


def resolve_path(
    path: str,
    workspace: Path | None = None,
    allowed_dir: Path | None = None,
    extra_allowed_dirs: list[Path] | None = None,
) -> Path:
    """解析工具路径：相对路径基于 workspace，绝对路径直接使用。

    当 allowed_dir 不为 None 时，检查路径是否在允许范围内。
    """
    p = Path(path).expanduser()
    if not p.is_absolute() and workspace:
        p = workspace / p
    resolved = p.resolve()
    if allowed_dir:
        all_dirs = [allowed_dir] + (extra_allowed_dirs or [])
        if not any(_is_under(resolved, d) for d in all_dirs):
            raise PermissionError(
                f"Path {path} is outside allowed directory {allowed_dir}"
            )
    return resolved


def _is_under(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory.resolve())
        return True
    except ValueError:
        return False


class FsTool(Tool):
    """文件系统工具基类 — 提供 workspace 注入和路径解析。"""

    def __init__(
        self,
        workspace: Path | None = None,
        allowed_dir: Path | None = None,
        extra_allowed_dirs: list[Path] | None = None,
    ) -> None:
        self._workspace = workspace
        self._allowed_dir = allowed_dir
        self._extra_allowed_dirs = extra_allowed_dirs

    def _resolve(self, path: str) -> Path:
        return resolve_path(
            path, self._workspace, self._allowed_dir, self._extra_allowed_dirs,
        )
