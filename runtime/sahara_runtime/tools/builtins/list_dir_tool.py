"""list_dir 工具 — 目录浏览，支持递归 + 智能忽略。

参考 nanobot/agent/tools/filesystem.py ListDirTool。
"""

from __future__ import annotations

from typing import Any

from sahara_runtime.tools.builtins.fs_base import FsTool

_DEFAULT_MAX = 200
_IGNORE_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv",
    "dist", "build", ".tox", ".mypy_cache", ".pytest_cache",
    ".ruff_cache", ".coverage", "htmlcov", ".eggs", ".cache",
}


class ListDirTool(FsTool):
    """列出目录内容，支持递归和智能忽略。"""

    @property
    def name(self) -> str:
        return "list_dir"

    @property
    def description(self) -> str:
        return (
            "List directory contents. Set recursive=true for nested structure. "
            "Common noise directories (.git, node_modules, __pycache__, etc.) are auto-ignored."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory path. Relative paths resolve from workspace.",
                },
                "recursive": {
                    "type": "boolean",
                    "description": "Recursively list all files (default false)",
                },
                "max_entries": {
                    "type": "integer",
                    "description": f"Maximum entries to return (default {_DEFAULT_MAX})",
                    "minimum": 1,
                },
            },
            "required": ["path"],
        }

    async def execute(
        self,
        path: str,
        recursive: bool = False,
        max_entries: int | None = None,
        **kwargs: Any,
    ) -> str:
        try:
            dp = self._resolve(path)
        except PermissionError as e:
            return f"Error: {e}"

        if not dp.exists():
            return f"Error: Directory not found: {path}"
        if not dp.is_dir():
            return f"Error: Not a directory: {path}"

        cap = max_entries or _DEFAULT_MAX
        items: list[str] = []
        total = 0

        try:
            if recursive:
                for item in sorted(dp.rglob("*")):
                    if any(p in _IGNORE_DIRS for p in item.parts):
                        continue
                    total += 1
                    if len(items) < cap:
                        rel = item.relative_to(dp)
                        suffix = "/" if item.is_dir() else ""
                        items.append(f"{rel}{suffix}")
            else:
                for item in sorted(dp.iterdir()):
                    if item.name in _IGNORE_DIRS:
                        continue
                    total += 1
                    if len(items) < cap:
                        prefix = "[dir]  " if item.is_dir() else "[file] "
                        items.append(f"{prefix}{item.name}")
        except Exception as e:
            return f"Error listing directory: {e}"

        if not items and total == 0:
            return f"Directory {path} is empty"

        result = "\n".join(items)
        if total > cap:
            result += f"\n\n(Showing first {cap} of {total} entries)"
        return result
