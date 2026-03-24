"""grep 工具 — 在文件/目录中搜索文本或正则表达式。

比 exec("grep ...") 更安全：
- 路径基于 workspace 解析
- 输出结构化（文件名:行号:内容）
- 自动忽略二进制文件和噪声目录
- 结果截断防止上下文爆炸
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from sahara_runtime.tools.builtins.fs_base import FsTool

_MAX_RESULTS = 100
_MAX_LINE_LEN = 300
_IGNORE_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv",
    "dist", "build", ".tox", ".mypy_cache", ".pytest_cache",
    ".ruff_cache", ".eggs", ".cache",
}
_BINARY_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico", ".svg",
    ".pdf", ".zip", ".gz", ".tar", ".whl", ".pyc", ".so",
    ".dll", ".exe", ".bin", ".dat", ".db", ".sqlite",
}


class GrepTool(FsTool):
    """在文件或目录中搜索文本/正则表达式。"""

    @property
    def name(self) -> str:
        return "grep"

    @property
    def description(self) -> str:
        return (
            "Search for text or regex patterns in files. "
            "Returns matching lines with file paths and line numbers. "
            "Searches recursively in directories, auto-ignoring "
            ".git, node_modules, __pycache__, etc."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Text or regex pattern to search for",
                },
                "path": {
                    "type": "string",
                    "description": (
                        "File or directory to search in. "
                        "Defaults to workspace root ('.'). "
                        "Relative paths resolve from workspace."
                    ),
                },
                "include": {
                    "type": "string",
                    "description": (
                        "Glob pattern to filter files (e.g. '*.py', '*.ts'). "
                        "Only matching files are searched."
                    ),
                },
                "ignore_case": {
                    "type": "boolean",
                    "description": "Case-insensitive search (default false)",
                },
                "max_results": {
                    "type": "integer",
                    "description": f"Maximum matches to return (default {_MAX_RESULTS})",
                    "minimum": 1,
                    "maximum": 500,
                },
            },
            "required": ["pattern"],
        }

    async def execute(
        self,
        pattern: str,
        path: str = ".",
        include: str = "",
        ignore_case: bool = False,
        max_results: int | None = None,
        **kwargs: Any,
    ) -> str:
        try:
            target = self._resolve(path)
        except PermissionError as e:
            return f"Error: {e}"

        if not target.exists():
            return f"Error: Path not found: {path}"

        try:
            flags = re.IGNORECASE if ignore_case else 0
            regex = re.compile(pattern, flags)
        except re.error as e:
            return f"Error: Invalid regex pattern: {e}"

        cap = max_results or _MAX_RESULTS
        matches: list[str] = []
        files_searched = 0
        total_matches = 0

        if target.is_file():
            _search_file(target, regex, matches, cap, target.parent)
            files_searched = 1
            total_matches = len(matches)
        else:
            files = _collect_files(target, include)
            for fp in files:
                if len(matches) >= cap:
                    break
                before = len(matches)
                _search_file(fp, regex, matches, cap, target)
                if len(matches) > before:
                    files_searched += 1
                    total_matches += len(matches) - before

        if not matches:
            scope = f"in {path}" if path != "." else "in workspace"
            return f"No matches found for '{pattern}' {scope}"

        result = "\n".join(matches)
        if total_matches >= cap:
            result += f"\n\n(Results capped at {cap}. Use more specific pattern or path.)"
        else:
            result += f"\n\n({total_matches} matches in {files_searched} files)"
        return result


def _collect_files(directory: Path, include: str) -> list[Path]:
    """收集目录下的可搜索文件列表。"""
    files: list[Path] = []
    for item in sorted(directory.rglob(include or "*")):
        if any(p in _IGNORE_DIRS for p in item.parts):
            continue
        if not item.is_file():
            continue
        if item.suffix.lower() in _BINARY_EXTENSIONS:
            continue
        files.append(item)
    return files


def _search_file(
    fp: Path,
    regex: re.Pattern[str],
    matches: list[str],
    cap: int,
    base_dir: Path,
) -> None:
    """在单个文件中搜索，结果追加到 matches。"""
    try:
        text = fp.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return

    try:
        rel = fp.relative_to(base_dir)
    except ValueError:
        rel = fp

    for lineno, line in enumerate(text.splitlines(), 1):
        if len(matches) >= cap:
            break
        if regex.search(line):
            display = line.strip()
            if len(display) > _MAX_LINE_LEN:
                display = display[:_MAX_LINE_LEN] + "..."
            matches.append(f"{rel}:{lineno}: {display}")
