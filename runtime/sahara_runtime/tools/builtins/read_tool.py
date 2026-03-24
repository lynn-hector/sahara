"""read_file 工具 — 读取文件内容，支持分页。

参考 nanobot/agent/tools/filesystem.py ReadFileTool。
"""

from __future__ import annotations

from typing import Any

from sahara_runtime.tools.builtins.fs_base import FsTool

_MAX_CHARS = 128_000
_DEFAULT_LIMIT = 2000


class ReadFileTool(FsTool):
    """读取文件内容，返回带行号的文本。"""

    @property
    def name(self) -> str:
        return "read_file"

    @property
    def description(self) -> str:
        return (
            "Read the contents of a file. Returns numbered lines. "
            "Use offset and limit to paginate through large files."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path. Relative paths resolve from workspace.",
                },
                "offset": {
                    "type": "integer",
                    "description": "Line number to start reading from (1-indexed, default 1)",
                    "minimum": 1,
                },
                "limit": {
                    "type": "integer",
                    "description": f"Maximum lines to read (default {_DEFAULT_LIMIT})",
                    "minimum": 1,
                },
            },
            "required": ["path"],
        }

    async def execute(
        self,
        path: str,
        offset: int = 1,
        limit: int | None = None,
        **kwargs: Any,
    ) -> str:
        try:
            fp = self._resolve(path)
        except PermissionError as e:
            return f"Error: {e}"

        if not fp.exists():
            return f"Error: File not found: {path}"
        if not fp.is_file():
            return f"Error: Not a file: {path}"

        try:
            all_lines = fp.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception as e:
            return f"Error reading file: {e}"

        total = len(all_lines)
        if total == 0:
            return f"(Empty file: {path})"
        if offset < 1:
            offset = 1
        if offset > total:
            return f"Error: offset {offset} is beyond end of file ({total} lines)"

        start = offset - 1
        end = min(start + (limit or _DEFAULT_LIMIT), total)
        numbered = [f"{start + i + 1}| {line}" for i, line in enumerate(all_lines[start:end])]
        result = "\n".join(numbered)

        if len(result) > _MAX_CHARS:
            trimmed: list[str] = []
            chars = 0
            for line in numbered:
                chars += len(line) + 1
                if chars > _MAX_CHARS:
                    break
                trimmed.append(line)
            end = start + len(trimmed)
            result = "\n".join(trimmed)

        if end < total:
            result += (
                f"\n\n(Showing lines {offset}-{end} of {total}. "
                f"Use offset={end + 1} to continue.)"
            )
        else:
            result += f"\n\n(End of file — {total} lines total)"
        return result
