"""write_file 工具 — 写入文件内容。

参考 nanobot/agent/tools/filesystem.py WriteFileTool。
"""

from __future__ import annotations

from typing import Any

from sahara_runtime.tools.builtins.fs_base import FsTool


class WriteFileTool(FsTool):
    """写入文件，自动创建父目录。"""

    @property
    def name(self) -> str:
        return "write_file"

    @property
    def description(self) -> str:
        return (
            "Write content to a file. Creates parent directories if needed. "
            "Relative paths resolve from workspace."
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
                "content": {
                    "type": "string",
                    "description": "Content to write to the file",
                },
            },
            "required": ["path", "content"],
        }

    async def execute(self, path: str, content: str, **kwargs: Any) -> str:
        try:
            fp = self._resolve(path)
        except PermissionError as e:
            return f"Error: {e}"

        try:
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(content, encoding="utf-8")
            return f"Successfully wrote {len(content)} bytes to {fp}"
        except Exception as e:
            return f"Error writing file: {e}"
