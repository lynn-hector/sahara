"""write 工具 — 写入文件内容，路径基于 workspace 解析。"""

from __future__ import annotations

import os
from pathlib import Path

from sahara_runtime.tools.workspace import resolve_path

INPUT_SCHEMA = {
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
        "append": {
            "type": "boolean",
            "description": "If true, append to existing file instead of overwriting (default: false)",
            "default": False,
        },
    },
    "required": ["path", "content"],
}


async def write_file(
    path: str,
    content: str,
    append: bool = False,
    *,
    workspace: Path,
) -> str:
    """写入文件内容，自动创建父目录。"""
    resolved = str(resolve_path(path, workspace))

    dir_path = os.path.dirname(resolved)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    mode = "a" if append else "w"
    with open(resolved, mode, encoding="utf-8") as f:
        f.write(content)

    size = os.path.getsize(resolved)
    action = "appended to" if append else "written to"
    return f"Successfully {action} {resolved} ({size} bytes)"
