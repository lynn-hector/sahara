"""write 工具 — 写入文件内容。"""

from __future__ import annotations

import os

from sahara_runtime.tools.registry import ToolDef


async def write_file(path: str, content: str, append: bool = False) -> str:
    """写入文件内容。"""
    path = os.path.expanduser(path)

    dir_path = os.path.dirname(path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    mode = "a" if append else "w"
    with open(path, mode, encoding="utf-8") as f:
        f.write(content)

    size = os.path.getsize(path)
    action = "appended to" if append else "written to"
    return f"Successfully {action} {path} ({size} bytes)"


WRITE_TOOL = ToolDef(
    name="write",
    description="Write content to a file. Creates the file and parent directories if they don't exist.",
    input_schema={
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute or relative path to the file",
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
    },
    func=write_file,
    tier=0,
)
