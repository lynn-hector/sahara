"""read 工具 — 读取文件内容。"""

from __future__ import annotations

import os

from sahara_runtime.tools.registry import ToolDef

MAX_FILE_SIZE = 100_000  # 100KB


async def read_file(path: str, offset: int = 0, limit: int = 0) -> str:
    """读取文件内容, 支持偏移和行数限制。"""
    path = os.path.expanduser(path)

    if not os.path.exists(path):
        return f"Error: file not found: {path}"

    if os.path.isdir(path):
        entries = os.listdir(path)
        return f"Directory listing ({len(entries)} entries):\n" + "\n".join(entries[:200])

    size = os.path.getsize(path)
    if size > MAX_FILE_SIZE:
        return f"Error: file too large ({size} bytes, max {MAX_FILE_SIZE})"

    with open(path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    if offset > 0:
        lines = lines[offset:]
    if limit > 0:
        lines = lines[:limit]

    numbered = []
    start = max(offset, 0) + 1
    for i, line in enumerate(lines):
        numbered.append(f"{start + i:6d}|{line.rstrip()}")

    return "\n".join(numbered)


READ_TOOL = ToolDef(
    name="read",
    description="Read the contents of a file. Returns numbered lines. Can read partial files with offset and limit.",
    input_schema={
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute or relative path to the file",
            },
            "offset": {
                "type": "integer",
                "description": "Line number to start reading from (0-based, default: 0)",
                "default": 0,
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of lines to read (0 = all, default: 0)",
                "default": 0,
            },
        },
        "required": ["path"],
    },
    func=read_file,
    tier=0,
    sandboxed=True,
)
