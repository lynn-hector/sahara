"""read 工具 — 读取文件内容，路径基于 workspace 解析。"""

from __future__ import annotations

import os
from pathlib import Path

from sahara_runtime.tools.workspace import resolve_path

MAX_FILE_SIZE = 100_000  # 100KB

INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "path": {
            "type": "string",
            "description": "File path. Relative paths resolve from workspace.",
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
}


async def read_file(
    path: str,
    offset: int = 0,
    limit: int = 0,
    *,
    workspace: Path,
) -> str:
    """读取文件内容，支持偏移和行数限制。"""
    resolved = str(resolve_path(path, workspace))

    if not os.path.exists(resolved):
        return f"Error: file not found: {resolved}"

    if os.path.isdir(resolved):
        entries = os.listdir(resolved)
        return f"Directory listing ({len(entries)} entries):\n" + "\n".join(
            entries[:200]
        )

    size = os.path.getsize(resolved)
    if size > MAX_FILE_SIZE:
        return f"Error: file too large ({size} bytes, max {MAX_FILE_SIZE})"

    with open(resolved, encoding="utf-8", errors="replace") as f:
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
