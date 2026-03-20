"""exec 工具 — 在 workspace 目录下执行 shell 命令。"""

from __future__ import annotations

import asyncio
from pathlib import Path

from sahara_runtime.tools.workspace import resolve_path

MAX_OUTPUT = 8000

INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "command": {
            "type": "string",
            "description": "The shell command to execute",
        },
        "timeout": {
            "type": "integer",
            "description": "Timeout in seconds (default: 30)",
            "default": 30,
        },
        "working_dir": {
            "type": "string",
            "description": "Working directory for the command (default: workspace root). "
            "Relative paths resolve from workspace.",
        },
    },
    "required": ["command"],
}


async def exec_command(
    command: str,
    timeout: int = 30,
    working_dir: str | None = None,
    *,
    workspace: Path,
) -> str:
    """执行 shell 命令，cwd 默认为 workspace 目录。"""
    if working_dir:
        cwd = str(resolve_path(working_dir, workspace))
    else:
        cwd = str(workspace)

    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return f"Command timed out after {timeout}s"

    output = ""
    if stdout:
        output += stdout.decode(errors="replace")
    if stderr:
        output += stderr.decode(errors="replace")

    if len(output) > MAX_OUTPUT:
        output = output[:MAX_OUTPUT] + "\n... [output truncated]"

    exit_info = f"\n[exit code: {proc.returncode}]"
    return output + exit_info
