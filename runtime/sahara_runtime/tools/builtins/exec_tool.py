"""exec 工具 — 在沙箱中执行命令。

Phase 1: 使用 subprocess 本地执行 (Sprint 3 接入 Docker 沙箱)
"""

from __future__ import annotations

import asyncio

from sahara_runtime.tools.registry import ToolDef

MAX_OUTPUT = 8000


async def exec_command(command: str, timeout: int = 30) -> str:
    """执行 shell 命令, 返回 stdout+stderr。"""
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
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


EXEC_TOOL = ToolDef(
    name="exec",
    description="Execute a shell command and return the output. Use for running programs, scripts, and system commands.",
    input_schema={
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
        },
        "required": ["command"],
    },
    func=exec_command,
    tier=0,
    sandboxed=True,
)
