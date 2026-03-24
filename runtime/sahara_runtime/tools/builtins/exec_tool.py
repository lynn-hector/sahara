"""exec 工具 — Shell 命令执行，内置安全防护。

参考 nanobot/agent/tools/shell.py ExecTool:
- deny_patterns 危险命令拦截
- restrict_to_workspace 路径穿越检测
- 超时保护 + 输出截断（首尾保留）
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
from pathlib import Path
from typing import Any

from sahara_runtime.tools.base import Tool
from sahara_runtime.tools.builtins.fs_base import resolve_path

_MAX_OUTPUT = 10_000
_MAX_TIMEOUT = 600
_DEFAULT_TIMEOUT = 60

_DENY_PATTERNS = [
    re.compile(r"\brm\s+-[rf]{1,2}\b"),
    re.compile(r"\bdel\s+/[fq]\b"),
    re.compile(r"\brmdir\s+/s\b"),
    re.compile(r"(?:^|[;&|]\s*)format\b"),
    re.compile(r"\b(?:mkfs|diskpart)\b"),
    re.compile(r"\bdd\s+if="),
    re.compile(r">\s*/dev/sd"),
    re.compile(r"\b(?:shutdown|reboot|poweroff)\b"),
    re.compile(r":\(\)\s*\{.*\};\s*:"),
]


class ExecTool(Tool):
    """在 workspace 目录下执行 shell 命令。"""

    def __init__(
        self,
        workspace: Path | None = None,
        deny_patterns: list[re.Pattern[str]] | None = None,
        restrict_to_workspace: bool = False,
    ) -> None:
        self._workspace = workspace
        self._deny_patterns = deny_patterns if deny_patterns is not None else _DENY_PATTERNS
        self._restrict_to_workspace = restrict_to_workspace

    @property
    def name(self) -> str:
        return "exec"

    @property
    def description(self) -> str:
        return (
            "Execute a shell command and return its output. "
            "Commands run in the workspace directory by default."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to execute",
                },
                "working_dir": {
                    "type": "string",
                    "description": "Working directory (default: workspace root). "
                    "Relative paths resolve from workspace.",
                },
                "timeout": {
                    "type": "integer",
                    "description": (
                        f"Timeout in seconds (default {_DEFAULT_TIMEOUT}, "
                        f"max {_MAX_TIMEOUT})"
                    ),
                    "minimum": 1,
                    "maximum": _MAX_TIMEOUT,
                },
            },
            "required": ["command"],
        }

    async def execute(
        self,
        command: str,
        working_dir: str | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> str:
        if working_dir and self._workspace:
            cwd = str(resolve_path(working_dir, self._workspace))
        else:
            cwd = str(self._workspace) if self._workspace else os.getcwd()

        guard_error = self._guard_command(command, cwd)
        if guard_error:
            return guard_error

        effective_timeout = min(timeout or _DEFAULT_TIMEOUT, _MAX_TIMEOUT)

        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=effective_timeout,
                )
            except TimeoutError:
                proc.kill()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                return f"Error: Command timed out after {effective_timeout}s"

            parts: list[str] = []
            if stdout:
                parts.append(stdout.decode(errors="replace"))
            if stderr:
                stderr_text = stderr.decode(errors="replace").strip()
                if stderr_text:
                    parts.append(f"STDERR:\n{stderr_text}")
            parts.append(f"\nExit code: {proc.returncode}")

            result = "\n".join(parts) if parts else "(no output)"

            if len(result) > _MAX_OUTPUT:
                half = _MAX_OUTPUT // 2
                result = (
                    result[:half]
                    + f"\n\n... ({len(result) - _MAX_OUTPUT:,} chars truncated) ...\n\n"
                    + result[-half:]
                )
            return result

        except Exception as e:
            return f"Error executing command: {e}"

    def _guard_command(self, command: str, cwd: str) -> str | None:
        lower = command.strip().lower()
        for pattern in self._deny_patterns:
            if pattern.search(lower):
                return "Error: Command blocked by safety guard (dangerous pattern detected)"

        if self._restrict_to_workspace and self._workspace:
            if "..\\" in command or "../" in command:
                return "Error: Command blocked by safety guard (path traversal detected)"
            cwd_path = Path(cwd).resolve()
            for raw in self._extract_absolute_paths(command):
                try:
                    p = Path(os.path.expandvars(raw.strip())).expanduser().resolve()
                except Exception:
                    continue
                if p.is_absolute() and cwd_path not in p.parents and p != cwd_path:
                    return "Error: Command blocked by safety guard (path outside working dir)"
        return None

    @staticmethod
    def _extract_absolute_paths(command: str) -> list[str]:
        posix = re.findall(r'(?:^|[\s|>\'"])(/[^\s"\'>;|<]+)', command)
        home = re.findall(r'(?:^|[\s|>\'"])(~[^\s"\'>;|<]*)', command)
        return posix + home
