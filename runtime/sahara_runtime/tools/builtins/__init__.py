"""内置工具: exec, read, write — 注册时绑定 workspace 路径。"""

from __future__ import annotations

from functools import partial
from pathlib import Path

from sahara_runtime.tools.builtins.exec_tool import INPUT_SCHEMA as EXEC_SCHEMA
from sahara_runtime.tools.builtins.exec_tool import exec_command
from sahara_runtime.tools.builtins.read_tool import INPUT_SCHEMA as READ_SCHEMA
from sahara_runtime.tools.builtins.read_tool import read_file
from sahara_runtime.tools.builtins.write_tool import INPUT_SCHEMA as WRITE_SCHEMA
from sahara_runtime.tools.builtins.write_tool import write_file
from sahara_runtime.tools.registry import ToolDef, ToolRegistry


def register_builtins(registry: ToolRegistry, workspace: Path) -> None:
    """注册所有内置工具，通过 partial 绑定 workspace。"""
    registry.register(ToolDef(
        name="exec",
        description=(
            "Execute a shell command and return the output. "
            "Commands run in the workspace directory by default."
        ),
        input_schema=EXEC_SCHEMA,
        func=partial(exec_command, workspace=workspace),
        tier=0,
        sandboxed=True,
    ))
    registry.register(ToolDef(
        name="read",
        description=(
            "Read the contents of a file. Returns numbered lines. "
            "Relative paths resolve from the workspace directory."
        ),
        input_schema=READ_SCHEMA,
        func=partial(read_file, workspace=workspace),
        tier=0,
        sandboxed=True,
    ))
    registry.register(ToolDef(
        name="write",
        description=(
            "Write content to a file. Creates parent directories if needed. "
            "Relative paths resolve from the workspace directory."
        ),
        input_schema=WRITE_SCHEMA,
        func=partial(write_file, workspace=workspace),
        tier=0,
        sandboxed=True,
    ))
