"""内置工具注册 — 参考 nanobot/agent/loop.py _register_default_tools。"""

from __future__ import annotations

from pathlib import Path

from sahara_runtime.tools.registry import ToolRegistry


def register_builtins(registry: ToolRegistry, workspace: Path) -> None:
    """注册所有内置工具，注入 workspace 路径。"""
    from sahara_runtime.tools.builtins.edit_tool import EditFileTool
    from sahara_runtime.tools.builtins.exec_tool import ExecTool
    from sahara_runtime.tools.builtins.grep_tool import GrepTool
    from sahara_runtime.tools.builtins.list_dir_tool import ListDirTool
    from sahara_runtime.tools.builtins.read_tool import ReadFileTool
    from sahara_runtime.tools.builtins.web_fetch_tool import WebFetchTool
    from sahara_runtime.tools.builtins.web_search_tool import WebSearchTool
    from sahara_runtime.tools.builtins.write_tool import WriteFileTool

    # 文件系统工具
    registry.register(ReadFileTool(workspace=workspace))
    registry.register(WriteFileTool(workspace=workspace))
    registry.register(EditFileTool(workspace=workspace))
    registry.register(ListDirTool(workspace=workspace))
    registry.register(GrepTool(workspace=workspace))

    # Shell
    registry.register(ExecTool(workspace=workspace))

    # Web
    registry.register(WebSearchTool())
    registry.register(WebFetchTool())
