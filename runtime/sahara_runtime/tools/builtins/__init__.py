"""内置工具: exec, read, write"""

from sahara_runtime.tools.builtins.exec_tool import EXEC_TOOL
from sahara_runtime.tools.builtins.read_tool import READ_TOOL
from sahara_runtime.tools.builtins.write_tool import WRITE_TOOL
from sahara_runtime.tools.registry import ToolRegistry


def register_builtins(registry: ToolRegistry) -> None:
    """注册所有内置工具。"""
    registry.register(EXEC_TOOL)
    registry.register(READ_TOOL)
    registry.register(WRITE_TOOL)
