"""ToolRegistry — 工具注册、Schema 生成、执行。

参考 nanobot/agent/tools/registry.py:
- register/unregister 动态管理工具
- execute() 统一 cast → validate → execute 流程
- get_definitions() 生成 OpenAI 格式 schema 列表
"""

from __future__ import annotations

from typing import Any

import structlog

from sahara_runtime.tools.base import Tool

logger = structlog.get_logger(__name__)

_ERROR_HINT = "\n\n[Analyze the error above and try a different approach.]"


class ToolRegistry:
    """工具注册中心 — 管理所有可用工具。"""

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool
        logger.info("tool_registered", name=tool.name)

    def unregister(self, name: str) -> bool:
        removed = self._tools.pop(name, None)
        if removed:
            logger.info("tool_unregistered", name=name)
        return removed is not None

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def has(self, name: str) -> bool:
        return name in self._tools

    def get_definitions(self) -> list[dict[str, Any]]:
        """返回 OpenAI 格式的 tool schema 列表。"""
        return [tool.to_schema() for tool in self._tools.values()]

    async def execute(self, name: str, params: dict[str, Any]) -> dict[str, Any]:
        """统一执行: cast → validate → execute。

        Returns:
            {"content": str} 成功 | {"content": str, "is_error": True} 失败
        """
        tool = self._tools.get(name)
        if not tool:
            available = ", ".join(self.names())
            return _error(f"Error: Tool '{name}' not found. Available: {available}")

        try:
            params = tool.cast_params(params)
            errors = tool.validate_params(params)
            if errors:
                return _error(
                    f"Error: Invalid parameters for '{name}': {'; '.join(errors)}"
                )
            result = await tool.execute(**params)
            if isinstance(result, str) and result.startswith("Error"):
                return _error(result)
            return {"content": result}
        except Exception as e:
            logger.exception("tool_execute_error", tool_name=name)
            return _error(f"Error executing {name}: {e}")

    def names(self) -> list[str]:
        return list(self._tools.keys())

    def __len__(self) -> int:
        return len(self._tools)

    def __contains__(self, name: str) -> bool:
        return name in self._tools


def _error(message: str) -> dict[str, Any]:
    return {"content": message + _ERROR_HINT, "is_error": True}
