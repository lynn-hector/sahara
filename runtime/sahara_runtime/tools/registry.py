"""ToolRegistry — 工具注册、Schema 生成、执行器查找。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Coroutine

import structlog

logger = structlog.get_logger(__name__)

ToolFunc = Callable[..., Coroutine[Any, Any, str]]


@dataclass
class ToolDef:
    """工具定义。"""

    name: str
    description: str
    input_schema: dict[str, Any]
    func: ToolFunc
    tier: int = 0  # 0=core, 1=extended, 2=web, 3=plugin
    source: str = "builtin"  # plugin system 预留


class ToolRegistry:
    """管理所有可用工具。"""

    def __init__(self) -> None:
        self._tools: dict[str, ToolDef] = {}

    def register(self, tool: ToolDef) -> None:
        self._tools[tool.name] = tool
        logger.info("tool_registered", name=tool.name, tier=tool.tier)

    def get(self, name: str) -> ToolDef | None:
        return self._tools.get(name)

    def list_schemas(self) -> list[dict[str, Any]]:
        """返回 Anthropic tool schema 列表。"""
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
            }
            for t in sorted(self._tools.values(), key=lambda x: (x.tier, x.name))
        ]

    def names(self) -> list[str]:
        return list(self._tools.keys())
