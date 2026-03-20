"""ToolRegistry — 工具注册、Schema 生成、参数校验、执行器查找。

重构改进（参考 nanobot/agent/tools/registry.py + base.py）:
- 新增 validate_params(): 基于 JSON Schema 的参数校验
- 新增 cast_params(): LLM 经常传错类型（str→int），自动安全转换
- execute() 内置校验 + 错误提示
- 新增 unregister() 支持动态注销
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

ToolFunc = Callable[..., Coroutine[Any, Any, str]]

_ERROR_HINT = "\n\n[Analyze the error above and try a different approach.]"


@dataclass
class ToolDef:
    """工具定义。"""

    name: str
    description: str
    input_schema: dict[str, Any]
    func: ToolFunc
    tier: int = 0  # 0=core, 1=extended, 2=web, 3=plugin
    source: str = "builtin"
    sandboxed: bool = False


class ToolRegistry:
    """管理所有可用工具，支持参数校验和安全类型转换。"""

    def __init__(self) -> None:
        self._tools: dict[str, ToolDef] = {}

    def register(self, tool: ToolDef) -> None:
        self._tools[tool.name] = tool
        logger.info("tool_registered", name=tool.name, tier=tool.tier)

    def unregister(self, name: str) -> bool:
        """注销工具，返回是否成功。"""
        removed = self._tools.pop(name, None)
        if removed:
            logger.info("tool_unregistered", name=name)
        return removed is not None

    def get(self, name: str) -> ToolDef | None:
        return self._tools.get(name)

    def has(self, name: str) -> bool:
        return name in self._tools

    def list_schemas(self) -> list[dict[str, Any]]:
        """返回 OpenAI 格式的 tool schema 列表（通用交换格式）。"""
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                },
            }
            for t in sorted(self._tools.values(), key=lambda x: (x.tier, x.name))
        ]

    def names(self) -> list[str]:
        return list(self._tools.keys())

    def __len__(self) -> int:
        return len(self._tools)

    def __contains__(self, name: str) -> bool:
        return name in self._tools

    @staticmethod
    def validate_params(params: dict[str, Any], schema: dict[str, Any]) -> list[str]:
        """基于 JSON Schema 校验参数，返回错误列表（空 = 通过）。

        仅校验 required 字段和基本类型，不做完整的 JSON Schema 校验。
        """
        errors: list[str] = []
        if not isinstance(params, dict):
            return [f"parameters must be an object, got {type(params).__name__}"]

        properties = schema.get("properties", {})
        required = schema.get("required", [])

        for field_name in required:
            if field_name not in params:
                errors.append(f"missing required parameter: {field_name}")

        for key, value in params.items():
            if key not in properties:
                continue
            prop_schema = properties[key]
            expected_type = prop_schema.get("type")
            if expected_type and not _check_type(value, expected_type):
                errors.append(
                    f"parameter '{key}' should be {expected_type}, "
                    f"got {type(value).__name__}"
                )

        return errors

    @staticmethod
    def cast_params(params: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
        """尝试安全地将参数转换为 schema 期望的类型。

        LLM 经常传 "30" 而非 30，此处自动转换以避免报错。
        """
        properties = schema.get("properties", {})
        result = dict(params)

        for key, value in result.items():
            if key not in properties:
                continue
            expected_type = properties[key].get("type")
            if expected_type is None:
                continue

            if expected_type == "integer" and isinstance(value, str):
                try:
                    result[key] = int(value)
                except ValueError:
                    pass
            elif expected_type == "number" and isinstance(value, str):
                try:
                    result[key] = float(value)
                except ValueError:
                    pass
            elif expected_type == "boolean" and isinstance(value, str):
                lower = value.lower()
                if lower in ("true", "1", "yes"):
                    result[key] = True
                elif lower in ("false", "0", "no"):
                    result[key] = False
            elif expected_type == "string" and not isinstance(value, str):
                if value is not None:
                    result[key] = str(value)

        return result


def _check_type(value: Any, expected: str) -> bool:
    """检查值是否匹配 JSON Schema 类型。"""
    type_map: dict[str, type | tuple[type, ...]] = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
        "object": dict,
    }
    expected_type = type_map.get(expected)
    if expected_type is None:
        return True
    if expected == "integer" and isinstance(value, bool):
        return False
    return isinstance(value, expected_type)
