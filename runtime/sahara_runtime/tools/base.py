"""Tool 抽象基类 — 参考 nanobot/agent/tools/base.py 设计。

每个工具是一个自包含的类实例:
- name/description/parameters 作为 property 自描述
- execute() 实现具体逻辑
- cast_params() / validate_params() 内置类型转换和校验
- to_schema() 生成 OpenAI 格式 schema
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Tool(ABC):
    """Agent 工具抽象基类。"""

    _TYPE_MAP: dict[str, type | tuple[type, ...]] = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
        "object": dict,
    }

    @property
    @abstractmethod
    def name(self) -> str:
        """工具名称，用于 function call。"""

    @property
    @abstractmethod
    def description(self) -> str:
        """工具描述，告诉 LLM 什么时候该用。"""

    @property
    @abstractmethod
    def parameters(self) -> dict[str, Any]:
        """JSON Schema 格式的参数定义。"""

    @abstractmethod
    async def execute(self, **kwargs: Any) -> str:
        """执行工具，返回字符串结果。"""

    def to_schema(self) -> dict[str, Any]:
        """生成 OpenAI function calling 格式的 schema。"""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }

    # ── 参数类型转换 ─────────────────────────────────

    def cast_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """根据 schema 安全转换参数类型（LLM 经常传 "30" 而非 30）。"""
        schema = self.parameters or {}
        if schema.get("type", "object") != "object":
            return params
        return self._cast_object(params, schema)

    def _cast_object(self, obj: Any, schema: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(obj, dict):
            return obj
        props = schema.get("properties", {})
        result = {}
        for key, value in obj.items():
            if key in props:
                result[key] = self._cast_value(value, props[key])
            else:
                result[key] = value
        return result

    def _cast_value(self, val: Any, schema: dict[str, Any]) -> Any:
        target = schema.get("type")
        if target == "integer" and isinstance(val, str):
            try:
                return int(val)
            except ValueError:
                return val
        if target == "number" and isinstance(val, str):
            try:
                return float(val)
            except ValueError:
                return val
        if target == "boolean" and isinstance(val, str):
            low = val.lower()
            if low in ("true", "1", "yes"):
                return True
            if low in ("false", "0", "no"):
                return False
            return val
        if target == "string" and val is not None and not isinstance(val, str):
            return str(val)
        if target == "array" and isinstance(val, list):
            items_schema = schema.get("items")
            if items_schema:
                return [self._cast_value(item, items_schema) for item in val]
        if target == "object" and isinstance(val, dict):
            return self._cast_object(val, schema)
        return val

    # ── 参数校验 ─────────────────────────────────────

    def validate_params(self, params: dict[str, Any]) -> list[str]:
        """校验参数，返回错误列表（空 = 通过）。"""
        if not isinstance(params, dict):
            return [f"parameters must be an object, got {type(params).__name__}"]
        schema = self.parameters or {}
        return self._validate(params, {**schema, "type": "object"}, "")

    def _validate(self, val: Any, schema: dict[str, Any], path: str) -> list[str]:
        t = schema.get("type")
        label = path or "parameter"

        if t == "integer" and (not isinstance(val, int) or isinstance(val, bool)):
            return [f"{label} should be integer"]
        if t == "number" and not isinstance(val, (int, float)):
            return [f"{label} should be number"]
        if t in self._TYPE_MAP and t not in ("integer", "number"):
            expected = self._TYPE_MAP[t]
            if not isinstance(val, expected):
                return [f"{label} should be {t}"]

        errors: list[str] = []
        if "enum" in schema and val not in schema["enum"]:
            errors.append(f"{label} must be one of {schema['enum']}")
        if t in ("integer", "number") and isinstance(val, (int, float)):
            if "minimum" in schema and val < schema["minimum"]:
                errors.append(f"{label} must be >= {schema['minimum']}")
            if "maximum" in schema and val > schema["maximum"]:
                errors.append(f"{label} must be <= {schema['maximum']}")
        if t == "string" and isinstance(val, str):
            if "minLength" in schema and len(val) < schema["minLength"]:
                errors.append(f"{label} must be at least {schema['minLength']} chars")
            if "maxLength" in schema and len(val) > schema["maxLength"]:
                errors.append(f"{label} must be at most {schema['maxLength']} chars")
        if t == "object":
            for k in schema.get("required", []):
                if k not in val:
                    errors.append(f"missing required {path + '.' + k if path else k}")
            props = schema.get("properties", {})
            for k, v in val.items():
                if k in props:
                    errors.extend(self._validate(v, props[k], f"{path}.{k}" if path else k))
        if t == "array" and isinstance(val, list) and "items" in schema:
            for i, item in enumerate(val):
                errors.extend(self._validate(
                    item, schema["items"], f"{path}[{i}]" if path else f"[{i}]",
                ))
        return errors
