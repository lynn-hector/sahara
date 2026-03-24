"""Tool.validate_params / cast_params 单元测试。"""

from typing import Any

from sahara_runtime.tools.base import Tool
from sahara_runtime.tools.registry import ToolRegistry

SCHEMA = {
    "type": "object",
    "properties": {
        "command": {"type": "string"},
        "timeout": {"type": "integer"},
        "verbose": {"type": "boolean"},
        "ratio": {"type": "number"},
        "tags": {"type": "array"},
    },
    "required": ["command"],
}


class _SchemaTool(Tool):
    """用于测试的工具 — parameters 可自定义。"""

    def __init__(self, schema: dict[str, Any] | None = None):
        self._schema = SCHEMA if schema is None else schema

    @property
    def name(self) -> str:
        return "test"

    @property
    def description(self) -> str:
        return "test tool"

    @property
    def parameters(self) -> dict[str, Any]:
        return self._schema

    async def execute(self, **kw: Any) -> str:
        return "ok"


def _tool(schema: dict[str, Any] | None = None) -> _SchemaTool:
    return _SchemaTool(schema)


class TestValidateParams:
    def test_valid_params(self):
        errors = _tool().validate_params({"command": "ls", "timeout": 30})
        assert errors == []

    def test_missing_required(self):
        errors = _tool().validate_params({"timeout": 30})
        assert len(errors) == 1
        assert "missing required" in errors[0].lower()
        assert "command" in errors[0]

    def test_wrong_type_integer(self):
        errors = _tool().validate_params({"command": "ls", "timeout": "not_a_number"})
        assert any("timeout" in e and "integer" in e for e in errors)

    def test_wrong_type_boolean(self):
        errors = _tool().validate_params({"command": "ls", "verbose": "yes"})
        assert any("verbose" in e and "boolean" in e for e in errors)

    def test_wrong_type_string(self):
        errors = _tool().validate_params({"command": 123})
        assert any("command" in e and "string" in e for e in errors)

    def test_extra_params_ignored(self):
        errors = _tool().validate_params({"command": "ls", "unknown_param": 42})
        assert errors == []

    def test_non_dict_input(self):
        errors = _tool().validate_params("not a dict")
        assert len(errors) == 1
        assert "must be an object" in errors[0]

    def test_bool_not_integer(self):
        errors = _tool().validate_params({"command": "ls", "timeout": True})
        assert any("timeout" in e for e in errors)

    def test_empty_schema(self):
        errors = _tool({}).validate_params({"a": 1})
        assert errors == []

    def test_number_accepts_int(self):
        errors = _tool().validate_params({"command": "ls", "ratio": 42})
        assert errors == []

    def test_number_accepts_float(self):
        errors = _tool().validate_params({"command": "ls", "ratio": 3.14})
        assert errors == []

    def test_array_type(self):
        errors = _tool().validate_params({"command": "ls", "tags": ["a", "b"]})
        assert errors == []

    def test_array_wrong_type(self):
        errors = _tool().validate_params({"command": "ls", "tags": "not_an_array"})
        assert any("tags" in e for e in errors)

    def test_enum_validation(self):
        schema = {
            "type": "object",
            "properties": {"mode": {"type": "string", "enum": ["fast", "slow"]}},
            "required": ["mode"],
        }
        errors = _tool(schema).validate_params({"mode": "invalid"})
        assert any("must be one of" in e for e in errors)

    def test_minimum_maximum(self):
        schema = {
            "type": "object",
            "properties": {"count": {"type": "integer", "minimum": 1, "maximum": 10}},
            "required": ["count"],
        }
        assert _tool(schema).validate_params({"count": 0}) != []
        assert _tool(schema).validate_params({"count": 11}) != []
        assert _tool(schema).validate_params({"count": 5}) == []


class TestCastParams:
    def test_string_to_integer(self):
        result = _tool().cast_params({"command": "ls", "timeout": "30"})
        assert result["timeout"] == 30

    def test_string_to_float(self):
        result = _tool().cast_params({"command": "ls", "ratio": "3.14"})
        assert result["ratio"] == 3.14

    def test_string_to_boolean_true(self):
        for val in ("true", "True", "1", "yes"):
            result = _tool().cast_params({"command": "ls", "verbose": val})
            assert result["verbose"] is True

    def test_string_to_boolean_false(self):
        for val in ("false", "False", "0", "no"):
            result = _tool().cast_params({"command": "ls", "verbose": val})
            assert result["verbose"] is False

    def test_int_to_string(self):
        result = _tool().cast_params({"command": 42})
        assert result["command"] == "42"

    def test_invalid_int_stays_string(self):
        result = _tool().cast_params({"command": "ls", "timeout": "abc"})
        assert result["timeout"] == "abc"

    def test_none_not_cast_to_string(self):
        result = _tool().cast_params({"command": None})
        assert result["command"] is None

    def test_unknown_params_untouched(self):
        result = _tool().cast_params({"command": "ls", "foo": 123})
        assert result["foo"] == 123

    def test_empty_schema(self):
        result = _tool({}).cast_params({"x": "1"})
        assert result == {"x": "1"}


class TestToolRegistryOperations:
    def test_unregister(self):
        reg = ToolRegistry()
        reg.register(_tool())
        assert reg.has("test")
        assert reg.unregister("test") is True
        assert not reg.has("test")
        assert reg.unregister("test") is False

    def test_contains_and_len(self):
        reg = ToolRegistry()
        assert len(reg) == 0
        reg.register(_tool())
        assert len(reg) == 1
        assert "test" in reg
        assert "nope" not in reg
