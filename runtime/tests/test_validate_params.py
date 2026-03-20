"""ToolRegistry.validate_params / cast_params 单元测试。"""

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


class TestValidateParams:
    def test_valid_params(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "timeout": 30}, SCHEMA,
        )
        assert errors == []

    def test_missing_required(self):
        errors = ToolRegistry.validate_params({"timeout": 30}, SCHEMA)
        assert len(errors) == 1
        assert "missing required parameter: command" in errors[0]

    def test_wrong_type_integer(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "timeout": "not_a_number"}, SCHEMA,
        )
        assert any("timeout" in e and "integer" in e for e in errors)

    def test_wrong_type_boolean(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "verbose": "yes"}, SCHEMA,
        )
        assert any("verbose" in e and "boolean" in e for e in errors)

    def test_wrong_type_string(self):
        errors = ToolRegistry.validate_params(
            {"command": 123}, SCHEMA,
        )
        assert any("command" in e and "string" in e for e in errors)

    def test_extra_params_ignored(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "unknown_param": 42}, SCHEMA,
        )
        assert errors == []

    def test_non_dict_input(self):
        errors = ToolRegistry.validate_params("not a dict", SCHEMA)
        assert len(errors) == 1
        assert "must be an object" in errors[0]

    def test_bool_not_integer(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "timeout": True}, SCHEMA,
        )
        assert any("timeout" in e for e in errors)

    def test_empty_schema(self):
        errors = ToolRegistry.validate_params({"a": 1}, {})
        assert errors == []

    def test_number_accepts_int(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "ratio": 42}, SCHEMA,
        )
        assert errors == []

    def test_number_accepts_float(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "ratio": 3.14}, SCHEMA,
        )
        assert errors == []

    def test_array_type(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "tags": ["a", "b"]}, SCHEMA,
        )
        assert errors == []

    def test_array_wrong_type(self):
        errors = ToolRegistry.validate_params(
            {"command": "ls", "tags": "not_an_array"}, SCHEMA,
        )
        assert any("tags" in e for e in errors)


class TestCastParams:
    def test_string_to_integer(self):
        result = ToolRegistry.cast_params({"command": "ls", "timeout": "30"}, SCHEMA)
        assert result["timeout"] == 30
        assert isinstance(result["timeout"], int)

    def test_string_to_float(self):
        result = ToolRegistry.cast_params({"command": "ls", "ratio": "3.14"}, SCHEMA)
        assert result["ratio"] == 3.14

    def test_string_to_boolean_true(self):
        for val in ("true", "True", "1", "yes"):
            result = ToolRegistry.cast_params({"command": "ls", "verbose": val}, SCHEMA)
            assert result["verbose"] is True

    def test_string_to_boolean_false(self):
        for val in ("false", "False", "0", "no"):
            result = ToolRegistry.cast_params({"command": "ls", "verbose": val}, SCHEMA)
            assert result["verbose"] is False

    def test_int_to_string(self):
        result = ToolRegistry.cast_params({"command": 42}, SCHEMA)
        assert result["command"] == "42"

    def test_invalid_int_stays_string(self):
        result = ToolRegistry.cast_params({"command": "ls", "timeout": "abc"}, SCHEMA)
        assert result["timeout"] == "abc"

    def test_none_not_cast_to_string(self):
        result = ToolRegistry.cast_params({"command": None}, SCHEMA)
        assert result["command"] is None

    def test_unknown_params_untouched(self):
        result = ToolRegistry.cast_params({"command": "ls", "foo": 123}, SCHEMA)
        assert result["foo"] == 123

    def test_already_correct_type(self):
        result = ToolRegistry.cast_params({"command": "ls", "timeout": 30}, SCHEMA)
        assert result["timeout"] == 30

    def test_empty_schema(self):
        result = ToolRegistry.cast_params({"x": "1"}, {})
        assert result == {"x": "1"}


class TestToolRegistryOperations:
    def test_unregister(self):
        from sahara_runtime.tools.registry import ToolDef

        async def noop(**kwargs):
            return ""

        reg = ToolRegistry()
        reg.register(ToolDef(name="tmp", description="d", input_schema={}, func=noop))
        assert reg.has("tmp")
        assert reg.unregister("tmp") is True
        assert not reg.has("tmp")
        assert reg.unregister("tmp") is False

    def test_contains_and_len(self):
        from sahara_runtime.tools.registry import ToolDef

        async def noop(**kwargs):
            return ""

        reg = ToolRegistry()
        assert len(reg) == 0
        reg.register(ToolDef(name="a", description="d", input_schema={}, func=noop))
        assert len(reg) == 1
        assert "a" in reg
        assert "b" not in reg
