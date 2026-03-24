"""新增工具 (grep, web_search, web_fetch) 单元测试。"""

import pytest


class TestGrepTool:
    @pytest.fixture
    def tool(self, tmp_path):
        from sahara_runtime.tools.builtins.grep_tool import GrepTool

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "hello.py").write_text("def hello():\n    print('hello world')\n")
        (ws / "main.py").write_text("import hello\nhello.hello()\n")
        (ws / "data.txt").write_text("line1\nline2\nline3 hello\n")
        sub = ws / "sub"
        sub.mkdir()
        (sub / "deep.py").write_text("# deep module\nx = 42\n")
        return GrepTool(workspace=ws)

    @pytest.mark.asyncio
    async def test_basic_search(self, tool):
        result = await tool.execute(pattern="hello")
        assert "hello.py" in result
        assert "data.txt" in result

    @pytest.mark.asyncio
    async def test_regex_search(self, tool):
        result = await tool.execute(pattern=r"def \w+\(")
        assert "hello.py:1" in result

    @pytest.mark.asyncio
    async def test_search_single_file(self, tool):
        result = await tool.execute(pattern="hello", path="hello.py")
        assert "hello" in result
        assert "main.py" not in result

    @pytest.mark.asyncio
    async def test_include_filter(self, tool):
        result = await tool.execute(pattern="hello", include="*.txt")
        assert "data.txt" in result
        assert "hello.py" not in result

    @pytest.mark.asyncio
    async def test_ignore_case(self, tool):
        result = await tool.execute(pattern="HELLO", ignore_case=True)
        assert "hello.py" in result

    @pytest.mark.asyncio
    async def test_no_matches(self, tool):
        result = await tool.execute(pattern="zzz_nonexistent")
        assert "No matches" in result

    @pytest.mark.asyncio
    async def test_invalid_regex(self, tool):
        result = await tool.execute(pattern="[invalid")
        assert "Error" in result

    @pytest.mark.asyncio
    async def test_recursive(self, tool):
        result = await tool.execute(pattern="42")
        assert "deep.py" in result

    @pytest.mark.asyncio
    async def test_nonexistent_path(self, tool):
        result = await tool.execute(pattern="x", path="nope/")
        assert "Error" in result


class TestWebSearchTool:
    def test_schema(self):
        from sahara_runtime.tools.builtins.web_search_tool import WebSearchTool

        tool = WebSearchTool()
        assert tool.name == "web_search"
        schema = tool.to_schema()
        assert schema["type"] == "function"
        assert "query" in schema["function"]["parameters"]["properties"]

    def test_cast_params(self):
        from sahara_runtime.tools.builtins.web_search_tool import WebSearchTool

        tool = WebSearchTool()
        params = tool.cast_params({"query": "test", "count": "3"})
        assert params["count"] == 3

    def test_validate_missing_query(self):
        from sahara_runtime.tools.builtins.web_search_tool import WebSearchTool

        tool = WebSearchTool()
        errors = tool.validate_params({})
        assert any("query" in e for e in errors)


class TestWebFetchTool:
    def test_schema(self):
        from sahara_runtime.tools.builtins.web_fetch_tool import WebFetchTool

        tool = WebFetchTool()
        assert tool.name == "web_fetch"
        params = tool.to_schema()["function"]["parameters"]
        assert "url" in params["properties"]

    @pytest.mark.asyncio
    async def test_blocks_internal_url(self):
        from sahara_runtime.tools.builtins.web_fetch_tool import WebFetchTool

        tool = WebFetchTool()
        result = await tool.execute(url="http://127.0.0.1/secret")
        assert "Error" in result
        assert "blocked" in result.lower() or "internal" in result.lower()

    @pytest.mark.asyncio
    async def test_blocks_invalid_scheme(self):
        from sahara_runtime.tools.builtins.web_fetch_tool import WebFetchTool

        tool = WebFetchTool()
        result = await tool.execute(url="ftp://example.com/file")
        assert "Error" in result

    def test_validate_missing_url(self):
        from sahara_runtime.tools.builtins.web_fetch_tool import WebFetchTool

        tool = WebFetchTool()
        errors = tool.validate_params({})
        assert any("url" in e for e in errors)


class TestToolRegistration:
    def test_all_builtins_registered(self, tmp_path):
        from sahara_runtime.tools.builtins import register_builtins
        from sahara_runtime.tools.registry import ToolRegistry

        ws = tmp_path / "workspace"
        ws.mkdir()
        reg = ToolRegistry()
        register_builtins(reg, workspace=ws)

        expected = {
            "read_file", "write_file", "edit_file", "list_dir",
            "grep", "exec", "web_search", "web_fetch",
        }
        assert set(reg.names()) == expected

    def test_all_have_valid_schema(self, tmp_path):
        from sahara_runtime.tools.builtins import register_builtins
        from sahara_runtime.tools.registry import ToolRegistry

        ws = tmp_path / "workspace"
        ws.mkdir()
        reg = ToolRegistry()
        register_builtins(reg, workspace=ws)

        definitions = reg.get_definitions()
        assert len(definitions) == 8
        for d in definitions:
            assert d["type"] == "function"
            assert "name" in d["function"]
            assert "description" in d["function"]
            assert "parameters" in d["function"]
