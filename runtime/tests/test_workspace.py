"""Workspace 路径解析、初始化、内置工具集成测试。"""

from pathlib import Path

import pytest

from sahara_runtime.tools.builtins.fs_base import resolve_path
from sahara_runtime.tools.workspace import ensure_workspace


class TestResolvePath:
    def test_relative_path(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        result = resolve_path("hello.py", ws)
        assert result == (ws / "hello.py").resolve()

    def test_nested_relative_path(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        result = resolve_path("src/main.py", ws)
        assert result == (ws / "src" / "main.py").resolve()

    def test_absolute_path(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        result = resolve_path("/etc/hosts", ws)
        assert result == Path("/etc/hosts").resolve()

    def test_tilde_path(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        result = resolve_path("~/somefile", ws)
        assert result == Path("~/somefile").expanduser().resolve()

    def test_dot_path(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        result = resolve_path(".", ws)
        assert result == ws.resolve()

    def test_allowed_dir_blocks_outside(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        with pytest.raises(PermissionError):
            resolve_path("/etc/passwd", ws, allowed_dir=ws)

    def test_allowed_dir_permits_inside(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        result = resolve_path("file.txt", ws, allowed_dir=ws)
        assert result == (ws / "file.txt").resolve()


class TestEnsureWorkspace:
    def test_creates_directory(self, tmp_path):
        ws_dir = str(tmp_path / "new" / "workspace")
        result = ensure_workspace(ws_dir)
        assert result.is_dir()

    def test_existing_directory(self, tmp_path):
        result = ensure_workspace(str(tmp_path))
        assert result.is_dir()

    def test_tilde_expansion(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HOME", str(tmp_path))
        result = ensure_workspace("~/.sahara/workspace")
        assert result.is_dir()
        assert str(tmp_path) in str(result)


class TestBuiltinToolsWithWorkspace:
    @pytest.mark.asyncio
    async def test_read_relative_path(self, tmp_path):
        from sahara_runtime.tools.builtins.read_tool import ReadFileTool

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "test.txt").write_text("line1\nline2\n")

        tool = ReadFileTool(workspace=ws)
        result = await tool.execute(path="test.txt")
        assert "line1" in result
        assert "line2" in result

    @pytest.mark.asyncio
    async def test_read_nonexistent(self, tmp_path):
        from sahara_runtime.tools.builtins.read_tool import ReadFileTool

        ws = tmp_path / "workspace"
        ws.mkdir()

        tool = ReadFileTool(workspace=ws)
        result = await tool.execute(path="nope.txt")
        assert "Error" in result
        assert "not found" in result.lower()

    @pytest.mark.asyncio
    async def test_write_creates_file(self, tmp_path):
        from sahara_runtime.tools.builtins.write_tool import WriteFileTool

        ws = tmp_path / "workspace"
        ws.mkdir()

        tool = WriteFileTool(workspace=ws)
        result = await tool.execute(path="output.txt", content="hello world")
        assert "Successfully wrote" in result
        assert (ws / "output.txt").read_text() == "hello world"

    @pytest.mark.asyncio
    async def test_write_nested_path(self, tmp_path):
        from sahara_runtime.tools.builtins.write_tool import WriteFileTool

        ws = tmp_path / "workspace"
        ws.mkdir()

        tool = WriteFileTool(workspace=ws)
        result = await tool.execute(path="sub/dir/file.txt", content="data")
        assert "Successfully wrote" in result
        assert (ws / "sub" / "dir" / "file.txt").read_text() == "data"

    @pytest.mark.asyncio
    async def test_edit_file(self, tmp_path):
        from sahara_runtime.tools.builtins.edit_tool import EditFileTool

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "code.py").write_text("x = 1\ny = 2\n")

        tool = EditFileTool(workspace=ws)
        result = await tool.execute(path="code.py", old_text="x = 1", new_text="x = 42")
        assert "Successfully edited" in result
        assert "x = 42" in (ws / "code.py").read_text()

    @pytest.mark.asyncio
    async def test_edit_not_found(self, tmp_path):
        from sahara_runtime.tools.builtins.edit_tool import EditFileTool

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "code.py").write_text("x = 1\n")

        tool = EditFileTool(workspace=ws)
        result = await tool.execute(path="code.py", old_text="not here", new_text="new")
        assert "Error" in result
        assert "not found" in result.lower()

    @pytest.mark.asyncio
    async def test_list_dir(self, tmp_path):
        from sahara_runtime.tools.builtins.list_dir_tool import ListDirTool

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "a.txt").touch()
        (ws / "b.py").touch()
        (ws / "sub").mkdir()

        tool = ListDirTool(workspace=ws)
        result = await tool.execute(path=".")
        assert "a.txt" in result
        assert "b.py" in result
        assert "sub" in result

    @pytest.mark.asyncio
    async def test_list_dir_recursive(self, tmp_path):
        from sahara_runtime.tools.builtins.list_dir_tool import ListDirTool

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "sub").mkdir()
        (ws / "sub" / "deep.txt").touch()

        tool = ListDirTool(workspace=ws)
        result = await tool.execute(path=".", recursive=True)
        assert "deep.txt" in result

    @pytest.mark.asyncio
    async def test_exec_in_workspace(self, tmp_path):
        from sahara_runtime.tools.builtins.exec_tool import ExecTool

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "marker.txt").write_text("found")

        tool = ExecTool(workspace=ws)
        result = await tool.execute(command="cat marker.txt")
        assert "found" in result

    @pytest.mark.asyncio
    async def test_exec_pwd_is_workspace(self, tmp_path):
        from sahara_runtime.tools.builtins.exec_tool import ExecTool

        ws = tmp_path / "workspace"
        ws.mkdir()

        tool = ExecTool(workspace=ws)
        result = await tool.execute(command="pwd")
        assert str(ws) in result

    @pytest.mark.asyncio
    async def test_exec_blocks_dangerous(self, tmp_path):
        from sahara_runtime.tools.builtins.exec_tool import ExecTool

        ws = tmp_path / "workspace"
        ws.mkdir()

        tool = ExecTool(workspace=ws)
        result = await tool.execute(command="rm -rf /")
        assert "blocked" in result.lower()


class TestPromptBuilderWorkspace:
    def test_workspace_in_prompt(self, tmp_path):
        from sahara_runtime.context.context_manager import ContextManager

        ws = tmp_path / "workspace"
        ws.mkdir()
        cm = ContextManager(workspace=ws)
        prompt = cm.build_system_prompt(
            session_key="test", model="mock", max_iterations=5,
        )
        assert str(ws) in prompt

    def test_no_workspace_no_section(self):
        from sahara_runtime.context.context_manager import ContextManager

        cm = ContextManager()
        prompt = cm.build_system_prompt(
            session_key="test", model="mock", max_iterations=5,
        )
        assert "workspace directory is" not in prompt.lower()
