"""Workspace 路径解析与初始化测试。"""

from pathlib import Path

import pytest

from sahara_runtime.tools.workspace import ensure_workspace, resolve_path


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


class TestEnsureWorkspace:
    def test_creates_directory(self, tmp_path):
        ws_dir = str(tmp_path / "new" / "workspace")
        result = ensure_workspace(ws_dir)
        assert result.is_dir()
        assert result == Path(ws_dir).resolve()

    def test_existing_directory(self, tmp_path):
        ws_dir = str(tmp_path)
        result = ensure_workspace(ws_dir)
        assert result.is_dir()

    def test_tilde_expansion(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HOME", str(tmp_path))
        result = ensure_workspace("~/.sahara/workspace")
        assert result.is_dir()
        assert str(tmp_path) in str(result)


class TestBuiltinToolsWithWorkspace:
    """测试内置工具与 workspace 的集成。"""

    @pytest.mark.asyncio
    async def test_read_relative_path(self, tmp_path):
        from sahara_runtime.tools.builtins.read_tool import read_file

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "test.txt").write_text("line1\nline2\n")

        result = await read_file("test.txt", workspace=ws)
        assert "line1" in result
        assert "line2" in result

    @pytest.mark.asyncio
    async def test_read_nonexistent(self, tmp_path):
        from sahara_runtime.tools.builtins.read_tool import read_file

        ws = tmp_path / "workspace"
        ws.mkdir()

        result = await read_file("nope.txt", workspace=ws)
        assert "Error: file not found" in result

    @pytest.mark.asyncio
    async def test_read_directory(self, tmp_path):
        from sahara_runtime.tools.builtins.read_tool import read_file

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "a.txt").touch()
        (ws / "b.txt").touch()

        result = await read_file(".", workspace=ws)
        assert "Directory listing" in result
        assert "a.txt" in result

    @pytest.mark.asyncio
    async def test_write_creates_file(self, tmp_path):
        from sahara_runtime.tools.builtins.write_tool import write_file

        ws = tmp_path / "workspace"
        ws.mkdir()

        result = await write_file("output.txt", "hello world", workspace=ws)
        assert "Successfully written" in result
        assert (ws / "output.txt").read_text() == "hello world"

    @pytest.mark.asyncio
    async def test_write_nested_path(self, tmp_path):
        from sahara_runtime.tools.builtins.write_tool import write_file

        ws = tmp_path / "workspace"
        ws.mkdir()

        result = await write_file("sub/dir/file.txt", "data", workspace=ws)
        assert "Successfully written" in result
        assert (ws / "sub" / "dir" / "file.txt").read_text() == "data"

    @pytest.mark.asyncio
    async def test_exec_in_workspace(self, tmp_path):
        from sahara_runtime.tools.builtins.exec_tool import exec_command

        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "marker.txt").write_text("found")

        result = await exec_command("cat marker.txt", workspace=ws)
        assert "found" in result

    @pytest.mark.asyncio
    async def test_exec_pwd_is_workspace(self, tmp_path):
        from sahara_runtime.tools.builtins.exec_tool import exec_command

        ws = tmp_path / "workspace"
        ws.mkdir()

        result = await exec_command("pwd", workspace=ws)
        assert str(ws) in result


class TestPromptBuilderWorkspace:
    def test_workspace_in_prompt(self):
        from sahara_runtime.prompt.builder import PromptBuilder

        builder = PromptBuilder()
        prompt = builder.build(
            session_key="test",
            model="mock",
            max_iterations=5,
            workspace_path="/home/user/.sahara/workspace",
        )
        assert "/home/user/.sahara/workspace" in prompt
        assert "relative paths" in prompt.lower() or "Relative paths" in prompt

    def test_no_workspace_no_section(self):
        from sahara_runtime.prompt.builder import PromptBuilder

        builder = PromptBuilder()
        prompt = builder.build(
            session_key="test",
            model="mock",
            max_iterations=5,
        )
        assert "workspace" not in prompt.lower() or "Workspace" not in prompt
