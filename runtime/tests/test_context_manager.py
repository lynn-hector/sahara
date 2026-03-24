"""ContextManager + ContextBuilder + PromptBuilder + TokenCounter 单元测试。"""

import pytest

from sahara_runtime.context.context_builder import ContextBuilder
from sahara_runtime.context.context_manager import ContextManager
from sahara_runtime.context.token_counter import TokenCounter
from sahara_runtime.skills.manager import SkillsManager
from sahara_runtime.skills.types import InvocationPolicy, SkillEntry, SkillMetadata


@pytest.fixture
def counter():
    return TokenCounter()


@pytest.fixture
def builder(counter):
    return ContextBuilder(counter)


@pytest.fixture
def mgr(tmp_path):
    ws = tmp_path / "workspace"
    ws.mkdir()
    return ContextManager(workspace=ws)


class FakeSkillLoader:
    def __init__(self, entries):
        self._entries = entries

    async def load_all(self):
        return self._entries


class FakeSkillFilter:
    def __init__(self, result=None):
        self._result = result

    def filter(self, entries):
        return self._result if self._result is not None else entries


class FailingSkillLoader:
    async def load_all(self):
        raise RuntimeError("skill loader boom")


class FakeSandbox:
    pass


# ── TokenCounter ──────────────────────────────────────


class TestTokenCounter:
    def test_count_text(self, counter):
        assert counter.count_text("") == 1
        assert counter.count_text("hello world!") == 3

    def test_count_message_str(self, counter):
        msg = {"role": "user", "content": "hello"}
        tokens = counter.count_message(msg)
        assert tokens >= 5

    def test_count_message_tool_use(self, counter):
        msg = {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "I'll help."},
                {"type": "tool_use", "id": "t1", "name": "exec", "input": {"command": "ls"}},
            ],
        }
        tokens = counter.count_message(msg)
        assert tokens > 10

    def test_count_messages(self, counter):
        msgs = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"},
        ]
        total = counter.count_messages(msgs)
        assert total > 0
        assert total == counter.count_message(msgs[0]) + counter.count_message(msgs[1])

    def test_count_system(self, counter):
        tokens = counter.count_system("You are a helpful assistant.")
        assert tokens > 4


# ── ContextBuilder.fit_context ────────────────────────


class TestContextBuilderFitContext:
    def test_fit_within_budget(self, builder):
        msgs = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"},
        ]
        result = builder.fit_context(
            msgs, max_context_tokens=100_000, system_prompt="sys",
        )
        assert len(result) == 2

    def test_eviction_removes_oldest(self, builder):
        msgs = [{"role": "user", "content": f"message {i}" * 100} for i in range(20)]
        result = builder.fit_context(
            msgs, max_context_tokens=500, system_prompt="sys",
        )
        assert len(result) < len(msgs)
        assert len(result) >= 2

    def test_preserves_minimum_messages(self, builder):
        msgs = [
            {"role": "user", "content": "x" * 10000},
            {"role": "assistant", "content": "y" * 10000},
        ]
        result = builder.fit_context(
            msgs, max_context_tokens=200, system_prompt="sys",
        )
        assert len(result) >= 1


# ── ContextBuilder.build_messages ─────────────────────


class TestContextBuilderMessages:
    def test_build_messages(self):
        history = [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}]
        result = ContextBuilder.build_messages(history, "new question")
        assert len(result) == 3
        assert result[-1]["role"] == "user"
        assert result[-1]["content"] == "new question"

    def test_add_tool_result(self):
        msgs: list[dict] = [{"role": "user", "content": "hi"}]
        result = ContextBuilder.add_tool_result(msgs, "tc1", "exec", "output")
        assert len(result) == 2
        assert result[-1]["role"] == "tool"
        assert result[-1]["tool_call_id"] == "tc1"
        assert result[-1]["name"] == "exec"
        assert result[-1]["content"] == "output"


# ── SkillsManager ─────────────────────────────────────


class TestSkillsManager:
    @pytest.mark.asyncio
    async def test_build_prompt_uses_real_file_path(self, tmp_path):
        skill_dir = tmp_path / "skills" / "demo"
        skill_dir.mkdir(parents=True)
        skill_file = skill_dir / "SKILL.md"
        skill_file.write_text("# demo")

        entry = SkillEntry(
            name="demo",
            description="Demo skill",
            base_dir=str(skill_dir),
            file_path=str(skill_file),
            source="configured",
            metadata=SkillMetadata(),
            invocation=InvocationPolicy(),
        )
        manager = SkillsManager(
            loader=FakeSkillLoader([entry]),
            skill_filter=FakeSkillFilter(),
        )

        prompt = await manager.build_prompt()
        assert "`read_file`" in prompt
        assert str(skill_file.resolve()) in prompt
        assert 'location="/workspace/skills/demo/SKILL.md"' not in prompt

    @pytest.mark.asyncio
    async def test_summarize_active(self, tmp_path):
        skill_dir = tmp_path / "skills" / "demo"
        skill_dir.mkdir(parents=True)
        skill_file = skill_dir / "SKILL.md"
        skill_file.write_text("# demo")

        entry = SkillEntry(
            name="demo",
            description="Demo skill",
            base_dir=str(skill_dir),
            file_path=str(skill_file),
            source="configured",
            metadata=SkillMetadata(tags=["a", "b"]),
            invocation=InvocationPolicy(),
        )
        manager = SkillsManager(
            loader=FakeSkillLoader([entry]),
            skill_filter=FakeSkillFilter(),
        )

        summary = await manager.summarize_active()
        assert summary == [{
            "name": "demo",
            "description": "Demo skill",
            "location": str(skill_file.resolve()),
            "tags": ["a", "b"],
        }]

    @pytest.mark.asyncio
    async def test_sync_to_sandbox(self, tmp_path, monkeypatch):
        skill_dir = tmp_path / "skills" / "demo"
        skill_dir.mkdir(parents=True)
        skill_file = skill_dir / "SKILL.md"
        skill_file.write_text("# demo")

        entry = SkillEntry(
            name="demo",
            description="Demo skill",
            base_dir=str(skill_dir),
            file_path=str(skill_file),
            source="configured",
            metadata=SkillMetadata(),
            invocation=InvocationPolicy(),
        )
        manager = SkillsManager(
            loader=FakeSkillLoader([entry]),
            skill_filter=FakeSkillFilter(),
        )
        sandbox = FakeSandbox()
        seen = {}

        async def fake_sync(entries, sandbox_obj):
            seen["entries"] = entries
            seen["sandbox"] = sandbox_obj
            return 1

        monkeypatch.setattr("sahara_runtime.skills.manager.sync_skills_to_sandbox", fake_sync)

        synced = await manager.sync_to_sandbox(sandbox)
        assert synced == 1
        assert seen["entries"] == [entry]
        assert seen["sandbox"] is sandbox


# ── ContextManager (外观层) ───────────────────────────


class TestContextManagerFacade:
    def test_internal_builders_are_not_public(self, mgr):
        assert not hasattr(mgr, "prompt_builder")
        assert not hasattr(mgr, "context_builder")
        assert not hasattr(mgr, "build_skills_prompt")

    def test_build_system_prompt(self, mgr):
        prompt = mgr.build_system_prompt(
            session_key="test",
            model="mock-model",
            max_iterations=5,
        )
        assert "Sahara" in prompt
        assert "mock-model" in prompt
        assert "test" in prompt

    def test_build_system_prompt_with_tools(self, mgr):
        tools = [{
            "type": "function",
            "function": {
                "name": "exec",
                "description": "d",
                "parameters": {},
            },
        }]
        prompt = mgr.build_system_prompt(
            session_key="s", model="m", max_iterations=5, tools=tools,
        )
        assert "exec" in prompt

    def test_build_messages_via_facade(self, mgr):
        result = mgr.build_messages([], "hello")
        assert len(result) == 1
        assert result[0]["content"] == "hello"

    def test_fit_context_via_facade(self, mgr):
        msgs = [{"role": "user", "content": "hi"}]
        result = mgr.fit_context(
            msgs, max_context_tokens=100_000, system_prompt="sys",
        )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_build_run_context_includes_skills_prompt(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        skill_dir = ws / "skills" / "demo"
        skill_dir.mkdir(parents=True)
        skill_file = skill_dir / "SKILL.md"
        skill_file.write_text("# demo")

        entry = SkillEntry(
            name="demo",
            description="Demo skill",
            base_dir=str(skill_dir),
            file_path=str(skill_file),
            source="configured",
            metadata=SkillMetadata(),
            invocation=InvocationPolicy(),
        )
        cm = ContextManager(
            workspace=ws,
            skills_manager=SkillsManager(
                loader=FakeSkillLoader([entry]),
                skill_filter=FakeSkillFilter(),
            ),
        )

        system_prompt, messages = await cm.build_run_context(
            session_key="s1",
            model="mock-model",
            max_iterations=5,
            history=[],
            user_message="new",
            tools=[],
        )
        assert "`read_file`" in system_prompt
        assert str(skill_file.resolve()) in system_prompt
        assert 'location="/workspace/skills/demo/SKILL.md"' not in system_prompt
        assert messages[-1]["content"] == "new"

    @pytest.mark.asyncio
    async def test_build_run_context_propagates_skill_errors(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        cm = ContextManager(
            workspace=ws,
            skills_manager=SkillsManager(loader=FailingSkillLoader()),
        )

        with pytest.raises(RuntimeError, match="skill loader boom"):
            await cm.build_run_context(
                session_key="s1",
                model="mock-model",
                max_iterations=5,
                history=[],
                user_message="new",
                tools=[],
            )

    @pytest.mark.asyncio
    async def test_build_run_context(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        cm = ContextManager(workspace=ws)

        system_prompt, messages = await cm.build_run_context(
            session_key="s1",
            model="mock-model",
            max_iterations=5,
            history=[{"role": "assistant", "content": "old"}],
            user_message="new",
            tools=[{
                "type": "function",
                "function": {"name": "read_file", "description": "Read file.", "parameters": {}},
            }],
        )
        assert "mock-model" in system_prompt
        assert "read_file" in system_prompt
        assert len(messages) == 2
        assert messages[-1]["content"] == "new"

    def test_get_context_stats(self, mgr):
        msgs = [{"role": "user", "content": "hi"}]
        stats = mgr.get_context_stats(
            messages=msgs,
            max_context_tokens=100_000,
            system_prompt="sys",
        )
        assert stats["input_tokens"] > 0
        assert stats["original_count"] == 1
        assert stats["needs_trim"] is False

    def test_fit_context_with_stats(self, mgr):
        msgs = [{"role": "user", "content": "x" * 5000} for _ in range(10)]
        fit = mgr.fit_context_with_stats(
            messages=msgs,
            max_context_tokens=500,
            system_prompt="sys",
        )
        assert fit["trimmed"] is True
        assert fit["original_count"] == 10
        assert fit["remaining_count"] <= 10
        assert len(fit["messages"]) == fit["remaining_count"]
        assert fit["pre_trim_total_tokens"] > fit["post_trim_total_tokens"]


# ── PromptBuilder 增强内容 ────────────────────────────


class TestPromptBuilderEnhanced:
    def test_has_tool_guidelines(self, mgr):
        prompt = mgr.build_system_prompt(
            session_key="s", model="m", max_iterations=5,
        )
        assert "before modifying" in prompt.lower() or "Before modifying" in prompt

    def test_has_time(self, mgr):
        prompt = mgr.build_system_prompt(
            session_key="s", model="m", max_iterations=5,
        )
        assert "Current time:" in prompt or "current time:" in prompt.lower()

    def test_has_workspace_path(self, mgr):
        prompt = mgr.build_system_prompt(
            session_key="s", model="m", max_iterations=5,
        )
        assert "workspace" in prompt.lower()

    def test_has_safety(self, mgr):
        prompt = mgr.build_system_prompt(
            session_key="s", model="m", max_iterations=5,
        )
        assert "untrusted" in prompt.lower()

    def test_bootstrap_files_loaded(self, tmp_path):
        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "AGENTS.md").write_text("Custom agent rules here")
        cm = ContextManager(workspace=ws)
        prompt = cm.build_system_prompt(
            session_key="s", model="m", max_iterations=5,
        )
        assert "Custom agent rules here" in prompt

    def test_no_workspace_no_crash(self):
        cm = ContextManager()
        prompt = cm.build_system_prompt(
            session_key="s", model="m", max_iterations=5,
        )
        assert "Sahara" in prompt
