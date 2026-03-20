"""HookRunner 单元测试。"""

import asyncio

import pytest

from sahara_runtime.hooks.builtins import register_builtin_hooks
from sahara_runtime.hooks.runner import HookContext, HookName, HookRegistration, HookRunner


@pytest.fixture
def runner():
    return HookRunner()


class TestHookRunner:
    def test_register_and_list(self, runner):
        async def dummy(ctx):
            return ctx

        runner.register(HookRegistration(name=HookName.ON_RUN_START, callback=dummy))
        hooks = runner.list_hooks(HookName.ON_RUN_START)
        assert len(hooks) == 1
        assert hooks[0]["hook"] == "on_run_start"

    def test_priority_ordering(self, runner):
        calls = []

        async def first(ctx):
            calls.append("first")
            return ctx

        async def second(ctx):
            calls.append("second")
            return ctx

        runner.register(HookRegistration(name=HookName.ON_RUN_START, callback=second, priority=200))
        runner.register(HookRegistration(name=HookName.ON_RUN_START, callback=first, priority=10))

        ctx = HookContext(hook_name=HookName.ON_RUN_START)
        asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert calls == ["first", "second"]

    def test_should_skip_stops_execution(self, runner):
        calls = []

        async def blocker(ctx):
            ctx.should_skip = True
            calls.append("blocker")
            return ctx

        async def after(ctx):
            calls.append("after")
            return ctx

        runner.register(HookRegistration(name=HookName.PRE_TOOL_EXEC, callback=blocker, priority=1))
        runner.register(HookRegistration(name=HookName.PRE_TOOL_EXEC, callback=after, priority=100))

        ctx = HookContext(hook_name=HookName.PRE_TOOL_EXEC)
        result = asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert result.should_skip
        assert calls == ["blocker"]

    def test_hook_error_isolation(self, runner):
        calls = []

        async def bad_hook(ctx):
            raise ValueError("boom")

        async def good_hook(ctx):
            calls.append("good")
            return ctx

        runner.register(HookRegistration(name=HookName.ON_RUN_COMPLETE, callback=bad_hook, priority=1))
        runner.register(HookRegistration(name=HookName.ON_RUN_COMPLETE, callback=good_hook, priority=100))

        ctx = HookContext(hook_name=HookName.ON_RUN_COMPLETE)
        asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert "good" in calls

    def test_unregister_by_source(self, runner):
        async def dummy(ctx):
            return ctx

        runner.register(HookRegistration(name=HookName.ON_RUN_START, callback=dummy, source="plugin_a"))
        runner.register(HookRegistration(name=HookName.ON_RUN_START, callback=dummy, source="builtin"))
        assert len(runner.list_hooks(HookName.ON_RUN_START)) == 2

        removed = runner.unregister(HookName.ON_RUN_START, "plugin_a")
        assert removed == 1
        assert len(runner.list_hooks(HookName.ON_RUN_START)) == 1

    def test_empty_hook_returns_ctx(self, runner):
        ctx = HookContext(hook_name=HookName.ON_CONTEXT_OVERFLOW, data={"key": "val"})
        result = asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert result.data == {"key": "val"}

    def test_data_passthrough(self, runner):
        async def mutator(ctx):
            ctx.data["added"] = True
            return ctx

        runner.register(HookRegistration(name=HookName.POST_LLM_CALL, callback=mutator))
        ctx = HookContext(hook_name=HookName.POST_LLM_CALL, data={"original": True})
        result = asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert result.data["original"]
        assert result.data["added"]


class TestBuiltinHooks:
    def test_builtins_register(self):
        runner = HookRunner()
        register_builtin_hooks(runner)
        all_hooks = runner.list_hooks()
        assert len(all_hooks) == 7

    def test_safety_blocks_dangerous_command(self):
        runner = HookRunner()
        register_builtin_hooks(runner)

        ctx = HookContext(
            hook_name=HookName.PRE_TOOL_EXEC,
            data={"tool_name": "exec", "command": "rm -rf / --no-preserve-root"},
        )
        result = asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert result.should_skip
        assert "blocked_reason" in result.data

    def test_safety_passes_safe_command(self):
        runner = HookRunner()
        register_builtin_hooks(runner)

        ctx = HookContext(
            hook_name=HookName.PRE_TOOL_EXEC,
            data={"tool_name": "exec", "command": "ls -la /tmp"},
        )
        result = asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert not result.should_skip

    def test_safety_ignores_non_exec_tools(self):
        runner = HookRunner()
        register_builtin_hooks(runner)

        ctx = HookContext(
            hook_name=HookName.PRE_TOOL_EXEC,
            data={"tool_name": "read", "path": "/etc/passwd"},
        )
        result = asyncio.get_event_loop().run_until_complete(runner.run(ctx))
        assert not result.should_skip
