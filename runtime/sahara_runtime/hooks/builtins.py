"""内置 Hooks — 4 个默认的生命周期钩子。

1. logging_hook: 记录所有 hook 事件到结构化日志
2. metrics_hook: 更新 Prometheus 指标
3. safety_hook: pre_tool_exec 安全检查 (危险命令拦截)
4. context_overflow_hook: 上下文溢出时的自动压缩通知
"""

from __future__ import annotations

import re

import structlog

from sahara_runtime.hooks.runner import (
    HookContext,
    HookName,
    HookRegistration,
    HookRunner,
)

logger = structlog.get_logger("hooks.builtins")

DANGEROUS_PATTERNS = [
    re.compile(r"\brm\s+-rf\s+/(?!\w)"),      # rm -rf /
    re.compile(r"\bdd\s+.*of=/dev/"),           # dd to device
    re.compile(r"\bmkfs\b"),                    # format filesystem
    re.compile(r":>\s*/etc/"),                  # truncate system files
    re.compile(r"\bshutdown\b"),                # shutdown
    re.compile(r"\breboot\b"),                  # reboot
]


async def _logging_hook(ctx: HookContext) -> HookContext:
    """记录 hook 事件到结构化日志。"""
    logger.info(
        "hook_event",
        hook=ctx.hook_name.value,
        session_key=ctx.session_key,
        task_id=ctx.task_id,
        data_keys=list(ctx.data.keys()),
    )
    return ctx


async def _metrics_hook(ctx: HookContext) -> HookContext:
    """更新 Prometheus 指标。"""
    try:
        from sahara_runtime.observability.metrics import (
            LLM_CALLS_TOTAL,
            TOOL_CALLS_TOTAL,
        )

        if ctx.hook_name == HookName.POST_LLM_CALL:
            provider = ctx.data.get("provider", "unknown")
            model = ctx.data.get("model", "unknown")
            LLM_CALLS_TOTAL.labels(provider=provider, model=model).inc()

        elif ctx.hook_name == HookName.POST_TOOL_EXEC:
            tool_name = ctx.data.get("tool_name", "unknown")
            success = str(ctx.data.get("success", True)).lower()
            TOOL_CALLS_TOTAL.labels(tool_name=tool_name, success=success).inc()

    except ImportError:
        pass

    return ctx


async def _safety_hook(ctx: HookContext) -> HookContext:
    """pre_tool_exec: 检测危险命令并阻止执行。"""
    if ctx.hook_name != HookName.PRE_TOOL_EXEC:
        return ctx

    tool_name = ctx.data.get("tool_name", "")
    if tool_name != "exec":
        return ctx

    command = ctx.data.get("command", "")
    for pattern in DANGEROUS_PATTERNS:
        if pattern.search(command):
            logger.warning(
                "dangerous_command_blocked",
                command=command,
                pattern=pattern.pattern,
                session_key=ctx.session_key,
            )
            ctx.should_skip = True
            ctx.data["blocked_reason"] = f"dangerous command pattern detected: {pattern.pattern}"
            return ctx

    return ctx


async def _context_overflow_hook(ctx: HookContext) -> HookContext:
    """上下文溢出时记录警告并提供压缩建议。"""
    original_count = ctx.data.get("original_messages", 0)
    remaining_count = ctx.data.get("remaining_messages", 0)
    tokens = ctx.data.get("total_tokens", 0)
    budget = ctx.data.get("budget_tokens", 0)

    logger.warning(
        "context_overflow_handled",
        original_messages=original_count,
        remaining_messages=remaining_count,
        tokens=tokens,
        budget=budget,
        session_key=ctx.session_key,
    )

    return ctx


def register_builtin_hooks(runner: HookRunner) -> None:
    """注册所有内置 hooks。"""
    runner.register(HookRegistration(
        name=HookName.ON_RUN_START,
        callback=_logging_hook,
        priority=10,
        source="builtin.logging",
    ))
    runner.register(HookRegistration(
        name=HookName.ON_RUN_COMPLETE,
        callback=_logging_hook,
        priority=10,
        source="builtin.logging",
    ))
    runner.register(HookRegistration(
        name=HookName.ON_RUN_ERROR,
        callback=_logging_hook,
        priority=10,
        source="builtin.logging",
    ))

    runner.register(HookRegistration(
        name=HookName.POST_LLM_CALL,
        callback=_metrics_hook,
        priority=20,
        source="builtin.metrics",
    ))
    runner.register(HookRegistration(
        name=HookName.POST_TOOL_EXEC,
        callback=_metrics_hook,
        priority=20,
        source="builtin.metrics",
    ))

    runner.register(HookRegistration(
        name=HookName.PRE_TOOL_EXEC,
        callback=_safety_hook,
        priority=1,
        source="builtin.safety",
    ))

    runner.register(HookRegistration(
        name=HookName.ON_CONTEXT_OVERFLOW,
        callback=_context_overflow_hook,
        priority=50,
        source="builtin.context",
    ))
