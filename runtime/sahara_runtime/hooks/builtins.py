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
            TOOL_CALLS_TOTAL.labels(tool_name=tool_name, success="true").inc()

        elif ctx.hook_name == HookName.POST_TOOL_EXEC_FAILURE:
            tool_name = ctx.data.get("tool_name", "unknown")
            TOOL_CALLS_TOTAL.labels(tool_name=tool_name, success="false").inc()

    except ImportError:
        pass

    return ctx


async def _tool_error_hook(ctx: HookContext) -> HookContext:
    """POST_TOOL_EXEC_FAILURE: 记录工具调用失败详情。"""
    logger.warning(
        "tool_call_failed",
        tool_name=ctx.data.get("tool_name", "unknown"),
        tool_call_id=ctx.data.get("tool_call_id", ""),
        error_message=ctx.data.get("error_message", "")[:200],
        session_key=ctx.session_key,
        task_id=ctx.task_id,
    )
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
    pre_tokens = ctx.data.get("pre_trim_total_tokens", ctx.data.get("total_tokens", 0))
    post_tokens = ctx.data.get("post_trim_total_tokens", ctx.data.get("total_tokens", 0))
    budget = ctx.data.get("budget_tokens", 0)

    logger.warning(
        "context_overflow_handled",
        original_messages=original_count,
        remaining_messages=remaining_count,
        pre_trim_total_tokens=pre_tokens,
        post_trim_total_tokens=post_tokens,
        budget=budget,
        session_key=ctx.session_key,
    )

    return ctx


async def _abort_hook(ctx: HookContext) -> HookContext:
    """ON_RUN_ABORT: 记录任务中止信息。"""
    logger.warning(
        "run_aborted",
        reason=ctx.data.get("reason", "unknown"),
        iterations=ctx.data.get("iterations", 0),
        duration_ms=ctx.data.get("duration_ms", 0),
        session_key=ctx.session_key,
        task_id=ctx.task_id,
    )
    return ctx


async def _pre_context_trim_hook(ctx: HookContext) -> HookContext:
    """PRE_CONTEXT_TRIM: 上下文裁剪前记录并可修改策略。"""
    logger.info(
        "context_trim_starting",
        current_tokens=ctx.data.get("current_tokens", ctx.data.get("total_tokens", 0)),
        budget_tokens=ctx.data.get("budget_tokens", 0),
        message_count=ctx.data.get("message_count", 0),
        session_key=ctx.session_key,
    )
    return ctx


async def _session_end_hook(ctx: HookContext) -> HookContext:
    """ON_SESSION_END: 记录会话级统计，用于资源清理。"""
    logger.info(
        "session_end",
        total_input_tokens=ctx.data.get("total_input_tokens", 0),
        total_output_tokens=ctx.data.get("total_output_tokens", 0),
        iterations=ctx.data.get("iterations", 0),
        duration_ms=ctx.data.get("duration_ms", 0),
        session_key=ctx.session_key,
        task_id=ctx.task_id,
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
        name=HookName.POST_TOOL_EXEC_FAILURE,
        callback=_metrics_hook,
        priority=20,
        source="builtin.metrics",
    ))
    runner.register(HookRegistration(
        name=HookName.POST_TOOL_EXEC_FAILURE,
        callback=_tool_error_hook,
        priority=10,
        source="builtin.logging",
    ))

    runner.register(HookRegistration(
        name=HookName.PRE_TOOL_EXEC,
        callback=_safety_hook,
        priority=1,
        source="builtin.safety",
    ))

    runner.register(HookRegistration(
        name=HookName.POST_CONTEXT_TRIM,
        callback=_context_overflow_hook,
        priority=50,
        source="builtin.context",
    ))

    # ── 新增 hooks ────────────────────────────────────
    runner.register(HookRegistration(
        name=HookName.ON_RUN_ABORT,
        callback=_abort_hook,
        priority=10,
        source="builtin.logging",
    ))
    runner.register(HookRegistration(
        name=HookName.PRE_CONTEXT_TRIM,
        callback=_pre_context_trim_hook,
        priority=50,
        source="builtin.context",
    ))
    runner.register(HookRegistration(
        name=HookName.ON_SESSION_START,
        callback=_logging_hook,
        priority=10,
        source="builtin.logging",
    ))
    runner.register(HookRegistration(
        name=HookName.ON_SESSION_END,
        callback=_session_end_hook,
        priority=10,
        source="builtin.logging",
    ))
