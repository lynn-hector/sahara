"""HookRunner — 生命周期钩子注册与执行引擎。

Hook 是 Agent Runtime 的内部扩展点:
- 在 Agent Loop 的关键节点插入自定义逻辑
- 按 priority 排序执行
- 支持 sync 和 async 回调
- source 字段预留给 Plugin 系统
"""

from __future__ import annotations

import asyncio
import enum
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

import structlog

logger = structlog.get_logger(__name__)

HookCallback = Callable[..., Coroutine[Any, Any, Any]]


class HookName(str, enum.Enum):
    """预定义的 Hook 点。"""

    PRE_LLM_CALL = "pre_llm_call"
    POST_LLM_CALL = "post_llm_call"
    PRE_TOOL_EXEC = "pre_tool_exec"
    POST_TOOL_EXEC = "post_tool_exec"
    ON_RUN_START = "on_run_start"
    ON_RUN_COMPLETE = "on_run_complete"
    ON_RUN_ERROR = "on_run_error"
    ON_CONTEXT_OVERFLOW = "on_context_overflow"


@dataclass
class HookRegistration:
    """单个 Hook 注册项。"""

    name: HookName
    callback: HookCallback
    priority: int = 100  # 越小越先执行
    source: str = "builtin"  # 预留给 Plugin 系统


@dataclass
class HookContext:
    """传递给 Hook 回调的上下文。"""

    hook_name: HookName
    session_key: str = ""
    task_id: str = ""
    run_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    should_skip: bool = False  # pre- hooks 可以设置此标志阻止后续操作


class HookRunner:
    """Hook 注册中心和执行器。"""

    def __init__(self) -> None:
        self._hooks: dict[HookName, list[HookRegistration]] = {
            name: [] for name in HookName
        }

    def register(self, registration: HookRegistration) -> None:
        self._hooks[registration.name].append(registration)
        self._hooks[registration.name].sort(key=lambda r: r.priority)
        logger.debug(
            "hook_registered",
            hook=registration.name.value,
            source=registration.source,
            priority=registration.priority,
        )

    def unregister(self, name: HookName, source: str) -> int:
        """移除指定 source 的所有 hooks, 返回移除数量。"""
        before = len(self._hooks[name])
        self._hooks[name] = [r for r in self._hooks[name] if r.source != source]
        removed = before - len(self._hooks[name])
        if removed:
            logger.debug("hooks_unregistered", hook=name.value, source=source, count=removed)
        return removed

    async def run(self, ctx: HookContext) -> HookContext:
        """执行指定 hook 的所有注册回调, 返回可能被修改的上下文。"""
        registrations = self._hooks.get(ctx.hook_name, [])
        if not registrations:
            return ctx

        for reg in registrations:
            start = time.time()
            try:
                result = await asyncio.wait_for(reg.callback(ctx), timeout=10.0)
                if isinstance(result, HookContext):
                    ctx = result
            except TimeoutError:
                logger.warning(
                    "hook_timeout",
                    hook=ctx.hook_name.value,
                    source=reg.source,
                )
            except Exception:
                logger.exception(
                    "hook_error",
                    hook=ctx.hook_name.value,
                    source=reg.source,
                )
            else:
                duration_ms = int((time.time() - start) * 1000)
                logger.debug(
                    "hook_executed",
                    hook=ctx.hook_name.value,
                    source=reg.source,
                    duration_ms=duration_ms,
                )

            if ctx.should_skip:
                logger.info(
                    "hook_skip_requested",
                    hook=ctx.hook_name.value,
                    source=reg.source,
                )
                break

        return ctx

    def list_hooks(self, name: HookName | None = None) -> list[dict[str, Any]]:
        """列出已注册的 hooks (调试用)。"""
        result = []
        targets = [name] if name else list(HookName)
        for hook_name in targets:
            for reg in self._hooks.get(hook_name, []):
                result.append({
                    "hook": hook_name.value,
                    "source": reg.source,
                    "priority": reg.priority,
                })
        return result
