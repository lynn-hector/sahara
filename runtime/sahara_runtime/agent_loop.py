"""AgentLoop — 驱动 LLM 交互循环的核心类。

参考 nanobot/agent/loop.py 设计:
- 所有循环状态作为实例属性
- LLM 调用通过 LLMProvider 抽象层
- 消息编排和上下文管理通过 ContextManager 统一处理
- Hook 系统在关键节点被调用
- Session 持久化保存完整消息（含 tool_calls）
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import structlog

from sahara_runtime.hooks.runner import HookContext, HookName
from sahara_runtime.llm.types import ToolCallRequest
from sahara_runtime.memory.session_store import SessionMessage

if TYPE_CHECKING:
    from sahara_runtime.context.context_manager import ContextManager
    from sahara_runtime.di.container import Container
    from sahara_runtime.events.emitter import RunEmitter
    from sahara_runtime.grpc.agent_servicer import TaskHandle
    from sahara_runtime.hooks.runner import HookRunner
    from sahara_runtime.llm.base import LLMProvider
    from sahara_runtime.memory.session_store import SessionStore
    from sahara_runtime.tools.executor import ToolExecutor

logger = structlog.get_logger(__name__)

TASK_TIMEOUT = 300
INPUT_WAIT_TIMEOUT = 120
CONFIRM_REQUIRED_TOOLS: set[str] = {"exec"}


def _metrics_task_start(agent_id: str) -> None:
    try:
        from sahara_runtime.observability.metrics import TASKS_ACTIVE
        TASKS_ACTIVE.inc()
    except ImportError:
        pass


def _metrics_task_end(agent_id: str, status: str, start_time: float) -> None:
    try:
        from sahara_runtime.observability.metrics import (
            TASK_DURATION,
            TASKS_ACTIVE,
            TASKS_TOTAL,
        )
        TASKS_TOTAL.labels(agent_id=agent_id, status=status).inc()
        TASKS_ACTIVE.dec()
        TASK_DURATION.labels(agent_id=agent_id).observe(time.time() - start_time)
    except ImportError:
        pass


def _metrics_llm_tokens(
    provider: str, model: str, input_tokens: int, output_tokens: int,
) -> None:
    try:
        from sahara_runtime.observability.metrics import LLM_TOKENS_TOTAL
        LLM_TOKENS_TOTAL.labels(
            provider=provider, model=model, direction="input",
        ).inc(input_tokens)
        LLM_TOKENS_TOTAL.labels(
            provider=provider, model=model, direction="output",
        ).inc(output_tokens)
    except ImportError:
        pass


class AgentLoop:
    """Agent 核心交互循环。"""

    def __init__(
        self,
        *,
        task_id: str,
        run_id: str,
        session_key: str,
        user_message: str,
        agent_id: str,
        metadata: dict[str, str],
        container: Container,
        on_complete: Callable[[str], None] | None = None,
        task_handle: TaskHandle | None = None,
    ) -> None:
        self.task_id = task_id
        self.run_id = run_id
        self.session_key = session_key
        self.user_message = user_message
        self.agent_id = agent_id
        self.metadata = metadata

        self._container = container
        self._on_complete = on_complete
        self._task_handle = task_handle

        self._emitter: RunEmitter = container.emitter_factory.for_run(
            run_id, session_key, task_id,
        )
        self._provider: LLMProvider = container.llm_provider
        self._tool_executor: ToolExecutor = container.tool_executor
        self._hook_runner: HookRunner | None = container.hook_runner
        self._session_store: SessionStore | None = container.session_store
        self._ctx: ContextManager = container.context_manager

        self._start_time = 0.0
        self._iteration = 0
        self._total_input_tokens = 0
        self._total_output_tokens = 0

    async def run(self) -> None:
        """执行完整的 Agent Loop 生命周期。"""
        self._start_time = time.time()
        _metrics_task_start(self.agent_id)

        await self._fire_hook(
            HookName.ON_SESSION_START,
            agent_id=self.agent_id,
        )

        try:
            async with asyncio.timeout(TASK_TIMEOUT):
                model_config = self._container.model_router.resolve(self.session_key)
                await self._emitter.emit_run_start(
                    agent_id=self.agent_id, model=model_config.model_id,
                )

                await self._fire_hook(
                    HookName.ON_RUN_START,
                    model=model_config.model_id,
                )

                await self._run_loop(model_config)

            _metrics_task_end(self.agent_id, "success", self._start_time)

        except TimeoutError:
            _metrics_task_end(self.agent_id, "timeout", self._start_time)
            logger.error("task_timeout", task_id=self.task_id, run_id=self.run_id)
            await self._emitter.emit_run_error("task timed out", retryable=False)
            await self._fire_hook(HookName.ON_RUN_ERROR, error="timeout")

        except asyncio.CancelledError:
            _metrics_task_end(self.agent_id, "cancelled", self._start_time)
            logger.info("task_cancelled", task_id=self.task_id, run_id=self.run_id)
            await self._emitter.emit_abort("task cancelled by user", aborted_by="user")
            await self._fire_hook(
                HookName.ON_RUN_ABORT,
                reason="cancelled_by_user",
                iterations=self._iteration,
                duration_ms=int((time.time() - self._start_time) * 1000),
            )

        except Exception:
            _metrics_task_end(self.agent_id, "error", self._start_time)
            logger.exception("agent_loop_error", task_id=self.task_id, run_id=self.run_id)
            await self._emitter.emit_run_error("internal error", retryable=True)
            await self._fire_hook(HookName.ON_RUN_ERROR, error="internal_error")

        finally:
            await self._fire_hook(
                HookName.ON_SESSION_END,
                total_input_tokens=self._total_input_tokens,
                total_output_tokens=self._total_output_tokens,
                iterations=self._iteration,
                duration_ms=int((time.time() - self._start_time) * 1000),
            )
            if self._on_complete:
                self._on_complete(self.task_id)

    async def _run_loop(self, model_config: Any) -> None:
        """核心 ReAct 循环：LLM 调用 -> 工具执行 -> 再次调用，直到无工具调用或达到上限。"""
        history = await self._load_history()
        tool_schemas = self._container.tool_registry.get_definitions()

        system_prompt, messages = await self._ctx.build_run_context(
            session_key=self.session_key,
            model=model_config.model_id,
            max_iterations=model_config.max_iterations,
            tools=tool_schemas or None,
            history=history,
            user_message=self.user_message,
        )

        ctx_stats = self._ctx.get_context_stats(
            messages=messages,
            max_context_tokens=model_config.max_context_tokens,
            system_prompt=system_prompt,
        )
        if ctx_stats["needs_trim"]:
            await self._fire_hook(
                HookName.PRE_CONTEXT_TRIM,
                current_tokens=ctx_stats["input_tokens"],
                total_tokens=ctx_stats["input_tokens"],
                budget_tokens=model_config.max_context_tokens,
                message_count=ctx_stats["original_count"],
            )
        fit = self._ctx.fit_context_with_stats(
            messages=messages,
            max_context_tokens=model_config.max_context_tokens,
            system_prompt=system_prompt,
        )
        if fit["trimmed"]:
            await self._fire_hook(
                HookName.POST_CONTEXT_TRIM,
                original_messages=fit["original_count"],
                remaining_messages=fit["remaining_count"],
                pre_trim_total_tokens=fit["pre_trim_total_tokens"],
                post_trim_total_tokens=fit["post_trim_total_tokens"],
                total_tokens=fit["post_trim_total_tokens"],
                budget_tokens=model_config.max_context_tokens,
            )
        messages = fit["messages"]

        final_text = ""
        response = None

        for self._iteration in range(1, model_config.max_iterations + 1):
            await self._fire_hook(
                HookName.PRE_LLM_CALL,
                iteration=self._iteration,
                provider=model_config.provider,
                model=model_config.model_id,
            )

            response = await self._provider.chat_with_retry(
                model_router=self._container.model_router,
                session_key=self.session_key,
                model_config=model_config,
                system=system_prompt,
                messages=messages,
                tools=tool_schemas or None,
                emitter=self._emitter,
            )

            self._total_input_tokens += response.input_tokens
            self._total_output_tokens += response.output_tokens

            _metrics_llm_tokens(
                model_config.provider, model_config.model_id,
                response.input_tokens, response.output_tokens,
            )

            await self._emitter.emit_usage(
                model=model_config.model_id,
                input_tokens=response.input_tokens,
                output_tokens=response.output_tokens,
                iteration=self._iteration,
            )

            await self._fire_hook(
                HookName.POST_LLM_CALL,
                iteration=self._iteration,
                provider=model_config.provider,
                model=model_config.model_id,
                has_tool_calls=response.has_tool_calls,
            )

            if not response.has_tool_calls:
                final_text = response.content or ""
                messages = self._ctx.add_assistant_message(messages, response)
                break

            messages = self._ctx.add_assistant_message(messages, response)
            messages = await self._execute_tool_calls(
                response.tool_calls, messages,
            )
        else:
            final_text = (response.content if response else None) or "[max iterations reached]"

        pre_resp_ctx = await self._fire_hook(
            HookName.PRE_RESPONSE,
            final_text=final_text,
            iterations=self._iteration,
        )
        if pre_resp_ctx and pre_resp_ctx.data.get("final_text"):
            final_text = pre_resp_ctx.data["final_text"]

        duration_ms = int((time.time() - self._start_time) * 1000)
        await self._emitter.emit_run_complete(
            final_text=final_text,
            iterations=self._iteration,
            duration_ms=duration_ms,
        )

        await self._fire_hook(
            HookName.ON_RUN_COMPLETE,
            iterations=self._iteration,
            duration_ms=duration_ms,
            final_text_len=len(final_text),
        )

        await self._persist_turn(messages, history)

    async def _execute_tool_calls(
        self,
        tool_calls: list[ToolCallRequest],
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """执行一轮工具调用，追加 tool result 消息。"""
        for tc in tool_calls:
            await self._fire_hook(
                HookName.PRE_TOOL_EXEC,
                tool_name=tc.name,
                tool_call_id=tc.id,
                command=tc.arguments.get("command", ""),
            )

            if self._task_handle and tc.name in CONFIRM_REQUIRED_TOOLS:
                confirmed = await self._wait_for_tool_confirmation(tc)
                if not confirmed:
                    messages = self._ctx.add_tool_result(
                        messages, tc.id, tc.name,
                        "User rejected this tool execution.",
                    )
                    continue

            try:
                input_str = json.dumps(tc.arguments)
            except (TypeError, ValueError):
                logger.warning("tool_input_serialize_failed", tool_name=tc.name)
                input_str = "{}"

            result = await self._tool_executor.execute(
                tool_call_id=tc.id,
                tool_name=tc.name,
                input_json=input_str,
                emitter=self._emitter,
            )

            if result is None:
                result = {"content": "", "is_error": True}

            messages = self._ctx.add_tool_result(
                messages, tc.id, tc.name, result.get("content", ""),
            )

            is_error = result.get("is_error", False)
            if is_error:
                await self._fire_hook(
                    HookName.POST_TOOL_EXEC_FAILURE,
                    tool_name=tc.name,
                    tool_call_id=tc.id,
                    error_message=result.get("content", ""),
                    arguments=tc.arguments,
                )
            else:
                await self._fire_hook(
                    HookName.POST_TOOL_EXEC,
                    tool_name=tc.name,
                    tool_call_id=tc.id,
                )

        return messages

    async def _wait_for_tool_confirmation(self, tc: ToolCallRequest) -> bool:
        """暂停 agent 等待用户确认高危工具执行。"""
        from sahara.common.v1 import common_pb2

        task_handle = self._task_handle
        if task_handle is None:
            return True

        if task_handle.input_channel is None:
            task_handle.input_channel = asyncio.Queue(maxsize=1)

        task_handle.waiting_for_input = True
        task_handle.state = common_pb2.TASK_STATE_WAITING_FOR_INPUT

        from sahara_runtime.grpc.agent_servicer import UserInput

        await self._emitter.emit_tool_confirm_required(
            tool_call_id=tc.id,
            tool_name=tc.name,
            input_json=json.dumps(tc.arguments, ensure_ascii=False),
            timeout_seconds=INPUT_WAIT_TIMEOUT,
        )

        try:
            user_input: UserInput = await asyncio.wait_for(
                task_handle.input_channel.get(),
                timeout=INPUT_WAIT_TIMEOUT,
            )
            approved = user_input.action == "approve"
            logger.info(
                "tool_confirm_received",
                tool_call_id=tc.id,
                action=user_input.action,
                approved=approved,
            )
            return approved
        except TimeoutError:
            logger.warning("tool_confirm_timeout", tool_call_id=tc.id)
            return False
        finally:
            task_handle.waiting_for_input = False
            task_handle.state = common_pb2.TASK_STATE_RUNNING

    async def _load_history(self) -> list[dict[str, Any]]:
        """从 SessionStore 加载历史消息。"""
        if self._session_store is None:
            return []
        try:
            stored = await self._session_store.load(self.session_key)
            return [self._ctx.session_msg_to_dict(msg) for msg in stored]
        except Exception:
            logger.warning("session_load_failed", session_key=self.session_key, exc_info=True)
            return []

    async def _persist_turn(
        self,
        messages: list[dict[str, Any]],
        original_history: list[dict[str, Any]],
    ) -> None:
        """将本轮新增的消息持久化到 SessionStore。"""
        if self._session_store is None:
            return

        try:
            stored = await self._session_store.load(self.session_key)
            seq_base = (stored[-1].seq + 1) if stored else 1

            skip = len(original_history)
            new_messages = messages[skip:]
            for i, msg in enumerate(new_messages):
                content = msg.get("content")
                if content is None:
                    content = ""

                extra: dict[str, Any] = {}
                if msg.get("tool_calls"):
                    extra["tool_calls"] = msg["tool_calls"]
                if msg.get("tool_call_id"):
                    extra["tool_call_id"] = msg["tool_call_id"]
                if msg.get("name"):
                    extra["name"] = msg["name"]
                if msg.get("thinking"):
                    extra["thinking"] = msg["thinking"]
                if msg.get("reasoning_content"):
                    extra["reasoning_content"] = msg["reasoning_content"]

                await self._session_store.append(
                    self.session_key,
                    SessionMessage(
                        role=msg.get("role", ""),
                        content=content,
                        seq=seq_base + i,
                        extra=extra,
                    ),
                )
        except Exception:
            logger.warning(
                "session_persist_failed",
                session_key=self.session_key,
                exc_info=True,
            )

    async def _fire_hook(self, hook_name: HookName, **data: Any) -> HookContext | None:
        """触发生命周期 Hook。"""
        if self._hook_runner is None:
            return None
        ctx = HookContext(
            hook_name=hook_name,
            session_key=self.session_key,
            task_id=self.task_id,
            run_id=self.run_id,
            data=data,
        )
        try:
            return await self._hook_runner.run(ctx)
        except Exception:
            logger.warning("hook_fire_failed", hook=hook_name.value, exc_info=True)
            return ctx


async def run_agent_loop(
    *,
    task_id: str,
    run_id: str,
    session_key: str,
    user_message: str,
    agent_id: str,
    metadata: dict[str, str],
    container: Container,
    on_complete: Callable[[str], None] | None = None,
    task_handle: TaskHandle | None = None,
) -> None:
    """兼容入口 — 保持与 AgentServicer 的调用契约不变。"""
    loop = AgentLoop(
        task_id=task_id,
        run_id=run_id,
        session_key=session_key,
        user_message=user_message,
        agent_id=agent_id,
        metadata=metadata,
        container=container,
        on_complete=on_complete,
        task_handle=task_handle,
    )
    await loop.run()
