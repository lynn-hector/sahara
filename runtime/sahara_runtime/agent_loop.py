"""AgentLoop — 驱动 LLM 交互循环的核心类。

参考 nanobot/agent/loop.py 设计，将原有的过程式函数重构为类封装：
- 所有循环状态作为实例属性
- LLM 调用通过 LLMProvider 抽象层
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
from sahara_runtime.llm.types import LLMResponse, ToolCallRequest
from sahara_runtime.memory.session_store import SessionMessage

if TYPE_CHECKING:
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
    """Agent 核心交互循环。

    每个 SubmitTask 调用创建一个 AgentLoop 实例并执行 run()。
    参考 nanobot 的 AgentLoop 设计，将循环状态封装为实例属性。
    """

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

        self._start_time = 0.0
        self._iteration = 0
        self._total_input_tokens = 0
        self._total_output_tokens = 0

    async def run(self) -> None:
        """执行完整的 Agent Loop 生命周期。"""
        self._start_time = time.time()
        _metrics_task_start(self.agent_id)

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

        except Exception:
            _metrics_task_end(self.agent_id, "error", self._start_time)
            logger.exception("agent_loop_error", task_id=self.task_id, run_id=self.run_id)
            await self._emitter.emit_run_error("internal error", retryable=True)
            await self._fire_hook(HookName.ON_RUN_ERROR, error="internal_error")

        finally:
            if self._on_complete:
                self._on_complete(self.task_id)

    async def _run_loop(self, model_config: Any) -> None:
        """核心 ReAct 循环：LLM 调用 -> 工具执行 -> 再次调用，直到无工具调用或达到上限。"""
        tool_schemas = self._container.tool_registry.list_schemas()

        skills_prompt = await self._build_skills_prompt()
        ws = self._container.workspace
        system_prompt = self._container.prompt_builder.build(
            session_key=self.session_key,
            model=model_config.model_id,
            max_iterations=model_config.max_iterations,
            tools=tool_schemas or None,
            skills_prompt=skills_prompt,
            workspace_path=str(ws) if ws else "",
        )

        history = await self._load_history()
        messages: list[dict[str, Any]] = history + [
            {"role": "user", "content": self.user_message},
        ]

        messages = self._fit_context(messages, model_config, system_prompt)

        final_text = ""

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
                messages = _add_assistant_message(messages, response)
                break

            messages = _add_assistant_message(messages, response)
            messages = await self._execute_tool_calls(
                response.tool_calls, messages,
            )
        else:
            final_text = (response.content if response else None) or "[max iterations reached]"

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
        """执行一轮工具调用，以 OpenAI role:"tool" 消息追加结果。"""
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
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "name": tc.name,
                        "content": "User rejected this tool execution.",
                    })
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

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "name": tc.name,
                "content": result.get("content", ""),
            })

            await self._fire_hook(
                HookName.POST_TOOL_EXEC,
                tool_name=tc.name,
                tool_call_id=tc.id,
                success=not result.get("is_error", False),
            )

        return messages

    async def _wait_for_tool_confirmation(self, tc: ToolCallRequest) -> bool:
        """暂停 agent 等待用户确认高危工具执行。"""
        from sahara.common.v1 import common_pb2

        from sahara_runtime.grpc.agent_servicer import UserInput

        task_handle = self._task_handle
        if task_handle is None:
            return True


        if task_handle.input_channel is None:
            task_handle.input_channel = asyncio.Queue(maxsize=1)

        task_handle.waiting_for_input = True
        task_handle.state = common_pb2.TASK_STATE_WAITING_FOR_INPUT

        await self._emitter.emit_tool_confirm_required(
            tool_call_id=tc.id,
            tool_name=tc.name,
            input_json=json.dumps(tc.arguments),
            risk_description=f"Tool '{tc.name}' requires confirmation before execution.",
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

    async def _build_skills_prompt(self) -> str:
        """加载并过滤 Skills，构建 prompt 片段。"""
        if not self._container.skill_loader or not self._container.skill_filter:
            return ""
        try:
            from sahara_runtime.skills.prompt import build_skills_prompt
            all_skills = await self._container.skill_loader.load_all()
            active_skills = self._container.skill_filter.filter(all_skills)
            return build_skills_prompt(active_skills)
        except Exception:
            logger.warning("skills_prompt_build_failed", exc_info=True)
            return ""

    def _fit_context(
        self,
        messages: list[dict[str, Any]],
        model_config: Any,
        system_prompt: str,
    ) -> list[dict[str, Any]]:
        """使用 ContextManager 裁剪消息以适配上下文窗口。"""
        ctx_mgr = self._container.context_manager
        tk_counter = self._container.token_counter
        if not ctx_mgr or not tk_counter:
            return messages

        system_tokens = tk_counter.count_system(system_prompt)
        return ctx_mgr.fit_messages(
            messages,
            max_context_tokens=model_config.max_context_tokens,
            system_tokens=system_tokens,
        )

    async def _load_history(self) -> list[dict[str, Any]]:
        """从 SessionStore 加载历史消息。"""
        if self._session_store is None:
            return []
        try:
            stored = await self._session_store.load(self.session_key)
            return [_session_msg_to_dict(msg) for msg in stored]
        except Exception:
            logger.warning("session_load_failed", session_key=self.session_key, exc_info=True)
            return []

    async def _persist_turn(
        self,
        messages: list[dict[str, Any]],
        original_history: list[dict[str, Any]],
    ) -> None:
        """将本轮新增的消息持久化到 SessionStore。

        OpenAI 格式消息的额外字段 (tool_calls, tool_call_id, name) 存入 extra。
        """
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


def build_assistant_message(
    content: str | None,
    tool_calls: list[dict[str, Any]] | None = None,
    thinking: str | None = None,
    reasoning_content: str | None = None,
) -> dict[str, Any]:
    """构建 OpenAI 格式的 assistant message。

    参考 nanobot/utils/helpers.py build_assistant_message。
    """
    msg: dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    if thinking is not None:
        msg["thinking"] = thinking
    if reasoning_content is not None:
        msg["reasoning_content"] = reasoning_content
    return msg


def _add_assistant_message(
    messages: list[dict[str, Any]],
    response: LLMResponse,
) -> list[dict[str, Any]]:
    """将 LLMResponse 转换为 OpenAI 通用格式的 assistant message 并追加到 messages。"""
    tool_call_dicts = [tc.to_openai_tool_call() for tc in response.tool_calls] or None
    messages.append(build_assistant_message(
        response.content,
        tool_calls=tool_call_dicts,
        thinking=response.thinking,
        reasoning_content=response.reasoning_content,
    ))
    return messages


def _session_msg_to_dict(msg: SessionMessage) -> dict[str, Any]:
    """将 SessionMessage 转换为 OpenAI 通用格式的 LLM message dict。

    如果 extra 中有 tool_calls / tool_call_id 等字段则还原到顶层。
    同时兼容旧的 Anthropic 格式数据（content 为 JSON content blocks）。
    """
    result: dict[str, Any] = {"role": msg.role, "content": msg.content}

    extra = msg.extra or {}
    if extra.get("tool_calls"):
        result["tool_calls"] = extra["tool_calls"]
    if extra.get("tool_call_id"):
        result["tool_call_id"] = extra["tool_call_id"]
    if extra.get("name"):
        result["name"] = extra["name"]
    if extra.get("thinking"):
        result["thinking"] = extra["thinking"]
    if extra.get("reasoning_content"):
        result["reasoning_content"] = extra["reasoning_content"]

    if not extra:
        result = _maybe_convert_legacy_anthropic(result)

    return result


def _maybe_convert_legacy_anthropic(msg: dict[str, Any]) -> dict[str, Any]:
    """兼容旧 Anthropic 格式: 将 content blocks 转换为 OpenAI 格式。

    旧格式 assistant: {"content": [{"type":"text",...}, {"type":"tool_use",...}]}
    旧格式 user:      {"content": [{"type":"tool_result","tool_use_id":...}]}
    """
    content = msg.get("content")
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            return msg
        if not isinstance(content, list):
            return msg

    if not isinstance(content, list):
        return msg

    if msg.get("role") == "assistant":
        text_parts: list[str] = []
        tool_calls: list[dict[str, Any]] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                tool_calls.append({
                    "id": block.get("id", ""),
                    "type": "function",
                    "function": {
                        "name": block.get("name", ""),
                        "arguments": json.dumps(
                            block.get("input", {}), ensure_ascii=False,
                        ),
                    },
                })
        result: dict[str, Any] = {
            "role": "assistant",
            "content": "".join(text_parts) or None,
        }
        if tool_calls:
            result["tool_calls"] = tool_calls
        return result

    return msg


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
