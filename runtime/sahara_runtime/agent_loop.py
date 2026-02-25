"""
Agent Loop 核心 — 驱动 LLM 交互循环

支持:
- Anthropic SDK 流式响应
- 工具调用循环 (max N iterations)
- thinking content blocks
- Mock LLM fallback (无 API key 时)
- SessionStore 多轮对话
- ContextManager token 预算控制
- Fallback: 自动重试 + Key 轮换 (429/5xx)
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Callable

import structlog

from sahara_runtime.events.emitter import RunEmitter
from sahara_runtime.memory.session_store import SessionMessage, SessionStore
from sahara_runtime.model_router.router import ModelConfig
from sahara_runtime.tools.executor import ToolExecutor

if TYPE_CHECKING:
    from sahara_runtime.di.container import Container

logger = structlog.get_logger(__name__)

TASK_TIMEOUT = 300
MAX_LLM_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # seconds


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
) -> None:
    """Agent 核心交互循环。每个 SubmitTask 调用触发此函数。"""
    emitter: RunEmitter = container.emitter_factory.for_run(run_id, session_key, task_id)
    start_time = time.time()

    try:
        async with asyncio.timeout(TASK_TIMEOUT):
            model_config = container.model_router.resolve(session_key)
            await emitter.emit_run_start(agent_id=agent_id, model=model_config.model_id)

            if container.model_router and container.llm_client is not None:
                await _run_real_llm(
                    emitter=emitter,
                    user_message=user_message,
                    session_key=session_key,
                    model_config=model_config,
                    container=container,
                    start_time=start_time,
                )
            else:
                await _run_mock_llm(
                    emitter=emitter,
                    user_message=user_message,
                    model=model_config.model_id,
                    start_time=start_time,
                    session_store=container.session_store,
                    session_key=session_key,
                )
    except TimeoutError:
        logger.error("task_timeout", task_id=task_id, run_id=run_id)
        await emitter.emit_run_error("task timed out", retryable=False)
    except asyncio.CancelledError:
        logger.info("task_cancelled", task_id=task_id, run_id=run_id)
        await emitter.emit_abort("task cancelled by user", aborted_by="user")
    except Exception:
        logger.exception("agent_loop_error", task_id=task_id, run_id=run_id)
        await emitter.emit_run_error("internal error", retryable=True)
    finally:
        if on_complete:
            on_complete(task_id)


# ── Session 持久化 ──────────────────────────────────


async def _persist_turn(
    store: SessionStore | None,
    session_key: str,
    user_message: str,
    assistant_text: str,
    seq_base: int,
) -> None:
    """将一轮对话 (user + assistant) 持久化到 SessionStore。

    seq_base 用于消息排序: user 消息 = seq_base, assistant 消息 = seq_base + 1。
    """
    if store is None:
        return
    try:
        await store.append(
            session_key,
            SessionMessage(role="user", content=user_message, seq=seq_base),
        )
        await store.append(
            session_key,
            SessionMessage(role="assistant", content=assistant_text, seq=seq_base + 1),
        )
    except Exception:
        logger.warning("session_persist_failed", session_key=session_key, exc_info=True)


async def _load_history(
    store: SessionStore | None,
    session_key: str,
) -> list[dict[str, Any]]:
    """从 SessionStore 加载历史消息, 转换为 LLM messages 格式 [{role, content}, ...]。"""
    if store is None:
        return []
    try:
        stored = await store.load(session_key)
        return [{"role": msg.role, "content": msg.content} for msg in stored]
    except Exception:
        logger.warning("session_load_failed", session_key=session_key, exc_info=True)
        return []


# ── Mock LLM ────────────────────────────────────────


async def _run_mock_llm(
    *,
    emitter: RunEmitter,
    user_message: str,
    model: str,
    start_time: float,
    session_store: SessionStore | None = None,
    session_key: str = "",
) -> None:
    """Mock LLM — 无 API key 时的降级路径, 回显用户输入并模拟流式推送。"""
    response = f'Hello from Sahara Runtime! You said: "{user_message}"'

    chunks = [response[i : i + 10] for i in range(0, len(response), 10)]
    for chunk in chunks:
        await emitter.emit_delta(chunk)
        await asyncio.sleep(0.05)

    duration_ms = int((time.time() - start_time) * 1000)
    await emitter.emit_usage(
        model=model, input_tokens=10, output_tokens=len(response), iteration=1
    )
    await emitter.emit_run_complete(
        final_text=response, iterations=1, duration_ms=duration_ms
    )

    if session_store and session_key:
        stored = await session_store.load(session_key)
        seq_base = (stored[-1].seq + 1) if stored else 1
        await _persist_turn(session_store, session_key, user_message, response, seq_base)


# ── Real LLM with Fallback ──────────────────────────


async def _run_real_llm(
    *,
    emitter: RunEmitter,
    user_message: str,
    session_key: str,
    model_config: ModelConfig,
    container: Container,
    start_time: float,
) -> None:
    """Real LLM agent loop with retry + key rotation fallback."""
    tool_executor = ToolExecutor(
        container.tool_registry,
        sandbox_manager=container.sandbox_manager,
    )
    tool_schemas = container.tool_registry.list_schemas()

    system_prompt = container.prompt_builder.build(
        session_key=session_key,
        model=model_config.model_id,
        max_iterations=model_config.max_iterations,
        tools=tool_schemas if tool_schemas else None,
    )

    history = await _load_history(container.session_store, session_key)
    messages: list[dict[str, Any]] = history + [
        {"role": "user", "content": user_message},
    ]

    # Fit to context window via Container's context_manager
    ctx_mgr = container.context_manager
    tk_counter = container.token_counter
    if ctx_mgr and tk_counter:
        system_tokens = tk_counter.count_system(system_prompt)
        messages = ctx_mgr.fit_messages(
            messages,
            max_context_tokens=model_config.max_context_tokens,
            system_tokens=system_tokens,
        )

    final_text = ""
    total_input_tokens = 0
    total_output_tokens = 0
    iteration = 0

    for iteration in range(1, model_config.max_iterations + 1):
        response_text, tool_calls, usage = await _call_with_fallback(
            container=container,
            session_key=session_key,
            model_config=model_config,
            system=system_prompt,
            messages=messages,
            tools=tool_schemas if tool_schemas else None,
            emitter=emitter,
        )

        total_input_tokens += usage.get("input_tokens", 0)
        total_output_tokens += usage.get("output_tokens", 0)

        await emitter.emit_usage(
            model=model_config.model_id,
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            iteration=iteration,
        )

        container.model_router.report_key_success(session_key)

        if not tool_calls:
            final_text = response_text
            break

        assistant_content: list[dict] = []
        if response_text:
            assistant_content.append({"type": "text", "text": response_text})
        for tc in tool_calls:
            assistant_content.append({
                "type": "tool_use",
                "id": tc["id"],
                "name": tc["name"],
                "input": tc["input"],
            })
        messages.append({"role": "assistant", "content": assistant_content})

        tool_results: list[dict] = []
        for tc in tool_calls:
            result = await tool_executor.execute(
                tool_call_id=tc["id"],
                tool_name=tc["name"],
                input_json=json.dumps(tc["input"]),
                emitter=emitter,
            )
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tc["id"],
                "content": result.get("content", ""),
                **({"is_error": True} if result.get("is_error") else {}),
            })

        messages.append({"role": "user", "content": tool_results})
    else:
        final_text = response_text or "[max iterations reached]"

    duration_ms = int((time.time() - start_time) * 1000)
    await emitter.emit_run_complete(
        final_text=final_text,
        iterations=iteration,
        duration_ms=duration_ms,
    )

    stored = await _load_history(container.session_store, session_key)
    seq_base = len(stored) + 1
    await _persist_turn(container.session_store, session_key, user_message, final_text, seq_base)


# ── Fallback: Retry + Key Rotation ──────────────────


async def _call_with_fallback(
    *,
    container: Container,
    session_key: str,
    model_config: ModelConfig,
    system: str,
    messages: list[dict],
    tools: list[dict] | None,
    emitter: RunEmitter,
) -> tuple[str, list[dict], dict]:
    """LLM 调用 with 自动重试 + Key 轮换。

    Fallback 策略:
    1. 首次调用: 使用当前 session 绑定的 client
    2. 429/5xx/连接错误: 等待 backoff → 轮换 Key → 重建 client → 重试
    3. 401/403: 熔断当前 Key → 轮换 → 重试
    4. 超过 MAX_LLM_RETRIES: 抛出最后一个异常
    """
    last_exc: Exception | None = None

    for attempt in range(1, MAX_LLM_RETRIES + 1):
        try:
            client = container.model_router.get_client(session_key, model_config)
            return await _call_anthropic_stream(
                client=client,
                model=model_config.model_id,
                system=system,
                messages=messages,
                tools=tools,
                emitter=emitter,
            )
        except Exception as exc:
            last_exc = exc
            status_code = _extract_status_code(exc)
            retryable = _is_retryable(status_code, exc)

            logger.warning(
                "llm_call_failed",
                attempt=attempt,
                max_retries=MAX_LLM_RETRIES,
                error_type=type(exc).__name__,
                status_code=status_code,
                retryable=retryable,
                session_key=session_key,
            )

            if status_code:
                container.model_router.report_key_error(session_key, status_code)

            if not retryable or attempt >= MAX_LLM_RETRIES:
                raise

            # Rotate key: release current binding, next get_client picks a new key
            container.model_router.release_session(session_key)

            backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            logger.info("llm_retry_backoff", seconds=backoff, attempt=attempt)
            await asyncio.sleep(backoff)

    raise last_exc  # type: ignore[misc]


def _extract_status_code(exc: Exception) -> int:
    """从 SDK 异常中提取 HTTP 状态码。

    优先检测 anthropic.APIStatusError, 然后回退到通用的 .status_code 属性。
    返回 0 表示无法提取。
    """
    try:
        import anthropic

        if isinstance(exc, anthropic.APIStatusError):
            return exc.status_code
    except ImportError:
        pass

    if hasattr(exc, "status_code"):
        return getattr(exc, "status_code")
    return 0


def _is_retryable(status_code: int, exc: Exception) -> bool:
    """判断 LLM 调用异常是否可通过重试/Key 轮换恢复。

    可重试条件:
        - 429 (rate limited) / 5xx (server error) / 529 (overloaded) → 退避重试
        - 401/403 (auth error) → 当前 key 被熔断, 轮换到下一个 key 重试
        - 异常类名包含 "timeout" 或 "connection" → 网络层重试
    """
    if status_code in (429, 500, 502, 503, 529):
        return True
    if status_code in (401, 403):
        return True
    exc_name = type(exc).__name__
    if "timeout" in exc_name.lower() or "connection" in exc_name.lower():
        return True
    return False


# ── Anthropic Stream ─────────────────────────────────


async def _call_anthropic_stream(
    *,
    client: Any,
    model: str,
    system: str,
    messages: list[dict],
    tools: list[dict] | None,
    emitter: RunEmitter,
) -> tuple[str, list[dict], dict]:
    """通过 Anthropic Messages API 进行流式调用。

    处理以下 SSE 事件类型:
        - message_start:        提取 input_tokens
        - content_block_start:  检测 tool_use block, 初始化 tool 状态
        - content_block_delta:  累积 text / tool JSON / thinking 内容,
                                text_delta 实时通过 emitter 推送给客户端
        - content_block_stop:   解析 tool JSON → tool_calls 列表
        - message_delta:        提取 output_tokens

    Returns:
        (response_text, tool_calls, usage_info)
        - response_text: 拼接的纯文本输出
        - tool_calls: [{id, name, input}, ...] 如果 LLM 请求工具调用
        - usage_info: {"input_tokens": N, "output_tokens": N}
    """
    kwargs: dict[str, Any] = {
        "model": model,
        "max_tokens": 8192,
        "system": system,
        "messages": messages,
    }
    if tools:
        kwargs["tools"] = tools

    text_parts: list[str] = []
    tool_calls: list[dict] = []
    current_tool: dict | None = None
    current_tool_json = ""
    usage_info: dict = {}

    async with client.messages.stream(**kwargs) as stream:
        async for event in stream:
            event_type = event.type

            if event_type == "content_block_start":
                block = event.content_block
                if block.type == "tool_use":
                    current_tool = {"id": block.id, "name": block.name, "input": {}}
                    current_tool_json = ""

            elif event_type == "content_block_delta":
                delta = event.delta
                if delta.type == "text_delta":
                    text_parts.append(delta.text)
                    await emitter.emit_delta(delta.text)
                elif delta.type == "input_json_delta":
                    if current_tool is not None:
                        current_tool_json += delta.partial_json
                elif delta.type == "thinking_delta":
                    await emitter.emit_thinking(delta.thinking)

            elif event_type == "content_block_stop":
                if current_tool is not None:
                    try:
                        current_tool["input"] = json.loads(current_tool_json) if current_tool_json else {}
                    except json.JSONDecodeError:
                        current_tool["input"] = {"raw": current_tool_json}
                    tool_calls.append(current_tool)
                    current_tool = None
                    current_tool_json = ""

            elif event_type == "message_delta":
                if hasattr(event, "usage") and event.usage:
                    usage_info["output_tokens"] = getattr(event.usage, "output_tokens", 0)

            elif event_type == "message_start":
                if hasattr(event, "message") and hasattr(event.message, "usage"):
                    usage_info["input_tokens"] = getattr(event.message.usage, "input_tokens", 0)

    return "".join(text_parts), tool_calls, usage_info
