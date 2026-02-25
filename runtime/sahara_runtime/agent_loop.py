"""
Agent Loop 核心 — 驱动 LLM 交互循环

支持:
- Anthropic SDK 流式响应
- 工具调用循环 (max N iterations)
- thinking content blocks
- Mock LLM fallback (无 API key 时)
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Callable

import structlog

from sahara_runtime.events.emitter import RunEmitter
from sahara_runtime.model_router.router import ModelConfig
from sahara_runtime.tools.executor import ToolExecutor

if TYPE_CHECKING:
    from sahara_runtime.di.container import Container

logger = structlog.get_logger(__name__)

TASK_TIMEOUT = 300


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


async def _run_mock_llm(
    *,
    emitter: RunEmitter,
    user_message: str,
    model: str,
    start_time: float,
) -> None:
    """Mock LLM: simulates streaming by emitting deltas for a canned response."""
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


async def _run_real_llm(
    *,
    emitter: RunEmitter,
    user_message: str,
    session_key: str,
    model_config: ModelConfig,
    container: Container,
    start_time: float,
) -> None:
    """Run the full Anthropic SDK agent loop with tool use."""
    client = container.model_router.get_client(session_key, model_config)
    tool_executor = ToolExecutor(container.tool_registry)
    tool_schemas = container.tool_registry.list_schemas()

    system_prompt = container.prompt_builder.build(
        session_key=session_key,
        model=model_config.model_id,
        max_iterations=model_config.max_iterations,
        tools=tool_schemas if tool_schemas else None,
    )

    messages: list[dict[str, Any]] = [
        {"role": "user", "content": user_message},
    ]

    final_text = ""
    total_input_tokens = 0
    total_output_tokens = 0

    for iteration in range(1, model_config.max_iterations + 1):
        response_text, tool_calls, usage = await _call_anthropic_stream(
            client=client,
            model=model_config.model_id,
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

        # Build assistant message with text + tool_use blocks
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

        # Execute tools and build tool_result messages
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


async def _call_anthropic_stream(
    *,
    client: Any,
    model: str,
    system: str,
    messages: list[dict],
    tools: list[dict] | None,
    emitter: RunEmitter,
) -> tuple[str, list[dict], dict]:
    """Call Anthropic messages API with streaming. Returns (text, tool_calls, usage)."""
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
                if block.type == "text":
                    pass
                elif block.type == "tool_use":
                    current_tool = {"id": block.id, "name": block.name, "input": {}}
                    current_tool_json = ""
                elif block.type == "thinking":
                    pass

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
