"""ToolExecutor — 串行执行工具调用, 超时控制, 输出截断。"""

from __future__ import annotations

import asyncio
import json
import time

import structlog

from sahara_runtime.events.emitter import RunEmitter
from sahara_runtime.tools.registry import ToolRegistry

logger = structlog.get_logger(__name__)

MAX_OUTPUT_LENGTH = 16_000  # 截断输出
TOOL_TIMEOUT = 60  # 单工具超时 (秒)


class ToolExecutor:
    """串行执行工具调用并发射事件。"""

    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry

    async def execute(
        self,
        *,
        tool_call_id: str,
        tool_name: str,
        input_json: str,
        emitter: RunEmitter,
    ) -> dict:
        """执行单个工具调用, 返回 tool_result 内容块。"""
        await emitter.emit_tool_start(tool_call_id, tool_name, input_json)
        start = time.time()

        tool_def = self._registry.get(tool_name)
        if tool_def is None:
            output = f"Error: unknown tool '{tool_name}'"
            duration_ms = int((time.time() - start) * 1000)
            await emitter.emit_tool_result(tool_call_id, tool_name, False, output, duration_ms)
            return {"type": "tool_result", "tool_use_id": tool_call_id, "content": output, "is_error": True}

        try:
            params = json.loads(input_json)
        except json.JSONDecodeError as e:
            output = f"Error: invalid tool input JSON: {e}"
            duration_ms = int((time.time() - start) * 1000)
            await emitter.emit_tool_result(tool_call_id, tool_name, False, output, duration_ms)
            return {"type": "tool_result", "tool_use_id": tool_call_id, "content": output, "is_error": True}

        try:
            result = await asyncio.wait_for(tool_def.func(**params), timeout=TOOL_TIMEOUT)

            if len(result) > MAX_OUTPUT_LENGTH:
                result = result[:MAX_OUTPUT_LENGTH] + "\n... [output truncated]"

            duration_ms = int((time.time() - start) * 1000)
            await emitter.emit_tool_result(tool_call_id, tool_name, True, result, duration_ms)
            return {"type": "tool_result", "tool_use_id": tool_call_id, "content": result}

        except TimeoutError:
            output = f"Error: tool '{tool_name}' timed out after {TOOL_TIMEOUT}s"
            duration_ms = int((time.time() - start) * 1000)
            await emitter.emit_tool_result(tool_call_id, tool_name, False, output, duration_ms)
            return {"type": "tool_result", "tool_use_id": tool_call_id, "content": output, "is_error": True}

        except Exception as e:
            output = f"Error: {type(e).__name__}: {e}"
            duration_ms = int((time.time() - start) * 1000)
            await emitter.emit_tool_result(tool_call_id, tool_name, False, output, duration_ms)
            logger.exception("tool_execution_error", tool_name=tool_name)
            return {"type": "tool_result", "tool_use_id": tool_call_id, "content": output, "is_error": True}
