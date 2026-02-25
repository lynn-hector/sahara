"""ToolExecutor — 串行执行工具调用, 超时控制, 输出截断, 沙箱集成。"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING

import structlog

from sahara_runtime.events.emitter import RunEmitter
from sahara_runtime.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from sahara_runtime.sandbox.base import Sandbox, SandboxManager

logger = structlog.get_logger(__name__)

MAX_OUTPUT_LENGTH = 16_000
TOOL_TIMEOUT = 60


class ToolExecutor:
    """串行执行工具调用并发射事件。支持沙箱隔离执行。"""

    def __init__(
        self,
        registry: ToolRegistry,
        sandbox_manager: SandboxManager | None = None,
    ) -> None:
        self._registry = registry
        self._sandbox_manager = sandbox_manager

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
            if tool_def.sandboxed and self._sandbox_manager:
                result = await asyncio.wait_for(
                    self._execute_in_sandbox(tool_name, params),
                    timeout=TOOL_TIMEOUT,
                )
            else:
                result = await asyncio.wait_for(
                    tool_def.func(**params),
                    timeout=TOOL_TIMEOUT,
                )

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

    async def _execute_in_sandbox(self, tool_name: str, params: dict) -> str:
        """通过 SandboxManager 在沙箱中执行工具。"""
        sandbox: Sandbox = await self._sandbox_manager.acquire()
        try:
            if tool_name == "exec":
                output, exit_code = await sandbox.exec(
                    params["command"],
                    timeout=params.get("timeout", 30),
                )
                return f"{output}\n[exit code: {exit_code}]"

            elif tool_name == "read":
                return await sandbox.read_file(params["path"])

            elif tool_name == "write":
                await sandbox.write_file(params["path"], params["content"])
                return f"Successfully written to {params['path']}"

            else:
                raise ValueError(f"sandbox execution not supported for tool: {tool_name}")
        finally:
            await self._sandbox_manager.release(sandbox)
