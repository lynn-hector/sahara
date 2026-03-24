"""ToolExecutor — 工具执行引擎（薄包装层）。

核心逻辑（cast/validate/execute）已在 ToolRegistry.execute() 中统一处理。
ToolExecutor 只负责:
- 事件发射（tool_start / tool_result）
- 超时保护
- Sandbox 路由（sandbox_enabled=true 时）
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any

import structlog

from sahara_runtime.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from sahara_runtime.events.emitter import RunEmitter
    from sahara_runtime.sandbox.base import Sandbox, SandboxManager

logger = structlog.get_logger(__name__)

TOOL_TIMEOUT = 60


class ToolExecutor:
    """执行工具调用并发射事件。"""

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
    ) -> dict[str, Any]:
        """执行单个工具调用，返回结果。"""
        await emitter.emit_tool_start(tool_call_id, tool_name, input_json)
        start = time.time()

        try:
            params = json.loads(input_json)
        except json.JSONDecodeError as e:
            return await self._emit_error(
                emitter, tool_call_id, tool_name, start,
                f"Error: invalid tool input JSON: {e}",
            )

        try:
            if self._sandbox_manager and self._should_sandbox(tool_name):
                result_str = await asyncio.wait_for(
                    self._execute_in_sandbox(tool_name, params),
                    timeout=TOOL_TIMEOUT,
                )
                result: dict[str, Any] = {"content": result_str}
            else:
                result = await asyncio.wait_for(
                    self._registry.execute(tool_name, params),
                    timeout=TOOL_TIMEOUT,
                )
        except TimeoutError:
            return await self._emit_error(
                emitter, tool_call_id, tool_name, start,
                f"Error: tool '{tool_name}' timed out after {TOOL_TIMEOUT}s",
            )
        except Exception as e:
            logger.exception("tool_execution_error", tool_name=tool_name)
            return await self._emit_error(
                emitter, tool_call_id, tool_name, start,
                f"Error: {type(e).__name__}: {e}",
            )

        is_error = result.get("is_error", False)
        duration_ms = int((time.time() - start) * 1000)
        content = result.get("content", "")

        await emitter.emit_tool_result(
            tool_call_id, tool_name, not is_error, content, duration_ms,
        )
        return result

    @staticmethod
    def _should_sandbox(tool_name: str) -> bool:
        return tool_name in ("exec", "read_file", "write_file")

    async def _execute_in_sandbox(self, tool_name: str, params: dict) -> str:
        """通过 SandboxManager 在容器中执行工具。"""
        assert self._sandbox_manager is not None
        sandbox: Sandbox = await self._sandbox_manager.acquire()
        try:
            if tool_name == "exec":
                output, exit_code = await sandbox.exec(
                    params["command"], timeout=params.get("timeout", 30),
                )
                return f"{output}\n[exit code: {exit_code}]"
            elif tool_name == "read_file":
                return await sandbox.read_file(params["path"])
            elif tool_name == "write_file":
                await sandbox.write_file(params["path"], params["content"])
                return f"Successfully written to {params['path']}"
            else:
                raise ValueError(f"sandbox execution not supported for tool: {tool_name}")
        finally:
            await self._sandbox_manager.release(sandbox)

    async def _emit_error(
        self,
        emitter: RunEmitter,
        tool_call_id: str,
        tool_name: str,
        start: float,
        output: str,
    ) -> dict[str, Any]:
        duration_ms = int((time.time() - start) * 1000)
        await emitter.emit_tool_result(
            tool_call_id, tool_name, False, output, duration_ms,
        )
        return {"content": output, "is_error": True}
