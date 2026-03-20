"""ToolExecutor — 工具执行引擎。

重构改进（参考 nanobot/agent/tools/registry.py）:
- 执行前进行参数类型转换和校验
- 错误返回附带 hint 引导 LLM 换方法
- 统一的 _make_error_result / _make_success_result 减少重复
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

MAX_OUTPUT_LENGTH = 16_000
TOOL_TIMEOUT = 60
_ERROR_HINT = "\n\n[Analyze the error above and try a different approach.]"


class ToolExecutor:
    """执行工具调用并发射事件。支持参数校验和沙箱隔离。"""

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
        """执行单个工具调用，返回 tool_result 内容块。"""
        await emitter.emit_tool_start(tool_call_id, tool_name, input_json)
        start = time.time()

        tool_def = self._registry.get(tool_name)
        if tool_def is None:
            available = ", ".join(self._registry.names())
            return await self._emit_error(
                emitter, tool_call_id, tool_name, start,
                f"Error: unknown tool '{tool_name}'. Available: {available}",
            )

        try:
            params = json.loads(input_json)
        except json.JSONDecodeError as e:
            return await self._emit_error(
                emitter, tool_call_id, tool_name, start,
                f"Error: invalid tool input JSON: {e}",
            )

        params = ToolRegistry.cast_params(params, tool_def.input_schema)

        errors = ToolRegistry.validate_params(params, tool_def.input_schema)
        if errors:
            return await self._emit_error(
                emitter, tool_call_id, tool_name, start,
                f"Error: invalid parameters for '{tool_name}': {'; '.join(errors)}",
            )

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
            await emitter.emit_tool_result(
                tool_call_id, tool_name, True, result, duration_ms,
            )
            return {"content": result}

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

    async def _emit_error(
        self,
        emitter: RunEmitter,
        tool_call_id: str,
        tool_name: str,
        start: float,
        output: str,
    ) -> dict[str, Any]:
        """统一的错误返回 + 事件发射。"""
        output_with_hint = output + _ERROR_HINT
        duration_ms = int((time.time() - start) * 1000)
        await emitter.emit_tool_result(
            tool_call_id, tool_name, False, output_with_hint, duration_ms,
        )
        return {"content": output_with_hint, "is_error": True}

    async def _execute_in_sandbox(self, tool_name: str, params: dict) -> str:
        """通过 SandboxManager 在沙箱中执行工具。"""
        assert self._sandbox_manager is not None
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
                raise ValueError(
                    f"sandbox execution not supported for tool: {tool_name}"
                )
        finally:
            await self._sandbox_manager.release(sandbox)
