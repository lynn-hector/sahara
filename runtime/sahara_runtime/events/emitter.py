"""EventEmitter — 构造 AgentEvent protobuf 消息并通过 Backend 发布。"""

from __future__ import annotations

import time

import structlog
import ulid
from sahara.event.v1 import event_pb2

from sahara_runtime.events.backend import EventBackend

logger = structlog.get_logger(__name__)


def _inc_event_metric(event_type: int) -> None:
    try:
        from sahara_runtime.observability.metrics import EVENTS_EMITTED
        EVENTS_EMITTED.labels(event_type=str(event_type)).inc()
    except Exception:
        pass


def _inc_event_error_metric() -> None:
    try:
        from sahara_runtime.observability.metrics import EVENT_PUBLISH_ERRORS
        EVENT_PUBLISH_ERRORS.inc()
    except Exception:
        pass


class EventEmitterFactory:
    """工厂: 根据 run 创建 RunEmitter 实例。"""

    def __init__(self, backend: EventBackend, worker_id: str) -> None:
        self._backend = backend
        self._worker_id = worker_id

    def for_run(self, run_id: str, session_key: str, task_id: str) -> RunEmitter:
        return RunEmitter(
            backend=self._backend,
            worker_id=self._worker_id,
            run_id=run_id,
            session_key=session_key,
            task_id=task_id,
        )


class RunEmitter:
    """单次执行的事件发射器, 维护 seq 自增。"""

    def __init__(
        self,
        backend: EventBackend,
        worker_id: str,
        run_id: str,
        session_key: str,
        task_id: str,
    ) -> None:
        self._backend = backend
        self._worker_id = worker_id
        self._run_id = run_id
        self._session_key = session_key
        self._task_id = task_id
        self._seq = 0
        self._topic = f"events:{session_key}"

    async def _emit(self, event_type: event_pb2.EventType, **payload_fields) -> None:
        self._seq += 1
        event = event_pb2.AgentEvent(
            event_id=str(ulid.new()),
            run_id=self._run_id,
            session_key=self._session_key,
            task_id=self._task_id,
            type=event_type,
            timestamp_ms=int(time.time() * 1000),
            seq=self._seq,
        )

        for field_name, value in payload_fields.items():
            if value is not None:
                getattr(event, field_name).CopyFrom(value)

        data = event.SerializeToString()
        try:
            await self._backend.publish(self._topic, data)
            _inc_event_metric(event_type)
        except Exception:
            _inc_event_error_metric()
            logger.exception("event_publish_failed", event_type=event_type, seq=self._seq)

    # ── 便捷方法 ──────────────────────────────────────

    async def emit_run_start(self, agent_id: str, model: str) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_RUN_START,
            run_start=event_pb2.RunStartPayload(
                agent_id=agent_id,
                model=model,
                started_at_ms=int(time.time() * 1000),
            ),
        )

    async def emit_delta(self, text: str, stream: str = "assistant") -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_DELTA,
            delta=event_pb2.DeltaPayload(text=text, stream=stream),
        )

    async def emit_thinking(self, text: str) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_THINKING,
            thinking=event_pb2.ThinkingPayload(text=text),
        )

    async def emit_tool_start(self, tool_call_id: str, tool_name: str, input_json: str) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_TOOL_START,
            tool_start=event_pb2.ToolStartPayload(
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                input_json=input_json,
            ),
        )

    async def emit_tool_result(
        self,
        tool_call_id: str,
        tool_name: str,
        success: bool,
        output: str,
        duration_ms: int,
    ) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_TOOL_RESULT,
            tool_result=event_pb2.ToolResultPayload(
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                success=success,
                output=output,
                duration_ms=duration_ms,
            ),
        )

    async def emit_run_complete(
        self, final_text: str, iterations: int, duration_ms: int
    ) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_RUN_COMPLETE,
            run_complete=event_pb2.RunCompletePayload(
                final_text=final_text,
                iterations=iterations,
                duration_ms=duration_ms,
            ),
        )

    async def emit_run_error(self, error_message: str, retryable: bool = False) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_RUN_ERROR,
            run_error=event_pb2.RunErrorPayload(
                error_code="AGENT_ERROR",
                error_message=error_message,
                retryable=retryable,
            ),
        )

    async def emit_abort(self, reason: str, aborted_by: str = "system") -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_RUN_ABORT,
            run_abort=event_pb2.RunAbortPayload(
                reason=reason,
                aborted_by=aborted_by,
            ),
        )

    async def emit_input_required(
        self,
        prompt: str,
        input_type: str = "text_input",
        timeout_seconds: int = 120,
    ) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_INPUT_REQUIRED,
            input_required=event_pb2.InputRequiredPayload(
                input_type=input_type,
                prompt=prompt,
                timeout_seconds=timeout_seconds,
            ),
        )

    async def emit_tool_confirm_required(
        self,
        tool_call_id: str,
        tool_name: str,
        input_json: str,
        risk_description: str = "",
        timeout_seconds: int = 120,
    ) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_TOOL_CONFIRM_REQUIRED,
            tool_confirm_required=event_pb2.ToolConfirmRequiredPayload(
                input_type="tool_confirm",
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                input_json=input_json,
                risk_description=risk_description,
                timeout_seconds=timeout_seconds,
            ),
        )

    async def emit_usage(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
        iteration: int,
    ) -> None:
        await self._emit(
            event_pb2.EVENT_TYPE_USAGE,
            usage=event_pb2.UsagePayload(
                model=model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                iteration=iteration,
            ),
        )
