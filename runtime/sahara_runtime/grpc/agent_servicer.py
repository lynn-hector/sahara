"""
AgentService gRPC 实现 — 接收 Gateway 提交的任务并驱动 Agent Loop

参考: D4 §3 gRPC Server 设计
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import grpc
import structlog
from sahara.agent.v1 import agent_pb2, agent_pb2_grpc
from sahara.common.v1 import common_pb2

if TYPE_CHECKING:
    from sahara_runtime.di.container import Container

logger = structlog.get_logger(__name__)


@dataclass
class UserInput:
    """用户通过 SendInput RPC 投递的输入。"""

    action: str  # "approve" / "reject" / "input"
    text: str = ""
    task_id: str = ""


@dataclass
class TaskHandle:
    """Runtime 内存中的活跃任务句柄。"""

    task: asyncio.Task
    task_id: str
    run_id: str
    session_key: str
    agent_id: str
    started_at: float
    state: int = common_pb2.TASK_STATE_RUNNING
    current_iteration: int = 0
    total_tokens: int = 0
    error_message: str = ""

    # 人机交互 (P2-15)
    input_channel: asyncio.Queue | None = None
    waiting_for_input: bool = False


class AgentServicer(agent_pb2_grpc.AgentServiceServicer):
    """实现 AgentService RPC。"""

    def __init__(self, container: Container) -> None:
        self._container = container
        self._max_tasks = container.settings.max_concurrent_tasks
        self._tasks: dict[str, TaskHandle] = {}
        self._draining = False
        self._drain_event: asyncio.Event | None = None

    @property
    def active_count(self) -> int:
        return len(self._tasks)

    @property
    def available_slots(self) -> int:
        return max(0, self._max_tasks - len(self._tasks))

    @property
    def is_draining(self) -> bool:
        return self._draining

    def start_drain(self) -> None:
        """Enter drain mode — reject new tasks, signal when all complete."""
        self._draining = True
        self._drain_event = asyncio.Event()
        if not self._tasks:
            self._drain_event.set()

    async def wait_drain(self, timeout: float = 60) -> int:
        """Wait for all tasks to complete (up to timeout). Returns remaining count."""
        if self._drain_event is None:
            return 0
        try:
            await asyncio.wait_for(self._drain_event.wait(), timeout=timeout)
        except TimeoutError:
            pass
        return len(self._tasks)

    async def SubmitTask(
        self,
        request: agent_pb2.SubmitTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> agent_pb2.SubmitTaskResponse:
        if self._draining:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "worker is draining")

        if self.available_slots <= 0:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "worker is busy")

        task_id = request.task_id or str(uuid.uuid4())
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        session_key = request.session_key

        user_text = ""
        if request.HasField("user_message"):
            user_text = request.user_message.text
        if not user_text:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "user_message.text is required")

        logger.info(
            "submit_task",
            task_id=task_id,
            run_id=run_id,
            session_key=session_key,
            agent_id=request.agent_id,
        )

        handle = TaskHandle(
            task=None,  # set below after task creation
            task_id=task_id,
            run_id=run_id,
            session_key=session_key,
            agent_id=request.agent_id,
            started_at=time.time(),
        )
        self._tasks[task_id] = handle

        from sahara_runtime.agent_loop import run_agent_loop

        coro = run_agent_loop(
            task_id=task_id,
            run_id=run_id,
            session_key=session_key,
            user_message=user_text,
            agent_id=request.agent_id,
            metadata=dict(request.metadata),
            container=self._container,
            on_complete=lambda tid: self._on_task_done(tid),
            task_handle=handle,
        )

        async_task = asyncio.create_task(coro, name=f"agent-{task_id}")
        handle.task = async_task

        return agent_pb2.SubmitTaskResponse(
            run_id=run_id,
            worker_id=self._container.settings.worker_id,
            accepted_at_ms=int(time.time() * 1000),
        )

    async def AbortTask(
        self,
        request: agent_pb2.AbortTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> agent_pb2.AbortTaskResponse:
        handle = self._tasks.get(request.task_id)
        if handle is None:
            return agent_pb2.AbortTaskResponse(
                previous_state=common_pb2.TASK_STATE_UNSPECIFIED,
            )

        prev_state = handle.state
        handle.state = common_pb2.TASK_STATE_ABORTED
        handle.task.cancel()

        logger.info("abort_task", task_id=request.task_id, reason=request.reason)

        return agent_pb2.AbortTaskResponse(previous_state=prev_state)

    async def GetTaskStatus(
        self,
        request: agent_pb2.GetTaskStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> agent_pb2.GetTaskStatusResponse:
        handle = self._tasks.get(request.task_id)
        if handle is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "task not found")

        return agent_pb2.GetTaskStatusResponse(
            task_id=handle.task_id,
            run_id=handle.run_id,
            session_key=handle.session_key,
            state=handle.state,
            started_at_ms=int(handle.started_at * 1000),
            current_iteration=handle.current_iteration,
            total_tokens_used=handle.total_tokens,
            error_message=handle.error_message,
        )

    async def ListActiveTasks(
        self,
        request: agent_pb2.ListActiveTasksRequest,
        context: grpc.aio.ServicerContext,
    ) -> agent_pb2.ListActiveTasksResponse:
        tasks = []
        for h in self._tasks.values():
            if request.session_key_filter and h.session_key != request.session_key_filter:
                continue
            tasks.append(
                agent_pb2.ActiveTask(
                    task_id=h.task_id,
                    run_id=h.run_id,
                    session_key=h.session_key,
                    state=h.state,
                    started_at_ms=int(h.started_at * 1000),
                    current_iteration=h.current_iteration,
                )
            )
        return agent_pb2.ListActiveTasksResponse(tasks=tasks)

    async def SendInput(
        self,
        request: agent_pb2.SendInputRequest,
        context: grpc.aio.ServicerContext,
    ) -> agent_pb2.SendInputResponse:
        handle = self._tasks.get(request.task_id)
        if handle is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "task not found")

        if not handle.waiting_for_input or handle.input_channel is None:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "task is not waiting for input",
            )

        user_input = UserInput(
            action=request.action,
            text=request.input,
            task_id=request.task_id,
        )

        try:
            handle.input_channel.put_nowait(user_input)
        except asyncio.QueueFull:
            await context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                "input channel is full",
            )

        logger.info(
            "send_input_delivered",
            task_id=request.task_id,
            action=request.action,
        )
        return agent_pb2.SendInputResponse(delivered=True)

    def _on_task_done(self, task_id: str) -> None:
        handle = self._tasks.pop(task_id, None)
        if handle:
            logger.info("task_done", task_id=task_id, run_id=handle.run_id)

        # Signal drain completion when all tasks finish
        if self._draining and not self._tasks and self._drain_event:
            self._drain_event.set()
            logger.info("drain_complete")
