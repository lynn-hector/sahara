"""
WorkerService gRPC 实现 — Worker 负载查询、排空、热配置

参考: D4 §3 gRPC Server 设计, worker.proto
"""

from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING

import grpc
import structlog

from sahara.worker.v1 import worker_pb2, worker_pb2_grpc

if TYPE_CHECKING:
    from sahara_runtime.grpc.agent_servicer import AgentServicer
    from sahara_runtime.di.container import Container

logger = structlog.get_logger(__name__)

VERSION = "0.1.0"


class WorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    """实现 WorkerService RPC — Gateway 用于负载感知和 Worker 管理。"""

    def __init__(self, container: Container, agent_servicer: AgentServicer) -> None:
        self._container = container
        self._agent_servicer = agent_servicer
        self._state = worker_pb2.WORKER_STATE_READY
        self._start_time = time.time()
        self._draining = False

    async def GetStatus(
        self,
        request: worker_pb2.GetStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> worker_pb2.GetStatusResponse:
        cpu_percent, mem_percent, mem_bytes = self._get_resource_usage()

        sandbox_stats = {"idle": 0, "in_use": 0, "total": 0}
        if self._container.sandbox_manager:
            sandbox_stats = self._container.sandbox_manager.pool_stats

        return worker_pb2.GetStatusResponse(
            worker_id=self._container.settings.worker_id,
            active_tasks=self._agent_servicer.active_count,
            max_tasks=self._container.settings.max_concurrent_tasks,
            queued_tasks=0,
            cpu_usage_percent=cpu_percent,
            memory_usage_percent=mem_percent,
            memory_used_bytes=mem_bytes,
            sandbox_pool_idle=sandbox_stats.get("idle", 0),
            sandbox_pool_in_use=sandbox_stats.get("in_use", 0),
            sandbox_pool_total=sandbox_stats.get("total", 0),
            state=self._state,
            uptime_seconds=int(time.time() - self._start_time),
            version=VERSION,
        )

    async def Drain(
        self,
        request: worker_pb2.DrainRequest,
        context: grpc.aio.ServicerContext,
    ) -> worker_pb2.DrainResponse:
        self._state = worker_pb2.WORKER_STATE_DRAINING
        self._draining = True

        timeout_s = request.timeout_seconds or 60

        # Enter drain mode — rejects new SubmitTask calls
        self._agent_servicer.start_drain()

        remaining_before = self._agent_servicer.active_count
        logger.info(
            "worker_draining",
            remaining_tasks=remaining_before,
            timeout_seconds=timeout_s,
        )

        # Wait for running tasks to complete (non-blocking for the gRPC response)
        remaining_after = await self._agent_servicer.wait_drain(timeout=timeout_s)
        estimated_complete_ms = int(time.time() * 1000)

        if remaining_after == 0:
            self._state = worker_pb2.WORKER_STATE_OFFLINE
            logger.info("worker_drained_clean")
        else:
            logger.warning("worker_drain_timeout", remaining=remaining_after)

        return worker_pb2.DrainResponse(
            remaining_tasks=remaining_after,
            estimated_complete_at_ms=estimated_complete_ms,
        )

    async def UpdateConfig(
        self,
        request: worker_pb2.UpdateConfigRequest,
        context: grpc.aio.ServicerContext,
    ) -> worker_pb2.UpdateConfigResponse:
        settings = self._container.settings

        if request.max_tasks > 0:
            self._agent_servicer._max_tasks = request.max_tasks
            logger.info("config_updated", max_tasks=request.max_tasks)

        if request.log_level:
            settings.log_level = request.log_level
            logger.info("config_updated", log_level=request.log_level)

        current = {
            "worker_id": settings.worker_id,
            "max_tasks": self._agent_servicer._max_tasks,
            "log_level": settings.log_level,
            "default_model": settings.default_model,
            "sandbox_enabled": settings.sandbox_enabled,
        }

        return worker_pb2.UpdateConfigResponse(
            current_config_json=json.dumps(current),
        )

    @property
    def is_draining(self) -> bool:
        return self._draining

    @staticmethod
    def _get_resource_usage() -> tuple[float, float, int]:
        """获取当前进程的 CPU 和内存使用率。

        优先使用 psutil (精确), 降级到 resource 模块 (仅内存峰值)。
        macOS 和 Linux 的 ru_maxrss 单位不同, 需分别处理。

        Returns:
            (cpu_percent, memory_percent, memory_bytes)
        """
        try:
            import resource

            rusage = resource.getrusage(resource.RUSAGE_SELF)
            mem_bytes = rusage.ru_maxrss
            if os.uname().sysname == "Darwin":
                pass  # macOS ru_maxrss is already in bytes
            else:
                mem_bytes *= 1024  # Linux ru_maxrss is in KB

            try:
                import psutil

                proc = psutil.Process()
                cpu_percent = proc.cpu_percent(interval=0)
                mem_info = proc.memory_info()
                mem_bytes = mem_info.rss
                total_mem = psutil.virtual_memory().total
                mem_percent = (mem_bytes / total_mem * 100) if total_mem else 0
                return cpu_percent, mem_percent, mem_bytes
            except ImportError:
                return 0.0, 0.0, mem_bytes

        except Exception:
            logger.debug("resource_usage_error", exc_info=True)
            return 0.0, 0.0, 0
