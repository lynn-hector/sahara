"""
gRPC Server 入口 — Sahara Runtime Worker

启动方式: python -m sahara_runtime.server
参考: D4 §3 gRPC Server 设计
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from concurrent import futures

# Ensure generated protobuf code is importable
_gen_path = os.path.join(os.path.dirname(__file__), "..", "gen")
if os.path.isdir(_gen_path) and _gen_path not in sys.path:
    sys.path.insert(0, os.path.abspath(_gen_path))

import grpc
import structlog

from sahara_runtime.config.settings import Settings
from sahara_runtime.di.container import Container

logger = structlog.get_logger(__name__)


async def serve() -> None:
    """启动 gRPC server."""
    settings = Settings()

    container = Container(settings)
    await container.startup()

    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ("grpc.max_send_message_length", 50 * 1024 * 1024),
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
        ],
    )

    # Register AgentService
    from sahara.agent.v1 import agent_pb2_grpc
    from sahara_runtime.grpc.agent_servicer import AgentServicer

    agent_servicer = AgentServicer(container)
    agent_pb2_grpc.add_AgentServiceServicer_to_server(agent_servicer, server)

    # Register WorkerService
    from sahara.worker.v1 import worker_pb2_grpc
    from sahara_runtime.grpc.worker_servicer import WorkerServicer

    worker_servicer = WorkerServicer(container, agent_servicer)
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)

    # gRPC Health Check (standard protocol)
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set(
        "sahara.agent.v1.AgentService",
        health_pb2.HealthCheckResponse.SERVING,
    )
    health_servicer.set(
        "sahara.worker.v1.WorkerService",
        health_pb2.HealthCheckResponse.SERVING,
    )

    listen_addr = f"[::]:{settings.grpc_port}"
    server.add_insecure_port(listen_addr)

    logger.info(
        "sahara-rt starting",
        addr=listen_addr,
        worker_id=settings.worker_id,
        max_tasks=settings.max_concurrent_tasks,
    )

    await server.start()

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()

    logger.info("shutting down gRPC server...")
    await server.stop(grace=30)
    await container.shutdown()
    logger.info("sahara-rt stopped")


def main() -> None:
    """入口函数."""
    try:
        import uvloop

        uvloop.install()
        logger.info("uvloop installed")
    except ImportError:
        logger.warning("uvloop not available, using default event loop")

    asyncio.run(serve())


if __name__ == "__main__":
    main()
