"""DockerSandbox — Docker 容器池沙箱实现。

Phase 1: 预创建 N 个 idle 容器, 分配/回收/补充。
"""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING

import structlog

from sahara_runtime.sandbox.base import Sandbox, SandboxManager

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)


class DockerSandbox(Sandbox):
    """基于 Docker 容器的沙箱实例。"""

    def __init__(self, container_id: str, docker_client: any) -> None:
        self._container_id = container_id
        self._docker = docker_client

    async def exec(self, command: str, timeout: int = 30) -> tuple[str, int]:
        loop = asyncio.get_event_loop()
        container = self._docker.containers.get(self._container_id)

        def _exec():
            result = container.exec_run(
                ["sh", "-c", command],
                demux=False,
            )
            output = result.output.decode(errors="replace") if result.output else ""
            return output, result.exit_code

        try:
            output, exit_code = await asyncio.wait_for(
                loop.run_in_executor(None, _exec),
                timeout=timeout,
            )
            return output, exit_code
        except TimeoutError:
            return f"Command timed out after {timeout}s", -1

    async def read_file(self, path: str) -> str:
        output, _ = await self.exec(f"cat {path}")
        return output

    async def write_file(self, path: str, content: str) -> None:
        import base64

        encoded = base64.b64encode(content.encode()).decode()
        await self.exec(f"echo {encoded} | base64 -d > {path}")

    async def cleanup(self) -> None:
        await self.exec("rm -rf /workspace/*")

    @property
    def sandbox_id(self) -> str:
        return self._container_id[:12]


class DockerSandboxManager(SandboxManager):
    """Docker 容器池管理。"""

    def __init__(
        self,
        image: str = "sahara-sandbox:latest",
        pool_size: int = 4,
    ) -> None:
        self._image = image
        self._pool_size = pool_size
        self._idle: list[DockerSandbox] = []
        self._in_use: set[str] = set()
        self._docker = None

    async def startup(self) -> None:
        import docker

        self._docker = docker.from_env()

        logger.info("warming sandbox pool", image=self._image, pool_size=self._pool_size)

        for _ in range(self._pool_size):
            try:
                sandbox = await self._create_container()
                self._idle.append(sandbox)
            except Exception:
                logger.exception("failed to create sandbox container")
                break

        logger.info("sandbox pool ready", idle=len(self._idle))

    async def _create_container(self) -> DockerSandbox:
        loop = asyncio.get_event_loop()

        def _create():
            container = self._docker.containers.run(
                self._image,
                command="sleep infinity",
                detach=True,
                mem_limit="256m",
                cpu_period=100000,
                cpu_quota=50000,
                network_mode="none",
                working_dir="/workspace",
                labels={"sahara.sandbox": "true"},
            )
            return container.id

        container_id = await loop.run_in_executor(None, _create)
        return DockerSandbox(container_id, self._docker)

    async def acquire(self) -> DockerSandbox:
        if not self._idle:
            sandbox = await self._create_container()
        else:
            sandbox = self._idle.pop()

        self._in_use.add(sandbox.sandbox_id)
        return sandbox

    async def release(self, sandbox: Sandbox) -> None:
        assert isinstance(sandbox, DockerSandbox)
        self._in_use.discard(sandbox.sandbox_id)
        await sandbox.cleanup()
        self._idle.append(sandbox)

        # Replenish pool if below target
        while len(self._idle) < self._pool_size // 2:
            try:
                new_sb = await self._create_container()
                self._idle.append(new_sb)
            except Exception:
                break

    async def shutdown(self) -> None:
        logger.info("shutting down sandbox pool")
        loop = asyncio.get_event_loop()

        all_sandboxes = list(self._idle)
        self._idle.clear()

        for sb in all_sandboxes:
            try:

                def _remove(cid=sb._container_id):
                    container = self._docker.containers.get(cid)
                    container.remove(force=True)

                await loop.run_in_executor(None, _remove)
            except Exception:
                logger.exception("failed to remove container", container=sb.sandbox_id)

    @property
    def pool_stats(self) -> dict[str, int]:
        return {
            "idle": len(self._idle),
            "in_use": len(self._in_use),
            "total": len(self._idle) + len(self._in_use),
        }
