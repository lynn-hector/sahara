"""E2BSandbox — E2B 云端沙箱实现。

使用 E2B (e2b.dev) 提供的云端隔离 VM 作为代码执行沙箱。
适用于联调和生产环境，无需本地 Docker 镜像。

配置:
    SAHARA_SANDBOX_PROVIDER=e2b
    SAHARA_E2B_API_KEY=your-api-key
    SAHARA_E2B_TEMPLATE=base          # 可选, 自定义沙箱模板
    SAHARA_E2B_TIMEOUT=300            # 沙箱存活时间 (秒)
"""

from __future__ import annotations

import asyncio

import structlog

from sahara_runtime.sandbox.base import Sandbox, SandboxManager

logger = structlog.get_logger(__name__)


class E2BSandbox(Sandbox):
    """基于 E2B 云端 VM 的沙箱实例。"""

    def __init__(self, sbx: object) -> None:
        self._sbx = sbx

    async def exec(self, command: str, timeout: int = 30) -> tuple[str, int]:
        loop = asyncio.get_event_loop()

        def _run():
            result = self._sbx.commands.run(cmd=command, timeout=float(timeout))
            stdout = result.stdout or ""
            stderr = result.stderr or ""
            output = stdout
            if stderr:
                output = f"{stdout}\n{stderr}" if stdout else stderr
            return output, result.exit_code

        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, _run),
                timeout=timeout + 5,
            )
        except TimeoutError:
            return f"Command timed out after {timeout}s", -1
        except Exception as exc:
            logger.error("e2b_exec_error", command=command[:100], error=str(exc))
            return f"E2B exec error: {exc}", -1

    async def read_file(self, path: str) -> str:
        loop = asyncio.get_event_loop()

        def _read():
            return self._sbx.files.read(path, format="text")

        return await loop.run_in_executor(None, _read)

    async def write_file(self, path: str, content: str) -> None:
        loop = asyncio.get_event_loop()

        def _write():
            self._sbx.files.write(path, content)

        await loop.run_in_executor(None, _write)

    async def cleanup(self) -> None:
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._sbx.commands.run("rm -rf /home/user/workspace/*", timeout=10),
            )
        except Exception:
            logger.debug("e2b_cleanup_error", sandbox_id=self.sandbox_id, exc_info=True)

    @property
    def sandbox_id(self) -> str:
        return getattr(self._sbx, "sandbox_id", "e2b-unknown")


class E2BSandboxManager(SandboxManager):
    """E2B 云端沙箱池管理。

    与 DockerSandboxManager 不同，E2B 沙箱按需创建，无需预热。
    每个 acquire() 创建一个新的云端 VM，release() 时可选择保留或销毁。
    """

    def __init__(
        self,
        api_key: str,
        template: str = "base",
        timeout: int = 300,
        pool_size: int = 2,
    ) -> None:
        self._api_key = api_key
        self._template = template
        self._timeout = timeout
        self._pool_size = pool_size
        self._idle: list[E2BSandbox] = []
        self._in_use: set[str] = set()

    async def startup(self) -> None:
        from e2b import Sandbox as E2BSbx

        logger.info(
            "e2b sandbox manager starting",
            template=self._template,
            timeout=self._timeout,
            pool_size=self._pool_size,
        )

        loop = asyncio.get_event_loop()
        for i in range(self._pool_size):
            try:
                sbx = await loop.run_in_executor(
                    None,
                    lambda: E2BSbx.create(
                        template=self._template,
                        timeout=self._timeout,
                        api_key=self._api_key,
                    ),
                )
                self._idle.append(E2BSandbox(sbx))
            except Exception:
                logger.warning("e2b_sandbox_create_failed", attempt=i + 1, exc_info=True)
                break

        logger.info("e2b sandbox pool ready", idle=len(self._idle))

    async def acquire(self) -> E2BSandbox:
        if self._idle:
            sandbox = self._idle.pop()
        else:
            from e2b import Sandbox as E2BSbx

            loop = asyncio.get_event_loop()
            sbx = await loop.run_in_executor(
                None,
                lambda: E2BSbx.create(
                    template=self._template,
                    timeout=self._timeout,
                    api_key=self._api_key,
                ),
            )
            sandbox = E2BSandbox(sbx)

        self._in_use.add(sandbox.sandbox_id)
        return sandbox

    async def release(self, sandbox: Sandbox) -> None:
        assert isinstance(sandbox, E2BSandbox)
        self._in_use.discard(sandbox.sandbox_id)
        await sandbox.cleanup()

        if len(self._idle) < self._pool_size:
            self._idle.append(sandbox)
        else:
            await self._kill_sandbox(sandbox)

    async def shutdown(self) -> None:
        logger.info("shutting down e2b sandbox pool", count=len(self._idle))
        for sb in list(self._idle):
            await self._kill_sandbox(sb)
        self._idle.clear()

    async def _kill_sandbox(self, sandbox: E2BSandbox) -> None:
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, lambda: sandbox._sbx.kill())
        except Exception:
            logger.debug("e2b_kill_error", sandbox_id=sandbox.sandbox_id, exc_info=True)

    @property
    def pool_stats(self) -> dict[str, int]:
        return {
            "idle": len(self._idle),
            "in_use": len(self._in_use),
            "total": len(self._idle) + len(self._in_use),
        }
