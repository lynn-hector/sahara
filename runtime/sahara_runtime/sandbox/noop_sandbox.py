"""NoopSandbox — 测试用沙箱, 直接在本地执行。"""

from __future__ import annotations

import asyncio
import os
import uuid

from sahara_runtime.sandbox.base import Sandbox, SandboxManager


class NoopSandbox(Sandbox):
    """直接在本地进程中执行, 不隔离。用于开发和测试。"""

    def __init__(self) -> None:
        self._id = f"noop-{uuid.uuid4().hex[:8]}"

    async def exec(self, command: str, timeout: int = 30) -> tuple[str, int]:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            return f"Command timed out after {timeout}s", -1

        output = ""
        if stdout:
            output += stdout.decode(errors="replace")
        if stderr:
            output += stderr.decode(errors="replace")
        return output, proc.returncode or 0

    async def read_file(self, path: str) -> str:
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
        if os.path.isdir(path):
            entries = os.listdir(path)
            return f"Directory listing ({len(entries)} entries):\n" + "\n".join(
                entries[:200]
            )
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()

    async def write_file(self, path: str, content: str) -> None:
        path = os.path.expanduser(path)
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    async def cleanup(self) -> None:
        pass

    @property
    def sandbox_id(self) -> str:
        return self._id


class NoopSandboxManager(SandboxManager):
    """NoopSandbox 管理器 — 每次 acquire 返回新实例。"""

    def __init__(self) -> None:
        self._count = 0

    async def acquire(self) -> NoopSandbox:
        self._count += 1
        return NoopSandbox()

    async def release(self, sandbox: Sandbox) -> None:
        self._count = max(0, self._count - 1)
        await sandbox.cleanup()

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    @property
    def pool_stats(self) -> dict[str, int]:
        return {"idle": 0, "in_use": self._count, "total": self._count}
