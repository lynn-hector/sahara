"""Sandbox 抽象接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod


class Sandbox(ABC):
    """单个沙箱实例的抽象。"""

    @abstractmethod
    async def exec(self, command: str, timeout: int = 30) -> tuple[str, int]:
        """在沙箱中执行命令, 返回 (output, exit_code)。"""

    @abstractmethod
    async def read_file(self, path: str) -> str:
        """读取沙箱内的文件。"""

    @abstractmethod
    async def write_file(self, path: str, content: str) -> None:
        """写入文件到沙箱。"""

    @abstractmethod
    async def cleanup(self) -> None:
        """清理沙箱资源。"""

    @property
    @abstractmethod
    def sandbox_id(self) -> str:
        """沙箱标识。"""


class SandboxManager(ABC):
    """沙箱池管理器抽象。"""

    @abstractmethod
    async def acquire(self) -> Sandbox:
        """从池中获取一个可用沙箱。"""

    @abstractmethod
    async def release(self, sandbox: Sandbox) -> None:
        """归还沙箱到池中。"""

    @abstractmethod
    async def startup(self) -> None:
        """预热沙箱池。"""

    @abstractmethod
    async def shutdown(self) -> None:
        """清理所有沙箱。"""

    @property
    @abstractmethod
    def pool_stats(self) -> dict[str, int]:
        """返回池统计: idle, in_use, total。"""
