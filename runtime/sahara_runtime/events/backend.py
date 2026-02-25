"""EventBackend 抽象接口 — 所有 MQ 实现必须实现此接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod


class EventBackend(ABC):
    @abstractmethod
    async def publish(self, topic: str, data: bytes) -> None:
        """发布一条序列化后的事件到指定 topic。"""

    @abstractmethod
    async def health_check(self) -> bool:
        """检查后端连接是否健康。"""

    @abstractmethod
    async def close(self) -> None:
        """优雅关闭连接。"""

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """后端名称, 用于日志和指标标签。"""
