"""
错误分类与弹性 — 统一异常体系

参考: D4 §16 错误处理与弹性
"""

from __future__ import annotations


class SaharaError(Exception):
    """Sahara Runtime 基础异常."""

    def __init__(self, message: str, *, error_code: str = "SAHARA_INTERNAL_ERROR"):
        super().__init__(message)
        self.error_code = error_code


class TaskNotFoundError(SaharaError):
    """任务未找到."""

    def __init__(self, task_id: str):
        super().__init__(f"task not found: {task_id}", error_code="SAHARA_RT_TASK_NOT_FOUND")


class WorkerBusyError(SaharaError):
    """Worker 过载."""

    def __init__(self):
        super().__init__("worker is busy", error_code="SAHARA_RT_BUSY")


class LLMError(SaharaError):
    """LLM 调用失败."""

    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message, error_code="SAHARA_RT_LLM_ERROR")
        self.retryable = retryable


class ToolExecutionError(SaharaError):
    """工具执行失败."""

    def __init__(self, tool_name: str, message: str):
        super().__init__(f"tool '{tool_name}' failed: {message}", error_code="SAHARA_RT_TOOL_ERROR")
        self.tool_name = tool_name


class SandboxError(SaharaError):
    """沙箱错误."""

    def __init__(self, message: str):
        super().__init__(message, error_code="SAHARA_RT_SANDBOX_ERROR")
