"""ContextManager — 上下文管理统一外观 + 编排器。

agent_loop 唯一交互对象，内部编排:
- PromptBuilder: system prompt 构建
- ContextBuilder: 消息编排 + 上下文裁剪
- TokenCounter: token 计算
- SkillsManager: skills prompt 生成
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from sahara_runtime.context.context_builder import ContextBuilder
from sahara_runtime.context.prompt_builder import PromptBuilder
from sahara_runtime.context.token_counter import TokenCounter

if TYPE_CHECKING:
    from sahara_runtime.llm.types import LLMResponse
    from sahara_runtime.memory.session_store import SessionMessage
    from sahara_runtime.skills.manager import SkillsManager
    from sahara_runtime.tools.registry import ToolRegistry

class ContextManager:
    """上下文管理统一外观 — agent_loop 的唯一入口。

    编排 PromptBuilder + ContextBuilder + SkillsManager，
    让 agent_loop 不直接接触内部组件。
    Skills prompt 仅作为内部 system prompt 组装细节。
    """

    def __init__(
        self,
        workspace: Path | None = None,
        token_counter: TokenCounter | None = None,
        skills_manager: SkillsManager | None = None,
        tool_registry: ToolRegistry | None = None,
    ) -> None:
        self._token_counter = token_counter or TokenCounter()
        self._prompt_builder = PromptBuilder(workspace)
        self._context_builder = ContextBuilder(self._token_counter)
        self._skills_manager = skills_manager
        self._tool_registry = tool_registry

    # ── 高层编排 API ─────────────────────────────────

    async def build_run_context(
        self,
        *,
        session_key: str,
        model: str,
        max_iterations: int,
        history: list[dict[str, Any]],
        user_message: str,
        tools: list[dict[str, Any]] | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        """一次性构建 system_prompt + messages。

        Returns:
            (system_prompt, messages)
        """
        if tools is None and self._tool_registry:
            tools = self._tool_registry.get_definitions()

        skills_prompt = await self._build_skills_prompt_section()
        system_prompt = self._prompt_builder.build(
            session_key=session_key,
            model=model,
            max_iterations=max_iterations,
            tools=tools,
            skills_prompt=skills_prompt,
        )
        messages = self._context_builder.build_messages(history, user_message)
        return system_prompt, messages

    def count_total_tokens(
        self,
        *,
        messages: list[dict[str, Any]],
        system_prompt: str,
    ) -> int:
        """计算 messages + system_prompt 的总 token 数。"""
        return (
            self._token_counter.count_messages(messages)
            + self._token_counter.count_system(system_prompt)
        )

    def get_context_stats(
        self,
        *,
        messages: list[dict[str, Any]],
        max_context_tokens: int,
        system_prompt: str,
    ) -> dict[str, Any]:
        """返回裁剪前的上下文统计信息。"""
        input_tokens = self.count_total_tokens(
            messages=messages, system_prompt=system_prompt,
        )
        original_count = len(messages)
        return {
            "input_tokens": input_tokens,
            "original_count": original_count,
            "needs_trim": input_tokens > max_context_tokens,
        }

    def fit_context_with_stats(
        self,
        *,
        messages: list[dict[str, Any]],
        max_context_tokens: int,
        system_prompt: str,
    ) -> dict[str, Any]:
        """裁剪上下文并返回统计信息。

        Returns:
            {
                "messages": list[dict],
                "trimmed": bool,
                "pre_trim_total_tokens": int,
                "post_trim_total_tokens": int,
                "original_count": int,
                "remaining_count": int,
            }
        """
        stats = self.get_context_stats(
            messages=messages,
            max_context_tokens=max_context_tokens,
            system_prompt=system_prompt,
        )
        input_tokens = stats["input_tokens"]
        original_count = stats["original_count"]

        if not stats["needs_trim"]:
            return {
                "messages": messages,
                "trimmed": False,
                "pre_trim_total_tokens": input_tokens,
                "post_trim_total_tokens": input_tokens,
                "original_count": original_count,
                "remaining_count": original_count,
            }

        trimmed = self._context_builder.fit_context(
            messages,
            max_context_tokens=max_context_tokens,
            system_prompt=system_prompt,
        )
        trimmed_tokens = self.count_total_tokens(
            messages=trimmed,
            system_prompt=system_prompt,
        )
        return {
            "messages": trimmed,
            "trimmed": True,
            "pre_trim_total_tokens": input_tokens,
            "post_trim_total_tokens": trimmed_tokens,
            "original_count": original_count,
            "remaining_count": len(trimmed),
        }

    def build_system_prompt(
        self,
        *,
        session_key: str,
        model: str,
        max_iterations: int,
        tools: list[dict[str, Any]] | None = None,
        skills_prompt: str = "",
    ) -> str:
        """直接构建 system prompt（用于测试和独立预览）。"""
        return self._prompt_builder.build(
            session_key=session_key,
            model=model,
            max_iterations=max_iterations,
            tools=tools,
            skills_prompt=skills_prompt,
        )

    async def _build_skills_prompt_section(self) -> str:
        """生成 system prompt 内部使用的 skills 段落。"""
        if not self._skills_manager:
            return ""
        return await self._skills_manager.build_prompt()

    # ── 消息编排（代理到 ContextBuilder）────────────

    def build_messages(
        self,
        history: list[dict[str, Any]],
        user_message: str,
    ) -> list[dict[str, Any]]:
        return self._context_builder.build_messages(history, user_message)

    def add_assistant_message(
        self,
        messages: list[dict[str, Any]],
        response: LLMResponse,
    ) -> list[dict[str, Any]]:
        return self._context_builder.add_assistant_message(messages, response)

    def add_tool_result(
        self,
        messages: list[dict[str, Any]],
        tool_call_id: str,
        name: str,
        content: str,
    ) -> list[dict[str, Any]]:
        return self._context_builder.add_tool_result(
            messages, tool_call_id, name, content,
        )

    def fit_context(
        self,
        messages: list[dict[str, Any]],
        *,
        max_context_tokens: int,
        system_prompt: str,
    ) -> list[dict[str, Any]]:
        return self._context_builder.fit_context(
            messages,
            max_context_tokens=max_context_tokens,
            system_prompt=system_prompt,
        )

    @staticmethod
    def session_msg_to_dict(msg: SessionMessage) -> dict[str, Any]:
        return ContextBuilder.session_msg_to_dict(msg)
