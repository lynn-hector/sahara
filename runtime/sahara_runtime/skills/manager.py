"""SkillsManager — 统一编排技能加载、过滤、摘要、同步与 prompt 生成。"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from sahara_runtime.skills.sync import sync_skills_to_sandbox

if TYPE_CHECKING:
    from sahara_runtime.sandbox.base import Sandbox
    from sahara_runtime.skills.filter import SkillFilter
    from sahara_runtime.skills.loader import SkillLoader
    from sahara_runtime.skills.types import SkillEntry

logger = structlog.get_logger(__name__)


class SkillsManager:
    """统一封装 SkillLoader + SkillFilter 的高层入口。"""

    def __init__(
        self,
        loader: SkillLoader | None = None,
        skill_filter: SkillFilter | None = None,
    ) -> None:
        self._loader = loader
        self._filter = skill_filter

    async def load_all(self) -> list[SkillEntry]:
        """加载全部技能。未配置 loader 时返回空列表。"""
        if not self._loader:
            return []
        return await self._loader.load_all()

    async def load_active(self) -> list[SkillEntry]:
        """加载并过滤当前可用技能。"""
        entries = await self.load_all()
        if not self._filter:
            return entries
        return self._filter.filter(entries)

    async def summarize_active(self) -> list[dict[str, Any]]:
        """返回当前可用技能的简要摘要，便于诊断和调试。"""
        active_skills = await self.load_active()
        return [
            {
                "name": entry.name,
                "description": entry.description,
                "location": self._resolve_location(entry.file_path),
                "tags": list(entry.metadata.tags) if entry.metadata else [],
            }
            for entry in active_skills
        ]

    async def sync_to_sandbox(self, sandbox: Sandbox | None) -> int:
        """将当前可用技能同步到沙箱。未提供沙箱时为 no-op。"""
        if sandbox is None:
            return 0
        active_skills = await self.load_active()
        return await sync_skills_to_sandbox(active_skills, sandbox)

    async def build_prompt(self) -> str:
        """生成当前可用技能的 prompt 片段。"""
        active_skills = await self.load_active()
        prompt_entries = [
            entry for entry in active_skills
            if not (entry.invocation and entry.invocation.disable_model_invocation)
        ]

        if not prompt_entries:
            return ""

        prompt_entries = sorted(prompt_entries, key=lambda entry: entry.sort_key)
        lines = [
            "## Skills (mandatory)",
            "Before replying: scan <available_skills> <description> entries.",
            (
                "- If exactly one skill clearly applies: read its SKILL.md at "
                "<location> with `read_file`, then follow it."
            ),
            "- If multiple could apply: choose the most specific one, then read/follow it.",
            "- If none clearly apply: do not read any SKILL.md.",
            "Constraints: never read more than one skill up front; only read after selecting.",
            "",
            "<available_skills>",
        ]

        for entry in prompt_entries:
            location = self._resolve_location(entry.file_path)
            tag_attr = ""
            if entry.metadata and entry.metadata.tags:
                tag_attr = f' tags="{",".join(entry.metadata.tags)}"'
            lines.append(
                f'<skill name="{entry.name}" location="{location}"{tag_attr}>'
                f"\n  <description>{entry.description}</description>"
                f"\n</skill>"
            )

        lines.append("</available_skills>")
        prompt = "\n".join(lines)
        logger.debug(
            "skills_prompt_built",
            active_count=len(prompt_entries),
            enabled=bool(prompt),
        )
        return prompt

    @staticmethod
    def _resolve_location(path: str) -> str:
        if path:
            try:
                return str(Path(path).expanduser().resolve())
            except Exception:
                return path
        return path
