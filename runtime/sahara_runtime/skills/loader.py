"""SkillLoader — 从多个目录加载技能，解析 SKILL.md frontmatter，按优先级合并。

参考: D4 §10.4 技能加载

目录扫描顺序 (来源覆盖优先级从低到高):
  bundled < configured < managed

同名技能按来源优先级覆盖 — 托管市场技能可替换业务配置技能和内置技能。
"""

from __future__ import annotations

import os
import re
from typing import Any

import structlog
import yaml

from sahara_runtime.skills.types import (
    InvocationPolicy,
    SkillEntry,
    SkillInstallSpec,
    SkillMetadata,
    SkillRequirements,
    SkillTier,
)

logger = structlog.get_logger(__name__)

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


class SkillLoader:
    """从多个目录加载技能，按优先级合并。"""

    def __init__(
        self,
        bundled_dir: str = "",
        configured_dir: str = "",
        managed_dir: str = "",
    ) -> None:
        self._sources: list[tuple[str, str]] = []
        for source_name, path in [
            ("bundled", bundled_dir),
            ("configured", configured_dir),
            ("managed", managed_dir),
        ]:
            if path:
                self._sources.append((source_name, path))

    async def load_all(self) -> list[SkillEntry]:
        """加载并合并所有技能。返回按 sort_key 排序的列表。"""
        merged: dict[str, SkillEntry] = {}

        for source_name, skills_dir in self._sources:
            if not os.path.isdir(skills_dir):
                logger.debug("skills dir not found, skipping", dir=skills_dir, source=source_name)
                continue

            entries = self._load_from_dir(skills_dir, source_name)
            for entry in entries:
                merged[entry.name] = entry

        result = list(merged.values())
        result.sort(key=lambda e: e.sort_key)

        logger.info("skills loaded", count=len(result), names=[e.name for e in result])
        return result

    def _load_from_dir(self, dir_path: str, source: str) -> list[SkillEntry]:
        """扫描目录下的 SKILL.md 文件，解析技能定义。"""
        entries: list[SkillEntry] = []

        try:
            items = sorted(os.listdir(dir_path))
        except OSError:
            logger.warning("cannot list skills dir", dir=dir_path)
            return entries

        for item in items:
            skill_md = os.path.join(dir_path, item, "SKILL.md")
            if not os.path.isfile(skill_md):
                continue

            try:
                with open(skill_md, "r", encoding="utf-8") as f:
                    raw = f.read()

                frontmatter, _body = self._parse_frontmatter(raw)

                entries.append(
                    SkillEntry(
                        name=frontmatter.get("name", item),
                        description=frontmatter.get("description", ""),
                        base_dir=os.path.join(dir_path, item),
                        file_path=skill_md,
                        source=source,
                        metadata=self._parse_metadata(frontmatter),
                        invocation=self._parse_invocation(frontmatter),
                    )
                )
            except Exception:
                logger.exception("failed to parse skill", path=skill_md)

        return entries

    @staticmethod
    def _parse_frontmatter(raw: str) -> tuple[dict[str, Any], str]:
        """解析 YAML frontmatter。返回 (frontmatter_dict, body_text)。"""
        m = _FRONTMATTER_RE.match(raw)
        if not m:
            return {}, raw

        try:
            fm = yaml.safe_load(m.group(1)) or {}
        except yaml.YAMLError:
            fm = {}

        body = raw[m.end():]
        return fm, body

    @staticmethod
    def _parse_metadata(fm: dict[str, Any]) -> SkillMetadata:
        """从 frontmatter 构建 SkillMetadata。"""
        meta_raw = fm.get("metadata", {})
        if not isinstance(meta_raw, dict):
            meta_raw = {}

        requires = None
        req_raw = meta_raw.get("requires")
        if isinstance(req_raw, dict):
            requires = SkillRequirements(
                bins=req_raw.get("bins", []) or [],
                any_bins=req_raw.get("any_bins", []) or [],
                env=req_raw.get("env", []) or [],
                config=req_raw.get("config", []) or [],
            )

        install = None
        install_raw = meta_raw.get("install")
        if isinstance(install_raw, list):
            install = [
                SkillInstallSpec(
                    kind=spec.get("kind", "pip"),
                    package=spec.get("package"),
                    url=spec.get("url"),
                    bins=spec.get("bins", []) or [],
                )
                for spec in install_raw
                if isinstance(spec, dict)
            ]

        tier_val = meta_raw.get("tier")
        tier = SkillTier(int(tier_val)) if tier_val is not None else SkillTier.MANAGED

        return SkillMetadata(
            always=bool(meta_raw.get("always", False)),
            skill_key=meta_raw.get("skill_key"),
            primary_env=meta_raw.get("primary_env"),
            emoji=meta_raw.get("emoji"),
            homepage=meta_raw.get("homepage"),
            os=meta_raw.get("os"),
            tier=tier,
            priority=int(meta_raw.get("priority", 100)),
            tags=meta_raw.get("tags", []) or [],
            requires=requires,
            install=install,
        )

    @staticmethod
    def _parse_invocation(fm: dict[str, Any]) -> InvocationPolicy:
        """从 frontmatter 构建 InvocationPolicy。"""
        inv_raw = fm.get("invocation", {})
        if not isinstance(inv_raw, dict):
            inv_raw = {}

        return InvocationPolicy(
            user_invocable=bool(inv_raw.get("user_invocable", True)),
            disable_model_invocation=bool(inv_raw.get("disable_model_invocation", False)),
        )
