"""Skills 类型定义 — SkillTier, SkillMetadata, SkillEntry 等核心数据结构。

参考: D4 §10.2 技能定义
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class SkillTier(IntEnum):
    """技能优先级层级。数值越小优先级越高。"""

    BUILTIN = 0
    CONFIGURED = 1
    MANAGED = 2
    USER = 3


@dataclass
class SkillRequirements:
    """技能运行环境要求。"""

    bins: list[str] = field(default_factory=list)
    any_bins: list[str] = field(default_factory=list)
    env: list[str] = field(default_factory=list)
    config: list[str] = field(default_factory=list)


@dataclass
class SkillInstallSpec:
    """技能安装规格（Phase 3 托管市场使用）。"""

    kind: str = "pip"
    package: str | None = None
    url: str | None = None
    bins: list[str] = field(default_factory=list)


@dataclass
class SkillMetadata:
    """技能元数据，从 SKILL.md frontmatter 解析。"""

    always: bool = False
    skill_key: str | None = None
    primary_env: str | None = None
    emoji: str | None = None
    homepage: str | None = None
    os: list[str] | None = None
    tier: SkillTier = SkillTier.MANAGED
    priority: int = 100
    tags: list[str] = field(default_factory=list)
    requires: SkillRequirements | None = None
    install: list[SkillInstallSpec] | None = None


@dataclass
class InvocationPolicy:
    """技能调用策略。"""

    user_invocable: bool = True
    disable_model_invocation: bool = False


_SOURCE_TIER_MAP: dict[str, SkillTier] = {
    "bundled": SkillTier.BUILTIN,
    "configured": SkillTier.CONFIGURED,
    "managed": SkillTier.MANAGED,
}


@dataclass
class SkillEntry:
    """加载后的完整技能条目。"""

    name: str
    description: str
    base_dir: str
    file_path: str
    source: str = "bundled"
    metadata: SkillMetadata | None = None
    invocation: InvocationPolicy | None = None

    @property
    def effective_tier(self) -> SkillTier:
        """有效 Tier — 来源隐含 Tier 与 SKILL.md 声明 Tier 取最小值。"""
        source_tier = _SOURCE_TIER_MAP.get(self.source, SkillTier.USER)
        declared_tier = self.metadata.tier if self.metadata else SkillTier.MANAGED
        return min(source_tier, declared_tier)

    @property
    def sort_key(self) -> tuple[int, int, str]:
        """排序键: (tier, priority, name)。"""
        meta = self.metadata or SkillMetadata()
        return (self.effective_tier, meta.priority, self.name)
