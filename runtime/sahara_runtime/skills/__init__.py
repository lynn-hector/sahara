"""Skills 管理 — SkillLoader + SkillFilter + 提示词生成 + 沙箱同步。

参考: D4 §10 Skills 管理

核心流程 (在 Agent Loop 中):
  1. SkillLoader.load_all()        → 加载技能
  2. SkillFilter.filter()          → 过滤可用技能
  3. sync_skills_to_sandbox()      → 同步到沙箱 (如需)
  4. build_skills_prompt()         → 生成 <available_skills> 注入系统提示词
"""

from sahara_runtime.skills.filter import SkillFilter
from sahara_runtime.skills.loader import SkillLoader
from sahara_runtime.skills.prompt import build_skills_prompt
from sahara_runtime.skills.sync import sync_skills_to_sandbox
from sahara_runtime.skills.types import (
    InvocationPolicy,
    SkillEntry,
    SkillMetadata,
    SkillRequirements,
    SkillTier,
)

__all__ = [
    "SkillFilter",
    "SkillLoader",
    "SkillEntry",
    "SkillMetadata",
    "SkillRequirements",
    "SkillTier",
    "InvocationPolicy",
    "build_skills_prompt",
    "sync_skills_to_sandbox",
]
