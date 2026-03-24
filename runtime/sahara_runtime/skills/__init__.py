"""Skills 管理 — SkillLoader + SkillFilter + 提示词生成 + 沙箱同步。

参考: D4 §10 Skills 管理

核心流程 (在 Agent Loop 中):
  1. SkillsManager.load_active()   → 加载并过滤技能
  2. sync_skills_to_sandbox()      → 同步到沙箱 (如需)
  3. SkillsManager.build_prompt()  → 生成 <available_skills> 注入系统提示词
"""

from sahara_runtime.skills.filter import SkillFilter
from sahara_runtime.skills.loader import SkillLoader
from sahara_runtime.skills.manager import SkillsManager
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
    "SkillsManager",
    "SkillEntry",
    "SkillMetadata",
    "SkillRequirements",
    "SkillTier",
    "InvocationPolicy",
    "sync_skills_to_sandbox",
]
