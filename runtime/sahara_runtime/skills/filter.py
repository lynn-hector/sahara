"""SkillFilter — 7 层过滤检查，确保只启用满足运行环境条件的技能。

参考: D4 §10.5 技能过滤

过滤决策流程:
  1. 显式禁用检查 → 排除
  2. OS 不兼容   → 排除
  3. always=true  → 包含 (跳过后续)
  4. 必需二进制   → 排除
  5. 任一二进制   → 排除
  6. 必需环境变量 → 排除
  7. 必需配置路径 → 排除 (预留)
  → 包含
"""

from __future__ import annotations

import os
import platform
import shutil

import structlog

from sahara_runtime.skills.types import SkillEntry, SkillMetadata

logger = structlog.get_logger(__name__)


class SkillFilter:
    """7 层过滤检查，确保只启用满足条件的技能。"""

    def __init__(self, disabled_skills: set[str] | None = None) -> None:
        self._disabled = disabled_skills or set()

    def filter(self, entries: list[SkillEntry]) -> list[SkillEntry]:
        """返回通过所有过滤条件的技能，保持输入顺序。"""
        result = []
        for entry in entries:
            ok, reason = self._check(entry)
            if ok:
                result.append(entry)
            else:
                logger.debug("skill filtered out", skill=entry.name, reason=reason)
        return result

    def _check(self, entry: SkillEntry) -> tuple[bool, str]:
        meta = entry.metadata or SkillMetadata()

        # 1. 显式禁用
        if entry.name in self._disabled:
            return False, "explicitly_disabled"

        # 2. OS 兼容性
        if meta.os:
            current_os = platform.system().lower()
            if current_os not in [o.lower() for o in meta.os]:
                return False, f"os_mismatch:{current_os}"

        # 3. always=true 始终启用
        if meta.always:
            return True, ""

        # 4. 必需二进制文件 (全部)
        if meta.requires and meta.requires.bins:
            for bin_name in meta.requires.bins:
                if not shutil.which(bin_name):
                    return False, f"missing_bin:{bin_name}"

        # 5. 任意一个二进制 (至少一个)
        if meta.requires and meta.requires.any_bins:
            if not any(shutil.which(b) for b in meta.requires.any_bins):
                return False, f"missing_any_bin:{meta.requires.any_bins}"

        # 6. 必需环境变量
        if meta.requires and meta.requires.env:
            for env_name in meta.requires.env:
                if not os.environ.get(env_name):
                    return False, f"missing_env:{env_name}"

        # 7. 必需配置路径 (Phase 3 扩展, 当前 pass-through)

        return True, ""
