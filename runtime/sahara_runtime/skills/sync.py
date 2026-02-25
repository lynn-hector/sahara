"""技能沙箱同步 — 将技能目录写入沙箱使 LLM 的 read 工具可访问。

参考: D4 §10.7 技能同步到沙箱

在 Agent Loop 步骤 3 (沙箱分配) 之后、步骤 4 (系统提示词) 之前执行。
沙箱未启用时为 no-op (NoopSandbox.write_file 直接返回)。
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from sahara_runtime.sandbox.base import Sandbox
    from sahara_runtime.skills.types import SkillEntry

logger = structlog.get_logger(__name__)

SANDBOX_SKILLS_DIR = "/workspace/skills"

_SKIP_PATTERNS = {".git", "__pycache__", ".DS_Store", "node_modules"}
_MAX_FILE_SIZE = 512 * 1024  # 512 KB


async def sync_skills_to_sandbox(
    entries: list[SkillEntry],
    sandbox: Sandbox,
) -> int:
    """将技能目录同步到沙箱的 /workspace/skills/。

    Returns:
        同步成功的技能数量。
    """
    if not entries:
        return 0

    await sandbox.exec(f"rm -rf {SANDBOX_SKILLS_DIR} && mkdir -p {SANDBOX_SKILLS_DIR}")

    synced = 0
    for entry in entries:
        try:
            skill_dir = f"{SANDBOX_SKILLS_DIR}/{entry.name}"
            await sandbox.exec(f"mkdir -p {skill_dir}")

            with open(entry.file_path, "r", encoding="utf-8") as f:
                content = f.read()
            await sandbox.write_file(f"{skill_dir}/SKILL.md", content)

            for extra_file in _list_extra_files(entry.base_dir):
                rel_path = os.path.relpath(extra_file, entry.base_dir)
                with open(extra_file, "r", encoding="utf-8") as f:
                    extra_content = f.read()
                await sandbox.write_file(f"{skill_dir}/{rel_path}", extra_content)

            synced += 1
        except Exception:
            logger.exception("failed to sync skill to sandbox", skill=entry.name)

    logger.info("skills synced to sandbox", total=len(entries), synced=synced)
    return synced


def _list_extra_files(base_dir: str) -> list[str]:
    """列出技能目录中 SKILL.md 之外的可同步文件。"""
    extras: list[str] = []
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if d not in _SKIP_PATTERNS]
        for fname in files:
            if fname == "SKILL.md":
                continue
            if fname.startswith("."):
                continue
            fpath = os.path.join(root, fname)
            if os.path.getsize(fpath) > _MAX_FILE_SIZE:
                continue
            extras.append(fpath)
    return extras
