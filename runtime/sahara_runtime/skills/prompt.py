"""技能提示词生成 — 将过滤后的技能格式化为 XML 段落注入系统提示词。

参考: D4 §10.6 技能提示词生成

输出的 <available_skills> 段落按 sort_key 排序 — 高优先级技能排在前面，
LLM 更倾向选择排在前面的技能。
"""

from __future__ import annotations

from sahara_runtime.skills.types import SkillEntry


def build_skills_prompt(entries: list[SkillEntry]) -> str:
    """生成技能提示词段落。

    排除 disable_model_invocation=True 的技能后，
    按 sort_key 排序输出 XML 格式供 LLM 消费。
    空列表返回空字符串。
    """
    prompt_entries = [
        e for e in entries
        if not (e.invocation and e.invocation.disable_model_invocation)
    ]

    if not prompt_entries:
        return ""

    prompt_entries = sorted(prompt_entries, key=lambda e: e.sort_key)

    lines = [
        "## Skills (mandatory)",
        "Before replying: scan <available_skills> <description> entries.",
        "- If exactly one skill clearly applies: read its SKILL.md at <location> with `read`, then follow it.",
        "- If multiple could apply: choose the most specific one, then read/follow it.",
        "- If none clearly apply: do not read any SKILL.md.",
        "Constraints: never read more than one skill up front; only read after selecting.",
        "",
        "<available_skills>",
    ]

    for entry in prompt_entries:
        location = f"/workspace/skills/{entry.name}/SKILL.md"
        tag_attr = ""
        if entry.metadata and entry.metadata.tags:
            tag_attr = f' tags="{",".join(entry.metadata.tags)}"'
        lines.append(
            f'<skill name="{entry.name}" location="{location}"{tag_attr}>'
            f"\n  <description>{entry.description}</description>"
            f"\n</skill>"
        )

    lines.append("</available_skills>")
    return "\n".join(lines)
