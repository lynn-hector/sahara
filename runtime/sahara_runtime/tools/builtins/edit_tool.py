"""edit_file 工具 — 搜索替换编辑，支持模糊匹配 + diff 提示。

参考 nanobot/agent/tools/filesystem.py EditFileTool。
"""

from __future__ import annotations

import difflib
from typing import Any

from sahara_runtime.tools.builtins.fs_base import FsTool


def _find_match(content: str, old_text: str) -> tuple[str | None, int]:
    """在 content 中定位 old_text：先精确匹配，再尝试忽略空白的滑动窗口。"""
    if old_text in content:
        return old_text, content.count(old_text)

    old_lines = old_text.splitlines()
    if not old_lines:
        return None, 0
    stripped_old = [line.strip() for line in old_lines]
    content_lines = content.splitlines()

    candidates: list[str] = []
    for i in range(len(content_lines) - len(stripped_old) + 1):
        window = content_lines[i : i + len(stripped_old)]
        if [line.strip() for line in window] == stripped_old:
            candidates.append("\n".join(window))

    if candidates:
        return candidates[0], len(candidates)
    return None, 0


class EditFileTool(FsTool):
    """通过搜索替换编辑文件，带模糊匹配和 diff 提示。"""

    @property
    def name(self) -> str:
        return "edit_file"

    @property
    def description(self) -> str:
        return (
            "Edit a file by replacing old_text with new_text. "
            "Supports minor whitespace differences. "
            "Set replace_all=true to replace every occurrence."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path to edit. Relative paths resolve from workspace.",
                },
                "old_text": {
                    "type": "string",
                    "description": "The text to find and replace",
                },
                "new_text": {
                    "type": "string",
                    "description": "The replacement text",
                },
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace all occurrences (default false)",
                },
            },
            "required": ["path", "old_text", "new_text"],
        }

    async def execute(
        self,
        path: str,
        old_text: str,
        new_text: str,
        replace_all: bool = False,
        **kwargs: Any,
    ) -> str:
        try:
            fp = self._resolve(path)
        except PermissionError as e:
            return f"Error: {e}"

        if not fp.exists():
            return f"Error: File not found: {path}"

        try:
            raw = fp.read_bytes()
            uses_crlf = b"\r\n" in raw
            content = raw.decode("utf-8").replace("\r\n", "\n")
        except Exception as e:
            return f"Error reading file: {e}"

        match, count = _find_match(content, old_text.replace("\r\n", "\n"))

        if match is None:
            return self._not_found_msg(old_text, content, path)
        if count > 1 and not replace_all:
            return (
                f"Warning: old_text appears {count} times. "
                "Provide more context to make it unique, or set replace_all=true."
            )

        norm_new = new_text.replace("\r\n", "\n")
        if replace_all:
            new_content = content.replace(match, norm_new)
        else:
            new_content = content.replace(match, norm_new, 1)

        if uses_crlf:
            new_content = new_content.replace("\n", "\r\n")

        try:
            fp.write_bytes(new_content.encode("utf-8"))
            return f"Successfully edited {fp}"
        except Exception as e:
            return f"Error writing file: {e}"

    @staticmethod
    def _not_found_msg(old_text: str, content: str, path: str) -> str:
        lines = content.splitlines(keepends=True)
        old_lines = old_text.splitlines(keepends=True)
        window = len(old_lines)

        best_ratio, best_start = 0.0, 0
        for i in range(max(1, len(lines) - window + 1)):
            ratio = difflib.SequenceMatcher(
                None, old_lines, lines[i : i + window],
            ).ratio()
            if ratio > best_ratio:
                best_ratio, best_start = ratio, i

        if best_ratio > 0.5:
            diff = "\n".join(difflib.unified_diff(
                old_lines,
                lines[best_start : best_start + window],
                fromfile="old_text (provided)",
                tofile=f"{path} (actual, line {best_start + 1})",
                lineterm="",
            ))
            return (
                f"Error: old_text not found in {path}.\n"
                f"Best match ({best_ratio:.0%} similar) at line {best_start + 1}:\n{diff}"
            )
        return (
            f"Error: old_text not found in {path}. "
            "No similar text found. Verify the file content."
        )
