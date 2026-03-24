# Findings

## Current Session
- `skills/prompt.py` 中仍提示使用 `read`，与真实工具 `read_file` 不一致。
- `ContextManager.build_skills_prompt()` 当前对异常静默降级为 `""`。
- `fit_context_with_stats()` 的 `total_tokens` 表示裁剪前 token，总体语义不够清晰。
- `ContextManager` 仍公开暴露 `prompt_builder` / `context_builder`，facade 边界可进一步收口。
- 用户补充建议：引入 `SkillsManager`，统一封装 `SkillLoader` + `SkillFilter`。
