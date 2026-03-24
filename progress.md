# Progress

## 2026-03-20
- 初始化本轮第二次验收修复任务的规划文件。
- 已读取 `ContextManager`、`skills`、`agent_loop` 与相关测试，开始实现小范围收口改造。
- 新增 `runtime/sahara_runtime/skills/manager.py`，把 `SkillLoader` + `SkillFilter` 收口为 `SkillsManager`。
- `ContextManager` 改为依赖 `SkillsManager`，并将内部 builder 改成私有属性，避免 facade 边界外泄。
- 修复 skills prompt 中的工具名为 `read_file`，并让 skills 构建异常直接向上抛出。
- 调整 context trim 统计，区分 `pre_trim_tokens` 与 `post_trim_tokens`，并更新 `POST_CONTEXT_TRIM` hook 上报值。
- 已通过 `uv run pytest tests/test_context_manager.py tests/test_agent_loop.py`。
- 已通过 `uv run ruff check` 对本次修改文件的检查。
- 最终收口中，移除了 `ContextManager.build_skills_prompt()` 公共入口，改为内部 `_build_skills_prompt_section()`。
- `fit_context_with_stats()` 进一步去歧义，改为显式返回 `input_tokens` / `pre_trim_total_tokens` / `post_trim_total_tokens`。
- `SkillsManager` 新增 `summarize_active()` 与 `sync_to_sandbox()`，开始承担 prompt 之外的 skills 子系统能力。
- `Container.startup()` 已拆分为多个私有初始化方法，组装顺序更清晰。
- 本轮最终验证已通过 `uv run pytest tests/test_context_manager.py tests/test_agent_loop.py`，共 `48 passed`。
- 本轮最终验证已通过 `uv run ruff check` 针对改动文件的检查。
