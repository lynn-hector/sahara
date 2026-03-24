# Task Plan

## Goal
修复第二次验收中的 1/2/3/4 四项问题，并评估/落地 `SkillsManager`，让 context/skills 边界进一步收口。

## Phases
- [completed] Phase 1: 审查现有 `ContextManager`、`skills` 相关实现和测试
- [completed] Phase 2: 实现 `SkillsManager` 并接入 `ContextManager` / `Container`
- [completed] Phase 3: 修复 skills tool name、异常可观测性、trim 统计语义、facade 边界
- [completed] Phase 4: 补充/更新测试并运行验证
- [completed] Phase 5: 完成最终收口优化（弱化 ContextManager skills API、拆分 Container.startup、扩展 SkillsManager）

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
