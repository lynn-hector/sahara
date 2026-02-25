---
name: create-rule
description: Create persistent AI guidance rules (RULE.md) to customize agent behavior for specific projects or workflows.
metadata:
  tier: 0
  priority: 10
  always: true
  emoji: "📏"
  tags: ["meta", "configuration", "rules"]
invocation:
  user_invocable: true
  disable_model_invocation: false
---

# Create Rule

Use this skill when the user asks you to create a rule, add coding standards, set up project conventions, or configure file-specific patterns.

## What is a Rule?

A **Rule** is a persistent instruction file (`RULE.md`) that guides your behavior for a specific project, directory, or file pattern. Rules are loaded automatically when relevant files are being worked on.

## How to Create a Rule

1. Ask the user what behavior they want to standardize
2. Determine the scope:
   - **Project-wide**: Place in the project root
   - **Directory-specific**: Place in the target directory
   - **File-pattern**: Specify glob patterns in the frontmatter
3. Write the `RULE.md` file with YAML frontmatter and markdown body

## Rule File Format

```markdown
---
description: Brief description of what this rule does
globs:
  - "**/*.py"
  - "tests/**"
---

# Rule Title

Your instructions here. Be specific and actionable.

## Examples

Show good and bad examples when helpful.
```

## Best Practices

- Keep rules focused on one concern
- Use concrete examples, not vague guidelines
- Prefer positive instructions ("do X") over negative ones ("don't do Y")
- Include the "why" behind the rule when not obvious
