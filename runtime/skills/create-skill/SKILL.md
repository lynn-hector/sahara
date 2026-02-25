---
name: create-skill
description: Create new Agent Skills with proper SKILL.md structure, frontmatter, and instructions.
metadata:
  tier: 0
  priority: 20
  always: true
  emoji: "🛠️"
  tags: ["meta", "skills", "authoring"]
invocation:
  user_invocable: true
  disable_model_invocation: false
---

# Create Skill

Use this skill when the user wants to create, write, or author a new skill for the Sahara agent system.

## What is a Skill?

A **Skill** is a self-contained instruction package (`SKILL.md`) that teaches the agent how to perform a specific task. Skills are discovered automatically and presented to the LLM in the system prompt as `<available_skills>`.

## Skill Directory Structure

```
skills/
  my-skill/
    SKILL.md          # Required: skill definition + instructions
    templates/        # Optional: template files
    scripts/          # Optional: helper scripts
```

## SKILL.md Format

```markdown
---
name: my-skill
description: One-line description of what this skill does.
metadata:
  tier: 1            # 0=builtin, 1=configured, 2=managed, 3=user
  priority: 100      # Lower = higher priority within same tier
  tags: ["category1", "category2"]
  requires:
    bins: ["required-binary"]
    env: ["REQUIRED_ENV_VAR"]
invocation:
  user_invocable: true
  disable_model_invocation: false
---

# Skill Title

Step-by-step instructions for the agent to follow.

## When to Use

Describe the trigger conditions.

## Steps

1. First step
2. Second step
3. Third step

## Examples

Provide concrete examples.
```

## Frontmatter Fields

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Unique skill identifier (kebab-case) |
| `description` | Yes | One-line description shown in `<available_skills>` |
| `metadata.tier` | No | Priority tier (0-3), default: 2 |
| `metadata.priority` | No | Sort weight within tier, default: 100 |
| `metadata.tags` | No | Categorization tags |
| `metadata.requires.bins` | No | Required binaries (all must exist) |
| `metadata.requires.env` | No | Required environment variables |
| `metadata.always` | No | If true, skip environment checks |

## Best Practices

- Write clear, step-by-step instructions the LLM can follow
- Keep the description concise — it's what the LLM sees first
- Use concrete examples in the body
- Declare all external dependencies in `requires`
- Choose appropriate tier and priority values
