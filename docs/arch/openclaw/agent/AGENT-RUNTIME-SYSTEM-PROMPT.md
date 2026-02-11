# Agent Runtime 系统提示词构建详解

> 本文档详细解析 Agent Runtime 中系统提示词的构建过程、结构组成和各节段的实现逻辑。

---

## 目录

1. [构建入口](#一构建入口)
2. [提示词整体结构](#二提示词整体结构)
3. [各节段详解](#三各节段详解)
   - [Tooling 节段](#31-tooling-节段)
   - [Skills 节段](#32-skills-节段)
   - [Sandbox 节段](#33-sandbox-节段)
   - [Context Files 节段](#34-context-files-节段-bootstrap-文件加载)
4. [运行时信息](#四运行时信息)
5. [PromptMode 条件控制](#五promptmode-条件控制)
6. [上下文管理与溢出处理](#六上下文管理与溢出处理)
   - [总体架构](#61-总体架构四层防线)
   - [第一层：输入阶段截断](#62-第一层输入阶段截断)
   - [第二层：上下文裁剪](#63-第二层上下文裁剪-context-pruning)
   - [第三层：自动压缩](#64-第三层自动压缩-auto-compaction)
   - [第四层：溢出应急处理](#65-第四层溢出应急处理-overflow-compaction)
   - [对话历史限制](#66-对话历史限制-history-turns-limit)
   - [工具结果处理](#67-工具结果处理)

---

## 一、构建入口

系统提示词的构建有两层入口：

### 嵌入式入口

`buildEmbeddedSystemPrompt` 是 `runEmbeddedAttempt` 调用的直接入口，负责收集上下文参数并委托给核心构建函数：

```typescript
// src/agents/pi-embedded-runner/system-prompt.ts:10-75
export function buildEmbeddedSystemPrompt(params: {
  workspaceDir: string;
  defaultThinkLevel?: ThinkLevel;
  reasoningLevel?: ReasoningLevel;
  extraSystemPrompt?: string;
  ownerNumbers?: string[];
  reasoningTagHint: boolean;
  heartbeatPrompt?: string;
  skillsPrompt?: string;
  docsPath?: string;
  ttsHint?: string;
  reactionGuidance?: { level: "minimal" | "extensive"; channel: string };
  workspaceNotes?: string[];
  promptMode?: PromptMode;  // "full" | "minimal" | "none"
  runtimeInfo: {
    agentId?: string;
    host: string;
    os: string;
    arch: string;
    node: string;
    model: string;
    provider?: string;
    capabilities?: string[];
    channel?: string;
    channelActions?: string[];
  };
  messageToolHints?: string[];
  sandboxInfo?: EmbeddedSandboxInfo;
  tools: AgentTool[];
  modelAliasLines: string[];
  userTimezone: string;
  userTime?: string;
  userTimeFormat?: ResolvedTimeFormat;
  contextFiles?: EmbeddedContextFile[];
}): string {
  return buildAgentSystemPrompt({
    workspaceDir: params.workspaceDir,
    toolNames: params.tools.map((tool) => tool.name),
    toolSummaries: buildToolSummaryMap(params.tools),
    // ... 传递所有参数
  });
}
```

### 核心构建函数

`buildAgentSystemPrompt` 是实际组装提示词各节段的函数：

```typescript
// src/agents/system-prompt.ts:164-591
export function buildAgentSystemPrompt(params: {
  workspaceDir: string;
  toolNames: string[];
  toolSummaries: Record<string, string>;
  promptMode?: PromptMode;
  // ... 大量可选参数控制各节段
}): string {
  const isMinimal = params.promptMode === "minimal";
  const isFull = !isMinimal;
  const lines: string[] = [];

  // 依次组装各节段...
  lines.push(...identitySection);
  lines.push(...toolingSection);
  lines.push(...safetySection);
  // ...

  return lines.filter(Boolean).join("\n");
}
```

---

## 二、提示词整体结构

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         系统提示词结构                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│  身份定义                                                                       │
│  "You are a personal assistant running inside OpenClaw."                        │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Tooling (工具列表)                                                          │
│  Tool availability (filtered by policy):                                        │
│  - read: Read file contents                                                     │
│  - write: Create or overwrite files                                             │
│  - exec: Run shell commands (pty available for TTY-required CLIs)               │
│  - ...                                                                          │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Tool Call Style (工具调用风格)                                              │
│  Default: do not narrate routine, low-risk tool calls.                          │
│  Narrate only when it helps: multi-step work, complex problems...               │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Safety (安全规则)                                                           │
│  - No independent goals (no self-preservation, replication...)                  │
│  - Prioritize safety and human oversight over completion                        │
│  - Do not manipulate or bypass safeguards                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## OpenClaw CLI Quick Reference (CLI 参考)                                     │
│  - openclaw gateway status/start/stop/restart                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Skills (技能列表) [仅 full 模式]                                            │
│  Before replying: scan <available_skills> <description> entries.                │
│  <available_skills>                                                             │
│    <skill name="peekaboo" location="..." description="...">                     │
│    <skill name="mcporter" location="..." description="...">                     │
│  </available_skills>                                                            │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Memory Recall [仅 full 模式且有 memory 工具]                                │
│  Before answering about prior work: run memory_search...                        │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## OpenClaw Self-Update [仅 full 模式且有 gateway 工具]                        │
│  Get Updates is ONLY allowed when the user explicitly asks...                   │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Model Aliases [仅 full 模式]                                                │
│  - 4o → openai/gpt-4o                                                           │
│  - sonnet → anthropic/claude-sonnet-4-20250514                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Workspace (工作空间)                                                        │
│  Your working directory is: /path/to/workspace                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Documentation [仅 full 模式]                                                │
│  OpenClaw docs: ./docs                                                          │
│  Mirror: https://docs.openclaw.ai                                               │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Sandbox (沙箱信息) [仅当沙箱启用]                                           │
│  You are running in a sandboxed runtime (tools execute in Docker).              │
│  Sandbox workspace: /path/to/sandbox                                            │
│  Agent workspace access: ro (mounted at /workspace)                             │
│  Sandbox browser: enabled.                                                      │
│  Elevated exec: available. Current level: ask.                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## User Identity [仅 full 模式]                                                │
│  Owner numbers: +1234567890. Treat messages from these as the user.             │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Current Date & Time                                                         │
│  Time zone: America/New_York                                                    │
│  If you need the current date, use the session_status tool.                     │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Reply Tags [仅 full 模式]                                                   │
│  [[reply_to_current]], [[reply_to:<id>]]                                        │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Messaging [仅 full 模式]                                                    │
│  - Reply in current session → automatically routes to source channel            │
│  - Cross-session messaging → use sessions_send(sessionKey, message)             │
│  ### message tool                                                               │
│  - Use for proactive sends + channel actions                                    │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Voice (TTS) [仅 full 模式且配置了 TTS]                                      │
│  TTS is enabled. Use the tts tool for voice output.                             │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Group Chat Context / Subagent Context [如有 extraSystemPrompt]              │
│  (用户提供的额外上下文)                                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Reactions [如配置了 reactionGuidance]                                       │
│  Reactions are enabled for telegram in MINIMAL mode.                            │
│  React ONLY when truly relevant...                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Reasoning Format [如启用了 reasoningTagHint]                                │
│  ALL internal reasoning MUST be inside <think>...</think>.                      │
│  Format: <think>...</think> then <final>...</final>                             │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  # Project Context                                                              │
│  ## AGENTS.md                                                                   │
│  (文件内容)                                                                     │
│  ## SOUL.md                                                                     │
│  (文件内容)                                                                     │
│  ## BOOTSTRAP.md                                                                │
│  (文件内容)                                                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Silent Replies [仅 full 模式]                                               │
│  When you have nothing to say, respond with ONLY: ⌀                             │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Heartbeats [仅 full 模式]                                                   │
│  If you receive a heartbeat poll, reply exactly: HEARTBEAT_OK                   │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ## Runtime                                                                     │
│  Runtime: agent=pi | host=macbook | os=darwin (arm64) | node=v22.0.0 |          │
│           model=claude-sonnet-4-20250514 | channel=telegram |                   │
│           capabilities=inlineButtons | thinking=off                             │
│  Reasoning: off (hidden unless on/stream). Toggle /reasoning.                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、各节段详解

### 3.1 Tooling 节段

Tooling 节段向 LLM 描述当前可用的工具列表，影响 LLM 的工具选择行为：

```typescript
// src/agents/system-prompt.ts:217-308
const coreToolSummaries: Record<string, string> = {
  read: "Read file contents",
  write: "Create or overwrite files",
  edit: "Make precise edits to files",
  apply_patch: "Apply multi-file patches",
  grep: "Search file contents for patterns",
  find: "Find files by glob pattern",
  ls: "List directory contents",
  exec: "Run shell commands (pty available for TTY-required CLIs)",
  process: "Manage background exec sessions",
  web_search: "Search the web (Brave API)",
  web_fetch: "Fetch and extract readable content from a URL",
  browser: "Control web browser",
  canvas: "Present/eval/snapshot the Canvas",
  nodes: "List/describe/notify/camera/screen on paired nodes",
  cron: "Manage cron jobs and wake events (use for reminders...)",
  message: "Send messages and channel actions",
  gateway: "Restart, apply config, or run updates on the running OpenClaw process",
  agents_list: "List agent ids allowed for sessions_spawn",
  sessions_list: "List other sessions (incl. sub-agents) with filters/last",
  sessions_history: "Fetch history for another session/sub-agent",
  sessions_send: "Send a message to another session/sub-agent",
  sessions_spawn: "Spawn a sub-agent session",
  session_status: "Show a /status-equivalent status card...",
  image: "Analyze an image with the configured image model",
};

// 工具显示顺序
const toolOrder = [
  "read", "write", "edit", "apply_patch", "grep", "find", "ls",
  "exec", "process", "web_search", "web_fetch", "browser", "canvas",
  "nodes", "cron", "message", "gateway", "agents_list", "sessions_list",
  "sessions_history", "sessions_send", "session_status", "image",
];

// 构建工具列表
const toolLines = enabledTools.map((tool) => {
  const summary = coreToolSummaries[tool] ?? externalToolSummaries.get(tool);
  const name = resolveToolName(tool);
  return summary ? `- ${name}: ${summary}` : `- ${name}`;
});
```

工具列表只包含经过策略过滤后实际可用的工具。渠道工具和插件工具通过 `externalToolSummaries` 提供描述，不在核心列表中。

### 3.2 Skills 节段

Skills 节段指导 LLM 在回复前扫描可用技能并按需使用：

```typescript
// src/agents/system-prompt.ts:15-37
function buildSkillsSection(params: {
  skillsPrompt?: string;
  isMinimal: boolean;
  readToolName: string;
}) {
  if (params.isMinimal) {
    return [];  // 子代理 (minimal 模式) 不显示技能
  }
  const trimmed = params.skillsPrompt?.trim();
  if (!trimmed) {
    return [];
  }
  return [
    "## Skills (mandatory)",
    "Before replying: scan <available_skills> <description> entries.",
    `- If exactly one skill clearly applies: read its SKILL.md at <location> with \`${params.readToolName}\`, then follow it.`,
    "- If multiple could apply: choose the most specific one, then read/follow it.",
    "- If none clearly apply: do not read any SKILL.md.",
    "Constraints: never read more than one skill up front; only read after selecting.",
    trimmed,  // <available_skills>...</available_skills>
    "",
  ];
}
```

`skillsPrompt` 的内容由 `buildWorkspaceSkillsPrompt` 生成（详见 [AGENT-RUNTIME-SKILLS.md](./AGENT-RUNTIME-SKILLS.md)），格式为 XML：

```xml
<available_skills>
<skill name="peekaboo" location="/path/to/skills/peekaboo/SKILL.md">
  <description>Fast macOS screenshots with optional AI vision analysis.</description>
</skill>
<skill name="mcporter" location="/path/to/skills/mcporter/SKILL.md">
  <description>Use the mcporter CLI to list, configure, auth, and call MCP servers/tools directly.</description>
</skill>
</available_skills>
```

### 3.3 Sandbox 节段

当沙箱启用时，向 LLM 提供沙箱环境信息：

```typescript
// src/agents/system-prompt.ts:441-482
params.sandboxInfo?.enabled ? "## Sandbox" : "",
params.sandboxInfo?.enabled
  ? [
      "You are running in a sandboxed runtime (tools execute in Docker).",
      "Some tools may be unavailable due to sandbox policy.",
      "Sub-agents stay sandboxed (no elevated/host access). Need outside-sandbox read/write? Don't spawn; ask first.",
      params.sandboxInfo.workspaceDir
        ? `Sandbox workspace: ${params.sandboxInfo.workspaceDir}`
        : "",
      params.sandboxInfo.workspaceAccess
        ? `Agent workspace access: ${params.sandboxInfo.workspaceAccess}${
            params.sandboxInfo.agentWorkspaceMount
              ? ` (mounted at ${params.sandboxInfo.agentWorkspaceMount})`
              : ""
          }`
        : "",
      params.sandboxInfo.browserBridgeUrl ? "Sandbox browser: enabled." : "",
      params.sandboxInfo.browserNoVncUrl
        ? `Sandbox browser observer (noVNC): ${params.sandboxInfo.browserNoVncUrl}`
        : "",
      params.sandboxInfo.hostBrowserAllowed === true
        ? "Host browser control: allowed."
        : params.sandboxInfo.hostBrowserAllowed === false
          ? "Host browser control: blocked."
          : "",
      params.sandboxInfo.elevated?.allowed
        ? "Elevated exec is available for this session."
        : "",
      params.sandboxInfo.elevated?.allowed
        ? "User can toggle with /elevated on|off|ask|full."
        : "",
      params.sandboxInfo.elevated?.allowed
        ? `Current elevated level: ${params.sandboxInfo.elevated.defaultLevel} (ask runs exec on host with approvals; full auto-approves).`
        : "",
    ]
      .filter(Boolean)
      .join("\n")
  : "",
```

### 3.4 Context Files 节段 (Bootstrap 文件加载)

项目上下文文件是从工作空间目录加载的一组 Markdown 文件，它们的内容被完整注入到系统提示词中，使 LLM 了解项目约定、用户人格、工具说明等上下文。

#### 3.4.1 Bootstrap 文件定义

系统预定义了以下 Bootstrap 文件名：

```typescript
// src/agents/workspace.ts:21-29
export const DEFAULT_AGENTS_FILENAME = "AGENTS.md";     // 项目规则、Agent 指导
export const DEFAULT_SOUL_FILENAME = "SOUL.md";          // 人格/语气定义
export const DEFAULT_TOOLS_FILENAME = "TOOLS.md";        // 自定义工具说明
export const DEFAULT_IDENTITY_FILENAME = "IDENTITY.md";  // 身份信息
export const DEFAULT_USER_FILENAME = "USER.md";          // 用户信息
export const DEFAULT_HEARTBEAT_FILENAME = "HEARTBEAT.md";// 心跳提示
export const DEFAULT_BOOTSTRAP_FILENAME = "BOOTSTRAP.md";// 启动引导
export const DEFAULT_MEMORY_FILENAME = "MEMORY.md";      // 记忆文件 (可选)
export const DEFAULT_MEMORY_ALT_FILENAME = "memory.md";  // 记忆文件备选 (可选)
```

所有文件均位于工作空间根目录 (`~/.openclaw/workspace/` 或自定义目录)。

#### 3.4.2 加载完整流程

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Context Files 加载完整流程                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

runEmbeddedAttempt()
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 1: resolveBootstrapContextForRun()                                        │
│  src/agents/bootstrap-files.ts:43-60                                            │
│                                                                                 │
│  协调加载、过滤、Hook 覆盖和内容转换的完整流程                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 2: loadWorkspaceBootstrapFiles(workspaceDir)                              │
│  src/agents/workspace.ts:237-291                                                │
│                                                                                 │
│  从工作空间目录依次读取所有 Bootstrap 文件:                                     │
│                                                                                 │
│  const entries = [                                                              │
│    { name: "AGENTS.md",    filePath: "{dir}/AGENTS.md" },                       │
│    { name: "SOUL.md",      filePath: "{dir}/SOUL.md" },                         │
│    { name: "TOOLS.md",     filePath: "{dir}/TOOLS.md" },                        │
│    { name: "IDENTITY.md",  filePath: "{dir}/IDENTITY.md" },                     │
│    { name: "USER.md",      filePath: "{dir}/USER.md" },                         │
│    { name: "HEARTBEAT.md", filePath: "{dir}/HEARTBEAT.md" },                    │
│    { name: "BOOTSTRAP.md", filePath: "{dir}/BOOTSTRAP.md" },                    │
│  ];                                                                             │
│                                                                                 │
│  // 额外检测 MEMORY.md / memory.md (可选，去重)                                 │
│  entries.push(...resolveMemoryBootstrapEntries(dir));                            │
│                                                                                 │
│  // 逐个读取文件内容                                                            │
│  for (const entry of entries) {                                                 │
│    try {                                                                        │
│      const content = await fs.readFile(entry.filePath, "utf-8");                │
│      result.push({ name, path, content, missing: false });                      │
│    } catch {                                                                    │
│      result.push({ name, path, missing: true });  // 文件不存在                 │
│    }                                                                            │
│  }                                                                              │
│                                                                                 │
│  返回: WorkspaceBootstrapFile[]                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 3: filterBootstrapFilesForSession(files, sessionKey)                      │
│  src/agents/workspace.ts:295-303                                                │
│                                                                                 │
│  子代理会话只保留 AGENTS.md 和 TOOLS.md:                                        │
│                                                                                 │
│  const SUBAGENT_BOOTSTRAP_ALLOWLIST = new Set([                                 │
│    "AGENTS.md",                                                                 │
│    "TOOLS.md",                                                                  │
│  ]);                                                                            │
│                                                                                 │
│  if (isSubagentSessionKey(sessionKey)) {                                        │
│    return files.filter(f => SUBAGENT_BOOTSTRAP_ALLOWLIST.has(f.name));           │
│  }                                                                              │
│  return files;  // 主会话保留全部文件                                           │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 4: applyBootstrapHookOverrides(files, ...)                                │
│  src/agents/bootstrap-hooks.ts:7-31                                             │
│                                                                                 │
│  触发 agent:bootstrap 内部钩子，允许插件修改 Bootstrap 文件列表:                │
│                                                                                 │
│  const event = createInternalHookEvent(                                         │
│    "agent", "bootstrap", sessionKey, {                                          │
│      workspaceDir, bootstrapFiles: files, cfg, sessionKey, agentId              │
│  });                                                                            │
│  await triggerInternalHook(event);                                              │
│  // 插件可以修改 event.context.bootstrapFiles (增删改文件)                      │
│  return event.context.bootstrapFiles;                                           │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 5: buildBootstrapContextFiles(files, { maxChars, warn })                  │
│  src/agents/pi-embedded-helpers/bootstrap.ts:162-191                            │
│                                                                                 │
│  将 WorkspaceBootstrapFile[] 转换为 EmbeddedContextFile[]:                      │
│                                                                                 │
│  for (const file of files) {                                                    │
│    // 1. 文件缺失 → 标记为 [MISSING]                                           │
│    if (file.missing) {                                                          │
│      result.push({                                                              │
│        path: file.name,                                                         │
│        content: `[MISSING] Expected at: ${file.path}`,                          │
│      });                                                                        │
│      continue;                                                                  │
│    }                                                                            │
│                                                                                 │
│    // 2. 内容为空 → 跳过                                                       │
│    // 3. 内容超长 → 截断 (保留头 70% + 尾 20%，中间用 truncated 标记)           │
│    const trimmed = trimBootstrapContent(file.content, file.name, maxChars);     │
│    if (!trimmed.content) continue;                                              │
│                                                                                 │
│    result.push({ path: file.name, content: trimmed.content });                  │
│  }                                                                              │
│                                                                                 │
│  返回: EmbeddedContextFile[] = Array<{ path: string; content: string }>         │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 6: 注入到系统提示词                                                       │
│  src/agents/system-prompt.ts:535-552                                            │
│                                                                                 │
│  在 buildAgentSystemPrompt 中遍历 contextFiles:                                 │
│                                                                                 │
│  if (contextFiles.length > 0) {                                                 │
│    lines.push("# Project Context");                                             │
│    if (hasSoulFile) {                                                           │
│      lines.push("If SOUL.md is present, embody its persona...");                │
│    }                                                                            │
│    for (const file of contextFiles) {                                           │
│      lines.push(`## ${file.path}`, "", file.content, "");                       │
│    }                                                                            │
│  }                                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 3.4.3 Bootstrap 文件列表及作用

| 文件名 | 作用 | 主会话 | 子代理 |
| ---- | ---- | ---- | ---- |
| `AGENTS.md` | 项目规则、Agent 行为指导、编码规范 | 是 | 是 |
| `SOUL.md` | 人格定义、语气风格 (检测到时触发 persona 指令) | 是 | 否 |
| `TOOLS.md` | 自定义工具使用说明和注意事项 | 是 | 是 |
| `IDENTITY.md` | Agent 身份信息 | 是 | 否 |
| `USER.md` | 用户偏好和信息 | 是 | 否 |
| `HEARTBEAT.md` | 心跳消息提示 | 是 | 否 |
| `BOOTSTRAP.md` | 启动引导指令 (仅新工作空间创建) | 是 | 否 |
| `MEMORY.md` | 记忆/笔记 (可选，自动检测) | 是 | 否 |

#### 3.4.4 内容截断机制

当单个文件内容超过 `maxChars` (默认 20,000 字符) 时，采用头尾保留策略：

```typescript
// src/agents/pi-embedded-helpers/bootstrap.ts:84-136
export const DEFAULT_BOOTSTRAP_MAX_CHARS = 20_000;
const BOOTSTRAP_HEAD_RATIO = 0.7;   // 保留前 70%
const BOOTSTRAP_TAIL_RATIO = 0.2;   // 保留后 20%

function trimBootstrapContent(content: string, fileName: string, maxChars: number) {
  if (trimmed.length <= maxChars) {
    return { content: trimmed, truncated: false };
  }

  const headChars = Math.floor(maxChars * 0.7);  // 前 14000 字符
  const tailChars = Math.floor(maxChars * 0.2);  // 后 4000 字符
  const head = trimmed.slice(0, headChars);
  const tail = trimmed.slice(-tailChars);

  const marker = [
    "",
    `[...truncated, read ${fileName} for full content...]`,
    `...(truncated ${fileName}: kept ${headChars}+${tailChars} chars of ${trimmed.length})...`,
    "",
  ].join("\n");

  return { content: [head, marker, tail].join("\n"), truncated: true };
}
```

截断后的输出示例：

```text
(前 14000 字符的内容...)

[...truncated, read AGENTS.md for full content...]
...(truncated AGENTS.md: kept 14000+4000 chars of 25000)...

(后 4000 字符的内容...)
```

`maxChars` 可通过配置覆盖：

```yaml
# openclaw.yaml
agents:
  defaults:
    bootstrapMaxChars: 30000  # 自定义上限
```

#### 3.4.5 工作空间初始化

首次运行时，如果工作空间中缺少 Bootstrap 文件，系统会从模板创建：

```typescript
// src/agents/workspace.ts:125-198
export async function ensureAgentWorkspace(params?) {
  // 检查是否为全新工作空间 (所有文件都不存在)
  const isBrandNewWorkspace = existing.every((v) => !v);

  // 从模板目录加载默认内容
  const agentsTemplate = await loadTemplate("AGENTS.md");
  const soulTemplate = await loadTemplate("SOUL.md");
  // ... 其他模板

  // 只在文件不存在时写入 (flag: "wx" 确保不覆盖)
  await writeFileIfMissing(agentsPath, agentsTemplate);
  await writeFileIfMissing(soulPath, soulTemplate);
  // ...

  // BOOTSTRAP.md 仅在全新工作空间时创建
  if (isBrandNewWorkspace) {
    await writeFileIfMissing(bootstrapPath, bootstrapTemplate);
  }

  // 全新工作空间时初始化 git 仓库
  await ensureGitRepo(dir, isBrandNewWorkspace);
}
```

模板来自 `docs/reference/templates/` 目录，通过 `loadTemplate` 读取并自动剥离 YAML frontmatter。

#### 3.4.6 SOUL.md 的特殊处理

当 `contextFiles` 中包含名为 `SOUL.md` 的文件时，系统提示词中会额外插入一条人格遵从指令：

```typescript
// src/agents/system-prompt.ts:537-546
const hasSoulFile = contextFiles.some((file) => {
  const normalizedPath = file.path.trim().replace(/\\/g, "/");
  const baseName = normalizedPath.split("/").pop() ?? normalizedPath;
  return baseName.toLowerCase() === "soul.md";
});

if (hasSoulFile) {
  lines.push(
    "If SOUL.md is present, embody its persona and tone. " +
    "Avoid stiff, generic replies; follow its guidance " +
    "unless higher-priority instructions override it.",
  );
}
```

这使得用户可以通过编辑 `SOUL.md` 自定义 Agent 的回复风格和人格特征。

---

## 四、运行时信息

### 4.1 概念澄清：系统提示词中的"运行时信息"范围

系统提示词（System Prompt）是一段**静态文本**，在每次 LLM 调用前构建一次。它**不包含**以下动态内容：

| 信息类型 | 是否在系统提示词中 | 实际位置 |
| ---- | ---- | ---- |
| 环境元信息 (OS/模型/渠道等) | **是** (Runtime 行) | 系统提示词末尾 |
| 可用工具列表 | **是** (Tooling 节段) | 系统提示词开头 |
| 技能列表 | **是** (Skills 节段) | 系统提示词中段 |
| Bootstrap 文件内容 | **是** (Project Context) | 系统提示词末段 |
| 沙箱/工作空间信息 | **是** (Sandbox/Workspace) | 系统提示词中段 |
| **对话历史** | 否 | LLM 消息数组 (`messages`) |
| **工具调用记录** | 否 | LLM 消息数组 (`assistant` + `toolResult` 消息) |
| **历史工具输出** | 否 | LLM 消息数组 (`toolResult` 消息的 content) |
| **用户消息** | 否 | LLM 消息数组 (`user` 消息) |

系统提示词与 LLM 的关系：

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                      发送给 LLM 的完整请求                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  System Prompt (系统提示词) ← 本文档描述的内容                         │  │
│  │  ──────────────────────────────────────────────────────────────────    │  │
│  │  身份定义 + Tooling + Safety + Skills + Workspace + Sandbox +          │  │
│  │  Project Context (AGENTS.md/SOUL.md/...) + Runtime 行                  │  │
│  │                                                                        │  │
│  │  性质: 每次调用前重新生成的静态文本                                    │  │
│  │  作用: 告诉 LLM "你是谁、能做什么、有什么约束"                        │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Messages (消息数组) ← 不在本文档范围内                                │  │
│  │  ──────────────────────────────────────────────────────────────────    │  │
│  │  { role: "user",      content: "帮我写一个脚本" }                      │  │
│  │  { role: "assistant", content: [..., { type: "toolCall", ... }] }      │  │
│  │  { role: "toolResult", content: [{ type: "text", text: "..." }] }     │  │
│  │  { role: "assistant", content: "好的，脚本已创建" }                     │  │
│  │  { role: "user",      content: "谢谢" }                                │  │
│  │                                                                        │  │
│  │  性质: 累积的对话历史 (持久化在 session 文件中)                        │  │
│  │  作用: 提供对话上下文、工具调用记录                                    │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Tool Definitions (工具定义) ← 见 AGENT-RUNTIME-TOOLS.md              │  │
│  │  ──────────────────────────────────────────────────────────────────    │  │
│  │  通过 toToolDefinitions() 注入的 JSON Schema 工具声明                  │  │
│  │  LLM 根据这些定义生成 tool_use 块                                      │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Runtime 行

Runtime 行是系统提示词的最后一段，用一行紧凑格式汇总当前运行时的环境元信息。

#### 数据来源

`runtimeInfo` 在 `runEmbeddedAttempt` 中通过 `buildSystemPromptParams` 组装，数据来源均为系统 API 的实时调用：

```typescript
// src/agents/pi-embedded-runner/run/attempt.ts:317-333
const { runtimeInfo, userTimezone, userTime, userTimeFormat } = buildSystemPromptParams({
  config: params.config,
  agentId: sessionAgentId,
  workspaceDir: effectiveWorkspace,
  cwd: process.cwd(),
  runtime: {
    host: machineName,                           // ← os.hostname()
    os: `${os.type()} ${os.release()}`,          // ← Node.js os 模块
    arch: os.arch(),                             // ← Node.js os 模块
    node: process.version,                       // ← Node.js process 对象
    model: `${params.provider}/${params.modelId}`,// ← 当前选定的模型
    defaultModel: defaultModelLabel,             // ← 配置文件解析
    channel: runtimeChannel,                     // ← 触发消息的来源渠道
    capabilities: runtimeCapabilities,           // ← 渠道能力配置
    channelActions,                              // ← 渠道支持的操作
  },
});
```

`buildSystemPromptParams` 还会推导 `repoRoot`（向上查找 `.git` 目录）和用户时区信息：

```typescript
// src/agents/system-prompt-params.ts:33-58
export function buildSystemPromptParams(params) {
  const repoRoot = resolveRepoRoot({         // 从 workspaceDir/cwd 向上查找 .git
    config, workspaceDir, cwd
  });
  const userTimezone = resolveUserTimezone(   // 配置或系统默认
    config?.agents?.defaults?.userTimezone
  );
  const userTime = formatUserTime(new Date(), userTimezone, userTimeFormat);

  return {
    runtimeInfo: { agentId, ...params.runtime, repoRoot },
    userTimezone,
    userTime,
    userTimeFormat,
  };
}
```

#### 构建函数

```typescript
// src/agents/system-prompt.ts:594-629
export function buildRuntimeLine(
  runtimeInfo?: {
    agentId?: string;
    host?: string;
    os?: string;
    arch?: string;
    node?: string;
    model?: string;
    defaultModel?: string;
    repoRoot?: string;
  },
  runtimeChannel?: string,
  runtimeCapabilities: string[] = [],
  defaultThinkLevel?: ThinkLevel,
): string {
  return `Runtime: ${[
    runtimeInfo?.agentId ? `agent=${runtimeInfo.agentId}` : "",
    runtimeInfo?.host ? `host=${runtimeInfo.host}` : "",
    runtimeInfo?.repoRoot ? `repo=${runtimeInfo.repoRoot}` : "",
    runtimeInfo?.os
      ? `os=${runtimeInfo.os}${runtimeInfo?.arch ? ` (${runtimeInfo.arch})` : ""}`
      : "",
    runtimeInfo?.node ? `node=${runtimeInfo.node}` : "",
    runtimeInfo?.model ? `model=${runtimeInfo.model}` : "",
    runtimeInfo?.defaultModel ? `default_model=${runtimeInfo.defaultModel}` : "",
    runtimeChannel ? `channel=${runtimeChannel}` : "",
    runtimeChannel
      ? `capabilities=${runtimeCapabilities.length > 0 ? runtimeCapabilities.join(",") : "none"}`
      : "",
    `thinking=${defaultThinkLevel ?? "off"}`,
  ]
    .filter(Boolean)
    .join(" | ")}`;
}
```

**输出示例**:

```text
Runtime: agent=pi | host=macbook | repo=/Users/me/project | os=darwin (arm64) | node=v22.0.0 | model=anthropic/claude-sonnet-4-20250514 | default_model=anthropic/claude-sonnet-4-20250514 | channel=telegram | capabilities=inlineButtons,fileSharing | thinking=off
Reasoning: off (hidden unless on/stream). Toggle /reasoning; /status shows Reasoning when enabled.
```

#### 字段详解

| 字段 | 数据来源 | 含义 |
| ---- | ---- | ---- |
| `agent` | 会话路由解析 | Agent 标识 ID (如 `pi`) |
| `host` | `os.hostname()` | 主机名 |
| `repo` | 向上查找 `.git` 目录 | Git 仓库根目录路径 |
| `os` | `os.type()` + `os.release()` | 操作系统类型和版本 |
| `arch` | `os.arch()` | CPU 架构 (arm64/x64) |
| `node` | `process.version` | Node.js 版本 |
| `model` | `provider/modelId` 参数 | 当前使用的 LLM 模型 |
| `default_model` | 配置文件解析 | 默认模型 (如果与当前不同) |
| `channel` | 触发消息来源 | 消息渠道 (telegram/discord/slack...) |
| `capabilities` | 渠道配置 | 渠道能力 (inlineButtons/fileSharing...) |
| `thinking` | 用户设置 | 推理模式 (off/on/stream) |

### 4.3 系统提示词中的其他运行时上下文

除了 Runtime 行之外，系统提示词中散布了多处与当前运行时状态相关的信息：

| 节段 | 包含的运行时上下文 | 性质 |
| ---- | ---- | ---- |
| **Tooling** | 经过策略过滤后的可用工具列表 | 动态 (取决于当前策略配置) |
| **Workspace** | 当前工作目录路径 + 工作空间备注 | 半静态 |
| **Sandbox** | 沙箱是否启用、工作空间路径、挂载模式、浏览器状态、elevated 级别 | 动态 (每次运行可能不同) |
| **Date & Time** | 用户时区 + 当前时间 | 动态 (每次构建时刷新) |
| **Model Aliases** | 可用的模型别名映射 | 半静态 (取决于配置) |
| **Skills** | 经过过滤的可用技能列表及路径 | 动态 (取决于环境和配置) |
| **Project Context** | Bootstrap 文件的实际内容 (AGENTS.md 等) | 半静态 (文件变更时更新) |
| **Messaging** | 当前渠道的消息能力、inline buttons 状态 | 动态 (取决于渠道) |
| **Runtime** | 环境元信息汇总行 | 动态 |

### 4.4 对话历史和工具调用记录的位置

工具调用记录和对话上下文**不在系统提示词中**，而是以消息数组形式由 `AgentSession` 管理，持久化在 session JSONL 文件中。LLM 每次调用时，消息数组与系统提示词一起发送：

```text
session JSONL 文件 (~/.openclaw/agents/pi/sessions/*.jsonl)
    │
    ├── { role: "user", content: "帮我读取 package.json" }
    │
    ├── { role: "assistant", content: [
    │     { type: "text", text: "好的" },
    │     { type: "toolCall", id: "call_abc", name: "read", args: { path: "package.json" } }
    │   ]}
    │
    ├── { role: "toolResult", toolCallId: "call_abc", content: [
    │     { type: "text", text: '{"name": "openclaw", "version": "..."}' }
    │   ]}
    │
    └── { role: "assistant", content: "package.json 的内容是..." }
```

这些消息在上下文窗口不足时会被 **Auto Compaction** 机制压缩（详见 `src/agents/pi-embedded-runner/compact.ts`），但工具调用记录始终作为消息历史的一部分传递给 LLM，而非系统提示词的一部分。

---

## 五、PromptMode 条件控制

`promptMode` 参数决定提示词的详细程度，影响哪些节段会被包含：

| 节段 | full 模式 | minimal 模式 | 条件依赖 |
| ---- | ---- | ---- | ---- |
| 身份定义 | 是 | 是 | - |
| Tooling | 是 | 是 | - |
| Tool Call Style | 是 | 是 | - |
| Safety | 是 | 是 | - |
| CLI Reference | 是 | 否 | - |
| Skills | 是 | 否 | `skillsPrompt` 存在 |
| Memory Recall | 是 | 否 | 有 `memory_search` 工具 |
| Self-Update | 是 | 否 | 有 `gateway` 工具 |
| Model Aliases | 是 | 否 | `modelAliasLines` 存在 |
| Workspace | 是 | 是 | - |
| Documentation | 是 | 否 | `docsPath` 存在 |
| Sandbox | 是 | 是 | `sandboxInfo.enabled` |
| User Identity | 是 | 否 | `ownerNumbers` 存在 |
| Date & Time | 是 | 是 | - |
| Reply Tags | 是 | 否 | - |
| Messaging | 是 | 否 | - |
| Voice (TTS) | 是 | 否 | `ttsHint` 存在 |
| Extra Context | 是 | 是 | `extraSystemPrompt` 存在 |
| Reactions | 是 | 是 | `reactionGuidance` 存在 |
| Reasoning Format | 是 | 是 | `reasoningTagHint` 启用 |
| Project Context | 是 | 是 | `contextFiles` 存在 |
| Silent Replies | 是 | 否 | - |
| Heartbeats | 是 | 否 | - |
| Runtime | 是 | 是 | - |

**minimal 模式** 通常用于子代理 (subagent)，省略了技能、消息、自更新等不必要的节段以减少上下文占用。

### 提示词组装总览

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│  buildEmbeddedSystemPrompt()                                                    │
│       │                                                                         │
│       ├── 1. 身份定义 (固定)                                                    │
│       │                                                                         │
│       ├── 2. Tooling: 可用工具列表 + 描述                                       │
│       │                                                                         │
│       ├── 3. Tool Call Style: 工具调用风格指导                                  │
│       │                                                                         │
│       ├── 4. Safety: 安全规则                                                   │
│       │                                                                         │
│       ├── 5. CLI Reference: OpenClaw CLI 命令参考                               │
│       │                                                                         │
│       ├── 6. Skills: 技能列表 (full 模式)                                       │
│       │                                                                         │
│       ├── 7. Memory: 记忆搜索指导 (有 memory 工具时)                            │
│       │                                                                         │
│       ├── 8. Self-Update: 自更新指导 (有 gateway 工具时)                        │
│       │                                                                         │
│       ├── 9. Model Aliases: 模型别名 (full 模式)                                │
│       │                                                                         │
│       ├── 10. Workspace: 工作目录                                               │
│       │                                                                         │
│       ├── 11. Documentation: 文档链接 (full 模式)                               │
│       │                                                                         │
│       ├── 12. Sandbox: 沙箱信息 (沙箱启用时)                                    │
│       │                                                                         │
│       ├── 13. User Identity: 用户身份 (full 模式)                               │
│       │                                                                         │
│       ├── 14. Date & Time: 时区信息                                             │
│       │                                                                         │
│       ├── 15. Reply Tags: 回复标签 (full 模式)                                  │
│       │                                                                         │
│       ├── 16. Messaging: 消息指导 (full 模式)                                   │
│       │                                                                         │
│       ├── 17. Voice: TTS 指导 (配置了 TTS 时)                                   │
│       │                                                                         │
│       ├── 18. Extra Context: 额外上下文                                         │
│       │                                                                         │
│       ├── 19. Reactions: 反应指导 (配置了 reactionGuidance 时)                  │
│       │                                                                         │
│       ├── 20. Reasoning Format: 推理格式 (启用 reasoningTagHint 时)             │
│       │                                                                         │
│       ├── 21. Project Context: 项目上下文文件 (AGENTS.md, SOUL.md, ...)         │
│       │                                                                         │
│       ├── 22. Silent Replies: 静默回复指导 (full 模式)                          │
│       │                                                                         │
│       ├── 23. Heartbeats: 心跳指导 (full 模式)                                  │
│       │                                                                         │
│       └── 24. Runtime: 运行时信息行                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 六、上下文管理与溢出处理

OpenClaw Agent Runtime 采用 **四层防线** 机制来管理上下文窗口，确保即使在长时间对话或大量工具调用的场景下，LLM 请求也不会因为超出上下文窗口限制而失败。

### 6.1 总体架构：四层防线

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                      LLM 上下文窗口管理 - 四层防线                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  第一层：输入阶段截断（预防性）                                              │
│  ┌─────────────────────────────────────────────────────┐                    │
│  │  Bootstrap 文件截断（AGENTS.md, SOUL.md 等）         │                    │
│  │  默认上限: 20,000 字符/文件                           │                    │
│  │  策略: 保留前 70% + 后 20%，中间插入截断标记           │                    │
│  │                                                     │                    │
│  │  工具结果截断                                        │                    │
│  │  默认上限: 8,000 字符                                │                    │
│  │  错误信息上限: 400 字符                               │                    │
│  └─────────────────────────────────────────────────────┘                    │
│           │ 仍然可能超出？                                                   │
│           ▼                                                                 │
│  第二层：上下文裁剪（运行时在线裁剪）                                        │
│  ┌─────────────────────────────────────────────────────┐                    │
│  │  Context Pruning (cache-ttl 模式)                    │                    │
│  │  仅修改内存中的上下文，不影响磁盘 session              │                    │
│  │  两阶段: Soft Trim → Hard Clear                      │                    │
│  │  保护最近 N 轮 assistant 回复不被裁剪                  │                    │
│  └─────────────────────────────────────────────────────┘                    │
│           │ 仍然可能超出？                                                   │
│           ▼                                                                 │
│  第三层：自动压缩（SDK 内置）                                                │
│  ┌─────────────────────────────────────────────────────┐                    │
│  │  Auto Compaction (Pi SDK 内置机制)                    │                    │
│  │  触发: SDK 检测到上下文接近窗口上限                    │                    │
│  │  处理: 将旧消息摘要化，保留关键上下文                   │                    │
│  │  safeguard 模式: 分阶段摘要 + 超大消息降级             │                    │
│  │  保留: 工具失败记录、文件操作列表                       │                    │
│  └─────────────────────────────────────────────────────┘                    │
│           │ 仍然失败？                                                       │
│           ▼                                                                 │
│  第四层：溢出应急处理                                                        │
│  ┌─────────────────────────────────────────────────────┐                    │
│  │  Overflow Compaction (run loop 中的紧急措施)          │                    │
│  │  触发: LLM 返回 context_overflow 错误                 │                    │
│  │  处理: 强制执行一次完整的 compaction                   │                    │
│  │  限制: 仅尝试一次，失败则返回用户友好错误               │                    │
│  └─────────────────────────────────────────────────────┘                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 第一层：输入阶段截断

在数据进入 LLM 上下文之前，系统就对各类输入进行了预防性截断，避免单个大型输入占用过多上下文空间。

#### 6.2.1 Bootstrap 文件截断

**源文件**: `src/agents/pi-embedded-helpers/bootstrap.ts`

项目上下文文件（如 `AGENTS.md`、`SOUL.md` 等）在加载时受字符上限约束：

| 参数 | 默认值 | 说明 |
| ---- | ---- | ---- |
| `DEFAULT_BOOTSTRAP_MAX_CHARS` | 20,000 | 每个文件的最大字符数 |
| `BOOTSTRAP_HEAD_RATIO` | 0.7 (70%) | 截断时保留文件头部的比例 |
| `BOOTSTRAP_TAIL_RATIO` | 0.2 (20%) | 截断时保留文件尾部的比例 |

截断策略：当文件内容超过 `maxChars` 时，采用 **头尾保留** 策略：

```text
原始文件（30,000 字符）:
┌──────────────────────────────────────────────────────────┐
│ 前 14,000 字符 (70%)                                      │
├──────────────────────────────────────────────────────────┤
│ [...truncated, read AGENTS.md for full content...]        │  ← 截断标记
│ …(truncated AGENTS.md: kept 14000+4000 chars of 30000)…  │
├──────────────────────────────────────────────────────────┤
│ 后 4,000 字符 (20%)                                       │
└──────────────────────────────────────────────────────────┘
```

这样确保文件的开头（通常包含最重要的配置和规则）和结尾（通常包含最新添加的内容）都能被保留。

可以通过配置覆盖默认值：

```typescript
// src/agents/pi-embedded-helpers/bootstrap.ts
export function resolveBootstrapMaxChars(cfg?: OpenClawConfig): number {
  const raw = cfg?.agents?.defaults?.bootstrapMaxChars;
  if (typeof raw === "number" && Number.isFinite(raw) && raw > 0) {
    return Math.floor(raw);
  }
  return DEFAULT_BOOTSTRAP_MAX_CHARS;
}
```

#### 6.2.2 工具结果截断

**源文件**: `src/agents/pi-embedded-subscribe.tools.ts`

工具执行结果在返回给 LLM 之前也会被截断：

| 参数 | 默认值 | 说明 |
| ---- | ---- | ---- |
| `TOOL_RESULT_MAX_CHARS` | 8,000 | 工具输出的最大字符数 |
| `TOOL_ERROR_MAX_CHARS` | 400 | 错误信息的最大字符数 |

此外，`sanitizeToolResult` 函数还会对图像内容做特殊处理——删除实际的 base64 数据，只保留元信息（字节数、omitted 标记），避免图片数据大量消耗上下文空间。

### 6.3 第二层：上下文裁剪 (Context Pruning)

**源文件**: `src/agents/pi-extensions/context-pruning/`

Context Pruning 是一种 **运行时在线裁剪** 机制，以 Pi SDK Extension 的形式注册，在每次 LLM 请求前通过 `context` 事件对消息列表进行裁剪。

**关键特性**: 只修改内存中的上下文副本，不改写磁盘上的 session 文件。

#### 触发条件

```typescript
// src/agents/pi-extensions/context-pruning/extension.ts
api.on("context", (event: ContextEvent, ctx: ExtensionContext) => {
  // 1. 必须启用 cache-ttl 模式
  // 2. TTL 已过期（默认 5 分钟）
  // 3. 上下文占用比例 > softTrimRatio
});
```

#### 两阶段裁剪流程

```text
┌───────────────────────────────────────────────────────────────┐
│                   Context Pruning 流程                         │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  1. 计算上下文使用比例                                         │
│     ratio = estimateContextChars(messages) / charWindow        │
│     charWindow = contextWindowTokens × 4  (每 token ≈ 4 字符) │
│                                                               │
│  2. 确定保护区域                                               │
│     - 最近 N 个 assistant 回复（默认 3 个）不被裁剪             │
│     - 第一条 user 消息之前的消息不被裁剪（保护初始 bootstrap）   │
│                                                               │
│  3. 第一阶段：Soft Trim (ratio > 0.3 时触发)                   │
│     目标: 压缩过大的工具结果，保留有意义的头尾                   │
│     ┌──────────────────────────────────────────────┐           │
│     │ 对可裁剪的 toolResult 消息:                    │           │
│     │  if 文本长度 > 4,000 字符:                     │           │
│     │    保留前 1,500 字符 + 后 1,500 字符            │           │
│     │    中间替换为 "..."                             │           │
│     │    添加 "[Tool result trimmed: ...]" 注释       │           │
│     └──────────────────────────────────────────────┘           │
│                                                               │
│  4. 第二阶段：Hard Clear (ratio > 0.5 时触发)                  │
│     目标: 彻底清除旧的工具结果内容                               │
│     ┌──────────────────────────────────────────────┐           │
│     │ 对仍然可裁剪的 toolResult 消息:                │           │
│     │  替换为占位文本:                               │           │
│     │  "[Old tool result content cleared]"           │           │
│     │  逐个清除直到 ratio < hardClearRatio           │           │
│     └──────────────────────────────────────────────┘           │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

#### 配置参数

```typescript
// src/agents/pi-extensions/context-pruning/settings.ts
export const DEFAULT_CONTEXT_PRUNING_SETTINGS = {
  mode: "cache-ttl",        // 模式：仅支持 "cache-ttl"
  ttlMs: 5 * 60 * 1000,     // TTL 过期时间：5 分钟
  keepLastAssistants: 3,     // 保护最近 3 个 assistant 回复
  softTrimRatio: 0.3,        // 上下文占 30% 时开始 Soft Trim
  hardClearRatio: 0.5,       // 上下文占 50% 时开始 Hard Clear
  minPrunableToolChars: 50_000,  // 可裁剪工具结果总量 > 50K 字符才启动
  tools: {},                 // 工具 allow/deny 列表
  softTrim: {
    maxChars: 4_000,         // 单个工具结果 > 4K 字符才裁剪
    headChars: 1_500,        // 保留前 1,500 字符
    tailChars: 1_500,        // 保留后 1,500 字符
  },
  hardClear: {
    enabled: true,
    placeholder: "[Old tool result content cleared]",  // 清除后的占位文本
  },
};
```

#### 安全保护

- **图像内容豁免**: 含有图片的 toolResult 不会被裁剪（图像上下文通常直接相关）
- **Bootstrap 保护**: 第一条 user 消息之前的内容（如 SOUL.md 的初始读取）永远不被裁剪
- **最近回复保护**: 最近 N 个 assistant 回复及其关联的工具调用不被裁剪

### 6.4 第三层：自动压缩 (Auto Compaction)

**源文件**: `src/agents/pi-embedded-runner/compact.ts`, `src/agents/pi-extensions/compaction-safeguard.ts`, `src/agents/compaction.ts`

当 Pi SDK 检测到上下文接近窗口上限时，会触发 **Auto Compaction** — 将旧的对话历史压缩为一段摘要文本。

#### 基本模式 vs Safeguard 模式

| 特性 | 基本模式 (default) | Safeguard 模式 (safeguard) |
| ---- | ---- | ---- |
| 配置 | `compaction.mode = "default"` | `compaction.mode = "safeguard"` |
| 摘要方式 | SDK 默认单次摘要 | 分阶段摘要 (summarizeInStages) |
| 超大消息处理 | 可能失败 | 降级摘要 + 跳过超大消息 |
| 历史占比控制 | 无 | `maxHistoryShare` (默认 50%) |
| 工具失败保留 | 否 | 是（最多 8 条） |
| 文件操作记录 | 否 | 是（read/modified 文件列表） |

#### Safeguard 模式详解

Safeguard 模式通过 `compaction-safeguard` Extension 在 `session_before_compact` 事件中接管压缩流程：

```text
┌───────────────────────────────────────────────────────────────┐
│             Compaction Safeguard 流程                          │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  1. 检查历史占比                                               │
│     newContentTokens = tokensBefore - summarizableTokens      │
│     maxHistoryTokens = contextWindow × maxHistoryShare × 1.2  │
│     (1.2 = SAFETY_MARGIN，为 token 估算不准留缓冲)             │
│                                                               │
│  2. 如果新内容超出历史预算                                      │
│     ├── pruneHistoryForContextShare: 丢弃最旧的 chunk           │
│     └── 对丢弃的消息也生成摘要，作为主摘要的前置上下文           │
│                                                               │
│  3. 自适应分块                                                 │
│     ├── computeAdaptiveChunkRatio: 根据平均消息大小调整         │
│     │   - 正常: BASE_CHUNK_RATIO = 0.4 (40% 上下文窗口)       │
│     │   - 大消息: 递减至 MIN_CHUNK_RATIO = 0.15               │
│     └── chunkMessagesByMaxTokens: 按 token 上限分块            │
│                                                               │
│  4. 分阶段摘要 (summarizeInStages)                             │
│     ├── 将消息按 token 均分为 N 部分                            │
│     ├── 对每部分独立生成摘要                                    │
│     ├── 用 LLM 将部分摘要合并为统一摘要                         │
│     └── 降级: 跳过占上下文 50%+ 的超大单消息                    │
│                                                               │
│  5. 附加额外信息                                               │
│     ├── 工具失败记录（最多 8 条，每条 ≤ 240 字符）              │
│     └── 文件操作列表（已读文件 + 已修改文件）                    │
│                                                               │
│  6. 输出: 摘要文本 + firstKeptEntryId + tokensBefore           │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

#### Reserve Tokens 配置

压缩过程中需要预留一部分 token 给摘要本身：

```typescript
// src/agents/pi-settings.ts
export const DEFAULT_PI_COMPACTION_RESERVE_TOKENS_FLOOR = 20_000;
```

可以通过配置覆盖：

```yaml
agents:
  defaults:
    compaction:
      reserveTokensFloor: 30000
      mode: safeguard
      maxHistoryShare: 0.5
```

#### Compaction 事件流

压缩过程通过事件系统向外部通知状态变化：

```text
auto_compaction_start  →  处理中  →  auto_compaction_end
                                          │
                               ┌──────────┴──────────┐
                               │                     │
                         willRetry: true        willRetry: false
                               │                     │
                         重置状态，再次尝试       压缩完成
```

在 `subscribeEmbeddedPiSession` 中，这些事件通过 `handleAutoCompactionStart` 和 `handleAutoCompactionEnd` 处理，并通过 `onAgentEvent` 回调通知上层 UI。

### 6.5 第四层：溢出应急处理 (Overflow Compaction)

**源文件**: `src/agents/pi-embedded-runner/run.ts`

如果前三层防线都未能阻止溢出，LLM API 会返回错误。此时 run loop 中的应急机制介入：

```text
┌─────────────────────────────────────────────────────────────────┐
│                   Run Loop 溢出处理流程                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  runEmbeddedAttempt() 返回 promptError                           │
│       │                                                         │
│       ▼                                                         │
│  isContextOverflowError(errorText)?                              │
│       │                                                         │
│       ├── 是 compaction_failure? → 直接返回错误                   │
│       │   (说明前一次压缩已失败，再试也没用)                       │
│       │                                                         │
│       ├── 已经尝试过 overflow compaction? → 直接返回错误           │
│       │   (每次 run 只允许尝试一次)                               │
│       │                                                         │
│       └── 首次溢出 → 执行 compactEmbeddedPiSessionDirect()       │
│                │                                                 │
│                ├── 成功 → log.info("auto-compaction succeeded")  │
│                │         → continue (重试 LLM 请求)               │
│                │                                                 │
│                └── 失败 → log.warn("auto-compaction failed")     │
│                           → 返回用户友好错误:                     │
│                           "Context overflow: prompt too large    │
│                            for the model. Try again with less    │
│                            input or a larger-context model."     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 溢出错误检测

`isContextOverflowError` 函数通过多种模式匹配来识别不同 LLM 提供商的溢出错误：

```typescript
// src/agents/pi-embedded-helpers/errors.ts
// 匹配的关键词包括:
// - "request_too_large"
// - "context length exceeded"
// - "maximum context length"
// - "prompt is too long"
// - "exceeds model context window"
// - "context overflow"
// - 413 too large (HTTP 状态码)
```

### 6.6 对话历史限制 (History Turns Limit)

**源文件**: `src/agents/pi-embedded-runner/history.ts`

除了上述基于 token 的管理机制外，系统还提供按 **对话轮数** 限制历史的功能，主要用于长时间运行的 DM 会话：

```typescript
// 只保留最近 N 轮用户消息及其关联的 assistant 回复
export function limitHistoryTurns(
  messages: AgentMessage[],
  limit: number | undefined,
): AgentMessage[] {
  // 从后向前扫描，计数 user 消息
  // 超过 limit 时截断旧消息
}
```

这个限制可以按提供商和用户粒度配置：

```yaml
channels:
  telegram:
    dmHistoryLimit: 50        # 全局默认: 最近 50 轮
    dms:
      "12345678":
        historyLimit: 100     # 特定用户: 最近 100 轮
```

### 6.7 工具结果处理

工具调用的结果是上下文膨胀的主要来源之一。系统在多个层级对工具结果进行管理：

| 层级 | 机制 | 时机 | 影响范围 |
| ---- | ---- | ---- | ---- |
| 即时截断 | `truncateToolText` (8K 字符) | 工具执行完毕后 | 当次工具结果 |
| 图像清理 | `sanitizeToolResult` 移除 base64 | 工具结果返回时 | 图像类型结果 |
| Soft Trim | 保留头尾各 1.5K 字符 | Context Pruning 阶段 | 旧的 toolResult 消息 |
| Hard Clear | 替换为占位文本 | Context Pruning 阶段 | 更旧的 toolResult 消息 |
| 摘要压缩 | 整体摘要为文本 | Compaction 阶段 | 所有旧消息 |

### 各层防线触发条件总结

```text
对话开始
    │
    ▼
[第一层] Bootstrap 文件 > 20K 字符？ → 截断 (头 70% + 尾 20%)
         工具结果 > 8K 字符？       → 截断 (保留前 8K)
    │
    ▼
[第二层] 上下文占比 > 30%?    → Soft Trim (压缩工具结果)
         上下文占比 > 50%?    → Hard Clear (清除工具结果)
         (需要 cache-ttl 模式启用，且 TTL 已过期)
    │
    ▼
[第三层] SDK 检测到接近窗口上限 → Auto Compaction
         (safeguard 模式: 分阶段摘要、超大消息降级、保留工具失败记录)
    │
    ▼
[第四层] LLM 返回 context_overflow → Overflow Compaction (强制压缩一次)
         仍然失败 → 返回用户友好错误消息
```

---

## 关键源文件

| 文件 | 行数 | 核心功能 |
| ---- | ---- | -------- |
| `src/agents/system-prompt.ts` | ~630 | 系统提示词核心构建逻辑 |
| `src/agents/pi-embedded-runner/system-prompt.ts` | ~97 | 嵌入式提示词入口 |
| `src/agents/tool-summaries.ts` | ~20 | 工具摘要生成 |
| `src/agents/pi-embedded-helpers/bootstrap.ts` | ~192 | Bootstrap 文件截断和加载 |
| `src/agents/pi-extensions/context-pruning/pruner.ts` | ~347 | 上下文裁剪核心逻辑 |
| `src/agents/pi-extensions/context-pruning/settings.ts` | ~124 | 裁剪配置和默认值 |
| `src/agents/pi-extensions/compaction-safeguard.ts` | ~337 | Safeguard 模式压缩逻辑 |
| `src/agents/compaction.ts` | ~357 | 分阶段摘要和历史裁剪 |
| `src/agents/pi-embedded-runner/compact.ts` | ~490 | 压缩入口和环境准备 |
| `src/agents/pi-embedded-runner/history.ts` | ~99 | 对话轮数限制 |
| `src/agents/pi-embedded-runner/run.ts` | ~440 | Run loop 溢出应急处理 |
| `src/agents/pi-embedded-subscribe.tools.ts` | ~210 | 工具结果截断和清理 |
| `src/agents/pi-embedded-helpers/errors.ts` | ~624 | 溢出错误检测和分类 |
