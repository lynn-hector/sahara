# Agent Runtime 工具系统详解

> 本文档详细解析 Agent Runtime 中工具的创建、分类、策略系统、调用协议与执行流程。

---

## 目录

1. [工具创建入口](#一工具创建入口)
2. [工具分类与组成](#二工具分类与组成)
3. [核心工具实现](#三核心工具实现)
   - [exec 工具](#31-exec-工具-shell-执行)
   - [read 工具](#32-read-工具)
   - [OpenClaw 工具集](#33-openclaw-工具创建)
4. [工具策略系统](#四工具策略系统)
   - [策略系统的作用](#41-策略系统的作用)
   - [工具组](#42-工具组快捷引用)
   - [预设配置文件](#43-预设配置文件-profiles)
   - [逐层过滤](#44-逐层过滤七层策略链)
   - [alsoAllow](#45-alsoallow安全的工具追加)
   - [插件工具处理](#46-插件工具的特殊处理)
   - [配置示例](#47-配置示例)
   - [匹配模式](#48-策略匹配支持的模式)
5. [工具包装与增强](#五工具包装与增强)
6. [工具调用协议](#六工具调用协议)
   - [核心数据结构](#61-核心数据结构)
   - [工具定义适配](#62-工具定义适配)
   - [调用时序与事件流](#63-调用时序与事件流)
   - [Tool Call ID 处理](#64-tool-call-id-处理)
   - [结果处理与截断](#65-结果处理与截断)
7. [AgentSession 与事件通信](#七agentsession-与事件通信)
   - [AgentSession 的结构](#71-agentsession-的结构)
   - [Session 的创建与生命周期](#72-session-的创建与生命周期)
   - [事件订阅机制](#73-事件订阅机制-subscribe)
   - [EventHandler 的分发逻辑](#74-eventhandler-的分发逻辑)
   - [完整通信流程图](#75-完整通信流程图)
   - [事件类型判定机制](#76-事件类型判定机制)
   - [流式数据缓冲与处理](#77-流式数据缓冲与处理)
   - [从 EventHandler 到用户的消息投递链路](#78-从-eventhandler-到用户的消息投递链路)

---

## 一、工具创建入口

Agent Runtime 中所有工具的创建都通过 `createOpenClawCodingTools` 函数完成：

```typescript
// src/agents/pi-tools.ts:114-160
export function createOpenClawCodingTools(options?: {
  exec?: ExecToolDefaults & ProcessToolDefaults;
  messageProvider?: string;
  agentAccountId?: string;
  messageTo?: string;
  messageThreadId?: string | number;
  sandbox?: SandboxContext | null;           // 沙箱上下文
  sessionKey?: string;
  agentDir?: string;
  workspaceDir?: string;
  config?: OpenClawConfig;
  abortSignal?: AbortSignal;
  modelProvider?: string;                     // 当前模型提供商
  modelId?: string;                           // 当前模型ID
  modelAuthMode?: ModelAuthMode;              // 认证模式
  currentChannelId?: string;                  // Slack 自动线程
  currentThreadTs?: string;
  groupId?: string | null;                    // 群组策略
  groupChannel?: string | null;
  groupSpace?: string | null;
  spawnedBy?: string | null;                  // 父会话键
  senderId?: string | null;
  senderName?: string | null;
  senderUsername?: string | null;
  senderE164?: string | null;
  replyToMode?: "off" | "first" | "all";
  hasRepliedRef?: { value: boolean };
  modelHasVision?: boolean;                   // 模型是否有视觉能力
}): AnyAgentTool[]
```

---

## 二、工具分类与组成

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           OpenClaw 工具体系                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│  Coding Tools (来自 @mariozechner/pi-coding-agent)                              │
│  ───────────────────────────────────────────────────────────────────────────    │
│  • read     - 读取文件内容 (支持图片)                                           │
│  • write    - 创建/覆盖文件                                                     │
│  • edit     - 精确编辑文件                                                      │
│  • grep     - 搜索文件内容                                                      │
│  • find     - 按 glob 模式查找文件                                              │
│  • ls       - 列出目录内容                                                      │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Bash Tools (自实现)                                                            │
│  ───────────────────────────────────────────────────────────────────────────    │
│  • exec     - 执行 Shell 命令 (支持 PTY、后台、沙箱)                            │
│  • process  - 管理后台进程会话                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  OpenClaw Tools (自实现)                                                        │
│  ───────────────────────────────────────────────────────────────────────────    │
│  • browser      - 浏览器控制                                                    │
│  • canvas       - Canvas 展示/执行                                              │
│  • nodes        - 节点设备控制                                                  │
│  • cron         - 定时任务/提醒                                                 │
│  • message      - 消息发送                                                      │
│  • tts          - 文本转语音                                                    │
│  • gateway      - Gateway 管理                                                  │
│  • agents_list  - 列出可用 Agent                                                │
│  • sessions_list    - 列出会话                                                  │
│  • sessions_history - 获取会话历史                                              │
│  • sessions_send    - 发送消息到会话                                            │
│  • sessions_spawn   - 创建子代理                                                │
│  • session_status   - 会话状态                                                  │
│  • web_search   - Web 搜索                                                      │
│  • web_fetch    - 获取网页内容                                                  │
│  • image        - 图片分析                                                      │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Channel Tools (渠道特定)                                                       │
│  ───────────────────────────────────────────────────────────────────────────    │
│  • telegram_*   - Telegram 操作                                                 │
│  • discord_*    - Discord 操作                                                  │
│  • slack_*      - Slack 操作                                                    │
│  • whatsapp_*   - WhatsApp 操作                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Plugin Tools (插件扩展)                                                        │
│  ───────────────────────────────────────────────────────────────────────────    │
│  • 从 extensions/* 加载的工具                                                   │
│  • 受 toolAllowlist 策略控制                                                    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、核心工具实现

### 3.1 exec 工具 (Shell 执行)

`exec` 是最复杂的工具，支持多种执行模式：

```typescript
// src/agents/bash-tools.exec.ts:195-225
const execSchema = Type.Object({
  command: Type.String({ description: "Shell command to execute" }),
  workdir: Type.Optional(Type.String({ description: "Working directory (defaults to cwd)" })),
  env: Type.Optional(Type.Record(Type.String(), Type.String())),
  yieldMs: Type.Optional(
    Type.Number({
      description: "Milliseconds to wait before backgrounding (default 10000)",
    }),
  ),
  background: Type.Optional(Type.Boolean({ description: "Run in background immediately" })),
  timeout: Type.Optional(
    Type.Number({
      description: "Timeout in seconds (optional, kills process on expiry)",
    }),
  ),
  pty: Type.Optional(
    Type.Boolean({
      description:
        "Run in a pseudo-terminal (PTY) when available (TTY-required CLIs, coding agents)",
    }),
  ),
  elevated: Type.Optional(
    Type.Boolean({
      description: "Run on the host with elevated permissions (if allowed)",
    }),
  ),
  host: Type.Optional(
    Type.String({
      description: "Exec host (sandbox|gateway|node).",
    }),
  ),
});
```

**执行流程**:

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         exec 工具执行流程                                        │
└─────────────────────────────────────────────────────────────────────────────────┘

输入: { command, workdir?, env?, yieldMs?, background?, timeout?, pty?, elevated?, host? }
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 1: 安全检查                                                               │
│                                                                                 │
│  // 宿主机执行时验证环境变量                                                    │
│  if (!isSandboxed && env) {                                                     │
│    validateHostEnv(env);  // 禁止 LD_PRELOAD、PATH 等危险变量                   │
│  }                                                                              │
│                                                                                 │
│  // 危险变量黑名单                                                              │
│  const DANGEROUS_HOST_ENV_VARS = new Set([                                      │
│    "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT",                                 │
│    "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH",                                │
│    "NODE_OPTIONS", "NODE_PATH", "PYTHONPATH", "PYTHONHOME",                     │
│    "RUBYLIB", "PERL5LIB", "BASH_ENV", "ENV", "GCONV_PATH", "IFS",              │
│  ]);                                                                            │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 2: 执行主机选择                                                           │
│                                                                                 │
│  host 参数决定命令执行位置:                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  sandbox  - 在 Docker 容器内执行                                        │   │
│  │  gateway  - 在 Gateway 宿主机执行                                       │   │
│  │  node     - 在远程节点执行                                              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  // 沙箱执行时构建 docker exec 命令                                             │
│  if (sandbox) {                                                                 │
│    const dockerArgs = buildDockerExecArgs({                                     │
│      containerName: sandbox.containerName,                                      │
│      workdir: resolveSandboxWorkdir(params.workdir, sandbox),                   │
│      env: buildSandboxEnv(params.env, sandbox.env),                             │
│      command: params.command,                                                   │
│    });                                                                          │
│    // 实际执行: docker exec ... container command                               │
│  }                                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 3: 审批检查                                                               │
│                                                                                 │
│  // 检查命令是否需要审批                                                        │
│  const approvalNeeded = requiresExecApproval({                                  │
│    command: params.command,                                                     │
│    security,      // "off" | "safe" | "ask"                                     │
│    safeBins,      // 安全命令白名单                                             │
│    allowlist,     // 已审批命令列表                                             │
│  });                                                                            │
│                                                                                 │
│  if (approvalNeeded) {                                                          │
│    // 发送审批请求到用户                                                        │
│    const approval = await requestApproval(...);                                 │
│    if (approval === "deny") {                                                   │
│      return { content: [{ type: "text", text: "Command denied" }] };            │
│    }                                                                            │
│    if (approval === "always") {                                                 │
│      addAllowlistEntry(command);  // 记住此命令                                 │
│    }                                                                            │
│  }                                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 4: 进程启动                                                               │
│                                                                                 │
│  if (params.pty) {                                                              │
│    // PTY 模式 (终端 UI 程序)                                                   │
│    const pty = require("node-pty");                                             │
│    handle = pty.spawn(shell, ["-c", command], {                                 │
│      cwd: workdir,                                                              │
│      env: mergedEnv,                                                            │
│      cols: 120, rows: 40,                                                       │
│    });                                                                          │
│  } else {                                                                       │
│    // 普通模式                                                                  │
│    handle = spawn(shell, ["-c", command], {                                     │
│      cwd: workdir,                                                              │
│      env: mergedEnv,                                                            │
│      stdio: ["pipe", "pipe", "pipe"],                                           │
│    });                                                                          │
│  }                                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 5: 后台/超时处理                                                          │
│                                                                                 │
│  const yieldWindow = background ? 0 : (yieldMs ?? 10000);                       │
│                                                                                 │
│  // 等待命令完成或超时                                                          │
│  const result = await Promise.race([                                            │
│    handle.promise,                       // 命令完成                            │
│    sleep(yieldWindow),                   // 后台超时                            │
│    timeout ? sleep(timeout * 1000) : never,  // 硬超时                          │
│  ]);                                                                            │
│                                                                                 │
│  if (!handle.done && yieldWindow passed) {                                      │
│    // 转入后台                                                                  │
│    markBackgrounded(session);                                                   │
│    return {                                                                     │
│      content: [{                                                                │
│        type: "text",                                                            │
│        text: `[backgrounded] session=${slug} pid=${pid}`                        │
│      }]                                                                         │
│    };                                                                           │
│  }                                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 6: 输出处理                                                               │
│                                                                                 │
│  // 截断过长输出                                                                │
│  const truncated = truncateMiddle(output, MAX_OUTPUT_CHARS);                    │
│                                                                                 │
│  // 清理二进制内容                                                              │
│  const sanitized = sanitizeBinaryOutput(truncated);                             │
│                                                                                 │
│  return {                                                                       │
│    content: [{                                                                  │
│      type: "text",                                                              │
│      text: `exit_code=${exitCode}\n${sanitized}`                                │
│    }]                                                                           │
│  };                                                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 read 工具

```typescript
// src/agents/pi-tools.read.ts:286-302
export function createOpenClawReadTool(base: AnyAgentTool): AnyAgentTool {
  const patched = patchToolSchemaForClaudeCompatibility(base);
  return {
    ...patched,
    execute: async (toolCallId, params, signal) => {
      // 1. 参数规范化 (支持 Claude Code 风格的 file_path)
      const normalized = normalizeToolParams(params);
      
      // 2. 验证必需参数
      assertRequiredParams(record, CLAUDE_PARAM_GROUPS.read, base.name);
      
      // 3. 执行原始 read
      const result = await base.execute(toolCallId, normalized ?? params, signal);
      
      // 4. 图片结果规范化 (MIME 类型检测)
      const normalizedResult = await normalizeReadImageResult(result, filePath);
      
      // 5. 图片大小限制
      return sanitizeToolResultImages(normalizedResult, `read:${filePath}`);
    },
  };
}
```

**沙箱版本**:

```typescript
// src/agents/pi-tools.read.ts:271-284
export function createSandboxedReadTool(root: string) {
  const base = createReadTool(root) as unknown as AnyAgentTool;
  // 添加路径安全检查
  return wrapSandboxPathGuard(createOpenClawReadTool(base), root);
}

function wrapSandboxPathGuard(tool: AnyAgentTool, root: string): AnyAgentTool {
  return {
    ...tool,
    execute: async (toolCallId, args, signal, onUpdate) => {
      const filePath = record?.path;
      if (typeof filePath === "string" && filePath.trim()) {
        // 确保路径在沙箱根目录内
        await assertSandboxPath({ filePath, cwd: root, root });
      }
      return tool.execute(toolCallId, normalized ?? args, signal, onUpdate);
    },
  };
}
```

### 3.3 OpenClaw 工具创建

```typescript
// src/agents/openclaw-tools.ts:22-162
export function createOpenClawTools(options?: {...}): AnyAgentTool[] {
  // 图片分析工具 (需要 agentDir)
  const imageTool = options?.agentDir?.trim()
    ? createImageTool({
        config: options?.config,
        agentDir: options.agentDir,
        sandboxRoot: options?.sandboxRoot,
        modelHasVision: options?.modelHasVision,
      })
    : null;

  // Web 搜索工具
  const webSearchTool = createWebSearchTool({
    config: options?.config,
    sandboxed: options?.sandboxed,
  });

  // Web 获取工具
  const webFetchTool = createWebFetchTool({
    config: options?.config,
    sandboxed: options?.sandboxed,
  });

  const tools: AnyAgentTool[] = [
    createBrowserTool({...}),
    createCanvasTool(),
    createNodesTool({...}),
    createCronTool({...}),
    createMessageTool({...}),
    createTtsTool({...}),
    createGatewayTool({...}),
    createAgentsListTool({...}),
    createSessionsListTool({...}),
    createSessionsHistoryTool({...}),
    createSessionsSendTool({...}),
    createSessionsSpawnTool({...}),
    createSessionStatusTool({...}),
    ...(webSearchTool ? [webSearchTool] : []),
    ...(webFetchTool ? [webFetchTool] : []),
    ...(imageTool ? [imageTool] : []),
  ];

  // 加载插件工具
  const pluginTools = resolvePluginTools({
    context: { config, workspaceDir, agentDir, agentId, sessionKey, ... },
    existingToolNames: new Set(tools.map((tool) => tool.name)),
    toolAllowlist: options?.pluginToolAllowlist,
  });

  return [...tools, ...pluginTools];
}
```

---

## 四、工具策略系统

### 4.1 策略系统的作用

工具策略系统的核心目的是 **控制 LLM 能使用哪些工具**。不同场景下，LLM 可用的工具集应该不同：

- **在群聊中**: 不应让 LLM 执行 shell 命令（`exec`），避免安全风险
- **使用廉价小模型时**: 只给基础工具，避免小模型滥用复杂工具
- **子代理 (subagent)**: 不应管理 session 或系统配置
- **沙箱模式**: 限制可能绕过沙箱的工具

策略系统通过 `allow`（白名单）和 `deny`（黑名单）两个列表来控制，每条策略的判断逻辑：

```text
对于每个工具:
  1. 如果工具名在 deny 列表中 → 拒绝（deny 优先级最高）
  2. 如果 allow 列表为空 → 允许（没有白名单 = 不限制）
  3. 如果工具名在 allow 列表中 → 允许
  4. 否则 → 拒绝
```

```typescript
// src/agents/pi-tools.policy.ts:58-77
function makeToolPolicyMatcher(policy: SandboxToolPolicy) {
  const deny = compilePatterns(policy.deny);   // 编译黑名单模式
  const allow = compilePatterns(policy.allow);  // 编译白名单模式
  return (name: string) => {
    const normalized = normalizeToolName(name);
    if (matchesAny(normalized, deny)) return false;   // deny 优先
    if (allow.length === 0) return true;               // 无白名单 = 允许全部
    if (matchesAny(normalized, allow)) return true;    // 在白名单中
    return false;                                       // 不在白名单中 = 拒绝
  };
}
```

### 4.2 工具组：快捷引用

为了避免逐个列举工具名，系统定义了 **工具组 (Tool Groups)**，可以在 allow/deny 中用组名代替多个工具名：

```typescript
// src/agents/tool-policy.ts:13-57
export const TOOL_GROUPS: Record<string, string[]> = {
  "group:memory":     ["memory_search", "memory_get"],
  "group:web":        ["web_search", "web_fetch"],
  "group:fs":         ["read", "write", "edit", "apply_patch"],
  "group:runtime":    ["exec", "process"],
  "group:sessions":   ["sessions_list", "sessions_history", "sessions_send",
                        "sessions_spawn", "session_status"],
  "group:ui":         ["browser", "canvas"],
  "group:automation": ["cron", "gateway"],
  "group:messaging":  ["message"],
  "group:nodes":      ["nodes"],
  "group:openclaw":   [/* 所有 OpenClaw 原生工具 */],
};
```

例如，配置 `allow: ["group:fs"]` 等价于 `allow: ["read", "write", "edit", "apply_patch"]`。

另外还支持通配符（`*`）和插件组（`group:plugins`、具体插件 ID），以及工具名别名（`bash` → `exec`）。

### 4.3 预设配置文件 (Profiles)

Profile 是一组预定义的 allow 策略，适合快速切换工具集范围：

| Profile | 允许的工具 | 典型场景 |
| ---- | ---- | ---- |
| `minimal` | 仅 `session_status` | 只读状态查询，最安全 |
| `coding` | `group:fs` + `group:runtime` + `group:sessions` + `group:memory` + `image` | 编程开发 |
| `messaging` | `group:messaging` + 部分 session 工具 | 消息渠道交互 |
| `full` | 无限制（空 allow = 允许全部） | 完整功能 |

```yaml
# 配置示例
tools:
  profile: coding    # 全局使用 coding 配置
  byProvider:
    anthropic:
      profile: full  # Anthropic 模型使用 full 配置
```

### 4.4 逐层过滤：七层策略链

这是策略系统的核心设计。系统不是用一条策略决定工具的可用性，而是 **将多个来源的策略依次叠加**，每一层都在上一层的结果基础上进一步过滤。一个工具必须 **通过所有层的检查** 才能最终被 LLM 使用。

```text
初始工具集: [read, write, edit, exec, process, browser, canvas, message, gateway, cron, ...]
                                        约 30+ 个工具
    │
    │  Layer 1: Profile Policy (配置文件策略)
    │  来源: cfg.tools.profile
    │  作用: 根据预设配置文件做第一轮粗筛
    │  例: profile: "coding"
    │      allow = [read, write, edit, apply_patch, exec, process,
    │               sessions_*, session_status, memory_*, image]
    │      → 移除 browser, canvas, message, gateway, cron, nodes...
    ▼
剩余: [read, write, edit, exec, process, sessions_list, ..., memory_search, image]
    │
    │  Layer 2: Provider Profile Policy (提供商配置文件策略)
    │  来源: cfg.tools.byProvider.<provider>.profile
    │  作用: 针对特定 LLM 提供商进一步限制
    │  例: byProvider.google.profile: "minimal"
    │      → 如果用 Google 模型，只留 session_status
    │  (如果未配置，此层不过滤，全部通过)
    ▼
剩余: (通常不变，除非配了 byProvider)
    │
    │  Layer 3: Global Policy (全局策略)
    │  来源: cfg.tools.allow / cfg.tools.deny
    │  作用: 管理员级别的全局限制
    │  例: deny: ["gateway"]
    │      → 全局禁用 gateway 工具
    ▼
剩余: (去掉了 gateway)
    │
    │  Layer 4: Global Provider Policy (全局提供商策略)
    │  来源: cfg.tools.byProvider.<provider>.allow / deny
    │  作用: 针对特定提供商的全局限制（非 profile，而是直接 allow/deny）
    │  例: byProvider.openai.deny: ["web_search"]
    │      → OpenAI 模型不允许使用 web_search
    ▼
剩余: (去掉了 web_search，如果用的是 OpenAI)
    │
    │  Layer 5: Agent Policy (Agent 级别策略)
    │  来源: cfg.agents.<agentId>.tools.allow / deny
    │  作用: 特定 Agent 的工具限制
    │  例: agents.research.tools.allow: ["group:web", "group:fs"]
    │      → "research" Agent 只能用文件和网络工具
    ▼
剩余: (按 Agent 配置进一步缩小)
    │
    │  Layer 6: Agent Provider Policy (Agent 提供商策略)
    │  来源: cfg.agents.<agentId>.tools.byProvider.<provider>.allow / deny
    │  作用: 特定 Agent + 特定提供商的组合限制
    ▼
剩余: (通常不变，除非有极细粒度配置)
    │
    │  Layer 7: Group Policy (群组策略)
    │  来源: 渠道群组的工具策略 (Telegram 群、Discord 频道等)
    │  作用: 限制在特定群聊中可用的工具
    │  例: Telegram 群 "family" 中禁止 exec
    │      → 在该群中去掉 exec
    ▼
剩余: (按群组配置过滤)
    │
    │  Layer 8: Sandbox Policy (沙箱策略)
    │  来源: sandbox.tools (沙箱配置)
    │  作用: 沙箱模式下的额外限制
    ▼
剩余: (按沙箱配置过滤)
    │
    │  Layer 9: Subagent Policy (子代理策略)
    │  来源: 内置默认 + cfg.tools.subagents.tools
    │  作用: 子代理不应使用的工具
    │  默认 deny: [sessions_*, gateway, agents_list, cron,
    │             session_status, memory_*, whatsapp_login]
    │  (仅在 subagent session 中生效)
    ▼
最终可用工具集: [read, write, edit, exec, ...]
    → 注册为 LLM 的 Tool Definitions
```

#### 为什么要逐层过滤而不是合并为一条策略

1. **权限收窄原则**: 每一层只能缩小工具范围，不能扩大（一个工具被任意一层拒绝就无法使用）。这确保了安全性——即使 Agent 配置允许 `exec`，如果群组策略禁止了，`exec` 也不可用。

2. **职责分离**: 不同层对应不同的管理角色：
   - Profile/Global: 系统管理员设定基线
   - Agent: 每个 Agent 有独立的工具范围
   - Group: 群组管理员控制群内安全
   - Sandbox: 沙箱提供额外隔离
   - Subagent: 子代理天然受限

3. **灵活覆盖**: 可以在粗粒度层设宽松策略，在细粒度层做精确限制，无需修改全局配置。

#### 实际代码中的逐层调用

```typescript
// src/agents/pi-tools.ts:397-423 (简化)

// Layer 1: Profile
let result = filterToolsByPolicy(allTools, profilePolicy);

// Layer 2: Provider Profile
result = filterToolsByPolicy(result, providerProfilePolicy);

// Layer 3: Global
result = filterToolsByPolicy(result, globalPolicy);

// Layer 4: Global Provider
result = filterToolsByPolicy(result, globalProviderPolicy);

// Layer 5: Agent
result = filterToolsByPolicy(result, agentPolicy);

// Layer 6: Agent Provider
result = filterToolsByPolicy(result, agentProviderPolicy);

// Layer 7: Group
result = filterToolsByPolicy(result, groupPolicy);

// Layer 8: Sandbox
result = filterToolsByPolicy(result, sandboxPolicy);

// Layer 9: Subagent
result = filterToolsByPolicy(result, subagentPolicy);

// → result 就是最终可用的工具集
```

每次 `filterToolsByPolicy` 调用都以前一层的输出作为输入，逐步缩小范围。如果某一层没有配置策略（`undefined`），则跳过该层，直接传递给下一层。

### 4.5 alsoAllow：安全的工具追加

如果用了 Profile（如 `coding`），白名单会锁定工具范围。要在 Profile 的基础上额外添加工具（而不是重新定义白名单），可以用 `alsoAllow`：

```yaml
tools:
  profile: coding
  alsoAllow: ["browser", "web_search"]  # 在 coding 基础上追加
```

`alsoAllow` 会与 Profile 的 `allow` 合并，而不是替换：

```typescript
// src/agents/pi-tools.policy.ts:130-140
function unionAllow(base?: string[], extra?: string[]) {
  // 如果有 alsoAllow 但没有 allow，隐式等于 allow-all + alsoAllow
  if (!Array.isArray(base) || base.length === 0) {
    return Array.from(new Set(["*", ...extra]));
  }
  return Array.from(new Set([...base, ...extra]));
}
```

### 4.6 插件工具的特殊处理

当 `allow` 列表中只包含插件工具名（而不包含任何核心工具），系统会认为这是一个配置错误，并 **剥离该白名单**，避免意外禁用所有核心工具：

```typescript
// src/agents/tool-policy.ts:201-241
export function stripPluginOnlyAllowlist(policy, pluginGroups, coreTools) {
  // 如果 allow 中没有任何核心工具名 → 剥离整个 allow
  // 避免: allow: ["my-plugin-tool"] 意外禁用 exec, read, write 等
  if (strippedAllowlist) {
    return { ...policy, allow: undefined };
  }
}
```

日志会警告：`"Ignoring allowlist so core tools remain available. Use tools.alsoAllow for additive plugin tool enablement."`

### 4.7 配置示例

#### 场景 1：群聊中禁止 exec

```yaml
channels:
  telegram:
    groups:
      "-1001234567890":
        tools:
          deny: ["exec", "process", "gateway"]
```

效果：在该 Telegram 群中，LLM 无法执行 shell 命令或管理 gateway。

#### 场景 2：特定 Agent 只做编程

```yaml
agents:
  coder:
    tools:
      profile: coding
```

效果：`coder` Agent 只能使用文件读写、shell 执行、session 管理和记忆搜索工具。

#### 场景 3：对小模型限制工具

```yaml
tools:
  byProvider:
    google/gemini-flash:
      profile: minimal
      alsoAllow: ["group:fs"]
```

效果：当使用 `gemini-flash` 时，只允许 `session_status` + 文件操作工具。

#### 场景 4：子代理默认限制

子代理 (subagent) 有内置的 deny 列表，无需额外配置：

```typescript
// src/agents/pi-tools.policy.ts:79-96
const DEFAULT_SUBAGENT_TOOL_DENY = [
  "sessions_list", "sessions_history", "sessions_send", "sessions_spawn",
  "gateway", "agents_list", "whatsapp_login",
  "session_status", "cron",
  "memory_search", "memory_get",
];
```

这确保子代理不会干扰主 Agent 的 session 管理、系统配置和调度任务。

### 4.8 策略匹配支持的模式

| 模式类型 | 示例 | 说明 |
| ---- | ---- | ---- |
| 精确匹配 | `"exec"` | 匹配工具名为 `exec` 的工具 |
| 工具组 | `"group:fs"` | 展开为 `read`, `write`, `edit`, `apply_patch` |
| 通配符 | `"*"` | 匹配所有工具 |
| 正则通配 | `"sessions_*"` | 匹配所有以 `sessions_` 开头的工具 |
| 插件组 | `"group:plugins"` | 匹配所有插件提供的工具 |
| 插件 ID | `"myplugin"` | 匹配该插件提供的所有工具 |
| 别名 | `"bash"` | 自动解析为 `exec` |

---

## 五、工具包装与增强

工具在返回前会经过多层包装：

```typescript
// src/agents/pi-tools.ts:424-440
// 1. 规范化工具参数格式 (OpenAI 兼容)
const normalized = subagentFiltered.map(normalizeToolParameters);

// 2. 添加 before_tool_call Hook
const withHooks = normalized.map((tool) =>
  wrapToolWithBeforeToolCallHook(tool, {
    agentId,
    sessionKey: options?.sessionKey,
  }),
);

// 3. 添加中断信号支持
const withAbort = options?.abortSignal
  ? withHooks.map((tool) => wrapToolWithAbortSignal(tool, options.abortSignal))
  : withHooks;

return withAbort;
```

**参数规范化 (Claude Code 兼容)**:

```typescript
// src/agents/pi-tools.read.ts:108-122
export const CLAUDE_PARAM_GROUPS = {
  read: [{ keys: ["path", "file_path"], label: "path (path or file_path)" }],
  write: [{ keys: ["path", "file_path"], label: "path (path or file_path)" }],
  edit: [
    { keys: ["path", "file_path"], label: "path (path or file_path)" },
    { keys: ["oldText", "old_string"], label: "oldText (oldText or old_string)" },
    { keys: ["newText", "new_string"], label: "newText (newText or new_string)" },
  ],
};

// src/agents/pi-tools.read.ts:127-149
export function normalizeToolParams(params: unknown): Record<string, unknown> | undefined {
  // file_path → path
  if ("file_path" in normalized && !("path" in normalized)) {
    normalized.path = normalized.file_path;
    delete normalized.file_path;
  }
  // old_string → oldText, new_string → newText (同上)
  return normalized;
}
```

---

## 六、工具调用协议

本节描述 LLM 决策使用工具后，Agent Runtime 如何执行该工具、以及全过程中的信息协议。

### 6.1 核心数据结构

#### LLM 输出的工具调用请求

LLM 在决策使用工具时，返回的 assistant 消息中包含工具调用内容块：

```typescript
// assistant 消息的 content 数组中的工具调用块
{
  type: "toolCall" | "toolUse" | "functionCall",  // 不同提供商使用不同 type
  id: string,      // 工具调用唯一标识符 (toolCallId)
  name: string,    // 工具名称，如 "exec", "read", "write"
  args: unknown    // 工具参数对象 (JSON)
}
```

#### AgentTool 接口

每个工具实现统一的 `AgentTool` 接口：

```typescript
// AgentTool 接口 (来自 @mariozechner/pi-agent-core)
interface AgentTool<TParams, TDetails> {
  name: string;
  label?: string;
  description?: string;
  parameters: TSchema;  // TypeBox schema 定义参数结构
  execute: (
    toolCallId: string,
    params: TParams,
    signal?: AbortSignal,
    onUpdate?: AgentToolUpdateCallback<TDetails>
  ) => Promise<AgentToolResult<TDetails>>;
}
```

#### 工具执行结果

```typescript
// 工具执行结果格式
interface AgentToolResult<TDetails> {
  content: Array<{
    type: "text" | "image" | "json";
    text?: string;       // 文本内容
    data?: string;       // base64 编码的图片数据
    mimeType?: string;   // MIME 类型
  }>;
  details?: TDetails;    // 工具特定的详情
  isError?: boolean;     // 是否为错误结果
}

// 成功结果示例
{
  content: [
    { type: "text", text: '{"status": "success", "files": ["a.txt", "b.txt"]}' }
  ],
  details: { status: "success" }
}

// 错误结果示例 (通过 jsonResult 生成)
{
  content: [
    { type: "text", text: '{"status": "error", "tool": "exec", "error": "Command denied"}' }
  ],
  details: { status: "error", error: "Command denied" }
}
```

### 6.2 工具定义适配

Agent Runtime 通过 `toToolDefinitions` 将内部 `AgentTool` 转换为 SDK 使用的 `ToolDefinition` 格式。这是工具注册到 LLM 会话的关键桥梁。

```typescript
// src/agents/pi-tool-definition-adapter.ts:31-74
export function toToolDefinitions(tools: AnyAgentTool[]): ToolDefinition[] {
  return tools.map((tool) => {
    const name = tool.name || "tool";
    const normalizedName = normalizeToolName(name);
    return {
      name,
      label: tool.label ?? name,
      description: tool.description ?? "",
      parameters: tool.parameters,  // TypeBox JSON Schema
      execute: async (
        toolCallId,
        params,
        signal,
        onUpdate,
      ): Promise<AgentToolResult<unknown>> => {
        try {
          // 直接调用原始工具的 execute 方法
          return await tool.execute(toolCallId, params, signal, onUpdate);
        } catch (err) {
          // 未被工具自身捕获的异常 → 转化为标准化错误结果
          if (signal?.aborted) throw err;  // 中断信号直接传播
          const described = describeToolExecutionError(err);
          logError(`[tools] ${normalizedName} failed: ${described.message}`);
          return jsonResult({
            status: "error",
            tool: normalizedName,
            error: described.message,
          });
        }
      },
    };
  });
}
```

工具在 `runEmbeddedAttempt` 中通过 `splitSdkTools` 注入到 LLM 会话：

```typescript
// src/agents/pi-embedded-runner/tool-split.ts:8-16
export function splitSdkTools(options: { tools: AnyAgentTool[]; sandboxEnabled: boolean }) {
  const { tools } = options;
  return {
    builtInTools: [],                    // 不使用 SDK 内置工具
    customTools: toToolDefinitions(tools),  // 全部走自定义工具
  };
}
```

### 6.3 调用时序与事件流

#### 事件驱动架构

LLM 会话 (`AgentSession`) 产生的流式事件通过订阅机制分发到事件处理器：

```typescript
// src/agents/pi-embedded-subscribe.ts:533
const unsubscribe = params.session.subscribe(
  createEmbeddedPiSessionEventHandler(ctx)
);
```

#### 事件分发 (handlers.ts)

```typescript
// src/agents/pi-embedded-subscribe.handlers.ts:22-63
export function createEmbeddedPiSessionEventHandler(ctx) {
  return (evt: EmbeddedPiSubscribeEvent) => {
    switch (evt.type) {
      case "message_start":       handleMessageStart(ctx, evt);       return;
      case "message_update":      handleMessageUpdate(ctx, evt);      return;
      case "message_end":         handleMessageEnd(ctx, evt);         return;
      case "tool_execution_start":  handleToolExecutionStart(ctx, evt);  return;
      case "tool_execution_update": handleToolExecutionUpdate(ctx, evt); return;
      case "tool_execution_end":    handleToolExecutionEnd(ctx, evt);    return;
      case "agent_start":          handleAgentStart(ctx);               return;
      case "agent_end":            handleAgentEnd(ctx);                 return;
      // ...
    }
  };
}
```

#### 工具执行三阶段

**Phase 1: tool_execution_start** - 工具开始执行

```typescript
// src/agents/pi-embedded-subscribe.handlers.tools.ts:39-114
export async function handleToolExecutionStart(ctx, evt) {
  // 刷新等待中的文本回复
  ctx.flushBlockReplyBuffer();

  const toolName = normalizeToolName(String(evt.toolName));
  const toolCallId = String(evt.toolCallId);
  const args = evt.args;

  // 生成工具元数据用于显示
  const meta = extendExecMeta(toolName, args, inferToolMetaFromArgs(toolName, args));
  ctx.state.toolMetaById.set(toolCallId, meta);

  // 发送事件通知 (用于 UI 显示)
  emitAgentEvent({
    runId: ctx.params.runId,
    stream: "tool",
    data: { phase: "start", name: toolName, toolCallId, args },
  });

  // 发送工具摘要 (用于消息通道显示)
  if (ctx.params.onToolResult && shouldEmitToolEvents) {
    ctx.emitToolSummary(toolName, meta);
  }
}
```

**Phase 2: tool_execution_update** - 工具执行中间更新

```typescript
// src/agents/pi-embedded-subscribe.handlers.tools.ts:116-146
export function handleToolExecutionUpdate(ctx, evt) {
  const toolName = normalizeToolName(String(evt.toolName));
  const toolCallId = String(evt.toolCallId);
  const sanitized = sanitizeToolResult(evt.partialResult);

  emitAgentEvent({
    runId: ctx.params.runId,
    stream: "tool",
    data: { phase: "update", name: toolName, toolCallId, partialResult: sanitized },
  });
}
```

**Phase 3: tool_execution_end** - 工具执行完成

```typescript
// src/agents/pi-embedded-subscribe.handlers.tools.ts:148-229
export function handleToolExecutionEnd(ctx, evt) {
  const toolName = normalizeToolName(String(evt.toolName));
  const toolCallId = String(evt.toolCallId);
  const isError = Boolean(evt.isError);
  const result = evt.result;

  // 错误检测 (显式标记或结果中的 status: "error")
  const isToolError = isError || isToolResultError(result);

  // 结果清理 (截断文本、移除图片数据)
  const sanitizedResult = sanitizeToolResult(result);

  // 记录错误摘要
  if (isToolError) {
    const errorMessage = extractToolErrorMessage(sanitizedResult);
    ctx.state.lastToolError = { toolName, meta, error: errorMessage };
  }

  // 发送结果事件
  emitAgentEvent({
    runId: ctx.params.runId,
    stream: "tool",
    data: {
      phase: "result",
      name: toolName,
      toolCallId,
      isError: isToolError,
      result: sanitizedResult,
    },
  });
}
```

#### 完整调用时序图

```text
User/System         LLM               AgentSession          EventHandler         Tool.execute()
    │                 │                     │                     │                     │
    │── 发送消息 ────▶│                     │                     │                     │
    │                 │                     │                     │                     │
    │                 │── assistant msg ──▶│                     │                     │
    │                 │  (含 tool_use 块)   │                     │                     │
    │                 │                     │                     │                     │
    │                 │                     │─ tool_execution ──▶│                     │
    │                 │                     │   _start            │                     │
    │                 │                     │                     │── execute() ───────▶│
    │                 │                     │                     │                     │
    │                 │                     │                     │  (可能发送 update)  │
    │                 │                     │◀─ tool_execution ──│                     │
    │                 │                     │   _update           │                     │
    │                 │                     │                     │                     │
    │                 │                     │                     │◀─ AgentToolResult ──│
    │                 │                     │◀─ tool_execution ──│                     │
    │                 │                     │   _end              │                     │
    │                 │                     │                     │                     │
    │                 │◀── toolResult msg ─│                     │                     │
    │                 │   (结果回传给 LLM)  │                     │                     │
    │                 │                     │                     │                     │
    │                 │── 继续推理 ────────▶│                     │                     │
    │                 │  (使用工具结果)      │                     │                     │
    │                 │                     │                     │                     │
    │◀── 最终回复 ───│                     │                     │                     │
```

### 6.4 Tool Call ID 处理

不同 LLM 提供商对 toolCallId 格式有不同要求。`tool-call-id.ts` 提供了统一的 ID 清理和去重机制：

```typescript
// src/agents/tool-call-id.ts:4-36
export type ToolCallIdMode = "strict" | "strict9";

// "strict" 模式: 仅允许 [a-zA-Z0-9] (大多数提供商)
// "strict9" 模式: 仅允许 [a-zA-Z0-9] 且长度 9 (Mistral 要求)

export function sanitizeToolCallId(id: string, mode: ToolCallIdMode = "strict"): string {
  if (mode === "strict9") {
    const alphanumericOnly = id.replace(/[^a-zA-Z0-9]/g, "");
    if (alphanumericOnly.length >= 9) {
      return alphanumericOnly.slice(0, 9);
    }
    return shortHash(alphanumericOnly, 9);
  }
  const alphanumericOnly = id.replace(/[^a-zA-Z0-9]/g, "");
  return alphanumericOnly.length > 0 ? alphanumericOnly : "sanitizedtoolid";
}
```

对整个消息历史进行 ID 清理，保持 assistant 工具调用和 toolResult 之间的 ID 一致性：

```typescript
// src/agents/tool-call-id.ts:169-221
export function sanitizeToolCallIdsForCloudCodeAssist(
  messages: AgentMessage[],
  mode: ToolCallIdMode = "strict",
): AgentMessage[] {
  // 建立全局 ID 映射表，避免清理后产生冲突
  const map = new Map<string, string>();
  const used = new Set<string>();

  const resolve = (id: string) => {
    const existing = map.get(id);
    if (existing) return existing;
    const next = makeUniqueToolId({ id, used, mode });
    map.set(id, next);
    used.add(next);
    return next;
  };

  // 重写 assistant 消息中的 toolCall ID 和 toolResult 的 toolCallId
  return messages.map((msg) => {
    if (msg.role === "assistant") return rewriteAssistantToolCallIds({ message: msg, resolve });
    if (msg.role === "toolResult") return rewriteToolResultIds({ message: msg, resolve });
    return msg;
  });
}
```

### 6.5 结果处理与截断

为防止工具结果过大影响 LLM 上下文窗口，结果在事件传播前会被清理：

```typescript
// src/agents/pi-embedded-subscribe.tools.ts:6-7
const TOOL_RESULT_MAX_CHARS = 8000;   // 文本最大长度
const TOOL_ERROR_MAX_CHARS = 400;     // 错误消息最大长度

// src/agents/pi-embedded-subscribe.tools.ts:63-91
export function sanitizeToolResult(result: unknown): unknown {
  // 文本内容截断
  if (type === "text" && typeof entry.text === "string") {
    return { ...entry, text: truncateToolText(entry.text) };
  }
  // 图片数据移除 (只保留元信息)
  if (type === "image") {
    const bytes = data ? data.length : undefined;
    return { ...cleaned, bytes, omitted: true };
  }
}
```

错误信息提取支持多种格式：

```typescript
// src/agents/pi-embedded-subscribe.tools.ts:138-165
export function extractToolErrorMessage(result: unknown): string | undefined {
  // 1. 从 details 字段提取
  const fromDetails = extractErrorField(record.details);
  if (fromDetails) return fromDetails;

  // 2. 从根对象提取
  const fromRoot = extractErrorField(record);
  if (fromRoot) return fromRoot;

  // 3. 从 content 文本中提取 (尝试 JSON 解析)
  const text = extractToolResultText(result);
  try {
    const parsed = JSON.parse(text);
    const fromJson = extractErrorField(parsed);
    if (fromJson) return fromJson;
  } catch { /* fall through */ }

  return normalizeToolErrorText(text);
}
```

---

## 七、AgentSession 与事件通信

> AgentSession 的完整内容（结构、生命周期、事件订阅、流式缓冲、消息投递链路）已迁移至 **[AGENT-RUNTIME-SESSION.md](./AGENT-RUNTIME-SESSION.md)** Part B（§九 ~ §十三），作为会话管理的核心组成部分统一维护。

本节仅保留与工具系统直接相关的要点摘要：

- **工具执行在 SDK 内部自动完成**: 当 LLM 返回 `tool_call` 时，SDK 自动查找已注册的 `ToolDefinition`，调用其 `execute` 方法，将结果注入消息历史，然后再次调用 LLM。Runtime 通过事件监听得知发生了什么。
- **工具事件类型**: `tool_execution_start` → `tool_execution_update` → `tool_execution_end`，由 `handlers.tools.ts` 处理。
- **工具结果清理**: `sanitizeToolResult()` 截断过长文本（8000 字符）、移除图片数据（只保留元信息）。

---

## 关键源文件

| 文件 | 行数 | 核心功能 |
| ---- | ---- | -------- |
| `src/agents/pi-tools.ts` | ~441 | 工具创建入口、策略过滤 |
| `src/agents/bash-tools.exec.ts` | ~1631 | exec 工具实现 |
| `src/agents/pi-tools.read.ts` | ~303 | read 工具包装 |
| `src/agents/openclaw-tools.ts` | ~163 | OpenClaw 工具集 |
| `src/agents/tool-policy.ts` | ~259 | 工具策略系统 |
| `src/agents/pi-tools.policy.ts` | ~340 | 策略过滤和逐层应用 |
| `src/agents/pi-tool-definition-adapter.ts` | ~122 | AgentTool → ToolDefinition 适配 |
| `src/agents/pi-embedded-subscribe.ts` | ~565 | 事件订阅系统入口 |
| `src/agents/pi-embedded-subscribe.handlers.ts` | ~63 | 事件分发 (switch) |
| `src/agents/pi-embedded-subscribe.handlers.types.ts` | ~108 | 事件上下文和状态类型 |
| `src/agents/pi-embedded-subscribe.handlers.tools.ts` | ~230 | 工具执行事件处理 |
| `src/agents/pi-embedded-subscribe.handlers.lifecycle.ts` | ~96 | 生命周期事件处理 |
| `src/agents/pi-embedded-subscribe.handlers.messages.ts` | - | 消息事件处理 |
| `src/agents/pi-embedded-subscribe.tools.ts` | ~210 | 工具结果清理与提取 |
| `src/agents/tool-call-id.ts` | ~222 | Tool Call ID 清理与去重 |
| `src/agents/pi-embedded-runner/run/attempt.ts` | ~906 | Session 创建和运行主循环 |
| `src/agents/pi-embedded-runner/compact.ts` | ~490 | Session 压缩 |
| `src/agents/pi-embedded-runner/tool-split.ts` | ~17 | 工具注入到 LLM 会话 |
| `src/agents/pi-embedded-block-chunker.ts` | ~272 | 流式文本分块器 |
| `src/infra/agent-events.ts` | ~84 | 全局事件总线 (emitAgentEvent/onAgentEvent) |
| `src/gateway/server-chat.ts` | ~313 | Gateway 事件处理 → WebSocket 广播 |
| `src/auto-reply/reply/reply-dispatcher.ts` | ~193 | 消息渠道回复分发器 |
| `src/auto-reply/reply/dispatch-from-config.ts` | ~460 | 回调绑定与消息渠道投递 |
