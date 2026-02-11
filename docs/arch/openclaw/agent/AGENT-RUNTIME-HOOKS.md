# Agent Runtime 钩子系统（Runtime 视角）

> 本文档从 Agent Runtime 内部视角详解钩子（Hooks）系统：14 种钩子类型的定义、在 Runtime 流程中的嵌入位置、执行模型（串行 vs 并行）、优先级与错误处理，以及插件如何注册钩子。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、钩子类型全览](#二钩子类型全览)
- [三、执行模型](#三执行模型)
- [四、Runtime 中的嵌入位置](#四runtime-中的嵌入位置)
- [五、已集成的钩子详解](#五已集成的钩子详解)
- [六、上下文对象](#六上下文对象)
- [七、插件注册与发现](#七插件注册与发现)
- [八、内部钩子 vs 插件钩子](#八内部钩子-vs-插件钩子)
- [九、关键源文件索引](#九关键源文件索引)

---

## 一、全局视角

### 1.1 钩子系统的角色

钩子系统是 Agent Runtime 的**扩展点**——让插件在不修改核心代码的情况下，在 Runtime 流程的关键节点注入自定义逻辑：拦截工具调用、修改消息内容、注入上下文、追踪统计等。

### 1.2 钩子在 Runtime 流程中的位置

```text
用户消息到达
    │
    ▼
┌── message_received ──┐  ← 观察入站消息 (fire-and-forget)
│                      │
│  路由 → 排队 → 执行   │
│                      │
├── before_agent_start ┤  ← 注入上下文 / 修改 prompt (可修改)
│                      │
│  session.prompt()    │
│       │              │
│       ├── before_tool_call ──┐  ← 修改参数 / 拦截工具 (可修改)
│       │                      │
│       │  工具执行              │
│       │                      │
│       ├── tool_result_persist ┤  ← 转换持久化结果 (同步, 可修改)
│       │                      │
│       └── [循环]              │
│                              │
├── agent_end ─────────┤  ← 追踪完成状态 (fire-and-forget)
│                      │
│  回复投递             │
│                      │
└──────────────────────┘
```

---

## 二、钩子类型全览

### 2.1 完整类型列表

OpenClaw 定义了 **14 种**钩子类型，分为 5 个生命周期类别：

| 类别 | 钩子名 | 执行模式 | 集成状态 |
| ---- | ---- | ---- | ---- |
| **Agent 生命周期** | `before_agent_start` | 串行 (可修改) | 已集成 |
| | `agent_end` | 并行 (观察) | 已集成 |
| **工具** | `before_tool_call` | 串行 (可修改/拦截) | 已集成 |
| | `after_tool_call` | 并行 (观察) | 已定义 |
| | `tool_result_persist` | 同步串行 (可修改) | 已集成 |
| **消息** | `message_received` | 并行 (观察) | 已集成 |
| | `message_sending` | 串行 (可修改/取消) | 已定义 |
| | `message_sent` | 并行 (观察) | 已定义 |
| **会话** | `session_start` | 并行 (观察) | 已定义 |
| | `session_end` | 并行 (观察) | 已定义 |
| **压缩** | `before_compaction` | 并行 (观察) | 已定义 |
| | `after_compaction` | 并行 (观察) | 已定义 |
| **Gateway** | `gateway_start` | 并行 (观察) | 已定义 |
| | `gateway_stop` | 并行 (观察) | 已定义 |

**已集成** = Runtime 代码中有调用点。**已定义** = 类型和执行器就绪，但 Runtime 中尚未调用（预留扩展点）。

### 2.2 每种钩子的输入与输出

#### Agent 生命周期

**`before_agent_start`** — 在 Agent 运行前注入上下文

- 输入: `{ prompt: string; messages?: unknown[] }`
- 输出: `{ systemPrompt?: string; prependContext?: string } | void`
- 能力: 在 prompt 前注入文本、覆盖系统提示

**`agent_end`** — 运行结束后追踪

- 输入: `{ messages: unknown[]; success: boolean; error?: string; durationMs?: number }`
- 输出: `void`
- 能力: 纯观察（统计、日志、告警）

#### 工具

**`before_tool_call`** — 拦截或修改工具调用

- 输入: `{ toolName: string; params: Record<string, unknown> }`
- 输出: `{ params?: Record<string, unknown>; block?: boolean; blockReason?: string } | void`
- 能力: 修改参数、阻止执行

**`after_tool_call`** — 观察工具执行结果

- 输入: `{ toolName: string; params: Record<string, unknown>; result?: unknown; error?: string; durationMs?: number }`
- 输出: `void`

**`tool_result_persist`** — 转换持久化到会话的工具结果

- 输入: `{ toolName?: string; toolCallId?: string; message: AgentMessage; isSynthetic?: boolean }`
- 输出: `{ message?: AgentMessage } | void`
- 能力: 修改/过滤持久化的消息内容（**同步**执行）

#### 消息

**`message_received`** — 入站消息到达

- 输入: `{ from: string; content: string; timestamp?: number; metadata?: Record<string, unknown> }`
- 输出: `void`

**`message_sending`** — 出站消息发送前

- 输入: `{ to: string; content: string; metadata?: Record<string, unknown> }`
- 输出: `{ content?: string; cancel?: boolean } | void`
- 能力: 修改内容、取消发送

**`message_sent`** — 出站消息发送后

- 输入: `{ to: string; content: string; success: boolean; error?: string }`
- 输出: `void`

---

## 三、执行模型

### 3.1 两种执行模式

> 源文件: `src/plugins/hooks.ts`

```text
┌────────────────────────────────────────────────────────────────┐
│  模式 1: 并行 (Void Hooks)                                     │
│                                                                │
│  handler1 ──→ ┐                                                │
│  handler2 ──→ ├── Promise.all() ──→ 全部完成                   │
│  handler3 ──→ ┘                                                │
│                                                                │
│  用途: 观察/追踪类钩子 (fire-and-forget)                       │
│  错误: 独立捕获，不影响其他 handler                             │
├────────────────────────────────────────────────────────────────┤
│  模式 2: 串行 (Modifying Hooks)                                │
│                                                                │
│  handler1 ──→ result1                                          │
│  handler2(result1) ──→ result2                                 │
│  handler3(result2) ──→ finalResult                             │
│                                                                │
│  用途: 可修改数据的钩子 (链式传递)                              │
│  优先级: 高 priority 的 handler 先执行                          │
│  错误: 按 catchErrors 配置 (默认: 捕获并继续)                  │
└────────────────────────────────────────────────────────────────┘
```

### 3.2 特殊模式: 同步执行

`tool_result_persist` 采用**同步**执行——因为它在会话转录追加（热路径）中被调用，不能引入异步开销：

```typescript
// 简化实现
function runToolResultPersist(event, ctx) {
  let message = event.message;
  for (const hook of hooks) {
    const result = hook.handler(event, ctx);  // 注意: 不是 await
    if (result?.message) {
      message = result.message;
      event = { ...event, message };  // 传递给下一个 handler
    }
  }
  return { message };
}
```

如果 handler 返回 Promise，系统会记录警告日志。

### 3.3 优先级

```text
hooks.sort((a, b) => (b.priority ?? 0) - (a.priority ?? 0))
```

- 高优先级 handler 先执行
- 默认优先级: `0`
- 仅对串行模式有意义（并行模式同时执行，顺序无关紧要）

### 3.4 错误处理

| 选项 | 行为 |
| ---- | ---- |
| `catchErrors: true` (默认) | 错误被捕获并记录日志，继续执行后续 handler |
| `catchErrors: false` | 错误直接抛出，中断执行链 |

Runtime 初始化 HookRunner 时默认启用 `catchErrors: true`，确保插件错误不会导致 Agent 运行失败。

---

## 四、Runtime 中的嵌入位置

### 4.1 时序图

```text
dispatchReplyFromConfig()
    │
    ├─── ① message_received (fire-and-forget)
    │        → dispatch-from-config.ts:150-198
    │        → 触发时机: 入站消息解析后、路由前
    │
    ▼
runEmbeddedAttempt()
    │
    ├─── ② before_agent_start (sequential, can modify)
    │        → attempt.ts:705-728
    │        → 触发时机: sanitizeHistory 后、session.prompt() 前
    │        → 可返回 prependContext → 拼接到 prompt 前
    │
    ├─── session.prompt(effectivePrompt)
    │        │
    │        │  ┌── SDK 内部工具循环 ──────────────────────┐
    │        │  │                                          │
    │        │  │  LLM 返回 tool_call                      │
    │        │  │       │                                  │
    │        │  │  ③ before_tool_call (sequential)         │
    │        │  │       → pi-tools.before-tool-call.ts     │
    │        │  │       → 每个工具都被 wrap                │
    │        │  │       → 可修改 params 或 block           │
    │        │  │       │                                  │
    │        │  │  tool.execute()                          │
    │        │  │       │                                  │
    │        │  │  SessionManager.appendMessage(result)    │
    │        │  │       │                                  │
    │        │  │  ④ tool_result_persist (sync)            │
    │        │  │       → session-tool-result-guard.ts     │
    │        │  │       → 可转换持久化的消息               │
    │        │  │       │                                  │
    │        │  │  结果回传 LLM → 继续循环                 │
    │        │  └──────────────────────────────────────────┘
    │        │
    │        └── prompt() 完成
    │
    ├─── ⑤ agent_end (fire-and-forget)
    │        → attempt.ts:833-849
    │        → 触发时机: prompt 完成后（成功或失败）
    │        → 接收: messages, success, error, durationMs
    │
    └─── 回复投递 → 用户
```

### 4.2 嵌入方式

| 钩子 | 嵌入方式 | 说明 |
| ---- | ---- | ---- |
| `message_received` | 直接调用 `hookRunner.runMessageReceived()` | 在 dispatch 主流程中 |
| `before_agent_start` | 直接调用 `hookRunner.runBeforeAgentStart()` | 在 attempt 主流程中 |
| `before_tool_call` | **工具包装**: `wrapToolWithBeforeToolCallHook(tool)` | 每个工具的 execute 被包装 |
| `tool_result_persist` | **SessionManager 守卫**: `guardSessionManager(sm)` | SM 的 appendMessage 被拦截 |
| `agent_end` | 直接调用 `hookRunner.runAgentEnd()` | 在 attempt 末尾，不 await |

---

## 五、已集成的钩子详解

### 5.1 before_agent_start

**用途示例**: 插件在每次 Agent 运行前注入项目特定的上下文。

```typescript
api.on("before_agent_start", async (event, ctx) => {
  const projectNotes = await loadProjectNotes(ctx.workspaceDir);
  return {
    prependContext: `## Project Notes\n${projectNotes}`,
  };
}, { priority: 10 });
```

**Runtime 集成**:

```typescript
// attempt.ts
const hookResult = await hookRunner.runBeforeAgentStart(
  { prompt: params.prompt, messages: activeSession.messages },
  { agentId, sessionKey, workspaceDir, messageProvider },
);
if (hookResult?.prependContext) {
  effectivePrompt = `${hookResult.prependContext}\n\n${params.prompt}`;
}
```

### 5.2 before_tool_call

**用途示例**: 插件审计所有 exec 命令，拦截危险操作。

```typescript
api.on("before_tool_call", async (event, ctx) => {
  if (event.toolName === "exec" && event.params.command?.includes("rm -rf")) {
    return { block: true, blockReason: "Dangerous command blocked by security plugin" };
  }
}, { priority: 100 });  // 高优先级，最先执行
```

**Runtime 集成**: 每个工具在创建时被 `wrapToolWithBeforeToolCallHook()` 包装：

```typescript
// 包装后的 execute
async execute(toolCallId, params, signal, onUpdate) {
  const outcome = await runBeforeToolCallHook({ toolName, params, toolCallId, ctx });
  if (outcome.blocked) {
    throw new Error(outcome.reason);  // → LLM 收到错误信息
  }
  return await originalExecute(toolCallId, outcome.params, signal, onUpdate);
}
```

### 5.3 tool_result_persist

**用途示例**: 插件过滤敏感信息，防止 API Key 等出现在会话转录中。

```typescript
api.on("tool_result_persist", (event, ctx) => {
  const sanitized = redactSecrets(event.message);
  return { message: sanitized };
});
```

**Runtime 集成**: 通过 `guardSessionManager()` 安装到 SessionManager 的消息追加流程中。**同步执行**，每个 handler 的输出传给下一个。

### 5.4 message_received

**用途示例**: 插件记录所有入站消息到外部分析服务。

```typescript
api.on("message_received", async (event, ctx) => {
  await analytics.track("message_received", {
    from: event.from,
    channel: ctx.channelId,
    length: event.content.length,
  });
});
```

### 5.5 agent_end

**用途示例**: 插件在 Agent 运行失败时发送告警。

```typescript
api.on("agent_end", async (event, ctx) => {
  if (!event.success) {
    await alerting.send(`Agent run failed: ${event.error}`, {
      session: ctx.sessionKey,
      duration: event.durationMs,
    });
  }
});
```

---

## 六、上下文对象

不同类别的钩子接收不同的上下文对象：

| 上下文类型 | 适用钩子 | 字段 |
| ---- | ---- | ---- |
| Agent | `before_agent_start`, `agent_end` | `agentId?`, `sessionKey?`, `workspaceDir?`, `messageProvider?` |
| Tool | `before_tool_call`, `after_tool_call` | `agentId?`, `sessionKey?`, `toolName` |
| Tool Persist | `tool_result_persist` | `agentId?`, `sessionKey?`, `toolName?`, `toolCallId?` |
| Message | `message_received`, `message_sending`, `message_sent` | `channelId`, `accountId?`, `conversationId?` |
| Session | `session_start`, `session_end` | `agentId?`, `sessionId` |
| Gateway | `gateway_start`, `gateway_stop` | `port?` |

---

## 七、插件注册与发现

### 7.1 注册方式

插件通过 `api.on()` 注册钩子（类型安全）：

```typescript
// 在插件的 activate() 函数中
export function activate(api: PluginApi) {
  api.on("before_agent_start", handler, { priority: 10 });
  api.on("before_tool_call", handler, { priority: 100 });
  api.on("agent_end", handler);
}
```

### 7.2 目录钩子发现

插件目录下的 `hooks/` 子目录会被自动扫描：

```text
extensions/my-plugin/
  ├── package.json
  ├── index.ts          ← 插件入口 (activate 函数)
  └── hooks/
      └── my-hook/
          ├── HOOK.md    ← 钩子元数据 (YAML front matter)
          └── handler.ts ← 钩子处理函数
```

**HOOK.md 格式**:

```yaml
---
name: my-hook
description: "审计所有工具调用"
metadata:
  openclaw:
    events: ["before_tool_call"]
    emoji: "🔒"
    requires:
      bins: ["node"]
---
```

### 7.3 全局 HookRunner 访问

```typescript
// 初始化 (Gateway 启动时)
initializeGlobalHookRunner(pluginRegistry);

// Runtime 中获取
const hookRunner = getGlobalHookRunner();
if (hookRunner?.hasHooks("before_agent_start")) {
  // 有插件注册了此钩子才执行
  const result = await hookRunner.runBeforeAgentStart(event, ctx);
}
```

**`hasHooks()` 检查**: Runtime 在调用钩子前总是先检查是否有注册的 handler，避免无插件时的无谓开销。

---

## 八、内部钩子 vs 插件钩子

OpenClaw 有**两套独立的**钩子系统：

| 维度 | 插件钩子 (Plugin Hooks) | 内部钩子 (Internal Hooks) |
| ---- | ---- | ---- |
| 注册方式 | `api.on(hookName, handler)` | `api.registerHook(event, handler)` |
| 类型安全 | 完全类型化 (`PluginHookName`) | 字符串事件名 |
| 执行模型 | 串行/并行 (由 HookRunner 管理) | 事件驱动 (triggerInternalHook) |
| 事件格式 | 专用类型 per hook | 通用 `InternalHookEvent` |
| 用途 | 插件扩展 Agent 行为 | 系统内部事件（bootstrap, 命令） |
| 示例事件 | `before_agent_start` | `agent:bootstrap`, `command:new`, `gateway:startup` |

**本文档聚焦插件钩子**。内部钩子用于 Gateway 自身的事件处理（如引导文件修改），不向插件开放。

---

## 九、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/plugins/types.ts` | 所有钩子类型定义、`PluginHookName`、handler 签名 |
| `src/plugins/hooks.ts` | HookRunner：串行/并行执行、优先级排序、错误处理 |
| `src/plugins/hook-runner-global.ts` | 全局 HookRunner 单例 (`getGlobalHookRunner`) |
| `src/plugins/registry.ts` | 插件注册表：`api.on()` 实现 |
| `src/agents/pi-tools.before-tool-call.ts` | `before_tool_call` 工具包装器 |
| `src/agents/session-tool-result-guard-wrapper.ts` | `tool_result_persist` 集成到 SessionManager |
| `src/agents/pi-embedded-runner/run/attempt.ts` | `before_agent_start` / `agent_end` 调用点 |
| `src/auto-reply/reply/dispatch-from-config.ts` | `message_received` 调用点 |
| `src/agents/bootstrap-hooks.ts` | 内部引导钩子（`agent:bootstrap`） |
| `src/hooks/plugin-hooks.ts` | 目录钩子发现与加载 |
| `src/hooks/types.ts` | 钩子元数据类型 (`OpenClawHookMetadata`) |
