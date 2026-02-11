# Agent Runtime API 参考手册

> 本文档是 Agent Runtime 的**类型与接口速查手册**。
> 只包含核心类型定义、函数签名和关键枚举，不包含流程叙述（流程请看 [AGENT-RUNTIME-MSG-FLOW.md](./AGENT-RUNTIME-MSG-FLOW.md)）。

---

## 目录

1. [入口与出口类型](#一入口与出口类型)
   - [RunEmbeddedPiAgentParams](#11-runembeddedpiagentparams)
   - [EmbeddedPiRunResult](#12-embeddedpirunresult)
   - [EmbeddedPiRunMeta](#13-embeddedpirunmeta)
   - [EmbeddedPiAgentMeta](#14-embeddedpiagentmeta)
   - [EmbeddedRunAttemptResult](#15-embeddedrunattemptresult)
2. [沙箱类型](#二沙箱类型)
   - [SandboxContext](#21-sandboxcontext)
3. [事件与流式通信类型](#三事件与流式通信类型)
   - [EmbeddedPiSubscribeEvent](#31-embeddedpisubscribeevent)
   - [EmbeddedPiSubscribeState](#32-embeddedpisubscribestate)
   - [EmbeddedPiSubscribeContext](#33-embeddedpisubscribecontext)
4. [核心函数签名](#四核心函数签名)
   - [runEmbeddedPiAgent](#41-runembeddedpiagent)
   - [runEmbeddedAttempt](#42-runembeddedattempt)
   - [createAgentSession](#43-createagentsession)
   - [subscribeEmbeddedPiSession](#44-subscribeembeddedpisession)
   - [createOpenClawCodingTools](#45-createopenclawcodingtools)
   - [buildEmbeddedSystemPrompt](#46-buildembeddedsystemprompt)
5. [辅助类型速查](#五辅助类型速查)

---

## 一、入口与出口类型

### 1.1 RunEmbeddedPiAgentParams

Runtime 的**总入口参数**，所有 Agent 运行所需信息的集合。

> 源文件: `src/agents/pi-embedded-runner/run/params.ts`

```typescript
type RunEmbeddedPiAgentParams = {
  // ── 会话标识 ──────────────────────────────────────────────────────
  sessionId: string;                    // 会话唯一 ID (必需)
  sessionKey?: string;                  // 会话键 "agentId:channel:accountId:..."
  sessionFile: string;                  // 会话文件路径 (.jsonl)

  // ── 消息来源 ──────────────────────────────────────────────────────
  messageChannel?: string;              // 渠道 (telegram/discord/slack/signal/...)
  messageProvider?: string;             // 备选渠道标识
  agentAccountId?: string;              // Agent 账号 ID
  messageTo?: string;                   // 投递目标 (如 telegram:group:123:topic:456)
  messageThreadId?: string | number;    // 线程/话题 ID

  // ── 发送者信息 ────────────────────────────────────────────────────
  senderId?: string | null;
  senderName?: string | null;
  senderUsername?: string | null;
  senderE164?: string | null;           // 电话号码 (E.164 格式)

  // ── 群组/工作空间 ────────────────────────────────────────────────
  groupId?: string | null;              // 群组 ID (工具策略解析)
  groupChannel?: string | null;         // 群组频道标签
  groupSpace?: string | null;           // 群组空间标签 (guild/team id)
  spawnedBy?: string | null;            // 父会话键 (子代理策略继承)

  // ── Slack 自动线程 ───────────────────────────────────────────────
  currentChannelId?: string;
  currentThreadTs?: string;
  replyToMode?: "off" | "first" | "all";
  hasRepliedRef?: { value: boolean };

  // ── 工作空间 ──────────────────────────────────────────────────────
  workspaceDir: string;                 // 工作目录 (必需)
  agentDir?: string;                    // Agent 配置目录
  config?: OpenClawConfig;              // 完整配置对象
  skillsSnapshot?: SkillSnapshot;       // 技能快照 (预计算，避免重复扫描)

  // ── 用户输入 ──────────────────────────────────────────────────────
  prompt: string;                       // 用户消息文本 (必需)
  images?: ImageContent[];              // 附带的图片
  clientTools?: ClientToolDefinition[]; // 客户端提供的工具 (OpenResponses)
  disableTools?: boolean;               // 禁用工具 (纯 LLM 模式)

  // ── 模型配置 ──────────────────────────────────────────────────────
  provider?: string;                    // 提供商 (anthropic/openai/google/...)
  model?: string;                       // 模型 ID
  authProfileId?: string;               // 认证配置 ID
  authProfileIdSource?: "auto" | "user";

  // ── 执行参数 ──────────────────────────────────────────────────────
  thinkLevel?: ThinkLevel;              // 思考级别 (off/low/medium/high)
  verboseLevel?: VerboseLevel;          // 详细级别
  reasoningLevel?: ReasoningLevel;      // 推理级别
  toolResultFormat?: ToolResultFormat;  // 工具结果格式
  execOverrides?: Pick<ExecToolDefaults, "host" | "security" | "ask" | "node">;
  bashElevated?: ExecElevatedDefaults;
  timeoutMs: number;                    // 超时时间 ms (必需)
  runId: string;                        // 运行 ID (必需)
  abortSignal?: AbortSignal;            // 中断信号

  // ── 流式回调 ──────────────────────────────────────────────────────
  onPartialReply?: (payload: { text?: string; mediaUrls?: string[] }) => void | Promise<void>;
  onAssistantMessageStart?: () => void | Promise<void>;
  onBlockReply?: (payload: {
    text?: string;
    mediaUrls?: string[];
    audioAsVoice?: boolean;
    replyToId?: string;
    replyToTag?: boolean;
    replyToCurrent?: boolean;
  }) => void | Promise<void>;
  onBlockReplyFlush?: () => void | Promise<void>;
  blockReplyBreak?: "text_end" | "message_end";
  blockReplyChunking?: BlockReplyChunking;
  onReasoningStream?: (payload: { text?: string; mediaUrls?: string[] }) => void | Promise<void>;
  onToolResult?: (payload: { text?: string; mediaUrls?: string[] }) => void | Promise<void>;
  onAgentEvent?: (evt: { stream: string; data: Record<string, unknown> }) => void;
  shouldEmitToolResult?: () => boolean;
  shouldEmitToolOutput?: () => boolean;

  // ── 其他 ──────────────────────────────────────────────────────────
  lane?: string;                        // 队列分组
  enqueue?: typeof enqueueCommand;      // 自定义入队函数
  extraSystemPrompt?: string;           // 额外系统提示
  streamParams?: AgentStreamParams;     // 流式参数
  ownerNumbers?: string[];              // 所有者电话号码
  enforceFinalTag?: boolean;            // 强制 final 标签
};
```

**参数分组说明**:

| 分组 | 用途 | 必需字段 |
| ---- | ---- | ---- |
| 会话标识 | 定位/创建会话文件 | `sessionId`, `sessionFile` |
| 消息来源 | 路由回复、权限检查 | 无 |
| 发送者信息 | 权限检查、日志 | 无 |
| 群组/工作空间 | 工具策略解析 | `workspaceDir` |
| 用户输入 | LLM 交互 | `prompt` |
| 模型配置 | 选择 LLM 提供商 | 无 (有默认值) |
| 执行参数 | 控制运行行为 | `timeoutMs`, `runId` |
| 流式回调 | 实时推送事件给调用方 | 无 |

---

### 1.2 EmbeddedPiRunResult

Runtime 的**最终输出结果**，返回给调用方（渠道适配层/Gateway）。

> 源文件: `src/agents/pi-embedded-runner/types.ts`

```typescript
type EmbeddedPiRunResult = {
  // 输出载荷 (用于发送给用户)
  payloads?: Array<{
    text?: string;              // 文本内容
    mediaUrl?: string;          // 媒体 URL (单个)
    mediaUrls?: string[];       // 媒体 URL 列表
    replyToId?: string;         // 回复的消息 ID
    isError?: boolean;          // 是否为错误消息
  }>;

  // 运行元数据
  meta: EmbeddedPiRunMeta;

  // 消息工具追踪 (判断是否需要抑制重复回复)
  didSendViaMessagingTool?: boolean;
  messagingToolSentTexts?: string[];
  messagingToolSentTargets?: MessagingToolSend[];
};
```

**`payloads` 为何是数组?** 一次 Agent 运行可能产出多段回复（如文本 + 图片 URL），每段对应一个 payload。

---

### 1.3 EmbeddedPiRunMeta

运行元数据，附在 `EmbeddedPiRunResult.meta` 上。

> 源文件: `src/agents/pi-embedded-runner/types.ts`

```typescript
type EmbeddedPiRunMeta = {
  durationMs: number;                       // 执行耗时 (ms)
  agentMeta?: EmbeddedPiAgentMeta;          // LLM 调用元信息
  aborted?: boolean;                        // 是否被中断
  systemPromptReport?: SessionSystemPromptReport;  // 系统提示报告 (调试)

  // 错误信息 (如有)
  error?: {
    kind: "context_overflow"                // 上下文窗口溢出
        | "compaction_failure"              // 压缩失败
        | "role_ordering"                   // 角色顺序错误
        | "image_size";                     // 图片超限
    message: string;
  };

  // 客户端工具调用 (OpenResponses hosted tools)
  stopReason?: string;                      // "completed" | "tool_calls"
  pendingToolCalls?: Array<{
    id: string;
    name: string;
    arguments: string;
  }>;
};
```

---

### 1.4 EmbeddedPiAgentMeta

LLM 调用的统计信息。

> 源文件: `src/agents/pi-embedded-runner/types.ts`

```typescript
type EmbeddedPiAgentMeta = {
  sessionId: string;        // 会话 ID
  provider: string;         // 实际使用的提供商
  model: string;            // 实际使用的模型 ID
  usage?: {
    input?: number;         // 输入 token 数
    output?: number;        // 输出 token 数
    cacheRead?: number;     // 缓存读取 token 数
    cacheWrite?: number;    // 缓存写入 token 数
    total?: number;         // 总 token 数
  };
};
```

---

### 1.5 EmbeddedRunAttemptResult

单次尝试的结果（重试循环内每次 `runEmbeddedAttempt` 的返回值）。

> 源文件: `src/agents/pi-embedded-runner/run/types.ts`

```typescript
type EmbeddedRunAttemptResult = {
  // ── 状态标志 ──────────────────────────────────────────────────────
  aborted: boolean;                     // 是否被中断
  timedOut: boolean;                    // 是否超时
  promptError: unknown;                 // 提示错误 (如有)

  // ── 会话信息 ──────────────────────────────────────────────────────
  sessionIdUsed: string;                // 实际使用的会话 ID
  systemPromptReport?: SessionSystemPromptReport;
  messagesSnapshot: AgentMessage[];     // 完整消息快照

  // ── 输出内容 ──────────────────────────────────────────────────────
  assistantTexts: string[];             // 助手输出的文本块列表
  toolMetas: Array<{                    // 工具调用元数据
    toolName: string;
    meta?: string;
  }>;
  lastAssistant: AssistantMessage | undefined;  // 最后的助手消息
  lastToolError?: {                     // 最后工具错误
    toolName: string;
    meta?: string;
    error?: string;
  };

  // ── 消息工具追踪 ─────────────────────────────────────────────────
  didSendViaMessagingTool: boolean;
  messagingToolSentTexts: string[];
  messagingToolSentTargets: MessagingToolSend[];

  // ── 特殊标志 ──────────────────────────────────────────────────────
  cloudCodeAssistFormatError: boolean;
  clientToolCall?: {                    // 客户端工具调用
    name: string;
    params: Record<string, unknown>;
  };
};
```

**与 `EmbeddedPiRunResult` 的关系**: `runEmbeddedPiAgent` 通过 `buildEmbeddedRunPayloads()` 将 `EmbeddedRunAttemptResult` 转化为面向外部的 `EmbeddedPiRunResult`。

---

## 二、沙箱类型

### 2.1 SandboxContext

沙箱环境的运行时上下文，由 `resolveSandboxContext()` 生成。

> 源文件: `src/agents/sandbox/types.ts`

```typescript
type SandboxContext = {
  enabled: boolean;                     // 沙箱是否启用
  sessionKey: string;                   // 关联的会话键
  workspaceDir: string;                 // 沙箱内的工作目录
  agentWorkspaceDir: string;            // Agent 配置目录
  workspaceAccess: SandboxWorkspaceAccess;  // 工作目录访问级别
  containerName: string;                // Docker 容器名
  containerWorkdir: string;             // 容器内工作目录
  docker: SandboxDockerConfig;          // Docker 配置
  tools: SandboxToolPolicy;            // 沙箱级工具策略
  browserAllowHostControl: boolean;     // 是否允许控制宿主浏览器
  browser?: SandboxBrowserContext;      // 沙箱浏览器信息
};
```

**关联枚举与子类型**:

```typescript
type SandboxWorkspaceAccess = "none" | "ro" | "rw";

type SandboxToolPolicy = {
  allow?: string[];                     // 沙箱内允许的工具
  deny?: string[];                      // 沙箱内拒绝的工具
};

type SandboxBrowserContext = {
  bridgeUrl: string;                    // 浏览器桥接 URL
  noVncUrl?: string;                    // VNC 远程桌面 URL
  containerName: string;                // 浏览器容器名
};
```

---

## 三、事件与流式通信类型

### 3.1 EmbeddedPiSubscribeEvent

事件处理器接收的统一事件类型。

> 源文件: `src/agents/pi-embedded-subscribe.handlers.types.ts`

```typescript
type EmbeddedPiSubscribeEvent =
  | AgentEvent                                   // SDK 标准事件
  | { type: string; [k: string]: unknown }       // 通用扩展事件
  | { type: "message_start"; message: AgentMessage };  // 消息开始
```

**常见事件 type 值**:

| type | 触发时机 | 关键字段 |
| ---- | ---- | ---- |
| `message_start` | 助手消息开始 | `message` |
| `message_update` | 流式文本增量 | `assistantMessageEvent` |
| `message_end` | 助手消息结束 | `message` |
| `tool_execution_start` | 工具开始执行 | `tool.name`, `tool.id` |
| `tool_execution_end` | 工具执行完成 | `tool.name`, `result` |
| `compaction_start` | 上下文压缩开始 | — |
| `compaction_end` | 上下文压缩结束 | — |

---

### 3.2 EmbeddedPiSubscribeState

事件处理器的**可变状态**，跟踪整个流式交互过程中的所有中间数据。

> 源文件: `src/agents/pi-embedded-subscribe.handlers.types.ts`

```typescript
type EmbeddedPiSubscribeState = {
  // ── 收集的输出 ────────────────────────────────────────────────────
  assistantTexts: string[];                 // 助手文本块列表
  toolMetas: Array<{ toolName?: string; meta?: string }>;
  toolMetaById: Map<string, string | undefined>;
  toolSummaryById: Set<string>;
  lastToolError?: ToolErrorSummary;

  // ── 流式控制配置 ─────────────────────────────────────────────────
  blockReplyBreak: "text_end" | "message_end";
  reasoningMode: ReasoningLevel;
  includeReasoning: boolean;
  shouldEmitPartialReplies: boolean;
  streamReasoning: boolean;

  // ── 流式缓冲区 (三层缓冲) ────────────────────────────────────────
  deltaBuffer: string;          // 第一层: delta 累积
  blockBuffer: string;          // 第二层: block 分块累积
  blockState: {                 // block 标签解析状态
    thinking: boolean;
    final: boolean;
    inlineCode: InlineCodeState;
  };

  // ── 流式追踪 ─────────────────────────────────────────────────────
  partialBlockState: { thinking: boolean; final: boolean; inlineCode: InlineCodeState };
  lastStreamedAssistant?: string;
  lastStreamedAssistantCleaned?: string;
  lastStreamedReasoning?: string;
  lastBlockReplyText?: string;
  assistantMessageIndex: number;
  lastAssistantTextMessageIndex: number;
  lastAssistantTextNormalized?: string;
  lastAssistantTextTrimmed?: string;
  assistantTextBaseline: number;
  suppressBlockChunks: boolean;
  lastReasoningSent?: string;

  // ── 压缩重试 ─────────────────────────────────────────────────────
  compactionInFlight: boolean;
  pendingCompactionRetry: number;
  compactionRetryResolve?: () => void;
  compactionRetryPromise: Promise<void> | null;

  // ── 消息工具追踪 ─────────────────────────────────────────────────
  messagingToolSentTexts: string[];
  messagingToolSentTextsNormalized: string[];
  messagingToolSentTargets: MessagingToolSend[];
  pendingMessagingTexts: Map<string, string>;
  pendingMessagingTargets: Map<string, MessagingToolSend>;
};
```

**三层缓冲机制**:

```text
LLM stream delta
      │
      ▼
┌─────────────┐    stripBlockTags     ┌─────────────┐    EmbeddedBlockChunker   ┌──────────────┐
│ deltaBuffer │ ───────────────────→  │ blockBuffer │ ─────────────────────────→│ assistantTexts│
│  (字符累积)  │    去除 <think>/<final>│  (分块累积)  │    按 minChars/maxChars  │  (最终文本)    │
└─────────────┘                       └─────────────┘    切分为投递块            └──────────────┘
```

---

### 3.3 EmbeddedPiSubscribeContext

事件处理器的**上下文对象**，包含状态和一组操作方法。

> 源文件: `src/agents/pi-embedded-subscribe.handlers.types.ts`

```typescript
type EmbeddedPiSubscribeContext = {
  params: SubscribeEmbeddedPiSessionParams;    // 订阅参数
  state: EmbeddedPiSubscribeState;             // 可变状态
  log: EmbeddedSubscribeLogger;                // 日志
  blockChunking?: BlockReplyChunking;          // 分块配置
  blockChunker: EmbeddedBlockChunker | null;   // 分块器实例

  // ── 方法 ──────────────────────────────────────────────────────────
  shouldEmitToolResult: () => boolean;
  shouldEmitToolOutput: () => boolean;
  emitToolSummary: (toolName?: string, meta?: string) => void;
  emitToolOutput: (toolName?: string, meta?: string, output?: string) => void;
  stripBlockTags: (text: string, state: BlockTagState) => string;
  emitBlockChunk: (text: string) => void;
  flushBlockReplyBuffer: () => void;
  emitReasoningStream: (text: string) => void;
  consumeReplyDirectives: (text: string, options?: { final?: boolean }) => ReplyDirectiveParseResult | null;
  consumePartialReplyDirectives: (text: string, options?: { final?: boolean }) => ReplyDirectiveParseResult | null;
  resetAssistantMessageState: (nextAssistantTextBaseline: number) => void;
  resetForCompactionRetry: () => void;
  finalizeAssistantTexts: (args: { text: string; addedDuringMessage: boolean; chunkerHasBuffered: boolean }) => void;
  trimMessagingToolSent: () => void;
  ensureCompactionPromise: () => void;
  noteCompactionRetry: () => void;
  resolveCompactionRetry: () => void;
  maybeResolveCompactionWait: () => void;
};
```

---

## 四、核心函数签名

### 4.1 runEmbeddedPiAgent

Runtime **总入口**，负责队列、模型解析、认证和重试循环。

> 源文件: `src/agents/pi-embedded-runner/run.ts`

```typescript
export async function runEmbeddedPiAgent(
  params: RunEmbeddedPiAgentParams,
): Promise<EmbeddedPiRunResult>
```

- **输入**: [RunEmbeddedPiAgentParams](#11-runembeddedpiagentparams)
- **输出**: [EmbeddedPiRunResult](#12-embeddedpirunresult)
- **详细流程**: [AGENT-RUNTIME-MSG-FLOW.md](./AGENT-RUNTIME-MSG-FLOW.md)

---

### 4.2 runEmbeddedAttempt

**单次执行尝试**，在重试循环内被调用，负责环境组装、LLM 交互。

> 源文件: `src/agents/pi-embedded-runner/run/attempt.ts`

```typescript
export async function runEmbeddedAttempt(
  params: EmbeddedRunAttemptParams,
): Promise<EmbeddedRunAttemptResult>
```

- **输入**: `EmbeddedRunAttemptParams` (内部类型，包含 model/authStorage/modelRegistry 等已解析的对象)
- **输出**: [EmbeddedRunAttemptResult](#15-embeddedrunattemptresult)

---

### 4.3 createAgentSession

创建 **AgentSession** 实例，封装 LLM 通信和会话管理。

> 来源: `@mariozechner/pi-coding-agent` (外部 SDK)

```typescript
async function createAgentSession(params: {
  cwd: string;
  agentDir?: string;
  authStorage: AuthStorage;
  modelRegistry: ModelRegistry;
  model: Model<Api>;
  thinkingLevel: ThinkingLevel;
  tools: AnyAgentTool[];
  customTools: ToolDefinition[];
  sessionManager: SessionManager;
  settingsManager: SettingsManager;
}): Promise<{ session: AgentSession }>
```

**AgentSession 核心方法** (推断自使用):

| 方法 | 说明 |
| ---- | ---- |
| `prompt(text, opts?)` | 发送用户消息，触发 LLM 交互循环 |
| `subscribe(handler)` | 订阅流式事件 |
| `abort()` | 中断当前运行 |
| `steer(text)` | 注入系统级引导消息 |
| `compact(opts?)` | 压缩上下文 |
| `dispose()` | 释放资源 |

**AgentSession 核心属性**:

| 属性 | 说明 |
| ---- | ---- |
| `agent` | 底层 Agent 实例 (含 `messages`, `systemPrompt`, `replaceMessages()`) |
| `messages` | 当前消息历史 |
| `isStreaming` | 是否正在流式输出 |

---

### 4.4 subscribeEmbeddedPiSession

注册事件处理器到 AgentSession，构建完整的**事件处理链**。

> 源文件: `src/agents/pi-embedded-subscribe.ts`

```typescript
export function subscribeEmbeddedPiSession(
  params: SubscribeEmbeddedPiSessionParams,
): {
  assistantTexts: string[];
  toolMetas: Array<{ toolName: string; meta?: string }>;
  unsubscribe: () => void;
  waitForCompactionRetry: () => Promise<void>;
  didSendViaMessagingTool: boolean;
  getLastToolError: () => ToolErrorSummary | undefined;
}
```

- **详细机制**: [AGENT-RUNTIME-SESSION.md §十](./AGENT-RUNTIME-SESSION.md)

---

### 4.5 createOpenClawCodingTools

创建 Agent 可用的**完整工具集**，并应用策略过滤。

> 源文件: `src/agents/pi-tools.ts`

```typescript
export function createOpenClawCodingTools(options?: {
  exec?: ExecToolDefaults & ProcessToolDefaults;
  sandbox?: SandboxContext;
  messageProvider?: string;
  agentAccountId?: string;
  sessionKey?: string;
  agentDir?: string;
  workspaceDir?: string;
  config?: OpenClawConfig;
  abortSignal?: AbortSignal;
  modelProvider?: string;
  modelId?: string;
  modelHasVision?: boolean;
}): ToolDefinition[]
```

- **详细机制**: [AGENT-RUNTIME-TOOLS.md §1-4](./AGENT-RUNTIME-TOOLS.md)

---

### 4.6 buildEmbeddedSystemPrompt

构建嵌入式运行的**系统提示词**。

> 源文件: `src/agents/pi-embedded-runner/system-prompt.ts`

```typescript
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
  workspaceNotes?: string[];
  reactionGuidance?: { level: "minimal" | "extensive"; channel: string };
  promptMode: "full" | "minimal";
  runtimeInfo: RuntimeInfo;
  messageToolHints?: string;
  sandboxInfo?: SandboxPromptInfo;
  tools: ToolDefinition[];
  modelAliasLines?: string;
  userTimezone?: string;
  userTime?: string;
  contextFiles?: ContextFile[];
}): string
```

- **详细机制**: [AGENT-RUNTIME-SYSTEM-PROMPT.md](./AGENT-RUNTIME-SYSTEM-PROMPT.md)

---

## 五、辅助类型速查

| 类型 | 定义位置 | 说明 |
| ---- | ---- | ---- |
| `ThinkLevel` | `src/agents/types.ts` | `"off" \| "low" \| "medium" \| "high"` |
| `VerboseLevel` | `src/agents/types.ts` | 详细输出级别 |
| `ReasoningLevel` | `src/agents/types.ts` | `"off" \| "low" \| "medium" \| "high"` |
| `ToolResultFormat` | `src/agents/types.ts` | 工具结果格式 |
| `ImageContent` | `src/agents/pi-embedded-runner/run/images.ts` | `{ data: Buffer; mimeType: string }` |
| `ClientToolDefinition` | `src/agents/pi-embedded-runner/run/params.ts` | 客户端工具定义 `{ type: "function"; function: { name; description?; parameters? } }` |
| `AgentMessage` | `@mariozechner/pi-coding-agent` | SDK 消息类型 |
| `AssistantMessage` | `@mariozechner/pi-coding-agent` | 助手消息 (extends AgentMessage) |
| `ToolDefinition` | `@mariozechner/pi-coding-agent` | 工具定义 |
| `SessionManager` | `@mariozechner/pi-coding-agent` | 会话持久化管理器 |
| `MessagingToolSend` | `src/agents/tools/message-tool.ts` | 消息工具发送目标 |
| `ToolErrorSummary` | `src/agents/pi-embedded-subscribe.handlers.types.ts` | `{ toolName; meta?; error? }` |
| `BlockReplyChunking` | `src/agents/pi-embedded-block-chunker.ts` | 分块配置 `{ minChars; maxChars; breakPreference }` |
| `OpenClawConfig` | `src/config/types.ts` | 完整配置对象 |
| `SkillSnapshot` | `src/agents/skills/workspace.ts` | 技能快照 |
| `SandboxDockerConfig` | `src/agents/sandbox/types.ts` | Docker 容器配置 |

---

> **使用方式**: 本文档适合在开发/调试时快速查阅类型结构。关于数据如何流转请看 [MSG-FLOW](./AGENT-RUNTIME-MSG-FLOW.md)，关于各子系统如何工作请看 [v2 架构总览](./AGENT-RUNTIME-v2.md)。
