# Agent Runtime 数据流全旅程

> 本文档追踪一条用户消息从**进入系统**到**回复送达用户**的完整数据流旅程。
> 重点不是"每个函数做了什么"，而是"数据在每一站如何变形、流向何方"。

---

## 目录

1. [旅程总览](#一旅程总览)
2. [第一站：消息进入系统](#二第一站消息进入系统)
3. [第二站：消息路由与排队](#三第二站消息路由与排队)
4. [第三站：模型选择与认证](#四第三站模型选择与认证)
5. [第四站：运行环境准备](#五第四站运行环境准备)
6. [第五站：LLM 交互循环](#六第五站llm-交互循环)
7. [第六站：流式事件处理](#七第六站流式事件处理)
8. [第七站：回复投递给用户](#八第七站回复投递给用户)
9. [第八站：错误恢复与重试](#九第八站错误恢复与重试)
10. [完整旅程图](#十完整旅程图)

---

## 一、旅程总览

```text
用户消息                                                          用户收到回复
  "帮我看看                                                        "package.json
   package.json"                                                    的内容是..."
      │                                                                  ▲
      ▼                                                                  │
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│  ① 入口  │──→│  ② 排队  │──→│  ③ 准备  │──→│  ④ LLM   │──→│  ⑤ 投递  │
│  消息渠道 │   │  模型选择 │   │  环境组装 │   │  交互循环 │   │  回复路由 │
└──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘

① 用户消息从 WhatsApp/Telegram/Discord/Web/CLI 等渠道进入
② 消息排队等待执行，同时解析模型和认证
③ 准备沙箱、加载技能、构建系统提示、创建工具集
④ 发送给 LLM，流式接收回复，自动执行工具调用
⑤ 流式文本通过事件系统和回复分发器送达用户
```

数据在这个旅程中经历了多次**形态变化**：

| 阶段 | 数据形态 | 关键转化 |
| ---- | ---- | ---- |
| 入口 | 渠道原始消息 (HTTP/WebSocket/CLI) | → 标准化 `MsgContext` |
| 路由 | `MsgContext` + 配置 | → `RunEmbeddedPiAgentParams` |
| 准备 | 参数 + 配置 | → `AgentSession` (含系统提示、工具、历史) |
| LLM | 用户 prompt + 系统提示 + 工具定义 | → 流式 `AgentEvent` 序列 |
| 投递 | `AgentEvent` 流 | → `ReplyPayload` → 渠道消息 API |

---

## 二、第一站：消息进入系统

用户消息从不同渠道进入，每个渠道都有自己的入口适配器，但最终都会汇聚到一个标准化结构。

```text
┌────────────────────────────────────────────────────────────────────────┐
│  消息来源 (多渠道入口)                                                  │
│                                                                        │
│  WhatsApp ──→ webhook handler ──→ ┐                                    │
│  Telegram ──→ bot update     ──→  │                                    │
│  Discord  ──→ gateway event  ──→  ├──→ MsgContext (标准化)              │
│  Signal   ──→ signal handler ──→  │                                    │
│  Web UI   ──→ WebSocket frame ──→ │                                    │
│  CLI      ──→ stdin / args   ──→  ┘                                    │
│                                                                        │
│  ┌── MsgContext (数据快照) ───────────────────────────────────────┐     │
│  │  Body:     "帮我看看 package.json"    ← 用户原始消息           │     │
│  │  From:     "+8613800138000"           ← 发送者标识             │     │
│  │  To:       "+1234567890"              ← 接收者标识             │     │
│  │  Provider: "whatsapp"                 ← 渠道类型               │     │
│  │  ChatType: "dm" | "group"             ← 聊天类型               │     │
│  │  GroupId:  "group-123" (if group)     ← 群组标识               │     │
│  │  Surface:  "whatsapp"                 ← 消息表面               │     │
│  │  MessageSid: "msg-abc123"             ← 消息唯一 ID           │     │
│  │  SenderName: "张三"                   ← 发送者名称             │     │
│  │  ...附带图片、线程 ID、回复引用等                               │     │
│  └─────────────────────────────────────────────────────────────────┘     │
│                                                                        │
│  关键转化:                                                              │
│  • 各渠道的原始消息格式被"抹平"为统一的 MsgContext                     │
│  • 图片/视频等媒体被提取为 ImageContent 格式                           │
│  • 发送者信息被标准化 (E.164 号码、用户名、显示名)                     │
└────────────────────────────────────────────────────────────────────────┘
```

**Web UI 路径特殊之处**: Web UI 消息不走渠道适配器，而是通过 Gateway WebSocket 协议直接进入。Gateway 将 `agent.request` 帧翻译为内部调用，绕过了渠道层但数据最终仍然进入相同的 Runtime 入口。

---

## 三、第二站：消息路由与排队

标准化的消息需要经过**路由决策**和**排队控制**，确定使用哪个会话、哪个模型，以及防止并发冲突。

```text
MsgContext
    │
    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  路由与调度层 (dispatch → getReplyFromConfig → runReplyAgent)          │
│                                                                        │
│  Step 1: 上下文确定                                                    │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  finalizeInboundContext(ctx)                                  │      │
│  │  • 确定 sessionKey (agentId:channel:accountId:...)           │      │
│  │  • 解析聊天类型 (dm / group / group-dm)                       │      │
│  │  • 确定 sessionFile 路径                                      │      │
│  │  • 加载 session 历史条目                                      │      │
│  └──────────────────────────────────────────────────────────────┘      │
│       │                                                                │
│       ▼                                                                │
│  Step 2: 命令 / 指令检测                                               │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  • /new, /reset → 会话重置                                    │      │
│  │  • /model claude-sonnet → 切换模型                            │      │
│  │  • /think high → 调整思考级别                                  │      │
│  │  • /compact → 压缩历史                                        │      │
│  │  • /skill xxx → 技能调用                                      │      │
│  │  • 普通消息 → 进入 Agent 流程                                  │      │
│  └──────────────────────────────────────────────────────────────┘      │
│       │                                                                │
│       ▼                                                                │
│  Step 3: 双层队列入队                                                  │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │                                                              │      │
│  │  enqueueSession(() =>        ← 会话级互斥锁                   │      │
│  │    enqueueGlobal(async () => ← 全局并发槽位                   │      │
│  │      { ... 实际执行 ... }                                     │      │
│  │    )                                                          │      │
│  │  )                                                            │      │
│  │                                                              │      │
│  │  为什么两层?                                                  │      │
│  │  • sessionLane: 同一会话不能并发写 (消息历史会冲突)            │      │
│  │  • globalLane:  总并发数限制 (防止 CPU/内存/API 过载)          │      │
│  │                                                              │      │
│  │  效果: 如果用户连发 5 条消息，它们会排队依次执行               │      │
│  └──────────────────────────────────────────────────────────────┘      │
│       │                                                                │
│       ▼                                                                │
│  数据输出: RunEmbeddedPiAgentParams                                    │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  prompt:         "帮我看看 package.json"                      │      │
│  │  sessionId:      "abc123"                                     │      │
│  │  sessionKey:     "pi:whatsapp:+86138...:dm"                   │      │
│  │  sessionFile:    "~/.openclaw/agents/pi/sessions/abc123.jsonl"│      │
│  │  provider:       "anthropic"                                  │      │
│  │  model:          "claude-sonnet-4-20250514"                   │      │
│  │  workspaceDir:   "~/projects/myapp"                           │      │
│  │  timeoutMs:      120000                                       │      │
│  │  onBlockReply:   fn  ← 流式回复回调                            │      │
│  │  onToolResult:   fn  ← 工具结果回调                            │      │
│  │  onAgentEvent:   fn  ← 通用事件回调                            │      │
│  │  ...                                                          │      │
│  └──────────────────────────────────────────────────────────────┘      │
└────────────────────────────────────────────────────────────────────────┘
```

**数据转化**: `MsgContext` (渠道视角) → `RunEmbeddedPiAgentParams` (Runtime 视角)。渠道信息被转化为 sessionKey、sessionFile 等 Runtime 概念，同时绑定了流式回调函数（`onBlockReply`、`onToolResult`），这些回调将在 LLM 输出时被调用。

---

## 四、第三站：模型选择与认证

Runtime 入口 (`runEmbeddedPiAgent`) 拿到参数后，首先解析模型和认证。这一步确保 LLM 请求能发出去。

```text
RunEmbeddedPiAgentParams
    │
    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  模型解析                                                              │
│                                                                        │
│  provider: "anthropic" ──→ resolveModel() ──→ Model 对象               │
│  model:    "claude-sonnet-4-20250514"         │                        │
│                                               ▼                        │
│                                   ┌────────────────────────┐          │
│                                   │ model:                  │          │
│                                   │   id: "claude-sonnet-4" │          │
│                                   │   api: "anthropic"      │          │
│                                   │   contextWindow: 200000 │          │
│                                   │   capabilities: [...]   │          │
│                                   │ authStorage: AuthStore  │          │
│                                   │ modelRegistry: Registry │          │
│                                   └────────────────────────┘          │
└────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  认证解析                                                              │
│                                                                        │
│  profileCandidates: [                                                  │
│    "anthropic:personal",    ← 首选                                     │
│    "anthropic:work",        ← 备选                                     │
│    "anthropic:shared",      ← 最后                                     │
│  ]                                                                     │
│       │                                                                │
│       ▼                                                                │
│  applyApiKeyInfo("anthropic:personal")                                 │
│       │                                                                │
│       ▼                                                                │
│  authStorage.setRuntimeApiKey("anthropic", "sk-ant-xxx...")            │
│                                                                        │
│  如果首选 Key 失败 → 自动切换到 "anthropic:work" → 重试                │
│  如果所有 Key 都失败 → 尝试 fallback 模型                               │
└────────────────────────────────────────────────────────────────────────┘
    │
    ▼
  进入重试循环 → runEmbeddedAttempt()
```

**数据转化**: 字符串形式的 `provider`/`model` → 完整的 `Model` 对象 + 已激活的 `AuthStorage`。此后 LLM 请求使用的 API Key 已就绪。

---

## 五、第四站：运行环境准备

`runEmbeddedAttempt` 是单次执行的核心。在调用 LLM 之前，它需要组装完整的运行环境。这是数据**聚合度最高**的阶段——多个子系统的产出在这里汇聚成 `AgentSession`。

```text
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  ┌─ 4A: 沙箱 ────────────────────┐                                    │
│  │                                │                                    │
│  │  resolveSandboxContext()       │                                    │
│  │                                │                                    │
│  │  输入: config, workspaceDir    │                                    │
│  │  输出: {                       │                                    │
│  │    enabled: true,              │                                    │
│  │    containerName: "oc-abc",    │                                    │
│  │    workspaceDir: "/sandbox/ws",│                                    │
│  │    workspaceAccess: "ro",      │──→ effectiveWorkspace              │
│  │  }                             │    (之后所有操作基于此目录)         │
│  │                                │                                    │
│  │  详见: AGENT-RUNTIME-SANDBOX.md│                                    │
│  └────────────────────────────────┘                                    │
│                                                                        │
│  ┌─ 4B: 技能 ────────────────────┐                                    │
│  │                                │                                    │
│  │  loadWorkspaceSkillEntries()   │                                    │
│  │  applySkillEnvOverrides()      │                                    │
│  │  resolveSkillsPromptForRun()   │                                    │
│  │                                │                                    │
│  │  输入: workspace skills/ 目录  │                                    │
│  │  输出:                         │──→ skillsPrompt                    │
│  │    "<available_skills>         │    (注入系统提示词)                 │
│  │       <skill name='weather'..> │                                    │
│  │    </available_skills>"        │                                    │
│  │                                │                                    │
│  │  详见: AGENT-RUNTIME-SKILLS.md │                                    │
│  └────────────────────────────────┘                                    │
│                                                                        │
│  ┌─ 4C: 上下文文件 ──────────────┐                                    │
│  │                                │                                    │
│  │  resolveBootstrapContextForRun()                                    │
│  │                                │                                    │
│  │  输入: workspace 中的          │                                    │
│  │    AGENTS.md, SOUL.md,         │                                    │
│  │    BOOTSTRAP.md 等文件         │                                    │
│  │  输出: contextFiles[]          │──→ contextFiles                    │
│  │    [{path, content}, ...]      │    (注入系统提示词)                 │
│  │                                │                                    │
│  │  详见: AGENT-RUNTIME-SYSTEM-   │                                    │
│  │        PROMPT.md               │                                    │
│  └────────────────────────────────┘                                    │
│                                                                        │
│  ┌─ 4D: 工具集 ──────────────────┐                                    │
│  │                                │                                    │
│  │  createOpenClawCodingTools()   │                                    │
│  │  + sanitizeToolsForGoogle()    │                                    │
│  │  + 策略过滤 (9层)              │                                    │
│  │                                │                                    │
│  │  输入: sandbox, config, model  │                                    │
│  │  输出: AgentTool[]             │──→ tools                           │
│  │    [exec, read, write, edit,   │    (注册到 Session)                 │
│  │     browser, web_search, ...]  │                                    │
│  │                                │                                    │
│  │  详见: AGENT-RUNTIME-TOOLS.md  │                                    │
│  └────────────────────────────────┘                                    │
│                                                                        │
│  ┌─ 4E: 系统提示词 ──────────────┐                                    │
│  │                                │                                    │
│  │  buildEmbeddedSystemPrompt()   │                                    │
│  │                                │                                    │
│  │  聚合以上所有产出:              │                                    │
│  │  • 身份定义                    │                                    │
│  │  • 运行时信息 (主机/OS/模型)    │                                    │
│  │  • skillsPrompt                │                                    │
│  │  • contextFiles                │──→ systemPromptText                │
│  │  • 工具使用指南                │    (约 5000-20000 字符)             │
│  │  • 沙箱说明                    │                                    │
│  │  • 安全规则                    │                                    │
│  │  • 渠道特定提示                │                                    │
│  │                                │                                    │
│  │  详见: AGENT-RUNTIME-SYSTEM-   │                                    │
│  │        PROMPT.md               │                                    │
│  └────────────────────────────────┘                                    │
│                                                                        │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  ▼ 所有产出汇聚 ▼                                                      │
│                                                                        │
│  ┌─ 4F: AgentSession 创建 ───────────────────────────────────────┐    │
│  │                                                                │    │
│  │  createAgentSession({                                          │    │
│  │    cwd:           effectiveWorkspace,                          │    │
│  │    model:         model,              ← 选定的 LLM 模型       │    │
│  │    tools:         builtInTools,        ← SDK 工具              │    │
│  │    customTools:   allCustomTools,      ← 自定义工具            │    │
│  │    sessionManager: sessionManager,     ← 会话持久化            │    │
│  │  })                                                            │    │
│  │                                                                │    │
│  │  + applySystemPromptOverrideToSession(session, systemPrompt)  │    │
│  │  + sanitizeSessionHistory(session.messages)                    │    │
│  │  + limitHistoryTurns(messages)                                 │    │
│  │                                                                │    │
│  │  结果: session 对象已就绪                                      │    │
│  │  • 系统提示词已设置                                            │    │
│  │  • 工具已注册                                                  │    │
│  │  • 历史消息已加载并清理                                        │    │
│  │  • API Key 已激活                                              │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

**数据转化**: 分散的配置/文件/环境 → 一个完整的 `AgentSession` 对象。这是**从离散到聚合**的关键节点——之后 LLM 交互只需要这一个对象。

---

## 六、第五站：LLM 交互循环

这是整个旅程的**核心引擎**。`session.prompt()` 触发一个可能包含多轮工具调用的循环。

```text
┌────────────────────────────────────────────────────────────────────────┐
│                       LLM 交互循环                                     │
│                                                                        │
│  session.prompt("帮我看看 package.json")                               │
│       │                                                                │
│       ▼                                                                │
│  ┌── 发送给 LLM API ─────────────────────────────────────────────┐    │
│  │                                                                │    │
│  │  POST https://api.anthropic.com/v1/messages                   │    │
│  │                                                                │    │
│  │  请求体:                                                       │    │
│  │  {                                                             │    │
│  │    model: "claude-sonnet-4-20250514",                          │    │
│  │    system: "你是 OpenClaw, 一个 AI 助手...",  ← systemPrompt  │    │
│  │    messages: [                                                 │    │
│  │      ...历史消息...,                         ← session.messages│    │
│  │      { role: "user", content: "帮我看看 package.json" }       │    │
│  │    ],                                                          │    │
│  │    tools: [                                                    │    │
│  │      { name: "read", input_schema: {...} },  ← ToolDefinition │    │
│  │      { name: "exec", input_schema: {...} },                   │    │
│  │      ...                                                       │    │
│  │    ],                                                          │    │
│  │    stream: true                                                │    │
│  │  }                                                             │    │
│  │                                                                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│       │                                                                │
│       │  SSE 流式响应                                                  │
│       ▼                                                                │
│  ┌── 第一轮: LLM 决定调用工具 ───────────────────────────────────┐    │
│  │                                                                │    │
│  │  LLM 输出:                                                     │    │
│  │  "好的，我来读取 package.json"                                 │    │
│  │  + tool_use: { name: "read", input: {path:"package.json"} }   │    │
│  │                                                                │    │
│  │  事件流:                                                       │    │
│  │    agent_start                                                 │    │
│  │    message_start (assistant)                                   │    │
│  │    message_update (text_delta: "好的")                         │    │
│  │    message_update (text_delta: "，我来")                       │    │
│  │    message_update (text_delta: "读取 package.json")            │    │
│  │    message_end                                                 │    │
│  │    tool_execution_start (name: "read", args: {path:...})      │    │
│  │                                                                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│       │                                                                │
│       ▼                                                                │
│  ┌── SDK 自动执行工具 ───────────────────────────────────────────┐    │
│  │                                                                │    │
│  │  readTool.execute("call_abc", {path: "package.json"})         │    │
│  │       │                                                        │    │
│  │       ▼                                                        │    │
│  │  Node.js fs.readFile("package.json")                          │    │
│  │  → 读取文件内容 (从 effectiveWorkspace)                        │    │
│  │       │                                                        │    │
│  │       ▼                                                        │    │
│  │  返回: { content: [{ type: "text", text: "{...}" }] }         │    │
│  │                                                                │    │
│  │  事件: tool_execution_end (result: {...})                      │    │
│  │                                                                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│       │                                                                │
│       │  SDK 将工具结果注入消息历史                                    │
│       │  messages.push({ role: "tool", content: "..." })               │
│       │                                                                │
│       ▼                                                                │
│  ┌── 第二轮: LLM 生成最终回复 ───────────────────────────────────┐    │
│  │                                                                │    │
│  │  POST /v1/messages (携带工具结果的完整上下文)                   │    │
│  │                                                                │    │
│  │  LLM 输出:                                                     │    │
│  │  "package.json 的内容如下：..."                                 │    │
│  │  (不再调用工具 → 循环结束)                                     │    │
│  │                                                                │    │
│  │  事件流:                                                       │    │
│  │    message_start (assistant)                                   │    │
│  │    message_update (text_delta: "package")                      │    │
│  │    message_update (text_delta: ".json 的")                     │    │
│  │    message_update (text_delta: "内容如下")                     │    │
│  │    ...                                                         │    │
│  │    message_end                                                 │    │
│  │    agent_end                                                   │    │
│  │                                                                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  session.prompt() 的 Promise 在 agent_end 后 resolve                   │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

**数据转化**: 用户 prompt 字符串 → HTTP 请求(系统提示+历史+工具定义) → SSE 流 → `AgentEvent` 序列。这个循环可能重复多次（LLM 连续调用多个工具），每次工具结果都被追加到消息历史中，直到 LLM 决定不再使用工具。

---

## 七、第六站：流式事件处理

LLM 的流式事件在到达用户之前，需要经过**缓冲、清洗和格式化**。这些处理在 `subscribeEmbeddedPiSession` 中完成。

```text
SDK 产出的 AgentEvent 流
    │
    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  EventHandler 分发 (switch on evt.type)                                │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────┐      │
│  │ message_update (text_delta)                                  │      │
│  │    │                                                         │      │
│  │    ▼                                                         │      │
│  │ ┌─ 三层缓冲处理 ──────────────────────────────────────────┐ │      │
│  │ │                                                          │ │      │
│  │ │ Layer 1: deltaBuffer (原始累加)                          │ │      │
│  │ │   "好的，我来读取 package.json"                          │ │      │
│  │ │   • 处理 text_end 重发去重                               │ │      │
│  │ │   • 作为完整文本的真实来源                               │ │      │
│  │ │          │                                               │ │      │
│  │ │          ▼                                               │ │      │
│  │ │ Layer 2: 标签清洗 + 指令解析                             │ │      │
│  │ │   stripBlockTags()  → 去除 <think>/<final>              │ │      │
│  │ │   parseReplyDirectives() → 提取 [[media:url]]           │ │      │
│  │ │          │                                               │ │      │
│  │ │          ▼                                               │ │      │
│  │ │ Layer 3: blockBuffer / BlockChunker (分块)               │ │      │
│  │ │   按 minChars/maxChars 在段落/换行/句号处断开            │ │      │
│  │ │   保护 Markdown 代码围栏不被拆断                         │ │      │
│  │ │                                                          │ │      │
│  │ └──────────────────────────────────────────────────────────┘ │      │
│  │    │                                                         │      │
│  │    ├──→ onPartialReply({text})       ← 实时流式片段          │      │
│  │    ├──→ onBlockReply({text})         ← 分块完整回复          │      │
│  │    └──→ emitAgentEvent({stream:"assistant", data:{text}})   │      │
│  │         ↓                                                    │      │
│  │    全局事件总线 → Gateway WebSocket 广播                      │      │
│  └─────────────────────────────────────────────────────────────┘      │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────┐      │
│  │ tool_execution_start / tool_execution_end                    │      │
│  │    │                                                         │      │
│  │    ├──→ onToolResult({text: "🔧 read: package.json"})      │      │
│  │    ├──→ emitAgentEvent({stream:"tool", data:{...}})         │      │
│  │    └──→ toolMetas.push({toolName, meta})                     │      │
│  └─────────────────────────────────────────────────────────────┘      │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────┐      │
│  │ agent_end                                                    │      │
│  │    │                                                         │      │
│  │    ├──→ 刷新所有缓冲区 (flushBlockReplyBuffer)              │      │
│  │    └──→ emitAgentEvent({stream:"lifecycle", data:{phase:"end"}}) │  │
│  └─────────────────────────────────────────────────────────────┘      │
│                                                                        │
│  最终收集:                                                              │
│  • assistantTexts: ["好的，我来...", "package.json 内容如下..."]       │
│  • toolMetas: [{toolName:"read", meta:"package.json"}]                 │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

**数据转化**: 原始 `AgentEvent` 流 → 经过缓冲/清洗/分块的 `onBlockReply`/`onPartialReply` 回调。同时，事件通过全局事件总线并行发往 Gateway WebSocket。

详见: [AGENT-RUNTIME-SESSION.md Part B](./AGENT-RUNTIME-SESSION.md#九agentsession-结构与生命周期)（AgentSession 结构、事件订阅、流式缓冲、消息投递）

---

## 八、第七站：回复投递给用户

处理后的消息通过两条并行路径到达最终用户。

```text
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  路径 A: 消息渠道 (WhatsApp / Telegram / Discord / Signal)              │
│  ─────────────────────────────────────────────────                      │
│                                                                        │
│  onBlockReply(payload)                                                  │
│       │                                                                │
│       ▼                                                                │
│  dispatchReplyFromConfig()                                              │
│       │                                                                │
│       ▼                                                                │
│  ReplyDispatcher.sendBlockReply(payload)                                │
│       │                                                                │
│       │  ┌── 内部处理 ──────────────────────────┐                      │
│       │  │  1. normalizeReplyPayload()           │                      │
│       │  │     添加 responsePrefix, 去空白       │                      │
│       │  │  2. 入队 (串行化保证顺序)             │                      │
│       │  │  3. humanDelay (可选模拟人类节奏)     │                      │
│       │  │  4. deliver(payload)                  │                      │
│       │  └──────────────────────────────────────┘                      │
│       │                                                                │
│       ▼                                                                │
│  渠道 API 调用                                                          │
│  ├── WhatsApp: POST /v1/messages → 用户手机                            │
│  ├── Telegram: sendMessage API → 用户聊天                               │
│  ├── Discord: POST /channels/{id}/messages → 频道/DM                   │
│  └── Signal: signal-cli send → 用户                                    │
│                                                                        │
│                                                                        │
│  路径 B: Web UI / Gateway WebSocket                                     │
│  ────────────────────────────────                                      │
│                                                                        │
│  emitAgentEvent({stream:"assistant", data:{text, delta}})               │
│       │                                                                │
│       ▼                                                                │
│  全局事件总线 (src/infra/agent-events.ts)                                │
│       │                                                                │
│       ▼                                                                │
│  Gateway AgentEventHandler                                              │
│       │                                                                │
│       │  ┌── 处理逻辑 ──────────────────────────┐                      │
│       │  │  assistant 事件:                      │                      │
│       │  │    缓存文本到 chatRunState.buffers    │                      │
│       │  │    150ms 节流后广播 chat delta        │                      │
│       │  │                                       │                      │
│       │  │  lifecycle end 事件:                   │                      │
│       │  │    广播 chat final (完整文本)          │                      │
│       │  │                                       │                      │
│       │  │  所有事件:                             │                      │
│       │  │    broadcast("agent", payload)         │                      │
│       │  └───────────────────────────────────────┘                      │
│       │                                                                │
│       ▼                                                                │
│  WebSocket 广播                                                         │
│  → Web UI 显示流式文本                                                   │
│  → TUI 控制台显示工具调用                                                │
│  → 远程节点同步                                                         │
│                                                                        │
│                                                                        │
│  路径 C: prompt() 完成后的最终回复 (兜底)                                │
│  ──────────────────────────────────────                                │
│                                                                        │
│  session.prompt() resolve                                               │
│       │                                                                │
│       ▼                                                                │
│  messagesSnapshot = session.messages.slice()                            │
│  assistantTexts = [收集的文本块]                                        │
│       │                                                                │
│       ▼                                                                │
│  buildEmbeddedRunPayloads()                                             │
│       │                                                                │
│       ▼                                                                │
│  EmbeddedPiRunResult {                                                  │
│    payloads: [{text: "package.json 的内容如下..."}],                    │
│    meta: { durationMs, agentMeta: {sessionId, provider, model, usage} } │
│  }                                                                      │
│       │                                                                │
│       ▼                                                                │
│  如果流式已发送 → payloads 被丢弃 (避免重复)                            │
│  如果流式未发送 → payloads 作为最终回复发送                              │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

**三条路径的关系**: 路径 A (消息渠道) 和路径 B (WebSocket) 是**实时流式**的，在 LLM 输出过程中就持续发送。路径 C 是**兜底机制**——当流式路径已经发送了内容，最终 payloads 会被去重跳过；只有在流式路径未能发送时（如渠道不支持流式），才使用 payloads 作为最终回复。

---

## 九、第八站：错误恢复与重试

真实世界不总是一帆风顺。Runtime 内置了多级弹性恢复机制。

```text
runEmbeddedAttempt() 返回
    │
    │  检查 promptError
    │
    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  错误恢复决策树                                                        │
│                                                                        │
│  promptError?                                                          │
│  ├── 否 → 成功，构建 payloads 返回                                    │
│  │                                                                     │
│  └── 是 → 判断错误类型:                                                │
│       │                                                                │
│       ├─ 上下文溢出 (context overflow)                                  │
│       │  └── 尝试自动压缩 (compactEmbeddedPiSessionDirect)             │
│       │      ├── 压缩成功 → continue (重试 prompt)                     │
│       │      └── 压缩失败 → 返回用户友好的错误消息                     │
│       │                                                                │
│       ├─ 认证失败 / Rate Limit (401/402/429)                            │
│       │  └── advanceAuthProfile()                                      │
│       │      ├── 有下一个 API Key → 切换并 continue                    │
│       │      └── 无可用 Key → 抛出 FailoverError                       │
│       │          └── runWithModelFallback 捕获                          │
│       │              ├── 有 fallback 模型 → 切换模型重试               │
│       │              └── 无 fallback → 返回错误                         │
│       │                                                                │
│       ├─ Thinking 不支持                                                │
│       │  └── pickFallbackThinkingLevel()                               │
│       │      ├── 可降级 → think: high → medium → low → off             │
│       │      └── 已最低 → 抛出错误                                     │
│       │                                                                │
│       ├─ 角色排序错误 (roles must alternate)                            │
│       │  └── 返回 "Message ordering conflict" 提示用户 /new            │
│       │                                                                │
│       └─ 其他错误                                                       │
│          └── 抛出，由上层处理                                           │
│                                                                        │
│  重试策略可视化:                                                        │
│                                                                        │
│  attempt 1: anthropic/claude-sonnet + Key-A + think:high               │
│       │ 失败: thinking 不支持                                          │
│       ▼                                                                │
│  attempt 2: anthropic/claude-sonnet + Key-A + think:low                │
│       │ 失败: rate limit (429)                                         │
│       ▼                                                                │
│  attempt 3: anthropic/claude-sonnet + Key-B + think:low                │
│       │ 失败: 401 unauthorized (Key-B 过期)                            │
│       ▼                                                                │
│  attempt 4: anthropic/claude-sonnet + Key-C + think:low                │
│       │ 失败: context overflow                                         │
│       ▼                                                                │
│  attempt 5: (压缩后) anthropic/claude-sonnet + Key-C + think:low       │
│       │ 成功!                                                          │
│       ▼                                                                │
│  返回结果                                                               │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 十、完整旅程图

将以上所有站点串联，这是一条消息的完整数据流旅程：

```text
用户发送 "帮我看看 package.json"
│
│  ┌─ 第一站: 消息进入 ────────────────────────────────────────────────┐
│  │  WhatsApp webhook → MsgContext { Body, From, Provider, ... }      │
│  └───────────────────────────────────────────────────────────────────┘
│
│  ┌─ 第二站: 路由与排队 ──────────────────────────────────────────────┐
│  │  MsgContext → finalizeInboundContext → 确定 sessionKey            │
│  │  → 命令检测 (非命令) → 双层队列入队                               │
│  │  → RunEmbeddedPiAgentParams { prompt, sessionId, callbacks... }   │
│  └───────────────────────────────────────────────────────────────────┘
│
│  ┌─ 第三站: 模型与认证 ──────────────────────────────────────────────┐
│  │  "anthropic"/"claude-sonnet-4" → resolveModel → Model 对象        │
│  │  → 加载 API Key → authStorage 就绪                                │
│  └───────────────────────────────────────────────────────────────────┘
│
│  ┌─ 第四站: 环境准备 ────────────────────────────────────────────────┐
│  │  resolveSandboxContext → effectiveWorkspace                        │
│  │  loadSkillEntries → skillsPrompt                                  │
│  │  resolveBootstrapContext → contextFiles                            │
│  │  createOpenClawCodingTools → tools[]                               │
│  │  buildEmbeddedSystemPrompt → systemPromptText                     │
│  │  createAgentSession → session 就绪 (提示词+工具+历史)              │
│  └───────────────────────────────────────────────────────────────────┘
│
│  ┌─ 第五站: LLM 交互循环 ───────────────────────────────────────────┐
│  │  session.prompt("帮我看看 package.json")                          │
│  │  → LLM: "好的" + tool_use: read(package.json)                    │
│  │  → SDK 执行 read 工具 → 返回文件内容                              │
│  │  → LLM: "package.json 的内容如下..."                              │
│  │  → agent_end                                                      │
│  └───────────────────────────────────────────────────────────────────┘
│         │                     │
│    ┌────┘                     └────┐
│    │ (流式事件)                    │ (最终结果)
│    ▼                               ▼
│  ┌─ 第六站: 事件处理 ──┐  ┌─ 第七站: 回复投递 ──────────────────────┐
│  │  deltaBuffer 累加    │  │                                         │
│  │  stripBlockTags      │  │  路径 A: onBlockReply → ReplyDispatcher │
│  │  BlockChunker 分块   │  │  → deliver → WhatsApp API → 用户手机    │
│  │  去重检测            │  │                                         │
│  └──────────────────────┘  │  路径 B: emitAgentEvent → Gateway       │
│                            │  → WebSocket broadcast → Web UI         │
│                            │                                         │
│                            │  路径 C: payloads (兜底，已流式则跳过)   │
│                            └─────────────────────────────────────────┘
│
▼
用户手机收到: "package.json 的内容如下：..."
Web UI 显示: (流式打字效果) "package.json 的内容如下：..."
```

---

## 关键源文件索引

| 阶段 | 文件 | 核心函数 |
| ---- | ---- | ---- |
| 消息入口 | `src/auto-reply/dispatch.ts` | `dispatchInboundMessage` |
| 路由调度 | `src/auto-reply/reply/dispatch-from-config.ts` | `dispatchReplyFromConfig` |
| 命令处理 | `src/auto-reply/reply/get-reply-run.ts` | `runPreparedReply` |
| Agent 运行器 | `src/auto-reply/reply/agent-runner.ts` | `runReplyAgent` |
| Runtime 入口 | `src/agents/pi-embedded-runner/run.ts` | `runEmbeddedPiAgent` |
| 单次执行 | `src/agents/pi-embedded-runner/run/attempt.ts` | `runEmbeddedAttempt` |
| 沙箱 | `src/agents/sandbox/context.ts` | `resolveSandboxContext` |
| 技能 | `src/agents/skills/workspace.ts` | `loadSkillEntries` |
| 上下文文件 | `src/agents/bootstrap-files.ts` | `resolveBootstrapContextForRun` |
| 工具创建 | `src/agents/pi-tools.ts` | `createOpenClawCodingTools` |
| 系统提示 | `src/agents/pi-embedded-runner/system-prompt.ts` | `buildEmbeddedSystemPrompt` |
| 事件订阅 | `src/agents/pi-embedded-subscribe.ts` | `subscribeEmbeddedPiSession` |
| 事件分发 | `src/agents/pi-embedded-subscribe.handlers.ts` | `createEmbeddedPiSessionEventHandler` |
| 消息处理 | `src/agents/pi-embedded-subscribe.handlers.messages.ts` | `handleMessageUpdate` |
| 分块器 | `src/agents/pi-embedded-block-chunker.ts` | `EmbeddedBlockChunker` |
| 全局事件 | `src/infra/agent-events.ts` | `emitAgentEvent` |
| Gateway 事件 | `src/gateway/server-chat.ts` | `createAgentEventHandler` |
| 回复分发 | `src/auto-reply/reply/reply-dispatcher.ts` | `createReplyDispatcher` |
| 模型降级 | `src/agents/model-fallback.ts` | `runWithModelFallback` |
| 上下文压缩 | `src/agents/pi-embedded-runner/compact.ts` | `compactEmbeddedPiSessionDirect` |

## 相关文档

| 文档 | 内容 |
| ---- | ---- |
| [AGENT-RUNTIME-TOOLS.md](./AGENT-RUNTIME-TOOLS.md) | 工具系统、策略、调用协议 |
| [AGENT-RUNTIME-SESSION.md](./AGENT-RUNTIME-SESSION.md) | 会话管理、AgentSession、事件通信、流式缓冲 |
| [AGENT-RUNTIME-SYSTEM-PROMPT.md](./AGENT-RUNTIME-SYSTEM-PROMPT.md) | 系统提示词构建、上下文管理 |
| [AGENT-RUNTIME-SKILLS.md](./AGENT-RUNTIME-SKILLS.md) | 技能加载、调用、同步 |
| [AGENT-RUNTIME-SANDBOX.md](./AGENT-RUNTIME-SANDBOX.md) | 沙箱机制实现 |
