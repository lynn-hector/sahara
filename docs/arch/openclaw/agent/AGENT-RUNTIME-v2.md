# OpenClaw Agent Runtime 架构总览

> 本文档是 Agent Runtime 的**架构入口**，阐述整体设计、核心子系统及其协作关系。
> 每个子系统的深入细节由专题文档覆盖，本文提供全局视角和导航索引。

---

## 目录

1. [Agent Runtime 是什么](#一agent-runtime-是什么)
2. [架构全景](#二架构全景)
3. [八大子系统](#三八大子系统)
   - [入口与调度](#31-入口与调度)
   - [模型与认证](#32-模型与认证)
   - [运行环境](#33-运行环境)
   - [系统提示词](#34-系统提示词)
   - [工具系统](#35-工具系统)
   - [AgentSession](#36-agentsession)
   - [事件与流式通信](#37-事件与流式通信)
   - [上下文管理](#38-上下文管理)
4. [子系统协作关系](#四子系统协作关系)
5. [关键设计决策](#五关键设计决策)
6. [关键依赖库](#六关键依赖库)
7. [源码导航](#七源码导航)
8. [专题文档索引](#八专题文档索引)

---

## 一、Agent Runtime 是什么

Agent Runtime 是 OpenClaw 的**核心执行引擎**。它接收用户消息，协调 LLM、工具、沙箱、技能等子系统，最终将 AI 回复送达用户。

```text
用户                                                              用户
"帮我读取                                                         "文件内容
 package.json"                                                     如下..."
     │                                                               ▲
     ▼                                                               │
┌──────────────────────────────────────────────────────────────────────┐
│                       Agent Runtime                                  │
│                                                                      │
│  消息入口 → 排队 → 模型选择 → 环境组装 → LLM 交互 → 事件处理 → 回复  │
└──────────────────────────────────────────────────────────────────────┘
```

**一句话定义**: Agent Runtime 把"一条用户文本"变成"一次完整的 AI 代理执行"——包括理解意图、调用工具、执行代码、生成回复。

**它不做什么**:

- 不负责消息渠道适配（WhatsApp/Telegram/Discord 的 webhook 由各渠道模块处理）
- 不负责 Gateway 协议（WebSocket 帧的解析由 Gateway 层处理）
- 不负责 UI 渲染（Web/TUI/移动端各自处理）

---

## 二、架构全景

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                          Agent Runtime 架构全景                              │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  入口与调度                                                          │   │
│  │  runEmbeddedPiAgent()                                                │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│  │  │ 双层队列     │  │ 模型解析    │  │ 认证管理     │                 │   │
│  │  │ session +   │  │ + 上下文窗口│  │ + Key 轮换  │                 │   │
│  │  │ global lane │  │   检查      │  │ + 冷却期    │                 │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                 │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  运行环境组装                                                        │   │
│  │  runEmbeddedAttempt()                                                │   │
│  │                                                                      │   │
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐           │   │
│  │  │  沙箱      │ │  技能      │ │  上下文    │ │  工具集    │           │   │
│  │  │  Sandbox  │ │  Skills   │ │  Bootstrap│ │  Tools    │           │   │
│  │  │  Context  │ │  Loader   │ │  Files    │ │  Creator  │           │   │
│  │  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘           │   │
│  │        │             │             │             │                   │   │
│  │        └─────────────┴──────┬──────┴─────────────┘                   │   │
│  │                             ▼                                        │   │
│  │                  ┌───────────────────────┐                           │   │
│  │                  │    系统提示词构建       │                           │   │
│  │                  │  buildSystemPrompt()  │                           │   │
│  │                  └───────────┬───────────┘                           │   │
│  │                             │                                        │   │
│  └─────────────────────────────┼────────────────────────────────────────┘   │
│                                │                                           │
│                                ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  核心交互层                                                          │   │
│  │                                                                      │   │
│  │  ┌──────────────────────────────────────────────────────────────┐   │   │
│  │  │  AgentSession                                                │   │   │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │   │   │
│  │  │  │ messages[]   │  │ agent       │  │ SessionManager      │ │   │   │
│  │  │  │ (消息历史)   │  │ (LLM 交互)  │  │ (磁盘持久化)        │ │   │   │
│  │  │  └─────────────┘  └──────┬──────┘  └─────────────────────┘ │   │   │
│  │  └──────────────────────────┼───────────────────────────────────┘   │   │
│  │                             │                                       │   │
│  │              session.prompt("用户消息")                              │   │
│  │                             │                                       │   │
│  │                             ▼                                       │   │
│  │  ┌───────────────── LLM 交互循环 ──────────────────────────────┐   │   │
│  │  │                                                              │   │   │
│  │  │   LLM API ──→ 流式响应 ──→ 工具调用? ──→ 执行工具           │   │   │
│  │  │     ▲                                       │                │   │   │
│  │  │     └───────── 工具结果注入 ←───────────────┘                │   │   │
│  │  │                                                              │   │   │
│  │  └──────────────────────────┬───────────────────────────────────┘   │   │
│  │                             │                                       │   │
│  │                        事件流 (AgentEvent)                          │   │
│  │                             │                                       │   │
│  └─────────────────────────────┼───────────────────────────────────────┘   │
│                                │                                           │
│                                ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  事件处理与回复投递                                                   │   │
│  │                                                                      │   │
│  │  ┌─────────────┐     ┌─────────────────────────────────────────┐    │   │
│  │  │ EventHandler│     │          输出路径                        │    │   │
│  │  │  (分发器)    │────→│  A: onBlockReply → 消息渠道 API         │    │   │
│  │  │             │     │  B: emitAgentEvent → Gateway WebSocket  │    │   │
│  │  │ • 缓冲      │     │  C: payloads → 兜底最终回复              │    │   │
│  │  │ • 清洗      │     └─────────────────────────────────────────┘    │   │
│  │  │ • 分块      │                                                    │   │
│  │  └─────────────┘                                                    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、八大子系统

### 3.1 入口与调度

**职责**: 接收外部请求，控制并发，启动执行。

| 组件 | 源文件 | 作用 |
| ---- | ---- | ---- |
| `runEmbeddedPiAgent` | `pi-embedded-runner/run.ts` | 总入口，协调重试循环 |
| Session Lane | `process/command-queue.ts` | 同一会话串行执行 |
| Global Lane | `process/command-queue.ts` | 全局并发数限制 |

```text
外部调用 → enqueueSession → enqueueGlobal → runEmbeddedAttempt
                 │                │
          防止会话并发      防止系统过载
```

**设计要点**: 双层队列保证同一会话不会并发写入（消息历史会冲突），同时全局队列防止 CPU/内存/API 过载。

### 3.2 模型与认证

**职责**: 将"字符串形式的模型名"解析为完整的 Model 对象和可用的 API Key。

| 组件 | 源文件 | 作用 |
| ---- | ---- | ---- |
| Model Resolver | `pi-embedded-runner/model.ts` | 模型解析与上下文窗口验证 |
| Auth Profiles | `auth-profiles/` | 多 Key 管理、轮换、冷却期 |
| Model Fallback | `model-fallback.ts` | 模型降级 (认证失败时切换) |

**弹性机制**:

```text
尝试 Key-A → 失败(429) → 尝试 Key-B → 失败(401) → 降级到 fallback 模型
```

支持的弹性恢复: API Key 轮换、Rate Limit 冷却、Thinking Level 降级、上下文溢出压缩、模型降级。

### 3.3 运行环境

**职责**: 为 LLM 交互准备所有必要的环境——沙箱、技能、上下文文件。

| 组件 | 源文件 | 产出 |
| ---- | ---- | ---- |
| Sandbox Context | `sandbox/context.ts` | `effectiveWorkspace` + Docker 容器 |
| Skills Loader | `skills/workspace.ts` | `skillsPrompt` (技能描述) |
| Bootstrap Files | `bootstrap-files.ts` | `contextFiles[]` (项目上下文) |

这三个子系统**独立运行、互不依赖**，它们的产出汇入系统提示词和工具集。

```text
┌──────────┐  ┌──────────┐  ┌──────────┐
│  沙箱     │  │  技能     │  │  上下文   │
│ Context  │  │ Loader   │  │  Files   │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │
     ▼             ▼             ▼
  effectiveWs   skillsPrompt  contextFiles
     │             │             │
     └─────────────┼─────────────┘
                   ▼
          系统提示词 + 工具集
```

**沙箱**决定了 `effectiveWorkspace`（工具实际操作的目录），同时影响工具的执行方式（主机 vs 容器）。
**技能**生成描述文本注入系统提示词，让 LLM 知道有哪些技能可用。
**上下文文件**（AGENTS.md、SOUL.md 等）提供项目特定的知识。

### 3.4 系统提示词

**职责**: 将所有环境产出聚合为一份完整的系统提示词。

```text
系统提示词由 ~24 个节段组成:

┌────────────────────────────────────────┐
│  身份定义 (你是 OpenClaw)               │
│  工具使用指南                           │
│  安全与合规规则                          │
│  技能列表 (<available_skills>)          │
│  项目上下文 (AGENTS.md, SOUL.md 等)     │
│  沙箱环境说明                           │
│  渠道特定提示                           │
│  运行时信息 (主机/OS/时间/模型)          │
│  ... (约 5,000-20,000 字符)            │
└────────────────────────────────────────┘
```

节段的包含/排除由 `PromptMode`（full/minimal）动态决定。子代理（subagent）使用 minimal 模式以节省 token。

### 3.5 工具系统

**职责**: 创建、过滤、注册 Agent 可调用的工具。

```text
工具来源:
  ├── Coding Tools (exec, read, write, edit, ...)     ← pi-coding-agent SDK
  ├── OpenClaw Tools (browser, web_search, message, ...) ← openclaw-tools.ts
  ├── Channel Tools (telegram_actions, discord_actions)  ← 渠道模块
  └── Plugin Tools                                       ← 插件扩展

        │ 全部创建后
        ▼
  9 层策略过滤 (Profile → Global → Agent → Group → Sandbox → Subagent)
        │
        ▼
  最终可用工具集 → 注册到 AgentSession
```

工具系统最重要的设计是**9 层策略过滤**——权限只能逐层收窄，不能扩大。这保证了安全性（比如群聊场景下限制危险工具）。

### 3.6 AgentSession

**职责**: Runtime 与 LLM 之间的**会话抽象层**。

```text
AgentSession
  ├── sessionId        会话标识
  ├── messages[]       消息历史
  ├── agent            底层 Agent (LLM 通信)
  │   ├── streamFn         流式请求函数
  │   ├── setSystemPrompt  设置系统提示
  │   └── replaceMessages  替换消息历史
  ├── prompt()         发送用户消息 → 触发 LLM 交互循环
  ├── subscribe()      注册事件监听 → 接收流式事件
  ├── steer()          流式中插入消息
  ├── compact()        压缩历史
  ├── abort()          中止请求
  └── dispose()        释放资源
```

AgentSession 屏蔽了不同 LLM 提供商的 API 差异。Runtime 只需调用 `session.prompt()`，SDK 内部自动处理流式请求、工具执行循环、消息持久化。

**SessionManager vs AgentSession**:

- `SessionManager` — 磁盘持久化（.jsonl 文件读写、分支管理）
- `AgentSession` — 运行时交互（内存消息、LLM 通信、事件分发）

### 3.7 事件与流式通信

**职责**: 将 LLM 的流式输出转化为结构化事件，并投递给最终用户。

```text
SDK 产出事件流                   EventHandler                    用户
─────────────                   ────────────                    ────
agent_start          ──→  switch(evt.type) ──→  onAgentEvent
message_start        ──→  handleMessageStart  → onAssistantMessageStart
message_update       ──→  handleMessageUpdate → deltaBuffer → 清洗 → 分块
  (text_delta)             │                     │
  (text_end)               │                     ├→ onPartialReply (实时片段)
                           │                     ├→ onBlockReply  (分块回复)
                           │                     └→ emitAgentEvent (WebSocket)
message_end          ──→  handleMessageEnd    → assistantTexts[]
tool_execution_start ──→  handleToolExecStart → onToolResult
tool_execution_end   ──→  handleToolExecEnd   → sanitize → onToolResult
agent_end            ──→  handleAgentEnd      → flush all buffers
```

10 种事件类型，分别由 4 个 handler 文件处理（lifecycle/messages/tools）。共享的 `EmbeddedPiSubscribeContext` 维护缓冲区状态和回调引用。

### 3.8 上下文管理

**职责**: 防止上下文超出 LLM 的 token 限制。

```text
四层防御:

  Layer 1: 输入阶段截断
  │  Bootstrap 文件 > 20,000 字符 → 头部 70% + 尾部 20%
  │  工具结果 > 8,000 字符 → 截断
  │
  Layer 2: 上下文剪枝 (Context Pruning)
  │  基于 cache-ttl 的软裁剪 → 硬清除
  │  保护最近的 assistant 消息和初始 bootstrap
  │
  Layer 3: 自动压缩 (Auto Compaction)
  │  SDK 内置，将旧消息摘要化
  │  Safeguard 模式: 分阶段摘要，保留工具失败和文件操作
  │
  Layer 4: 溢出压缩 (Overflow Compaction)
  │  LLM 返回 context_overflow 错误时紧急触发
  │  compactEmbeddedPiSessionDirect → 压缩后重试
```

---

## 四、子系统协作关系

八大子系统不是孤立的，它们通过明确的数据流相互协作：

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        子系统协作关系图                                       │
│                                                                             │
│                                                                             │
│  ① 入口与调度 ─────────────→ ② 模型与认证                                  │
│       │                           │                                         │
│       │  RunEmbeddedPiAgentParams │  Model + AuthStorage                   │
│       │                           │                                         │
│       └───────────┬───────────────┘                                         │
│                   │                                                         │
│                   ▼                                                         │
│           ③ 运行环境                                                        │
│           ┌───┬───┬───┐                                                     │
│           │沙箱│技能│上下文│                                                  │
│           └─┬─┴─┬─┴─┬─┘                                                     │
│             │   │   │                                                       │
│      effectiveWs│   │contextFiles                                           │
│      skillsPrompt   │                                                       │
│             │   │   │                                                       │
│             └───┼───┘                                                       │
│                 ▼                                                           │
│  ┌──── ④ 系统提示词 ◄──── ⑤ 工具系统 (工具名列表注入提示词)               │
│  │              │              │                                            │
│  │   systemPrompt     tools[] (ToolDefinition[])                           │
│  │              │              │                                            │
│  │              └──────┬───────┘                                            │
│  │                     ▼                                                    │
│  │             ⑥ AgentSession                                               │
│  │              (systemPrompt + tools + messages = LLM 请求)                │
│  │                     │                                                    │
│  │              session.prompt()                                            │
│  │                     │                                                    │
│  │                     ▼                                                    │
│  │    ⑦ 事件与流式通信 ◄──── LLM 流式响应                                  │
│  │         │                                                                │
│  │         ├──→ 消息渠道 (WhatsApp/Telegram/Discord)                       │
│  │         ├──→ Gateway WebSocket (Web UI/TUI)                             │
│  │         └──→ payloads (兜底回复)                                         │
│  │                                                                         │
│  └──── ⑧ 上下文管理 (贯穿整个生命周期，在输入/运行/溢出时介入)              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**关键协作节点**:

| 协作 | 说明 |
| ---- | ---- |
| 沙箱 → 工具 | 沙箱决定 `exec` 工具是在 Docker 容器还是主机执行 |
| 沙箱 → 工具 | 沙箱决定 `read`/`write` 工具的根目录 (effectiveWorkspace) |
| 技能 → 系统提示 | 技能描述 (`<available_skills>`) 注入系统提示词 |
| 上下文文件 → 系统提示 | AGENTS.md / SOUL.md 内容嵌入系统提示词 |
| 工具 → 系统提示 | 工具名列表注入系统提示词（让 LLM 知道有哪些工具） |
| AgentSession → 事件 | `session.subscribe(handler)` 注册事件监听 |
| 事件 → 回复投递 | EventHandler 通过回调/事件总线将消息送达用户 |
| 上下文管理 → AgentSession | 在输入阶段截断内容，运行时剪枝消息，溢出时压缩 |

---

## 五、关键设计决策

### 为什么用双层队列而不是简单的互斥锁?

- **Session Lane** 保证消息历史写入不冲突（同一会话串行）
- **Global Lane** 控制总 CPU/内存/API 消耗
- 分离后可以独立调优——比如 session 允许排队但 global 限制并发为 4

### 为什么工具执行在 SDK 内部自动完成?

Runtime 不手动调用工具。`session.prompt()` 触发后，SDK 自动处理"LLM 请求工具 → 执行 → 回注结果 → 继续生成"的循环。Runtime 只通过事件监听得知发生了什么。

好处：Runtime 代码更简洁；工具循环逻辑由 SDK 统一维护；Runtime 只关注事件处理和回复投递。

### 为什么回复有三条路径?

- **路径 A (消息渠道)**: 通过 `onBlockReply` 回调，经 `ReplyDispatcher` 串行投递到 WhatsApp/Telegram 等
- **路径 B (WebSocket)**: 通过 `emitAgentEvent` 全局事件总线，Gateway 广播给 Web UI
- **路径 C (payloads 兜底)**: `prompt()` 完成后的最终文本，仅在流式路径未发送时使用

三条路径并行存在是因为不同消费者需要不同粒度的数据——消息渠道需要完整的分块，Web UI 需要实时 delta，而某些场景（如 CLI）只需要最终结果。

### 为什么事件是单向的?

Session → EventHandler 是单向通知，EventHandler 不能通过返回值影响 Session。如需干预（中止、插入消息），要通过 `session.abort()` 或 `session.steer()` 另行调用。

这简化了并发模型——事件处理不会阻塞 LLM 输出流。

---

## 六、关键依赖库

| 库 | 作用 | 在 Runtime 中的角色 |
| ---- | ---- | ---- |
| `@mariozechner/pi-ai` | LLM API 抽象层 | 提供 `streamSimple` 流式调用函数 |
| `@mariozechner/pi-agent-core` | Agent 核心类型 | 定义 `AgentEvent`、`AgentMessage` 等类型 |
| `@mariozechner/pi-coding-agent` | 代码助手 Agent | 提供 `createAgentSession`、`SessionManager`、内置工具 |

Runtime 自身不直接调用 LLM API——它通过 `AgentSession.prompt()` 间接调用，SDK 内部使用 `pi-ai` 发起请求。

---

## 七、源码导航

### 目录结构

```text
src/agents/
├── pi-embedded-runner/          # Runtime 核心
│   ├── run.ts                   #   入口 (runEmbeddedPiAgent)
│   ├── run/
│   │   ├── attempt.ts           #   单次执行 (runEmbeddedAttempt)
│   │   ├── params.ts            #   参数类型
│   │   └── types.ts             #   返回类型
│   ├── system-prompt.ts         #   系统提示词入口
│   ├── compact.ts               #   溢出压缩
│   ├── history.ts               #   历史轮数限制
│   └── extensions.ts            #   上下文剪枝/压缩扩展注册
│
├── sandbox/                     # 沙箱系统
│   ├── context.ts               #   沙箱上下文解析入口
│   ├── docker.ts                #   Docker 容器管理
│   ├── workspace.ts             #   工作空间创建与文件同步
│   └── config.ts                #   沙箱配置解析
│
├── skills/                      # 技能系统
│   ├── workspace.ts             #   技能加载/过滤/同步/提示生成
│   ├── types.ts                 #   技能类型定义
│   ├── config.ts                #   技能过滤配置
│   └── env-overrides.ts         #   技能环境变量注入
│
├── tools/                       # 工具实现
│   ├── browser-tool.ts          #   浏览器控制
│   ├── web-tools.ts             #   web_search/web_fetch
│   ├── message-tool.ts          #   跨渠道消息发送
│   ├── nodes-tool.ts            #   设备节点控制
│   └── ...                      #   更多工具
│
├── pi-tools.ts                  # 工具创建入口 + 策略过滤
├── pi-tools.policy.ts           # 工具策略逻辑
├── tool-policy.ts               # 工具组/配置文件定义
├── openclaw-tools.ts            # OpenClaw 工具集
├── pi-tool-definition-adapter.ts # AgentTool → ToolDefinition 适配
│
├── pi-embedded-subscribe.ts     # 事件订阅系统入口
├── pi-embedded-subscribe.handlers.ts        # 事件分发 (switch)
├── pi-embedded-subscribe.handlers.messages.ts # 消息事件处理
├── pi-embedded-subscribe.handlers.tools.ts    # 工具事件处理
├── pi-embedded-subscribe.handlers.lifecycle.ts # 生命周期事件
├── pi-embedded-block-chunker.ts # 流式文本分块器
│
├── system-prompt.ts             # 系统提示词核心构建
├── system-prompt-params.ts      # 运行时参数收集
├── bootstrap-files.ts           # 上下文文件加载入口
├── workspace.ts                 # Bootstrap 文件定义与加载
│
├── pi-extensions/               # 上下文管理扩展
│   ├── context-pruning/         #   上下文剪枝
│   └── compaction-safeguard.ts  #   压缩安全护栏
│
├── model-fallback.ts            # 模型降级
├── model-auth.ts                # 模型认证模式
├── auth-profiles/               # API Key 管理
├── compaction.ts                # 摘要化压缩核心
└── defaults.ts                  # 默认配置
```

### 学习路径推荐

```text
Level 1: 理解全局流程
  └── AGENT-RUNTIME-MSG-FLOW.md (数据流旅程)

Level 2: 理解核心入口
  ├── pi-embedded-runner/run.ts (入口、重试、降级)
  └── pi-embedded-runner/run/attempt.ts (环境组装、LLM 调用)

Level 3: 深入各子系统 (按兴趣选择)
  ├── pi-tools.ts (工具创建与策略)
  ├── sandbox/context.ts (沙箱)
  ├── skills/workspace.ts (技能)
  ├── system-prompt.ts (系统提示词)
  └── pi-embedded-subscribe.ts (事件系统)

Level 4: 高级机制
  ├── pi-embedded-subscribe.handlers.messages.ts (流式缓冲)
  ├── pi-extensions/context-pruning/ (上下文剪枝)
  └── compact.ts (溢出压缩)
```

---

## 八、专题文档索引

| 文档 | 内容 | 适合回答的问题 | 状态 |
| ---- | ---- | ---- | ---- |
| [AGENT-RUNTIME-MSG-FLOW.md](./AGENT-RUNTIME-MSG-FLOW.md) | 数据流全旅程 | 一条消息从进入到回复经历了什么? | done |
| [AGENT-RUNTIME-TOOLS.md](./AGENT-RUNTIME-TOOLS.md) | 工具系统详解 | 工具怎么创建/过滤/调用? 工具策略? 调用协议? | done |
| [AGENT-RUNTIME-SYSTEM-PROMPT.md](./AGENT-RUNTIME-SYSTEM-PROMPT.md) | 系统提示词构建 | 系统提示包含什么? 上下文文件怎么加载? 溢出怎么处理? | done |
| [AGENT-RUNTIME-SKILLS.md](./AGENT-RUNTIME-SKILLS.md) | 技能管理 | 技能怎么加载/过滤/调用? LLM 怎么使用技能? | done |
| [AGENT-RUNTIME-SANDBOX.md](./AGENT-RUNTIME-SANDBOX.md) | 沙箱实现 | 文件怎么进沙箱? 工具在哪里执行? 结果怎么出来? | done |
| [AGENT-RUNTIME-API-REFERENCE.md](./AGENT-RUNTIME-API-REFERENCE.md) | API 参考手册 | 类型定义? 函数签名? 参数/返回值结构? | done |
| [AGENT-RUNTIME-SESSION.md](./AGENT-RUNTIME-SESSION.md) | 会话系统 | SessionManager 持久化? AgentSession 交互? 事件订阅/分发? 流式缓冲? 消息投递? | done |
| [AGENT-RUNTIME-SUBAGENT.md](./AGENT-RUNTIME-SUBAGENT.md) | 子 Agent 生命周期 | 子 Agent 怎么创建/追踪/清理? 结果怎么回传父 Agent? | done |
| [AGENT-RUNTIME-FALLBACK.md](./AGENT-RUNTIME-FALLBACK.md) | 模型降级与认证轮转 | 降级链怎么走? Auth Profile 怎么轮转? 冷却/退避策略? | done |
| [AGENT-RUNTIME-MEMORY.md](./AGENT-RUNTIME-MEMORY.md) | 记忆系统 | 向量索引怎么建? 混合搜索? 同步策略? Session Memory? | done |
| [AGENT-RUNTIME-MEDIA.md](./AGENT-RUNTIME-MEDIA.md) | 媒体处理 | 图像怎么检测/注入/清洗? Vision 模型怎么对接? TTS? | done |
| [AGENT-RUNTIME-REPLY-DISPATCH.md](./AGENT-RUNTIME-REPLY-DISPATCH.md) | 回复投递架构 | 回复怎么串行化? 跨渠道路由? 人性化延迟? 去重? | done |
| [AGENT-RUNTIME-HOOKS.md](./AGENT-RUNTIME-HOOKS.md) | 钩子系统（Runtime 视角） | hook 类型与触发点? 在 Runtime 流程中的嵌入位置? 执行顺序? | done |
| [AGENT-RUNTIME-PLUGIN.md](./AGENT-RUNTIME-PLUGIN.md) | 插件工具集成 | 插件工具怎么发现/加载? 名称冲突? 渠道对接? Runtime 扩展? | done |
