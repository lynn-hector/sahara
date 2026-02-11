# Agent Runtime 会话系统

> 本文档是 Agent Runtime 会话系统的完整参考：从全局架构到 SessionManager（持久化层）和 AgentSession（交互层）的各自细节，以及它们如何协作完成一条消息从接收到回复的完整生命周期。

---

## 目录

- [一、会话系统全局架构](#一会话系统全局架构)
- [二、三层标识体系](#二三层标识体系)

Part A — SessionManager（持久化层）

- [三、会话存储结构](#三会话存储结构)
- [四、会话生命周期](#四会话生命周期)
- [五、并发控制](#五并发控制)
- [六、历史消息处理管线](#六历史消息处理管线)
- [七、会话守卫与工具结果追踪](#七会话守卫与工具结果追踪)
- [八、缓存机制](#八缓存机制)

Part B — AgentSession（交互层）

- [九、AgentSession 结构与生命周期](#九agentsession-结构与生命周期)
  - [9.4 LLM 交互工程化细节](#94-llm-交互工程化细节)（streamFn 函数链、prompt 内部循环、提供商抽象、超时中止、steer 消息注入）
- [十、事件订阅与分发](#十事件订阅与分发)
- [十一、流式数据缓冲与处理](#十一流式数据缓冲与处理)
- [十二、消息投递链路](#十二消息投递链路)
- [十三、完整通信流程图](#十三完整通信流程图)

附录

- [十四、关键源文件索引](#十四关键源文件索引)

---

## 一、会话系统全局架构

### 1.1 为什么需要理解会话系统

会话系统是 Agent Runtime 的**状态基础**——所有 LLM 交互都依赖它。没有会话系统：

- LLM 不知道之前聊过什么（无上下文）
- 工具调用结果无法追踪（无配对）
- 流式回复无法投递给用户（无事件通道）
- 多条消息可能并发写入同一文件（无锁）

### 1.2 全景架构图

会话系统处于 Agent Runtime 的**中心位置**，连接着 LLM、工具、沙箱、事件系统和消息渠道。以下展示 Session 与周围组件的完整关系：

```text
                          用户消息
                             │
                ┌────────────┴────────────┐
                │     消息渠道 / Gateway    │
                │  WhatsApp Telegram CLI   │
                │  Discord  Signal  WebUI  │
                └────────────┬────────────┘
                             │ MsgContext → RunParams
                             ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│                        Agent Runtime                                       │
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    会话系统 (本文档的主题)                             │  │
│  │                                                                      │  │
│  │   持久化层                               交互层                       │  │
│  │  ┌──────────────────┐              ┌──────────────────┐              │  │
│  │  │  SessionManager  │── 注入 ──→   │  AgentSession     │              │  │
│  │  │                  │              │                   │              │  │
│  │  │  JSONL 文件      │←── 写回 ──   │  messages[]       │              │  │
│  │  │  树结构/分支      │              │  prompt()         │              │  │
│  │  │  消息追加        │              │  subscribe()      │              │  │
│  │  └──────┬───────────┘              └─────┬──────┬──────┘              │  │
│  │         │                                │      │                     │  │
│  │    ┌────┴────┐               ┌──────────┘      │                     │  │
│  │    │ 磁盘    │               │                  │                     │  │
│  │    │ .jsonl  │               ▼                  ▼                     │  │
│  │    └─────────┘     系统提示词 + messages    EventHandler              │  │
│  │                    + tool definitions            │                     │  │
│  └──────────────────────────┼──────────────────────┼─────────────────────┘  │
│                             │                      │                       │
│              ┌──────────────┴──────┐       ┌───────┴────────────┐          │
│              ▼                     ▼       ▼                    ▼          │
│  ┌─────────────────┐  ┌────────────────┐  ┌──────────┐  ┌────────────┐   │
│  │    LLM API      │  │   工具系统      │  │ 全局事件  │  │  回复分发   │   │
│  │                 │  │                │  │  总线     │  │  器        │   │
│  │  Anthropic      │  │ exec / read /  │  │          │  │            │   │
│  │  OpenAI         │  │ write / edit / │  │ emitAgent│  │ ReplyDis-  │   │
│  │  Google         │  │ browser /      │  │ Event()  │  │ patcher    │   │
│  │  ...            │  │ web_search /   │  │          │  │            │   │
│  │                 │  │ message /      │  │          │  │ onBlock-   │   │
│  │  流式响应 ──────│──│→ tool_call ────│──│→ 事件分发 │  │ Reply()    │   │
│  │  (SSE/JSON)     │  │  tool_result ──│──│→ 事件分发 │  │            │   │
│  └────────┬────────┘  └───────┬────────┘  └────┬─────┘  └─────┬──────┘   │
│           │                   │                │              │           │
│           │            ┌──────┴──────┐         │              │           │
│           │            ▼             ▼         ▼              ▼           │
│           │     ┌───────────┐ ┌──────────┐ ┌────────┐ ┌──────────────┐   │
│           │     │  沙箱      │ │  主机     │ │Gateway │ │ 消息渠道 API  │   │
│           │     │  (Docker)  │ │  (直接)   │ │WebSocket│ │ WhatsApp     │   │
│           │     │           │ │          │ │ 广播    │ │ Telegram     │   │
│           │     │ 隔离执行   │ │ 直接执行  │ │→Web UI │ │ Discord ...  │   │
│           │     └───────────┘ └──────────┘ └────────┘ └──────────────┘   │
│           │                                    │              │           │
│           └────────────────────────────────────┴──────────────┘           │
│                              最终回复送达用户                               │
└────────────────────────────────────────────────────────────────────────────┘
```

**Session 如何连接各组件**:

| 组件 | 与 Session 的关系 |
| ---- | ---- |
| LLM API | `AgentSession.prompt()` 将 systemPrompt + messages + tools 发给 LLM；流式响应通过 `subscribe` 回到 Session |
| 工具系统 | LLM 返回 `tool_call` → SDK 在 Session 内部自动执行工具 → 结果通过 `SessionManager` 写回磁盘 → 再次发给 LLM |
| 沙箱 (Docker) | exec/read/write 等工具在沙箱内执行；沙箱是工具的执行环境，不直接与 Session 交互 |
| EventHandler | `AgentSession.subscribe(handler)` 注册；Session 将 LLM 事件单向推送给 EventHandler |
| 全局事件总线 | EventHandler 通过 `emitAgentEvent()` 将事件推到总线 → Gateway 监听 → WebSocket 广播给 Web UI |
| ReplyDispatcher | EventHandler 通过 `onBlockReply()` 回调 → 分块文本投递到消息渠道（WhatsApp/Telegram 等） |
| SessionManager | 被注入到 AgentSession；所有新消息（user/assistant/tool_result）通过 SM 追加到 JSONL 文件 |

### 1.3 两层对比

| 维度 | SessionManager | AgentSession |
| ---- | ---- | ---- |
| 管理的数据 | 磁盘上的 JSONL 转录文件 | 内存中的消息列表 |
| 核心操作 | 读取/追加/分支/压缩 | prompt/subscribe/abort/steer |
| 生命周期 | 跨运行持久（文件长存） | 单次运行（prompt 开始到结束） |
| 并发安全 | 文件写锁 + 引用计数 | 双层队列保证串行 |
| 与 LLM 的关系 | 不直接通信 | 负责所有 LLM 通信 |
| 与工具的关系 | 持久化工具调用/结果 | 在内部自动执行工具 |
| 来源 | 外部 SDK | 外部 SDK |

### 1.4 协作数据流

一条用户消息经过会话系统的完整路径：

```text
用户消息到达
    │
    ▼
① sessionKey 路由 → sessions.json → 找到 sessionId + sessionFile
    │
    ▼
② acquireSessionWriteLock(sessionFile)  ← 获取文件锁
    │
    ▼
③ SessionManager.open(sessionFile)      ← 解析 JSONL，重建消息树
    │
    ▼
④ guardSessionManager(sm)               ← 安装工具结果守卫
    │
    ▼
⑤ prepareSessionManagerForRun()         ← 修复初始化怪癖
    │
    ▼
⑥ createAgentSession({ sessionManager, model, tools, ... })
    │  → SM 的消息历史加载到 AgentSession.messages
    │
    ▼
⑦ sanitize → validate → limit          ← 历史消息处理管线
    │
    ▼
⑧ session.subscribe(eventHandler)       ← 注册事件监听
    │
    ▼
⑨ session.prompt(userMessage)           ← 发送给 LLM
    │
    │  ┌─── LLM 流式交互循环 ───────────────────────────────┐
    │  │  text_delta → EventHandler → onPartialReply/emit    │
    │  │  tool_call  → SDK 执行工具 → 结果写回 SM → LLM 继续 │
    │  │  message_end → assistantTexts[] 收集                 │
    │  └─────────────────────────────────────────────────────┘
    │
    ▼
⑩ session.dispose() + lock.release()    ← 清理
    │
    ▼
返回 { payloads, meta } → 投递给用户
```

**关键节点**:

- **①-⑤** 由持久化层（SessionManager）主导
- **⑥** 是两层的绑定点（`createAgentSession` 注入 SM）
- **⑧-⑨** 由交互层（AgentSession）主导
- **⑦** 是两层的共享关注点（历史消息处理）

### 1.5 文档结构说明

本文档按两层架构组织：

- **Part A (§三 - §八)**: SessionManager 的持久化细节——存储格式、生命周期、锁、历史清洗、缓存
- **Part B (§九 - §十三)**: AgentSession 的交互细节——结构、事件分发、流式缓冲、消息投递

§一和§二是两层共享的全局概念。

---

## 二、三层标识体系

会话系统使用三层标识，理解它们的关系是理解会话管理的基础：

```text
sessionKey (路由键)
    │
    │  sessions.json 查找
    ▼
SessionEntry (会话条目)
    │
    │  路径解析
    ▼
sessionFile (JSONL 转录文件)
```

| 标识 | 格式 | 用途 | 示例 |
| ---- | ---- | ---- | ---- |
| `sessionKey` | `agent:<agentId>:<channel>:<chatType>:<userId>` | 路由消息到正确的会话 | `agent:default:telegram:dm:12345` |
| `sessionId` | UUID v4 | 唯一标识一个对话转录 | `a1b2c3d4-e5f6-...` |
| `sessionFile` | 文件路径 | JSONL 文件的磁盘位置 | `~/.openclaw/agents/default/sessions/a1b2c3d4.jsonl` |

**关键区别**:

- 一个 `sessionKey` 在其生命周期内可以指向**多个** `sessionId`（每次 `/new` 或 `/reset` 生成新 ID）
- `sessionId` 与 `sessionFile` 通常是一对一关系，但 `SessionEntry.sessionFile` 可以覆盖默认路径
- Telegram 话题线程会追加后缀：`<sessionId>-topic-<threadId>.jsonl`

**路径解析逻辑**:

```typescript
// src/config/sessions/paths.ts
function resolveSessionFilePath(sessionId, entry?, opts?): string {
  // 优先使用 SessionEntry 中的自定义路径
  const candidate = entry?.sessionFile?.trim();
  if (candidate) return candidate;
  // 否则使用默认路径
  return resolveSessionTranscriptPath(sessionId, opts?.agentId);
  // → ~/.openclaw/agents/<agentId>/sessions/<sessionId>.jsonl
}
```

---

## 三、会话存储结构

### 3.1 Session Store (sessions.json)

Session Store 是一个 JSON 文件，存储所有 `sessionKey → SessionEntry` 的映射关系。

> 路径: `~/.openclaw/agents/<agentId>/sessions.json`

```json
{
  "agent:default:telegram:dm:12345": {
    "sessionId": "a1b2c3d4-e5f6-...",
    "updatedAt": 1706000000000,
    "chatType": "dm",
    "channel": "telegram",
    "model": "claude-sonnet-4-20250514",
    "modelProvider": "anthropic",
    "compactionCount": 3,
    "label": "我的项目讨论"
  },
  "agent:default:main": {
    "sessionId": "f7e8d9c0-...",
    "updatedAt": 1706000001000,
    "chatType": "dm"
  }
}
```

**SessionEntry 关键字段**:

| 字段 | 说明 |
| ---- | ---- |
| `sessionId` | 当前对话的转录 ID |
| `updatedAt` | 最后更新时间戳 |
| `sessionFile?` | 自定义转录文件路径（覆盖默认） |
| `spawnedBy?` | 父会话键（子 Agent 继承策略） |
| `chatType?` | 聊天类型 (`dm` / `group` / `group-dm`) |
| `thinkingLevel?` | 会话级思考级别覆盖 |
| `modelOverride?` | 会话级模型覆盖 |
| `providerOverride?` | 会话级提供商覆盖 |
| `authProfileOverride?` | 会话级认证配置覆盖 |
| `compactionCount?` | 已发生的压缩次数 |
| `label?` | 用户自定义会话标签 |
| `queueMode?` | 消息排队模式 (`steer` / `followup` / `collect` / ...) |
| `inputTokens?` / `outputTokens?` | Token 使用统计 |
| `skillsSnapshot?` | 技能快照（避免重复扫描） |

**Session Store 自身也有缓存**:

- TTL: 45 秒（环境变量 `OPENCLAW_SESSION_STORE_CACHE_TTL_MS` 可调）
- 缓存键: 文件路径
- 失效条件: TTL 过期或文件 mtime 变化

---

### 3.2 JSONL 转录文件

每个对话的完整历史记录存储在一个 JSONL（JSON Lines）文件中，每一行是一个独立的 JSON 对象。

> 路径: `~/.openclaw/agents/<agentId>/sessions/<sessionId>.jsonl`

**文件结构**:

```text
┌──────────────────────────────────────────────────────────────────┐
│  第 1 行: 会话头                                                  │
│  { "type": "session", "id": "abc-123", "cwd": "/home/user",     │
│    "timestamp": 1706000000, "parentSession": null }              │
├──────────────────────────────────────────────────────────────────┤
│  第 2 行: 消息条目 (user)                                         │
│  { "id": "e1", "parentId": null, "type": "message",             │
│    "message": { "role": "user", "content": [...] } }            │
├──────────────────────────────────────────────────────────────────┤
│  第 3 行: 消息条目 (assistant)                                    │
│  { "id": "e2", "parentId": "e1", "type": "message",             │
│    "message": { "role": "assistant", "content": [...] } }       │
├──────────────────────────────────────────────────────────────────┤
│  第 4 行: 消息条目 (tool_result)                                  │
│  { "id": "e3", "parentId": "e2", "type": "message",             │
│    "message": { "role": "tool_result", "tool_call_id": "tc1",   │
│    "content": "文件内容..." } }                                   │
├──────────────────────────────────────────────────────────────────┤
│  ...                                                              │
├──────────────────────────────────────────────────────────────────┤
│  压缩摘要行 (如发生压缩)                                          │
│  { "id": "c1", "parentId": "e5", "type": "compaction",          │
│    "summary": "对话摘要..." }                                     │
├──────────────────────────────────────────────────────────────────┤
│  分支摘要行 (如发生分支导航)                                      │
│  { "id": "b1", "parentId": "e3", "type": "branch_summary",     │
│    "summary": "分支上下文..." }                                   │
└──────────────────────────────────────────────────────────────────┘
```

**条目类型**:

| type | 说明 | 进入模型上下文? |
| ---- | ---- | ---- |
| `session` | 文件头（仅第 1 行） | 否 |
| `message` | 对话消息（user / assistant / tool_result） | 是 |
| `custom_message` | 扩展注入的消息 | 是 |
| `custom` | 扩展状态数据 | 否 |
| `compaction` | 压缩后的摘要 | 是 (作为上下文) |
| `branch_summary` | 分支导航时的摘要 | 是 (作为上下文) |

**树结构**:

JSONL 不是简单的线性列表，而是通过 `id` + `parentId` 组成一棵**消息树**：

```text
session_header (root)
    │
    ├── e1 (user: "你好")
    │   └── e2 (assistant: "你好!有什么...")
    │       └── e3 (user: "帮我看看代码")
    │           └── e4 (assistant: "好的" + tool_call)
    │               └── e5 (tool_result: "文件内容")
    │                   └── e6 (assistant: "这个文件...")
    │
    └── e7 (user: "换个话题")     ← 分支：从 root 开始新对话
        └── e8 (assistant: "好的...")
```

`SessionManager` 通过 `leafId` 追踪当前活跃分支的末端节点，从叶节点向根回溯可以重建当前对话的完整消息列表。

---

## 四、会话生命周期

### 4.1 创建

```text
新消息到达 (sessionKey 无对应 SessionEntry)
    │
    ▼
生成 sessionId (crypto.randomUUID())
    │
    ▼
创建 SessionEntry 写入 sessions.json
    │
    ▼
SessionManager.open(sessionFile)
    → 创建 JSONL 文件
    → 写入 session header (第 1 行)
```

**何时创建新会话**:

- 首次收到某个 `sessionKey` 的消息
- 用户发送 `/new` 或 `/reset` 命令
- 子 Agent 被 `sessions_spawn` 工具创建

---

### 4.2 打开与准备

每次 Agent 运行前，会话经过以下准备步骤：

```text
┌──────────────────────────────────────────────────────────────────┐
│  Step 1: 预热文件缓存                                            │
│  prewarmSessionFile(sessionFile)                                 │
│  → 读取文件前 4KB，让 OS 将文件页面加载到缓存                    │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 2: 获取写锁                                                │
│  acquireSessionWriteLock({ sessionFile })                        │
│  → 创建 <sessionFile>.lock 文件                                  │
│  → 防止并发写入冲突                                              │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 3: 打开 SessionManager                                     │
│  sessionManager = guardSessionManager(                           │
│    SessionManager.open(sessionFile),                             │
│    { agentId, sessionKey, allowSyntheticToolResults }            │
│  )                                                               │
│  → 解析 JSONL 文件                                               │
│  → 重建消息树                                                    │
│  → 安装工具结果守卫                                              │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 4: 准备会话状态                                             │
│  prepareSessionManagerForRun({ sessionManager, ... })            │
│                                                                  │
│  新文件?                                                          │
│  → 设置 sessionId 和 cwd                                         │
│                                                                  │
│  已有文件但没有 assistant 消息?                                    │
│  → 重置文件 (清空 byId map，重置 leafId)                          │
│  → 修复持久化怪癖：SessionManager 的 flushed 标志                │
│    可能阻止第一条 user 消息被持久化                               │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 5: 创建 AgentSession                                       │
│  createAgentSession({                                            │
│    sessionManager,                                               │
│    model, authStorage, tools, ...                                │
│  })                                                              │
│  → AgentSession 绑定 SessionManager                              │
│  → 加载历史消息到内存                                             │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 6: 处理历史消息                                             │
│  → sanitizeSessionHistory()  清洗                                │
│  → validateGeminiTurns()     Gemini 校验                         │
│  → validateAnthropicTurns()  Anthropic 校验                      │
│  → limitHistoryTurns()       历史限制                            │
│  → agent.replaceMessages()   替换内存消息                        │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 7: 处理孤儿消息（分支）                                     │
│  如果叶节点是 user 消息（无后续 assistant 回复）                  │
│  → sessionManager.branch(parentId)  回退到父节点                 │
│  或 sessionManager.resetLeaf()      重置到根                     │
└──────────────────────────────────────────────────────────────────┘
```

> 源文件: `src/agents/pi-embedded-runner/run/attempt.ts`, `src/agents/pi-embedded-runner/session-manager-init.ts`

**`prepareSessionManagerForRun` 解决的怪癖**: SessionManager（SDK 内部）有一个 `flushed` 标志。如果 JSONL 文件已存在但没有 assistant 消息（比如上次运行中断了），SessionManager 会认为"已 flush"，导致后续的第一条 user 消息不会被写入磁盘。`prepareSessionManagerForRun` 通过重置内部状态来修复这个问题。

---

### 4.3 分支与树结构

JSONL 文件使用 `id` / `parentId` 形成树结构，支持对话分支：

```text
情景: 用户发消息 → Runtime 崩溃 → 用户再发消息

时间线:
  t1: user: "帮我看代码"     (id=e1, parentId=null)
  t2: [Runtime 崩溃，无 assistant 回复]
  t3: user: "换个问题"       (id=e2, parentId=null) ← 新分支

SessionManager 处理:
  1. 打开文件，发现叶节点 e1 是 user 消息
  2. 调用 branch(null) 回退到根
  3. e2 作为新分支的起点追加
```

**分支触发条件**:

```typescript
// attempt.ts: 孤儿消息检测
const leafEntry = sessionManager.getLeafEntry();
if (leafEntry?.type === "message" && leafEntry.message.role === "user") {
  // 叶节点是 user 消息 → 说明上次没有 assistant 回复
  if (leafEntry.parentId) {
    sessionManager.branch(leafEntry.parentId);  // 回退到父节点
  } else {
    sessionManager.resetLeaf();                 // 回退到根
  }
}
```

**分支的意义**:

- 防止连续的 user 消息（违反 LLM 的 turn 交替要求）
- 保留历史记录（孤儿消息不会被删除，只是从活跃路径移除）
- 支持未来的对话树导航功能

---

### 4.4 压缩与清理

当对话历史过长时，Runtime 通过压缩（Compaction）来管理上下文大小：

```text
压缩前:
  [user] [assistant] [user] [assistant] ... [user] [assistant]
         ← 旧消息 (被压缩) →              ← 保留的消息 →

压缩后:
  [compaction: "之前讨论了 X、Y、Z..."] [user] [assistant]
```

压缩由 `compactEmbeddedPiSessionDirect` 执行，它：

1. 使用与主运行相同的历史处理管线（清洗 → 校验 → 限制）
2. 调用 `session.compact(customInstructions)` 让 LLM 生成摘要
3. 摘要作为 `compaction` 类型条目追加到 JSONL

> 压缩的触发条件和四层防线在 [AGENT-RUNTIME-SYSTEM-PROMPT.md §6](./AGENT-RUNTIME-SYSTEM-PROMPT.md) 中有详细说明。

---

## 五、并发控制

会话管理面临的核心并发问题：同一个用户可能快速连发多条消息，或者同一会话被多个渠道同时访问。

### 5.1 文件写锁

> 源文件: `src/agents/session-write-lock.ts`

```text
┌─────────────────────────────────────────────────────────────────┐
│  文件写锁机制                                                    │
│                                                                 │
│  锁文件: <sessionFile>.lock                                      │
│  内容:   { "pid": 12345, "createdAt": 1706000000 }             │
│                                                                 │
│  获取锁:                                                         │
│  fs.open(lockPath, "wx")  ← 原子性独占创建                       │
│    ├── 成功 → 持有锁                                             │
│    └── 失败 (EEXIST) → 检查锁状态                                │
│         ├── 进程已死 (kill(pid, 0) 失败) → 回收过期锁            │
│         ├── 锁过期 (> 30min) → 强制回收                          │
│         └── 锁有效 → 重试直到超时 (默认 10s)                     │
│                                                                 │
│  引用计数:                                                       │
│  同一进程内多次 acquire → 计数 +1                                 │
│  release → 计数 -1，最后一次 release 删除锁文件                   │
│                                                                 │
│  信号处理:                                                       │
│  SIGINT/SIGTERM/exit → 自动释放所有锁                            │
└─────────────────────────────────────────────────────────────────┘
```

**设计要点**:

- 使用文件系统原子操作（`open("wx")`）保证跨进程安全
- 引用计数避免同进程内重复锁开销
- 过期锁回收防止死锁（崩溃后锁文件残留）
- 路径规范化处理符号链接（`fs.realpath()`）

---

### 5.2 双层队列

文件锁防止并发写入，双层队列防止并发执行：

```text
消息 1 ──→ ┐
消息 2 ──→ ├──→ sessionLane (会话级队列) ──→ globalLane (全局队列) ──→ 执行
消息 3 ──→ ┘         │                           │
                     │                           │
              同一会话串行              全局并发数限制
```

```typescript
// run.ts: 双层嵌套
return enqueueSession(() =>
  enqueueGlobal(async () => {
    // 实际执行逻辑
  }),
);
```

| 层级 | 粒度 | 作用 |
| ---- | ---- | ---- |
| `sessionLane` | 按 sessionKey 隔离 | 同一会话的消息严格串行（防止消息历史冲突） |
| `globalLane` | 全局共享 | 控制总并发数（防止 CPU/内存/API 过载） |

---

## 六、历史消息处理管线

### 6.1 处理流程总览

每次 Agent 运行和压缩操作前，历史消息都要经过完整的处理管线：

```text
SessionManager.messages (原始磁盘消息)
        │
        ▼
┌───────────────────────┐
│  ① sanitizeSession-   │  图片清洗、工具配对修复、
│     History()         │  Thinking 块规范化、
│                       │  Google 轮序修复
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│  ② validateGemini-    │  合并连续 assistant 消息
│     Turns()           │  (仅 Google 模型)
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│  ③ validateAnthropic- │  合并连续 user 消息
│     Turns()           │  (仅 Anthropic 模型)
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│  ④ limitHistoryTurns()│  截断到最后 N 个 user 轮次
│                       │  (可配置)
└───────────┬───────────┘
            ▼
agent.replaceMessages(limited)  → 替换内存中的消息列表
```

> 源文件: `src/agents/pi-embedded-runner/google.ts`, `src/agents/pi-embedded-helpers/turns.ts`, `src/agents/pi-embedded-runner/history.ts`

---

### 6.2 Transcript Policy（转录策略）

处理管线由 `TranscriptPolicy` 控制，不同 LLM 提供商需要不同的处理策略：

> 源文件: `src/agents/transcript-policy.ts`

```typescript
type TranscriptPolicy = {
  sanitizeMode: "full" | "images-only";        // 清洗范围
  sanitizeToolCallIds: boolean;                 // 是否清洗 tool call ID 格式
  toolCallIdMode?: "strict" | "strict9";        // tool call ID 格式要求
  repairToolUseResultPairing: boolean;          // 修复工具调用/结果配对
  preserveSignatures: boolean;                  // 保留 Antigravity 签名
  sanitizeThoughtSignatures?: object;           // Thinking 签名处理
  normalizeAntigravityThinkingBlocks: boolean;  // 规范化 Antigravity 思考块
  applyGoogleTurnOrdering: boolean;             // Google 轮序修复
  validateGeminiTurns: boolean;                 // Gemini turn 校验
  validateAnthropicTurns: boolean;              // Anthropic turn 校验
  allowSyntheticToolResults: boolean;           // 合成缺失的工具结果
};
```

**各提供商的策略差异**:

| 提供商 | 清洗 | Tool ID | 配对修复 | Turn 校验 | 合成结果 |
| ---- | ---- | ---- | ---- | ---- | ---- |
| Google/Gemini | full | strict | 是 | Gemini | 是 |
| Anthropic | full | — | 是 | Anthropic | 是 |
| OpenAI | images-only | — | 否 | 否 | 否 |
| Mistral | full | strict9 | 否 | 否 | 否 |
| OpenRouter Gemini | full | strict | 是 | Gemini | 是 |

**为什么需要不同策略**: 各 LLM 提供商对消息格式有不同的严格要求。例如 Gemini 不允许连续的 assistant 消息，Anthropic 不允许连续的 user 消息，而 OpenAI 相对宽松。如果不做适配，API 调用会直接报错。

---

### 6.3 清洗 (Sanitize)

> 源文件: `src/agents/pi-embedded-runner/google.ts` — `sanitizeSessionHistory()`

清洗步骤按顺序执行：

| 步骤 | 说明 | 触发条件 |
| ---- | ---- | ---- |
| 图片清洗 | 移除/缩放超限图片，修正格式 | 所有模型 |
| Tool Call ID 清洗 | 将 ID 格式化为提供商要求的格式 | Google, Mistral |
| Thinking 块规范化 | 规范化 Antigravity Claude 的思考块签名 | Antigravity Claude |
| 工具配对修复 | 修复错位或缺失的 tool_result 消息 | Google, Anthropic |
| OpenAI 推理降级 | 处理模型切换后的推理块兼容性 | OpenAI |
| Google 轮序修复 | 如果历史以 assistant 开头，前插一条 bootstrap user 消息 | Google |

**工具配对修复详解**: LLM 可能在 assistant 消息中发起 tool_call，但如果 Runtime 崩溃，对应的 tool_result 可能丢失。修复逻辑会检测未配对的 tool_call 并合成占位 tool_result。

---

### 6.4 Turn 校验

**Gemini Turn 校验** (`validateGeminiTurns`):

```text
问题: [assistant] [assistant] [user]   ← 连续 assistant 违规
修复: [assistant (合并)]       [user]   ← 合并内容数组
```

- 检测连续的 assistant 消息
- 合并它们的 `content` 数组
- 保留最后一个的 `usage`、`stopReason`、`errorMessage`

**Anthropic Turn 校验** (`validateAnthropicTurns`):

```text
问题: [user] [user] [assistant]         ← 连续 user 违规
修复: [user (合并)]  [assistant]         ← 合并内容和时间戳
```

- 检测连续的 user 消息
- 合并它们的内容
- 保留最新的时间戳

> 源文件: `src/agents/pi-embedded-helpers/turns.ts`

---

### 6.5 历史限制

> 源文件: `src/agents/pi-embedded-runner/history.ts`

```typescript
function limitHistoryTurns(messages: AgentMessage[], limit: number | undefined): AgentMessage[]
```

- 从末尾向前计数 user 消息轮次
- 保留最后 N 个 user 轮次及其关联的 assistant/tool 消息
- `limit` 为 `undefined`、0 或负数时不截断

**配置来源**:

```typescript
function getDmHistoryLimitFromSessionKey(sessionKey, config): number | undefined
```

- 按提供商查找: `channels.<provider>.dmHistoryLimit`
- 按用户查找: `channels.<provider>.dms.<userId>.historyLimit`
- 线程后缀会被自动去除（`:thread:123` / `:topic:456`）

---

## 七、会话守卫与工具结果追踪

> 源文件: `src/agents/session-tool-result-guard-wrapper.ts`, `src/agents/session-tool-result-guard.ts`

`guardSessionManager()` 在 `SessionManager` 外面包装了一层守卫，追踪工具调用/结果的配对关系：

```text
┌──────────────────────────────────────────────────────────────┐
│  GuardedSessionManager                                       │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Tool Result Guard                                    │   │
│  │                                                       │   │
│  │  pendingToolCalls: Map<toolCallId, toolName>         │   │
│  │                                                       │   │
│  │  当 assistant 消息包含 tool_call:                     │   │
│  │    → 记录到 pendingToolCalls                          │   │
│  │                                                       │   │
│  │  当 tool_result 消息到达:                             │   │
│  │    → 从 pendingToolCalls 中移除匹配项                 │   │
│  │                                                       │   │
│  │  当新的非工具消息到达且有未匹配的 tool_call:          │   │
│  │    → flush: 合成缺失的 tool_result                    │   │
│  │       (如果 allowSyntheticToolResults = true)         │   │
│  │                                                       │   │
│  │  Hook 集成:                                           │   │
│  │    → tool_result_persist hook                         │   │
│  │    → 允许插件在持久化前转换工具结果                    │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────┐                                       │
│  │  SessionManager   │  ← 底层实际 I/O                      │
│  └──────────────────┘                                       │
└──────────────────────────────────────────────────────────────┘
```

**合成工具结果 (`allowSyntheticToolResults`)**:

当 Runtime 崩溃导致 tool_result 丢失时，Guard 会在下次运行的 flush 阶段自动合成占位结果，防止 LLM 因为缺少工具结果而报错。这个行为根据提供商启用：Google 和 Anthropic 需要严格的 tool_call/tool_result 配对，OpenAI 则不需要。

---

## 八、缓存机制

### 文件缓存预热

> 源文件: `src/agents/pi-embedded-runner/session-manager-cache.ts`

```typescript
function prewarmSessionFile(sessionFile: string): Promise<void>
```

- 在 `SessionManager.open()` 之前调用
- 读取文件前 4KB，将文件页面加载到 OS 页面缓存
- 减少后续 JSONL 解析的磁盘 I/O 延迟

### 访问追踪

```typescript
function trackSessionManagerAccess(sessionFile: string): void
function isSessionManagerCached(sessionFile: string): boolean
```

- 记录每次 SessionManager 访问的时间戳
- TTL: 45 秒（环境变量 `OPENCLAW_SESSION_MANAGER_CACHE_TTL_MS` 可调）
- 不缓存 SessionManager 实例本身，只追踪访问时间（用于决定是否需要重新预热）

### Session Store 缓存

Session Store（`sessions.json`）也有独立的缓存层：

- TTL: 45 秒
- 缓存键: 文件路径（规范化后）
- 失效: TTL 过期 **或** 文件 `mtime` 变化
- 确保频繁的 sessionKey 查找不会每次都读磁盘

---

## Part B: AgentSession（交互层）

## 九、AgentSession 结构与生命周期

### 9.1 结构总览

`AgentSession` 来自 Pi SDK（`@mariozechner/pi-coding-agent`），是 Agent Runtime 与 LLM 交互的核心对象。它封装了会话状态、消息历史和 LLM 通信能力。

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│  AgentSession                                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────── 属性 ─────────────┐  ┌──────────── 方法 ──────────────┐     │
│  │                                │  │                                │     │
│  │  sessionId: string             │  │  prompt(text, opts?):          │     │
│  │    会话唯一标识符               │  │    Promise<void>              │     │
│  │                                │  │    发送用户消息给 LLM，        │     │
│  │  messages: AgentMessage[]      │  │    触发完整的推理+工具调用循环  │     │
│  │    当前会话的全部消息历史       │  │                                │     │
│  │    (user, assistant,           │  │  steer(text): Promise<void>    │     │
│  │     toolResult)                │  │    在 LLM 流式输出过程中        │     │
│  │                                │  │    注入额外的用户消息           │     │
│  │  isStreaming: boolean          │  │                                │     │
│  │    是否正在流式输出             │  │  abort(): Promise<void>        │     │
│  │                                │  │    中止当前 LLM 请求            │     │
│  │  agent: Agent                  │  │                                │     │
│  │    底层 Agent 实例              │  │  compact(instructions?):       │     │
│  │    (可操作系统提示词、          │  │    Promise<CompactResult>      │     │
│  │     消息替换、流式函数)         │  │    压缩旧消息历史为摘要         │     │
│  │                                │  │                                │     │
│  └────────────────────────────────┘  │  subscribe(handler):           │     │
│                                      │    () => void                  │     │
│  ┌──────── agent 子对象 ──────────┐  │    注册事件监听器，              │     │
│  │                                │  │    返回 unsubscribe 函数       │     │
│  │  agent.streamFn                │  │                                │     │
│  │    LLM 流式请求函数             │  │  dispose(): void              │     │
│  │    (可被 wrapper 替换)          │  │    释放会话资源                 │     │
│  │                                │  │                                │     │
│  │  agent.setSystemPrompt(text)   │  └────────────────────────────────┘     │
│  │    设置系统提示词               │                                        │
│  │                                │                                        │
│  │  agent.replaceMessages(msgs)   │                                        │
│  │    替换消息历史                 │                                        │
│  │                                │                                        │
│  └────────────────────────────────┘                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**AgentSession 的四大职责**:

1. **发送消息** (`prompt`) — 将用户输入和工具定义发送给 LLM
2. **接收事件** (`subscribe`) — 监听 LLM 的流式响应
3. **管理历史** (`messages`, `agent.replaceMessages`) — 读取和修改对话历史
4. **控制流程** (`abort`, `steer`, `compact`) — 中止、插入消息、压缩

**与 SessionManager 的关系回顾**:

- `SessionManager` 负责**磁盘持久化**（.jsonl 文件的读写、分支管理）
- `AgentSession` 负责**运行时交互**（内存中的消息、LLM 通信、事件分发）
- 二者在 `createAgentSession` 中关联：SessionManager 提供历史消息，AgentSession 在运行中产生的新消息通过 SessionManager 写回磁盘

---

### 9.2 创建与配置

```typescript
// src/agents/pi-embedded-runner/run/attempt.ts
const { session } = await createAgentSession({
  cwd:             effectiveWorkspace,  // 工作目录
  agentDir:        agentDir,            // Agent 配置目录
  authStorage:     authStorage,         // API Key 存储
  modelRegistry:   modelRegistry,       // 模型注册表
  model:           model,               // 选定的 LLM 模型
  thinkingLevel:   thinkingLevel,       // 思考深度
  tools:           builtInTools,        // SDK 内置工具
  customTools:     allCustomTools,      // 自定义工具
  sessionManager:  sessionManager,      // 会话持久化管理
  settingsManager: settingsManager,     // 设置管理
});
```

创建后的配置步骤：

```text
createAgentSession()          → session 对象已加载磁盘历史到 session.messages
    │
    ├── applySystemPromptOverrideToSession()  → 设置系统提示词
    ├── session.agent.streamFn = streamSimple  → 固定流式函数
    ├── applyExtraParamsToAgent()              → 设置额外参数
    └── sanitizeSessionHistory()               → 清理/验证消息历史 (见 §六)
```

---

### 9.3 生命周期

```text
┌───────────────────────────────────────────────────────────────────────┐
│  AgentSession 生命周期                                                 │
│                                                                       │
│  1. 创建  createAgentSession()                                        │
│     → 加载模型、工具、历史消息、绑定 SessionManager                     │
│                                                                       │
│  2. 配置  applySystemPrompt + sanitizeHistory                         │
│     → 设置系统提示词、清理历史消息                                      │
│                                                                       │
│  3. 订阅  subscribeEmbeddedPiSession(session)                         │
│     → session.subscribe(eventHandler) → 注册事件监听                    │
│                                                                       │
│  4. 执行  session.prompt(userMessage, { images })                     │
│     → LLM 开始流式输出                                                 │
│     → 事件通过 subscribe 回调流入 EventHandler                          │
│     → 工具调用在 SDK 内部自动执行                                       │
│     → 循环直到 LLM 输出完毕                                            │
│                                                                       │
│  5. 等待  waitForCompactionRetry()                                    │
│     → 等待可能的压缩重试完成                                            │
│                                                                       │
│  6. 清理  unsubscribe() + session.dispose() + sessionLock.release()   │
│     → 取消订阅、释放资源、释放写锁                                      │
└───────────────────────────────────────────────────────────────────────┘
```

**关键设计要点**:

- **工具执行在 SDK 内部自动完成**: 当 LLM 返回 `tool_call` 时，SDK 自动查找已注册的 `ToolDefinition`，调用其 `execute` 方法，将结果注入消息历史，然后再次调用 LLM。这个循环对 Runtime 是透明的——Runtime 只通过事件监听得知发生了什么。
- **事件是单向的**: Session → EventHandler 是单向通知，EventHandler 不能通过返回值影响 Session 的行为。如果需要干预（如中止），要通过 `session.abort()` 或 `session.steer()` 另行调用。
- **steer 的作用**: 在 LLM 正在流式输出时，可以通过 `session.steer(text)` 注入一条新的用户消息。常见场景是消息队列中有新的用户消息到达，需要"插队"告知 LLM。

---

### 9.4 LLM 交互工程化细节

`session.prompt()` 看起来只是一行调用，但其背后隐藏了大量工程化处理。本节深入解析 prompt 发出后的完整机制。

#### 9.4.1 streamFn — 流式请求函数链

`AgentSession` 不直接调用 LLM API，而是通过可替换的 `agent.streamFn` 函数。Runtime 在创建 Session 后会**包装**这个函数：

```text
streamSimple (SDK 基础实现，抹平各 LLM 提供商差异)
    │
    ▼  createStreamFnWithExtraParams() 包装
额外参数层 (temperature, maxTokens, cacheRetention)
    │
    ▼  cacheTrace.wrapStreamFn() 包装 (如启用)
缓存追踪层 (记录请求/响应用于调试)
    │
    ▼  anthropicPayloadLogger.wrapStreamFn() 包装 (如启用)
日志层 (记录 Anthropic 原始请求载荷)
    │
    = agent.streamFn (最终使用的函数)
```

> 源文件: `src/agents/pi-embedded-runner/run/attempt.ts:505-528`

**`streamSimple` 的角色**: 来自 `@mariozechner/pi-ai` SDK，它是 LLM 提供商的**统一抽象层**。无论目标是 Anthropic Messages API、OpenAI Chat Completions 还是 Google Gemini，`streamSimple` 都将它们统一为相同的流式接口。OpenClaw Runtime 不需要关心底层 API 格式差异。

**可配置的流式参数**:

| 参数 | 来源 | 说明 |
| ---- | ---- | ---- |
| `temperature` | `agents.defaults.models[provider/model].params` | 控制输出随机性 |
| `maxTokens` | 同上 | 最大输出 token 数 |
| `cacheRetention` | 同上 (`cacheControlTtl` 映射) | Anthropic 缓存策略: `"none"` / `"short"` / `"long"` |

```typescript
// src/agents/pi-embedded-runner/extra-params.ts (简化)
function createStreamFnWithExtraParams(baseStreamFn, extraParams, provider) {
  // 提取 temperature, maxTokens, cacheRetention
  const underlying = baseStreamFn ?? streamSimple;
  return (model, context, options) =>
    underlying(model, context, { ...streamParams, ...options });
}
```

#### 9.4.2 prompt() 的内部循环

`session.prompt(text)` 调用后，SDK 内部进入一个**自动循环**：

```text
┌──────────────────────────────────────────────────────────────────┐
│  session.prompt("帮我读取 package.json")                          │
│                                                                  │
│  Step 1: 将 user message 追加到消息历史                           │
│          → SessionManager.appendMessage(userMsg)                 │
│          → 写入 JSONL 文件                                        │
│                                                                  │
│  Step 2: 调用 streamFn(model, context, options)                  │
│          context = {                                             │
│            systemPrompt,    ← 系统提示词                          │
│            messages,        ← 完整消息历史                        │
│            tools,           ← 工具定义列表                        │
│            thinkingLevel,   ← 思考级别                            │
│          }                                                       │
│          → 通过 streamFn 发送给 LLM API                          │
│          → 开始接收 SSE/JSON 流                                   │
│                                                                  │
│  Step 3: 解析流式响应                                             │
│          → 文本片段 → 触发 message_update 事件                    │
│          → tool_call → 进入 Step 4                               │
│          → 无 tool_call → 进入 Step 5                            │
│                                                                  │
│  Step 4: 自动工具执行 (SDK 内部)                                  │
│          ┌────────────────────────────────────────┐              │
│          │ 查找已注册的 ToolDefinition             │              │
│          │ → tool.execute(toolCallId, args)        │              │
│          │ → 工具在沙箱或主机中执行                 │              │
│          │ → 结果作为 tool_result 追加到消息历史    │              │
│          │ → SessionManager.appendMessage(result)  │              │
│          │ → 返回 Step 2 (带着新历史再次调用 LLM)  │              │
│          └────────────────────────────────────────┘              │
│          ↑                                                       │
│          └─── 循环直到 LLM 不再请求工具                           │
│                                                                  │
│  Step 5: 完成                                                    │
│          → 最后的 assistant message 追加到历史                    │
│          → SessionManager.appendMessage(assistantMsg)            │
│          → 触发 agent_end 事件                                    │
│          → prompt() Promise resolve                              │
└──────────────────────────────────────────────────────────────────┘
```

**要点**: Runtime 不参与 Step 2-4 的循环控制——这完全由 SDK 自动完成。Runtime 只在 Step 1 之前准备环境，在 Step 5 之后收集结果。中间的工具调用循环对 Runtime 是**透明的**，只能通过 `subscribe` 事件观察。

#### 9.4.3 提供商抽象与转录策略

不同 LLM 提供商对消息格式有严格且不同的要求。Runtime 通过 `TranscriptPolicy` 在**调用前**清洗历史，通过 `streamSimple` 在**调用时**抹平 API 差异：

```text
                    调用前 (Runtime 负责)              调用时 (SDK 负责)
                    ─────────────────────           ─────────────────
历史消息 ──→ TranscriptPolicy 清洗 ──→ streamFn ──→ LLM API
                    │                                     │
                    │  Google:  tool ID 清洗               │  → Gemini REST
                    │          turn 排序修复               │
                    │          连续 assistant 合并         │
                    │                                     │  → Anthropic
                    │  Anthropic: 工具配对修复             │     Messages API
                    │            连续 user 合并            │
                    │                                     │  → OpenAI Chat
                    │  OpenAI: 仅图片清洗                  │     Completions
                    │          (最宽松)                    │
                    │                                     │  → 其他提供商
                    │  Mistral: strict9 tool ID           │
```

这种**分层职责**的设计意味着：

- 如果要支持新的 LLM 提供商，只需在 `TranscriptPolicy` 中添加清洗规则，`streamSimple` 中添加 API 适配
- 现有代码不需要修改

#### 9.4.4 超时与中止

LLM 调用可能因为网络问题或长时间推理而挂起。Runtime 实现了多层保护：

```text
┌──────────────────────────────────────────────────────────────────┐
│  超时控制 (src/agents/pi-embedded-runner/run/attempt.ts)          │
│                                                                  │
│  prompt() 开始                                                    │
│      │                                                           │
│      ├── abortTimer = setTimeout(timeoutMs)                      │
│      │   │  默认 600s (可配置)                                    │
│      │   │                                                       │
│      │   │  触发时:                                               │
│      │   ├── aborted = true, timedOut = true                     │
│      │   ├── runAbortController.abort()  → AbortSignal 传播      │
│      │   ├── activeSession.abort()       → SDK 取消流式请求       │
│      │   └── 10s 后检查 isStreaming                               │
│      │       → 如仍在流式 → 记录警告日志                          │
│      │                                                           │
│      ├── 外部 AbortSignal (可选)                                  │
│      │   → params.abortSignal 监听                               │
│      │   → 触发时同样调用 abortRun()                              │
│      │                                                           │
│      └── abortable(promise) 包装                                 │
│          → 将 Promise 变为可中止                                  │
│          → AbortSignal 触发时 reject                              │
└──────────────────────────────────────────────────────────────────┘
```

**中止后的行为**: `session.abort()` 调用后，SDK 会尝试取消正在进行的 HTTP 请求。但流式连接的取消不是瞬时的——可能需要等待 TCP 层面的超时。因此有 10 秒后的二次检查，如果流仍然活跃则记录警告。

#### 9.4.5 Steer — 运行中消息注入

`steer()` 是 AgentSession 的一个特殊能力：在 LLM **正在流式输出时**注入新的用户消息。

```text
场景: 用户快速连发两条消息

时间线:
  t0: 用户发送 "帮我看看 bug"    → session.prompt("帮我看看 bug")
  t1: LLM 正在流式回复中...
  t2: 用户发送 "算了，先帮我改 README"  → session.steer("算了，先帮我改 README")
  t3: LLM 看到新消息，调整回复方向
```

**触发条件** (全部满足才能 steer):

1. 当前会话有活跃运行 (`ACTIVE_EMBEDDED_RUNS.has(sessionId)`)
2. 正在流式输出 (`handle.isStreaming() === true`)
3. 不在压缩中 (`!handle.isCompacting()`)
4. 队列模式为 `"steer"` 或 `"steer-backlog"`

**与队列模式的关系**:

| 模式 | 行为 |
| ---- | ---- |
| `steer` | 注入消息到活跃流；如不在流式中则排队等下次 |
| `steer-backlog` | 注入消息到活跃流；如不在流式中则作为 followup 排队 |
| `followup` | 不 steer，排队等当前运行结束后作为新运行执行 |
| `collect` | 收集多条消息，合并后作为一次发送 |
| `interrupt` | 中止当前运行，用新消息重新开始 |

> 源文件: `src/agents/pi-embedded-runner/runs.ts`, `src/agents/pi-embedded-runner/run/attempt.ts:643-651`

---

## 十、事件订阅与分发

### 10.1 订阅机制

AgentSession 的 `subscribe` 方法是 Session 与 EventHandler 之间的桥梁，采用**观察者模式**：

```typescript
// src/agents/pi-embedded-subscribe.ts
const unsubscribe = params.session.subscribe(
  createEmbeddedPiSessionEventHandler(ctx)
);
```

```text
AgentSession 内部                    EventHandler
┌────────────┐                      ┌────────────────────┐
│            │   subscribe(handler)  │                    │
│  LLM 请求  │ ──────────────────→  │  注册 handler 函数  │
│            │                      └────────────────────┘
│            │
│  收到流式   │   handler(event)     ┌────────────────────┐
│  响应片段   │ ──────────────────→  │  事件分发 (switch)   │
│            │                      │  → handleXxx()     │
│  收到工具   │   handler(event)     │                    │
│  调用请求   │ ──────────────────→  │  → handleXxx()     │
│            │                      │                    │
│  ... 持续   │   handler(event)     │                    │
│  流式输出   │ ──────────────────→  │  ...               │
│            │                      │                    │
│  结束      │   handler(event)     │  → handleAgentEnd()│
│            │ ──────────────────→  │                    │
└────────────┘                      └────────────────────┘
```

**工作原理**:

1. `session.prompt()` 被调用后，SDK 内部发起 LLM 请求并解析流式响应
2. 每当收到一个事件（文字片段、工具调用、消息结束等），Session 调用已注册的 handler 回调
3. `subscribe` 返回 `unsubscribe` 函数，调用后不再接收事件

---

### 10.2 EventHandler 分发逻辑

`createEmbeddedPiSessionEventHandler` 创建的 handler 是一个 switch 分发器，将不同类型的事件路由到专门的处理函数：

> 源文件: `src/agents/pi-embedded-subscribe.handlers.ts`

```typescript
export function createEmbeddedPiSessionEventHandler(ctx) {
  return (evt: EmbeddedPiSubscribeEvent) => {
    switch (evt.type) {
      case "message_start":         handleMessageStart(ctx, evt);        return;
      case "message_update":        handleMessageUpdate(ctx, evt);       return;
      case "message_end":           handleMessageEnd(ctx, evt);          return;
      case "tool_execution_start":  handleToolExecutionStart(ctx, evt);  return;
      case "tool_execution_update": handleToolExecutionUpdate(ctx, evt); return;
      case "tool_execution_end":    handleToolExecutionEnd(ctx, evt);    return;
      case "agent_start":           handleAgentStart(ctx);               return;
      case "auto_compaction_start": handleAutoCompactionStart(ctx);      return;
      case "auto_compaction_end":   handleAutoCompactionEnd(ctx, evt);   return;
      case "agent_end":             handleAgentEnd(ctx);                 return;
    }
  };
}
```

**事件类型与处理**:

| 事件类型 | 处理函数 | 来源文件 | 作用 |
| ---- | ---- | ---- | ---- |
| `agent_start` | `handleAgentStart` | `handlers.lifecycle.ts` | Agent 开始运行，发射生命周期事件 |
| `message_start` | `handleMessageStart` | `handlers.messages.ts` | LLM 开始输出新消息 |
| `message_update` | `handleMessageUpdate` | `handlers.messages.ts` | LLM 输出文本片段 (streaming delta) |
| `message_end` | `handleMessageEnd` | `handlers.messages.ts` | LLM 完成一条消息，收集最终文本 |
| `tool_execution_start` | `handleToolExecutionStart` | `handlers.tools.ts` | LLM 请求调用工具，记录工具名和参数 |
| `tool_execution_update` | `handleToolExecutionUpdate` | `handlers.tools.ts` | 工具执行进度更新 (如 exec 输出) |
| `tool_execution_end` | `handleToolExecutionEnd` | `handlers.tools.ts` | 工具执行完毕，处理结果并通知回调 |
| `auto_compaction_start` | `handleAutoCompactionStart` | `handlers.lifecycle.ts` | 自动压缩开始 |
| `auto_compaction_end` | `handleAutoCompactionEnd` | `handlers.lifecycle.ts` | 自动压缩结束 (可能需要重试) |
| `agent_end` | `handleAgentEnd` | `handlers.lifecycle.ts` | Agent 运行结束，刷新缓冲区 |

---

### 10.3 共享上下文 (ctx)

所有 handler 共享一个 `EmbeddedPiSubscribeContext` 对象，连接事件处理和外部回调：

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  EmbeddedPiSubscribeContext (ctx)                                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  params (外部回调)                    state (共享状态)                   │
│  ├── onPartialReply()                 ├── assistantTexts[]              │
│  │   → 流式文本片段通知               │   → 收集的回复文本              │
│  │                                    │                                 │
│  ├── onBlockReply()                   ├── toolMetas[]                   │
│  │   → 分块回复通知                   │   → 工具调用元信息              │
│  │                                    │                                 │
│  ├── onToolResult()                   ├── deltaBuffer                   │
│  │   → 工具结果通知                   │   → 流式文本累积缓冲            │
│  │                                    │                                 │
│  ├── onReasoningStream()              ├── blockBuffer                   │
│  │   → 推理过程流通知                 │   → 分块回复缓冲区              │
│  │                                    │                                 │
│  ├── onAgentEvent()                   ├── compactionInFlight            │
│  │   → 通用事件通知                   │   → 是否正在压缩                │
│  │                                    │                                 │
│  └── onAssistantMessageStart()        └── messagingToolSentTexts[]      │
│      → 新消息开始通知                     → 已发送的消息列表             │
│                                                                         │
│  工具函数                                                                │
│  ├── stripBlockTags()     → 去除 <think>/<final> 等标签                 │
│  ├── emitBlockChunk()     → 发射一块回复文本                            │
│  ├── emitToolSummary()    → 发射工具调用摘要                            │
│  └── resetForCompactionRetry() → 压缩重试时重置状态                     │
└─────────────────────────────────────────────────────────────────────────┘
```

> `EmbeddedPiSubscribeContext` 和 `EmbeddedPiSubscribeState` 的完整类型定义见 [API 参考手册 §三](./AGENT-RUNTIME-API-REFERENCE.md)。

---

### 10.4 事件类型判定

LLM API 返回的是 SSE 或流式 JSON 数据。Pi SDK 在内部解析原始流数据，将它们转化为统一的事件对象后投递给 `subscribe` 回调。这一转化对 Runtime **完全透明**。

```text
LLM API (Anthropic/OpenAI/Google/...)
    │  SSE stream / chunked JSON (各厂商格式不同)
    ▼
Pi SDK 流式解析层
    │  统一化处理:
    │  ├── Anthropic: content_block_start → text_delta → content_block_stop
    │  ├── OpenAI:    choices[0].delta.content / choices[0].delta.tool_calls
    │  ├── Google:    candidates[0].content.parts[*]
    │  └── ... 其他提供商
    ▼
统一事件 (AgentEvent)
    ├── { type: "agent_start" }
    ├── { type: "message_start", message }
    ├── { type: "message_update", assistantMessageEvent: { type: "text_delta", delta } }
    ├── { type: "message_end", message }
    ├── { type: "tool_execution_start", toolCallId, name, args }
    ├── { type: "tool_execution_end", toolCallId, result }
    ├── { type: "auto_compaction_start" / "auto_compaction_end" }
    └── { type: "agent_end" }
```

**`message_update` 的子事件类型**:

`message_update` 事件内嵌 `assistantMessageEvent`，有三种子类型：

- **`text_delta`**: 最常见的增量文本片段
- **`text_start`**: 文本块开始（某些提供商发送）
- **`text_end`**: 文本块结束，可能携带完整内容的重发（需要去重）

Runtime 只处理这三种子类型，忽略其他（如 `thinking_delta`）。

---

## 十一、流式数据缓冲与处理

LLM 的流式输出是碎片化的（每次可能只有几个字符），而用户需要看到的是结构化的、可读的消息。EventHandler 内部维护了多层缓冲来完成这一转化。

### 11.1 三层缓冲架构

```text
LLM 流式 delta 片段: "好" "的" "，" "我" "来" "读" "取" ...
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  第一层: deltaBuffer (原始文本累加器)                                 │
│                                                                     │
│  state.deltaBuffer += chunk                                         │
│  • 逐字拼接所有收到的 delta                                         │
│  • 处理 text_end 的内容重发去重                                     │
│  • 作为"完整原始文本"的真实来源                                     │
│  • 去除 <think>/<final> 标签后用于 onPartialReply                  │
│  生命周期: message_start 时重置 → message_end 时清空                │
├─────────────────────────────────────────────────────────────────────┤
│  第二层: blockBuffer / BlockChunker (分块发送缓冲)                   │
│                                                                     │
│  ┌── 无 chunking 配置时 ──────┐  ┌── 有 chunking 配置时 ─────────┐ │
│  │ state.blockBuffer += chunk │  │ blockChunker.append(chunk)    │ │
│  │ text_end 时一次性发出      │  │ 达到 minChars 时寻找断点      │ │
│  │                            │  │ 切分并逐块发出                │ │
│  └────────────────────────────┘  └────────────────────────────────┘ │
│  • 控制发送给用户的消息粒度                                         │
│  • 避免碎片化消息 (每个字发一条)                                    │
│  • 在代码块内不拆分 (保持 Markdown 完整性)                          │
│  生命周期: message_start 时重置 → text_end/message_end 时强制 drain │
├─────────────────────────────────────────────────────────────────────┤
│  第三层: assistantTexts[] (最终回复收集器)                            │
│                                                                     │
│  完成的消息文本收集到此数组                                          │
│  • 去重: 跳过与上次相同的文本                                       │
│  • 通过消息工具发送的重复文本也会被检测并跳过                        │
│  生命周期: 整个 run 期间累积 → 最终作为 payloads 返回给上层          │
└─────────────────────────────────────────────────────────────────────┘
```

**`text_end` 重复内容处理**: 某些 LLM 提供商在 `text_end` 事件中会重发完整内容（而非增量），Runtime 使用前缀比对来去重——如果 `content` 是 `deltaBuffer` 的超集，只取新增部分；如果 `deltaBuffer` 已包含则忽略。

---

### 11.2 标签清洗与指令解析

LLM 输出可能包含 `<think>`/`<thinking>`/`<final>` 等标签。在缓冲过程中，Runtime 实时清洗：

```text
原始 deltaBuffer: "好的<thinking>让我看看哪个文件...</thinking>，我来读取 package.json"
    │
    ▼ stripBlockTags()  → 去除 <think>/<final> 标签和内容
    │
清洗后: "好的，我来读取 package.json"
    │
    ▼ parseReplyDirectives()  → 提取特殊指令
    │
最终: text + mediaUrls + audioAsVoice 等结构化字段
```

关键操作：

1. **`stripBlockTags`** — 去除标签及包裹内容，维护跨 chunk 的状态机追踪标签配对
2. **`parseReplyDirectives`** — 解析 `[[media:url]]`、`[[audio-as-voice]]`、`[[reply-to:id]]` 等指令
3. **去重检测** — 对比 `lastStreamedAssistantCleaned` 防止重复发送
4. **delta 计算** — 对比清洗后文本与上次已发送文本，仅提取真正增量

---

### 11.3 BlockChunker 分块策略

`EmbeddedBlockChunker` 是为消息渠道设计的分块器（Telegram、WhatsApp 有消息长度限制）：

> 源文件: `src/agents/pi-embedded-block-chunker.ts`

```text
配置:
  minChars: 最小发送字符数 (避免太碎)
  maxChars: 最大单块字符数 (避免太长)
  breakPreference: "paragraph" | "newline" | "sentence"

分块优先级 (breakPreference = "paragraph"):
  1. 优先在 \n\n (段落) 处断开
  2. 次选在 \n (换行) 处断开
  3. 再次在 . ! ? (句号) 处断开
  4. 最后在空白字符处断开
  5. 如果以上都找不到且达到 maxChars → 强制断开

Markdown 代码块保护:
  不在 ``` 代码围栏内部断开
  如必须断开 → 先关闭代码围栏，下一块重新打开
```

---

## 十二、消息投递链路

EventHandler 处理事件后产生的消息要经过多级传递才能到达用户。因通信渠道不同分为两条路径。

### 12.1 消息渠道路径

适用于 WhatsApp / Telegram / Discord / Signal 等消息渠道：

```text
EventHandler
    │
    ├── onBlockReply(payload)           ← 分块回复
    │       │
    │       ▼
    │   dispatchReplyFromConfig()
    │       │  配置回调: 可选 TTS 转换
    │       ▼
    │   ReplyDispatcher.sendBlockReply()
    │       │  ┌── 内部处理 ──────────────────────────┐
    │       │  │ 1. normalizeReplyPayload() — 格式化   │
    │       │  │ 2. 入队 (串行化保证顺序)               │
    │       │  │ 3. 可选 humanDelay (模拟人类节奏)      │
    │       │  │ 4. deliver(payload, {kind:"block"})    │
    │       │  └────────────────────────────────────────┘
    │       ▼
    │   deliver() — 各渠道的实际发送函数
    │       ├── WhatsApp: WhatsApp API → 用户手机
    │       ├── Telegram: Telegram Bot API → 用户聊天
    │       ├── Discord: Discord API → 频道/DM
    │       └── Signal: Signal protocol → 用户
    │
    └── (prompt 结束后) dispatcher.sendFinalReply(payload)
            └── 如已有 blockReply 发送 → 检测去重，可能跳过
```

### 12.2 Web/Gateway WebSocket 路径

适用于 Web UI 和 Gateway WebSocket 连接：

```text
EventHandler
    │
    ├── emitAgentEvent({stream, data})      ← 全局事件总线
    │       │
    │       ▼
    │   src/infra/agent-events.ts
    │       │  分配递增 seq，附加 ts 时间戳
    │       │  遍历所有 listeners
    │       ▼
    │   Gateway listener (createAgentEventHandler)
    │       │  ┌── server-chat.ts ────────────────────────────┐
    │       │  │  if (stream === "assistant") {                │
    │       │  │    // 限频 150ms                              │
    │       │  │    broadcast("chat", {state:"delta", ...})    │
    │       │  │  }                                            │
    │       │  │  if (stream === "lifecycle" && phase==="end"){│
    │       │  │    broadcast("chat", {state:"final", ...})    │
    │       │  │  }                                            │
    │       │  │  broadcast("agent", agentPayload)             │
    │       │  └───────────────────────────────────────────────┘
    │       ▼
    │   WebSocket broadcast → Web UI 显示流式文本
    │
    └── onAgentEvent(payload)               ← 通过回调参数上报
```

**Gateway WebSocket 限频**: 即使 LLM 每秒输出 20+ 个 delta，WebSocket 最多每 150ms 发送一次更新（约 6-7 次/秒）。每次发送的是**到目前为止的完整文本**（非增量），客户端直接替换显示即可。

---

### 12.3 两条路径的差异

| 特性 | 消息渠道路径 | Web/Gateway 路径 |
| ---- | ---- | ---- |
| 触发方式 | `onBlockReply`/`onToolResult` 回调 | `emitAgentEvent` 全局事件总线 |
| 发送粒度 | 分块 (BlockChunker 控制) | 每个 delta 都发 (限频 150ms) |
| 消息格式 | 纯文本/Markdown + 媒体 URL | JSON 帧 (chat/agent 事件) |
| 排序保证 | `ReplyDispatcher` 串行队列 | WebSocket 天然有序 |
| 去重机制 | 消息工具发送重复检测 | seq 编号 + 客户端去重 |
| 人类延迟 | 可配置 humanDelay | 无 (实时流式) |
| 限频 | 无 (由 chunking 控制) | 150ms 节流 |

---

## 十三、完整通信流程图

以下完整追踪一条消息 `"帮我读取 package.json"` 从发送到回复的全路径：

```text
session.prompt("帮我读取 package.json")
    │
    │  ┌──── SDK 内部 ─────────────────────────────────────────────┐
    │  │                                                           │
    │  │  将 systemPrompt + messages + userMessage + tools         │
    │  │  打包发送给 LLM API                                       │
    │  │                                                           │
    │  │  agent_start                                              │
    │  │    → handleAgentStart() → onAgentEvent({phase:"start"})   │
    │  │                                                           │
    │  │  message_start                                            │
    │  │    → handleMessageStart() → onAssistantMessageStart()     │
    │  │                                                           │
    │  │  message_update (text_delta: "好的，")                     │
    │  │    → handleMessageUpdate()                                │
    │  │    → deltaBuffer += "好的，"                               │
    │  │    → stripBlockTags() → onPartialReply()                  │
    │  │                                                           │
    │  │  message_end                                              │
    │  │    → handleMessageEnd()                                   │
    │  │    → assistantTexts.push("好的，我来...")                   │
    │  │                                                           │
    │  │  tool_execution_start (name:"read", path:"package.json")  │
    │  │    → handleToolExecutionStart()                           │
    │  │    → onToolResult() + onAgentEvent({stream:"tool"})       │
    │  │                                                           │
    │  │  ┌─── SDK 自动执行工具 ───────────────────┐               │
    │  │  │ readTool.execute("call_abc",            │               │
    │  │  │   {path:"package.json"})                │               │
    │  │  │ → 返回文件内容                          │               │
    │  │  └─────────────────────────────────────────┘               │
    │  │                                                           │
    │  │  tool_execution_end (result: {content:[{text:"..."}]})    │
    │  │    → handleToolExecutionEnd()                             │
    │  │    → sanitizeToolResult() → onToolResult()                │
    │  │                                                           │
    │  │  ┌─── SDK 将工具结果发回给 LLM ─────┐                     │
    │  │  │ LLM 继续生成最终回复              │                     │
    │  │  └───────────────────────────────────┘                     │
    │  │                                                           │
    │  │  message_start → message_update (流式) → message_end      │
    │  │    → "package.json 内容如下..."                            │
    │  │                                                           │
    │  │  agent_end                                                │
    │  │    → handleAgentEnd() → 刷新所有缓冲区                    │
    │  └───────────────────────────────────────────────────────────┘
    │
    └── prompt() 完成 (Promise resolve)
        │
        ├── messagesSnapshot = session.messages.slice()
        ├── unsubscribe()
        ├── session.dispose()
        └── 返回 { payloads, toolMetas, ... }
```

---

## 十四、关键源文件索引

### Part A: 持久化层

| 文件 | 核心功能 |
| ---- | ---- |
| `src/config/sessions/types.ts` | `SessionEntry` 类型定义 |
| `src/config/sessions/store.ts` | Session Store 读写（sessions.json），带 TTL 缓存 |
| `src/config/sessions/paths.ts` | 路径解析（sessionId → sessionFile） |
| `src/agents/pi-embedded-runner/session-manager-init.ts` | `prepareSessionManagerForRun`，修复 SessionManager 初始化怪癖 |
| `src/agents/pi-embedded-runner/session-manager-cache.ts` | 文件缓存预热、访问追踪 |
| `src/agents/session-write-lock.ts` | 文件写锁（跨进程互斥） |
| `src/agents/session-tool-result-guard-wrapper.ts` | `guardSessionManager`，安装工具结果守卫 |
| `src/agents/session-tool-result-guard.ts` | 工具调用/结果配对追踪与合成 |
| `src/agents/transcript-policy.ts` | `resolveTranscriptPolicy`，按提供商生成转录策略 |
| `src/agents/pi-embedded-runner/google.ts` | `sanitizeSessionHistory`，历史消息清洗 |
| `src/agents/pi-embedded-helpers/turns.ts` | `validateGeminiTurns` / `validateAnthropicTurns` |
| `src/agents/pi-embedded-runner/history.ts` | `limitHistoryTurns` / `getDmHistoryLimitFromSessionKey` |
| `src/agents/pi-embedded-runner/compact.ts` | `compactEmbeddedPiSessionDirect`，压缩操作 |
| `src/agents/pi-embedded-runner/run/attempt.ts` | 会话打开、准备、历史处理的主流程 |

### Part B: 交互层

| 文件 | 核心功能 |
| ---- | ---- |
| `src/agents/pi-embedded-subscribe.ts` | 事件订阅系统入口 |
| `src/agents/pi-embedded-subscribe.handlers.ts` | 事件分发 switch 路由 |
| `src/agents/pi-embedded-subscribe.handlers.types.ts` | `EmbeddedPiSubscribeContext` / `State` 类型 |
| `src/agents/pi-embedded-subscribe.handlers.messages.ts` | 消息事件处理（流式缓冲核心） |
| `src/agents/pi-embedded-subscribe.handlers.tools.ts` | 工具执行事件处理 |
| `src/agents/pi-embedded-subscribe.handlers.lifecycle.ts` | 生命周期事件处理 |
| `src/agents/pi-embedded-subscribe.tools.ts` | 工具结果清理与截断 |
| `src/agents/pi-embedded-block-chunker.ts` | 流式文本分块器 |
| `src/infra/agent-events.ts` | 全局事件总线 (`emitAgentEvent` / `onAgentEvent`) |
| `src/gateway/server-chat.ts` | Gateway 事件处理 → WebSocket 广播 |
| `src/auto-reply/reply/reply-dispatcher.ts` | 消息渠道回复分发器 |
| `src/auto-reply/reply/dispatch-from-config.ts` | 回调绑定与消息渠道投递 |
