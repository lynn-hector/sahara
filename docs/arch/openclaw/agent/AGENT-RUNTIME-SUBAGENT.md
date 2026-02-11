# Agent Runtime 子 Agent 生命周期

> 本文档详解子 Agent（Subagent）的完整生命周期：从创建、执行、结果通告到清理归档，以及 Agent 间通信和策略继承机制。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、创建与 Spawn](#二创建与-spawn)
- [三、Subagent Registry](#三subagent-registry)
- [四、生命周期追踪](#四生命周期追踪)
- [五、结果通告 (Announce)](#五结果通告-announce)
- [六、清理与归档](#六清理与归档)
- [七、策略继承与安全](#七策略继承与安全)
- [八、Agent 间通信 (A2A)](#八agent-间通信-a2a)
- [九、配置参考](#九配置参考)
- [十、关键源文件索引](#十关键源文件索引)

---

## 一、全局视角

### 1.1 什么是子 Agent

子 Agent 是由主 Agent 通过 `sessions_spawn` 工具创建的**临时性、任务导向**的独立会话。它拥有自己的 session、消息历史和 LLM 交互循环，但受限于更窄的工具集和更聚焦的系统提示。

```text
┌────────────────────────────────────────────────────────────────────┐
│  主 Agent (requester)                                              │
│  sessionKey: agent:default:telegram:dm:12345                       │
│                                                                    │
│  LLM 决策: "这个任务需要并行处理，我来创建子 Agent"                 │
│      │                                                             │
│      ▼  sessions_spawn({ task: "分析 README.md 的结构" })          │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────┐     │
│  │  子 Agent (child)                                        │     │
│  │  sessionKey: agent:default:subagent:<uuid>                │     │
│  │                                                          │     │
│  │  • 独立的 AgentSession                                   │     │
│  │  • 精简的系统提示（聚焦任务）                              │     │
│  │  • 受限的工具集（无 spawn/cron/memory 等）                 │     │
│  │  • 独立的 JSONL 转录文件                                  │     │
│  │                                                          │     │
│  │  执行完毕 → 结果通告回主 Agent                            │     │
│  └──────────────────────────────────────────────────────────┘     │
│                                                                    │
│  收到通告: "子 Agent 完成了，结果是..."                             │
│  → 继续主任务                                                      │
└────────────────────────────────────────────────────────────────────┘
```

### 1.2 完整生命周期

```text
① Spawn ──→ ② 注册 ──→ ③ 执行 ──→ ④ 完成 ──→ ⑤ 通告 ──→ ⑥ 清理
   │            │           │           │           │           │
   │  创建会话  │  写入      │  独立的   │  Registry │  构建消息 │  delete
   │  设置模型  │  Registry  │  LLM 交互 │  更新状态 │  投递父   │  或
   │  注入提示  │  启动追踪  │  工具循环  │  endedAt  │  Agent   │  archive
   │            │           │           │           │           │
   └────────────┴───────────┴───────────┴───────────┴───────────┘
                        Gateway RPC 贯穿全过程
```

---

## 二、创建与 Spawn

### 2.1 sessions_spawn 工具参数

> 源文件: `src/agents/tools/sessions-spawn-tool.ts`

| 参数 | 类型 | 必需 | 说明 |
| ---- | ---- | ---- | ---- |
| `task` | string | 是 | 子 Agent 的任务描述 |
| `label` | string | 否 | 可读标签（用于日志和 UI） |
| `agentId` | string | 否 | 在另一个 Agent ID 下创建（需配置 `allowAgents`） |
| `model` | string | 否 | 覆盖子 Agent 使用的模型 |
| `thinking` | string | 否 | 覆盖思考级别 |
| `runTimeoutSeconds` | number | 否 | 超时秒数（0 = 不限） |
| `cleanup` | `"delete"` / `"keep"` | 否 | 清理策略（默认 `"keep"`） |

### 2.2 创建流程

```text
sessions_spawn({ task: "分析 README.md 的结构", cleanup: "delete" })
    │
    ▼
① 验证: 调用者不能是子 Agent (子 Agent 不能再 spawn 子 Agent)
    │
    ▼
② 权限检查: agentId 是否在 allowAgents 白名单中
    │
    ▼
③ 生成 sessionKey: agent:<agentId>:subagent:<uuid>
    │
    ▼
④ 模型解析 (优先级从高到低):
    ├── 工具参数 model
    ├── per-agent 配置: agents.list[].subagents.model
    └── 全局配置: agents.defaults.subagents.model
    │
    ▼
⑤ 构建子 Agent 系统提示 (buildSubagentSystemPrompt)
    │
    ▼
⑥ 通过 Gateway RPC 启动子 Agent:
    gateway.agent({
      sessionKey: childKey,
      prompt: task,
      deliver: false,              // 不通过消息渠道投递
      lane: AGENT_LANE_SUBAGENT,   // 专用队列通道
      extraSystemPrompt: childPrompt,
      spawnedBy: requesterKey,     // 标记父会话
      timeout: runTimeoutSeconds,
    })
    │
    ▼
⑦ 注册到 SubagentRegistry → 开始生命周期追踪
    │
    ▼
⑧ 返回给 LLM: { status: "spawned", sessionKey: childKey }
```

### 2.3 子 Agent 系统提示

> 源文件: `src/agents/subagent-announce.ts` — `buildSubagentSystemPrompt()`

子 Agent 收到的系统提示与主 Agent 显著不同：

```text
┌────────────────────────────────────────────────────────┐
│  子 Agent 系统提示结构                                   │
│                                                        │
│  角色: "You are a **subagent** spawned by the main     │
│         agent to handle a specific task."              │
│                                                        │
│  任务: [task 参数的内容]                                 │
│                                                        │
│  规则:                                                  │
│  1. 只做分配的任务，不要偏离                              │
│  2. 完成任务后，最后的消息会自动报告给主 Agent            │
│  3. 不要主动发消息、不要设置 heartbeat                    │
│  4. 你是临时的，完成后可能被终止                          │
│                                                        │
│  禁止:                                                  │
│  • 不要与用户直接对话                                    │
│  • 不要发送外部消息（WhatsApp/Telegram 等）              │
│  • 不要创建 cron 定时任务                                │
│  • 不要 spawn 新的子 Agent                               │
│                                                        │
│  上下文: label, 父会话 key, 渠道信息                     │
└────────────────────────────────────────────────────────┘
```

**另外**, 系统提示构建函数 `buildOpenClawSystemPrompt` 检测到子 Agent 的 sessionKey 后会自动切换到 `promptMode = "minimal"`，跳过：技能列表、记忆系统、文档路径、模型别名、心跳提示、静默回复等不必要的段落。

---

## 三、Subagent Registry

### 3.1 Registry 的作用

SubagentRegistry 是子 Agent 生命周期管理的核心数据结构。它追踪所有子 Agent 运行的状态，支持跨进程恢复。

> 源文件: `src/agents/subagent-registry.ts`, `src/agents/subagent-registry.store.ts`

### 3.2 每个子 Agent 的记录

```typescript
type SubagentRunRecord = {
  // ── 标识 ────────────────────────────────────
  runId: string;                    // 运行唯一 ID
  childSessionKey: string;          // agent:<id>:subagent:<uuid>
  requesterSessionKey: string;      // 父会话 key
  requesterDisplayKey: string;      // 父会话可读 key

  // ── 任务 ────────────────────────────────────
  task: string;                     // 任务描述
  label?: string;                   // 可读标签
  cleanup: "delete" | "keep";       // 清理策略

  // ── 时间戳 ──────────────────────────────────
  createdAt: number;                // 创建时间
  startedAt?: number;               // 运行开始时间
  endedAt?: number;                 // 完成时间

  // ── 结果 ────────────────────────────────────
  outcome?: {
    status: "ok" | "error";
    error?: string;
  };

  // ── 清理追踪 ────────────────────────────────
  archiveAtMs?: number;             // 自动归档时间
  cleanupCompletedAt?: number;      // 清理完成时间
  cleanupHandled?: boolean;         // 是否正在处理清理
  requesterOrigin?: DeliveryContext; // 父会话渠道上下文
};
```

### 3.3 持久化

Registry 记录会被持久化到磁盘（JSON 文件），这意味着即使 Gateway 重启，未完成的子 Agent 也能被恢复追踪：

```text
Gateway 启动
    │
    ▼
restoreSubagentRunsOnce()
    │  从磁盘加载 registry
    │
    ├── 已完成 (endedAt 存在)?
    │   → 立即触发通告/清理流程
    │
    └── 未完成?
        → 重新调用 waitForSubagentCompletion()
        → 继续等待完成
```

---

## 四、生命周期追踪

### 4.1 双轨追踪

子 Agent 的生命周期通过两个并行机制追踪：

```text
┌─────────────────────────────────────────────────────────────────┐
│  轨道 1: 进程内事件监听                                          │
│                                                                 │
│  onAgentEvent 监听 stream="lifecycle"                            │
│    ├── phase="start"  → 记录 startedAt                          │
│    ├── phase="end"    → 记录 endedAt, outcome={status:"ok"}     │
│    └── phase="error"  → 记录 endedAt, outcome={status:"error"}  │
│                                                                 │
│  适用于: 子 Agent 在同一进程内运行时                               │
├─────────────────────────────────────────────────────────────────┤
│  轨道 2: Gateway RPC 等待                                        │
│                                                                 │
│  agent.wait({ runId, timeoutMs })                                │
│    → Gateway 内部订阅 onAgentEvent                               │
│    → 等待 phase="end"/"error"                                    │
│    → 返回 { status, startedAt, endedAt, error }                  │
│                                                                 │
│  适用于: 跨进程场景（子 Agent 在不同进程运行）                    │
└─────────────────────────────────────────────────────────────────┘
```

**为什么需要两轨**: Gateway 可能作为 RPC 中介协调不同进程的 Agent 运行。轨道 1 是低延迟的本地事件监听，轨道 2 是可靠的跨进程 RPC 等待。两轨先到者生效。

### 4.2 状态流转

```text
┌────────┐  spawn   ┌──────────┐  start   ┌──────────┐  end    ┌──────────┐
│  空     │────────→│  created  │────────→│  running  │───────→│ completed │
└────────┘          └──────────┘          └──────────┘         └─────┬─────┘
                                               │                     │
                                          error │              announce
                                               │                     │
                                               ▼                     ▼
                                          ┌──────────┐        ┌───────────┐
                                          │  failed   │───────→│  cleanup  │
                                          └──────────┘        └───────────┘
```

| 状态 | Registry 字段 | 触发条件 |
| ---- | ---- | ---- |
| created | `createdAt` 有值 | `registerSubagentRun()` |
| running | `startedAt` 有值 | lifecycle `phase="start"` 事件 |
| completed | `endedAt` 有值, `outcome.status="ok"` | lifecycle `phase="end"` |
| failed | `endedAt` 有值, `outcome.status="error"` | lifecycle `phase="error"` |
| cleanup | `cleanupCompletedAt` 有值 | announce 完成后 |

---

## 五、结果通告 (Announce)

### 5.1 通告流程

子 Agent 完成后，其结果需要**报告回主 Agent**，这就是通告（Announce）机制。

> 源文件: `src/agents/subagent-announce.ts`

```text
子 Agent 完成 (endedAt 设置)
    │
    ▼
beginSubagentCleanup()
    │
    ▼
runSubagentAnnounceFlow()
    │
    ├── ① 等待完成确认 (agent.wait)
    │       → 获取 status, startedAt, endedAt, error
    │
    ├── ② 读取最终回复
    │       → readLatestAssistantReply()
    │       → 从子 Agent 会话历史中提取最后的 assistant 消息
    │
    ├── ③ 构建统计信息 (buildSubagentStatsLine)
    │       → 运行时长、token 用量、预估成本、会话路径
    │
    ├── ④ 构建通告消息
    │       ┌────────────────────────────────────────────┐
    │       │  Subagent "{label}" completed successfully.│
    │       │                                            │
    │       │  Findings:                                 │
    │       │  {子 Agent 的最后回复内容}                  │
    │       │                                            │
    │       │  Stats: 45s, 3.2k tokens, $0.012           │
    │       │                                            │
    │       │  Summarize this naturally for the user.    │
    │       │  Keep it brief (1-2 sentences).            │
    │       └────────────────────────────────────────────┘
    │
    └── ⑤ 投递给主 Agent
            │
            ├── 方式 A: steer (注入到活跃流)
            │   → 主 Agent 正在运行时，直接注入消息
            │
            ├── 方式 B: followup (排队等待)
            │   → 主 Agent 当前不在运行，排入消息队列
            │
            └── 方式 C: collect (批量合并)
                → 多个子 Agent 同时完成时，合并后一次发送
```

### 5.2 通告队列

> 源文件: `src/agents/subagent-announce-queue.ts`

多个子 Agent 可能同时完成，通告队列负责协调投递：

| 队列模式 | 行为 |
| ---- | ---- |
| `steer` | 直接注入到主 Agent 的活跃流 |
| `followup` | 排队等当前运行结束后执行 |
| `collect` | 批量收集，合并后一次发送 |
| `steer-backlog` | 先尝试 steer，失败则 followup |
| `interrupt` | 中断当前运行，用通告消息重新开始 |

**队列配置**:

- `debounceMs`: 防抖间隔（默认 1000ms）
- `cap`: 队列容量上限（默认 20）
- `dropPolicy`: `"summarize"`（默认，超出时压缩）或 `"new"`（丢弃新的）

---

## 六、清理与归档

### 6.1 两种清理策略

```text
                  cleanup = "delete"                cleanup = "keep"
                  ─────────────────                 ─────────────────
完成 + 通告后:    立即删除会话文件               标记 cleanupCompletedAt
                  立即删除 Registry 记录          Registry 记录保留
                  JSONL 文件删除                  JSONL 文件保留
                                                       │
                                                  archiveAfterMinutes
                                                  (默认 60 分钟)
                                                       │
                                                       ▼
                                                 归档清扫器删除
                                                 (sweepSubagentRuns)
```

### 6.2 归档清扫器

```text
sweepSubagentRuns() — 每 60 秒运行一次
    │
    ▼
遍历 Registry 中所有记录
    │
    ├── archiveAtMs < now ?
    │   → 是: 删除会话 + 删除 Registry 记录
    │   → 否: 跳过
    │
    └── Registry 为空?
        → 停止定时器（节省资源）
        → 下次 spawn 时重新启动
```

**归档延迟配置**: `agents.defaults.subagents.archiveAfterMinutes`（默认 60 分钟，最小 1 分钟）

### 6.3 Gateway 重启恢复

```text
Gateway 重启
    │
    ▼
restoreSubagentRunsOnce()
    │
    ├── 记录有 endedAt?
    │   → 子 Agent 已完成但清理未执行
    │   → 立即触发通告 + 清理
    │
    └── 记录无 endedAt?
        → 子 Agent 可能仍在运行
        → 重新调用 waitForSubagentCompletion()
        → 等待完成事件
```

---

## 七、策略继承与安全

### 7.1 工具策略

子 Agent 默认被**拒绝**访问以下工具：

```text
拒绝列表 (默认):
  sessions_list       ← 会话管理
  sessions_history
  sessions_send
  sessions_spawn      ← 禁止子 Agent 再 spawn (防止无限递归)
  gateway             ← 系统管理
  agents_list
  whatsapp_login      ← 交互式设置
  session_status      ← 状态/调度
  cron
  memory_search       ← 记忆工具
  memory_get
```

**策略叠加**: 子 Agent 的工具策略 = 默认拒绝列表 + 配置的 `tools.subagents.tools.deny` + 父会话的群组策略继承。Deny 总是优先于 Allow。

### 7.2 群组策略继承 (spawnedBy)

如果主 Agent 运行在一个群组会话中，子 Agent 会**继承父会话的群组工具策略**：

```text
父会话: agent:main:whatsapp:group:trusted-team
配置:   channels.whatsapp.groups.trusted-team.tools.allow = ["read", "exec"]
    │
    ▼  spawnedBy 传递
    │
子 Agent: agent:main:subagent:<uuid>
    → 从 spawnedBy 解析出群组上下文
    → 继承 groupId, groupChannel, groupSpace
    → 应用群组工具策略: 只允许 read, exec
```

### 7.3 沙箱可见性

子 Agent 在沙箱环境中的会话可见性受 `sessionToolsVisibility` 控制：

| 配置值 | 效果 |
| ---- | ---- |
| `"spawned"` (默认) | 子 Agent 只能看到**自己 spawn 的**会话 |
| `"all"` | 子 Agent 可以看到所有会话 |

当 `visibility = "spawned"` 时：

- `sessions_list` 只返回 `spawnedBy` 匹配的会话
- `sessions_send` 只能发送到 `spawnedBy` 匹配的会话
- `sessions_history` 只能查看 `spawnedBy` 匹配的会话
- 子 Agent **不能**看到兄弟子 Agent 的会话（除非它们共享同一个父）

### 7.4 allowAgents 白名单

`sessions_spawn` 的 `agentId` 参数受白名单控制：

| 配置 | 效果 |
| ---- | ---- |
| 未配置 / `[]` | 只能在自己的 Agent ID 下 spawn |
| `["agent-a", "agent-b"]` | 只能在指定 ID 下 spawn |
| `["*"]` | 可以在任何 Agent ID 下 spawn |

---

## 八、Agent 间通信 (A2A)

### 8.1 概述

Agent 间通信允许不同 Agent（不仅是子 Agent）之间通过 `sessions_send` 工具互发消息。

### 8.2 启用与配置

```yaml
# config.yaml
tools:
  agentToAgent:
    enabled: true          # 默认 false
    allow: ["*"]           # 允许哪些 Agent 互通
```

**allow 模式匹配**:

- `"*"`: 允许所有 Agent
- `"agent-a"`: 只允许 agent-a
- `"agent-*"`: 通配符匹配所有以 agent- 开头的
- 双向检查：发送方和接收方都必须在 allow 列表中

**注意**: 同一个 Agent 内的会话间通信**始终允许**，不需要开启 A2A。

### 8.3 sessions_send 流程

```text
主 Agent 调用: sessions_send({ sessionKey: "agent:other:main", message: "请帮我..." })
    │
    ├── ① 解析目标会话 (sessionKey 或 label)
    ├── ② 检查沙箱可见性限制
    ├── ③ 检查 A2A 策略 (如跨 Agent)
    │
    ▼
④ 通过 Gateway 发送消息:
    gateway.agent({
      sessionKey: targetKey,
      prompt: message,
      deliver: false,
      lane: AGENT_LANE_NESTED,
      channel: INTERNAL_MESSAGE_CHANNEL,
      extraSystemPrompt: agentMessageContext,
    })
    │
    ├── timeoutSeconds > 0 ?
    │   → agent.wait(runId, timeoutMs)
    │   → 提取目标 Agent 的回复
    │   → 返回 { status: "ok", reply: "..." }
    │
    └── timeoutSeconds === 0 ?
        → 立即返回 { status: "accepted" }
        → 异步触发 A2A announce 流程
```

### 8.4 Ping-Pong 对话

当 `timeoutSeconds > 0` 时，`sessions_send` 支持多轮对话（ping-pong）：

- 最大轮数: `maxPingPongTurns`（默认 5，范围 0-5）
- 每轮：发送消息 → 等待回复 → 检查是否需要继续
- 特殊 token: `REPLY_SKIP` 和 `ANNOUNCE_SKIP` 可跳过回复/通告

---

## 九、配置参考

### Per-Agent 配置

```yaml
agents:
  list:
    - id: default
      subagents:
        allowAgents: ["*"]           # 允许 spawn 到哪些 Agent
        model: "anthropic/claude-sonnet-4-20250514"  # 子 Agent 默认模型
```

### 全局默认配置

```yaml
agents:
  defaults:
    subagents:
      maxConcurrent: 1               # 最大并发子 Agent 数
      archiveAfterMinutes: 60        # 归档延迟（分钟）
      model:                         # 全局默认模型
        primary: "anthropic/claude-sonnet-4-20250514"
        fallbacks: ["openai/gpt-4o"]
```

### 工具策略配置

```yaml
tools:
  subagents:
    model: "anthropic/claude-sonnet-4-20250514"
    tools:
      allow: []                      # 允许列表（空 = 使用默认）
      deny: ["browser"]              # 额外拒绝列表

  agentToAgent:
    enabled: true
    allow: ["*"]
```

### 沙箱配置

```yaml
agents:
  defaults:
    sandbox:
      sessionToolsVisibility: "spawned"  # "spawned" | "all"
```

---

## 十、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/agents/tools/sessions-spawn-tool.ts` | `sessions_spawn` 工具：创建子 Agent |
| `src/agents/subagent-registry.ts` | SubagentRegistry：生命周期追踪、持久化、清扫 |
| `src/agents/subagent-registry.store.ts` | Registry 磁盘持久化 |
| `src/agents/subagent-announce.ts` | 通告流程：构建消息、投递给父 Agent、系统提示 |
| `src/agents/subagent-announce-queue.ts` | 通告队列：防抖、批量、模式路由 |
| `src/agents/tools/sessions-send-tool.ts` | `sessions_send`：跨会话消息发送 |
| `src/agents/tools/sessions-list-tool.ts` | `sessions_list`：会话列表（含沙箱过滤） |
| `src/agents/tools/sessions-history-tool.ts` | `sessions_history`：会话历史查询 |
| `src/agents/tools/sessions-helpers.ts` | 会话工具辅助函数：A2A 策略、key 解析、kind 分类 |
| `src/agents/pi-tools.policy.ts` | 子 Agent 工具策略：默认拒绝列表、群组继承 |
| `src/agents/system-prompt.ts` | 子 Agent 提示模式：`promptMode = "minimal"` |
| `src/agents/pi-embedded-runner/runs.ts` | 活跃运行追踪：`ACTIVE_EMBEDDED_RUNS` |
| `src/gateway/server-methods/agent.ts` | Gateway RPC：`agent.wait`、`spawnedBy` 上下文继承 |
| `src/gateway/server-methods/agent-job.ts` | Gateway 运行等待：事件订阅、超时 |
| `src/agents/lanes.ts` | 队列通道定义：`AGENT_LANE_SUBAGENT` |
