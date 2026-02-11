# Gateway 架构总览

> 本文档以 Gateway 为中心，展示它在 OpenClaw 中的核心枢纽角色：上游连接用户客户端和消息渠道，下游调度 Agent Runtime、设备节点和外部服务，中间承载协议翻译、认证鉴权、会话路由和事件广播。

---

## 目录

- [一、Gateway 的定位](#一gateway-的定位)
- [二、全景架构图](#二全景架构图)
- [三、上游：谁连接 Gateway](#三上游谁连接-gateway)
- [四、Gateway 内部组件](#四gateway-内部组件)
- [五、下游：Gateway 调度什么](#五下游gateway-调度什么)
- [六、核心数据流](#六核心数据流)
- [七、Gateway 与其他子系统的边界](#七gateway-与其他子系统的边界)
- [八、专题文档索引](#八专题文档索引)

---

## 一、Gateway 的定位

Gateway 是 OpenClaw 的**控制平面 (Control Plane)**——它不直接执行 AI 推理或工具调用，而是作为**中央枢纽**协调所有组件的通信。

类比理解：

- **Gateway** = 前台 + 调度中心（接待、分发、协调）
- **Agent Runtime** = 执行团队（干活的人）
- **Channels** = 对外窗口（电话/邮件/即时通讯）
- **客户端** = 管理层（下达指令、查看进展）
- **设备节点** = 远程分支（执行本地任务）

**Gateway 做什么**:

- 接收所有外部连接（客户端 WebSocket、渠道消息、HTTP 请求）
- 认证和鉴权（token 验证、配对码）
- 消息路由（决定哪条消息交给哪个 Agent）
- 调度 Agent Runtime（启动运行、等待完成、广播事件）
- 管理渠道生命周期（启动/停止 Telegram bot、Discord gateway 等）
- 管理设备节点（注册、心跳、能力发现）
- 提供 HTTP 服务（Control UI、OpenAI 兼容 API、Webhooks）

**Gateway 不做什么**:

- 不调用 LLM API（由 Agent Runtime 负责）
- 不执行工具（由沙箱或主机负责）
- 不解析渠道原始消息格式（由各渠道适配器负责）
- 不持久化会话内容（由 SessionManager 负责）

---

## 二、全景架构图

```text
                          ┌─────────────────────────────────────┐
                          │            用户/管理员               │
                          └──────────────────┬──────────────────┘
                                             │
                ┌────────────────────────────┼────────────────────────────┐
                │                            │                            │
                ▼                            ▼                            ▼
     ┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐
     │  macOS/iOS/Android│       │   CLI Client     │       │   Web UI         │
     │  App              │       │                  │       │   (Browser)      │
     └────────┬─────────┘       └────────┬─────────┘       └────────┬─────────┘
              │ WebSocket                │ WebSocket                │ WebSocket
              └──────────────────────────┼──────────────────────────┘
                                         │
┌─────────────────── 外部消息平台 ────────┼──────────────────────────────────────┐
│                                         │                                      │
│  Telegram ── Bot API ──┐                │                                      │
│  Discord  ── Gateway ──┤                │                                      │
│  WhatsApp ── Baileys ──┤                │                                      │
│  Slack    ── Socket ───┤                │                                      │
│  Signal   ── REST ─────┤                │                                      │
│  iMessage ── AS ───────┤                │                                      │
│  Teams*   ── Bot FW ───┤                │                                      │
│  Matrix*  ── CS API ───┘                │                                      │
│                │                        │                                      │
└────────────────┼────────────────────────┼──────────────────────────────────────┘
                 │ MsgContext              │
                 │                        │
┌════════════════╪════════════════════════╪══════════════════════════════════════┐
║                ▼                        ▼                                      ║
║  ┌─────────────────────────────────────────────────────────────────────────┐  ║
║  │                                                                         │  ║
║  │                    G A T E W A Y                                        │  ║
║  │                    ws://127.0.0.1:18789                                 │  ║
║  │                                                                         │  ║
║  │  ┌───────────┐ ┌────────────┐ ┌────────────┐ ┌───────────────────────┐ │  ║
║  │  │ WebSocket │ │  Channel   │ │  HTTP      │ │  Node Registry       │ │  ║
║  │  │ Server    │ │  Manager   │ │  Services  │ │  (设备节点管理)       │ │  ║
║  │  │           │ │  (渠道     │ │            │ │                       │ │  ║
║  │  │ 客户端    │ │   生命周期)│ │ Control UI │ │  macOS / iOS /       │ │  ║
║  │  │ 节点连接  │ │            │ │ OpenAI API │ │  Android 节点        │ │  ║
║  │  │ 认证鉴权  │ │ 启动/停止  │ │ Webhooks   │ │  能力发现/心跳       │ │  ║
║  │  └─────┬─────┘ └─────┬──────┘ └─────┬──────┘ └───────────┬───────────┘ │  ║
║  │        │             │              │                     │             │  ║
║  │        └─────────────┴──────┬───────┴─────────────────────┘             │  ║
║  │                             │                                           │  ║
║  │  ┌──────────────────────────┴──────────────────────────────────────┐   │  ║
║  │  │                    调度与路由层                                   │   │  ║
║  │  │                                                                  │   │  ║
║  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │   │  ║
║  │  │  │ Agent Router │  │ Session      │  │  Event Broadcaster   │  │   │  ║
║  │  │  │ (消息→Agent) │  │ Store        │  │  (AgentEvent→客户端) │  │   │  ║
║  │  │  └──────┬───────┘  └──────────────┘  └──────────┬───────────┘  │   │  ║
║  │  │         │                                       │              │   │  ║
║  │  │  ┌──────┴───────┐  ┌──────────────┐  ┌─────────┴────────────┐ │   │  ║
║  │  │  │ Cron         │  │ Hook         │  │  Plugin Manager      │ │   │  ║
║  │  │  │ Scheduler    │  │ Executor     │  │  (插件加载/注册)      │ │   │  ║
║  │  │  └──────────────┘  └──────────────┘  └──────────────────────┘ │   │  ║
║  │  └─────────────────────────────────────────────────────────────────┘   │  ║
║  │                                                                         │  ║
║  └──────────────────────────────┬──────────────────────────────────────────┘  ║
║                                 │                                              ║
╚═════════════════════════════════╪══════════════════════════════════════════════╝
                                  │
            ┌─────────────────────┼─────────────────────┐
            │                     │                     │
            ▼                     ▼                     ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│  Agent Runtime   │  │  Device Nodes    │  │  External Services   │
│                  │  │                  │  │                      │
│  LLM 交互循环    │  │  macOS:          │  │  Browser (CDP)       │
│  工具执行        │  │   camera, screen │  │  Web Search          │
│  会话管理        │  │  iOS:            │  │  TTS                 │
│  事件产出        │  │   shortcuts      │  │  Memory Index        │
│                  │  │  Android:        │  │                      │
│  ┌────────────┐  │  │   shell, intent  │  │                      │
│  │ 沙箱       │  │  │                  │  │                      │
│  │ (Docker)   │  │  │                  │  │                      │
│  └────────────┘  │  │                  │  │                      │
└──────────────────┘  └──────────────────┘  └──────────────────────┘
```

---

## 三、上游：谁连接 Gateway

Gateway 的上游是所有需要与系统交互的**外部实体**：

### 3.1 客户端连接 (WebSocket)

| 客户端类型 | 连接方式 | 主要交互 |
| ---- | ---- | ---- |
| macOS App | WebSocket | `agent` / `chat.send` |
| iOS App | WebSocket | `agent` / `chat.history` |
| Android App | WebSocket | `agent` |
| CLI Client | WebSocket | `agent` / `sessions.*` |
| Web UI | WebSocket | `agent` / `chat.*` |
| Node (设备) | WebSocket | `node.register` / tool invoke |

所有客户端通过 WebSocket 连接 Gateway，使用统一的 JSON RPC 协议（`req`/`res`/`event` 帧）。

### 3.2 消息渠道连接

| 渠道 | 接入方式 | 生命周期管理 |
| ---- | ---- | ---- |
| Telegram | Bot API (webhook/polling) | Channel Manager |
| Discord | Gateway WebSocket | Channel Manager |
| WhatsApp | Baileys (WA Web 协议) | Channel Manager |
| Slack | Socket Mode | Channel Manager |
| Signal | signal-cli REST (SSE) | Channel Manager |
| iMessage | AppleScript 轮询 | Channel Manager |
| 扩展渠道 | 插件自定义 | Plugin Manager |

渠道不通过 WebSocket 连接 Gateway——它们由 **Channel Manager** 在 Gateway 进程内启动和管理。

### 3.3 HTTP 请求

| HTTP 服务 | 路径 | 用途 |
| ---- | ---- | ---- |
| Control UI | `/` | Web 管理界面 |
| OpenAI API | `/v1/chat/completions` | 兼容 API |
| Telegram Webhook | `/webhook/telegram/:id` | Bot webhook |
| Plugin HTTP | `/plugin/:id/*` | 插件路由 |

---

## 四、Gateway 内部组件

### 4.1 组件职责

```text
┌─────────────────────────────────────────────────────────────────┐
│  Gateway 内部组件                                                │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  接入层                                                  │   │
│  │  WebSocket Server — 管理所有 WS 连接，认证，帧解析       │   │
│  │  HTTP Server — Control UI, API, Webhooks               │   │
│  │  Channel Manager — 渠道启停、账号管理、健康监控         │   │
│  └──────────────────────────┬──────────────────────────────┘   │
│                              │                                  │
│  ┌──────────────────────────┴──────────────────────────────┐   │
│  │  调度层                                                  │   │
│  │  Agent Router — 消息路由 (sessionKey → agentId)          │   │
│  │  Session Store — sessions.json 读写                     │   │
│  │  Cron Scheduler — 定时任务调度                           │   │
│  │  Hook Executor — 插件钩子执行                            │   │
│  │  Plugin Manager — 插件发现/加载/注册                     │   │
│  └──────────────────────────┬──────────────────────────────┘   │
│                              │                                  │
│  ┌──────────────────────────┴──────────────────────────────┐   │
│  │  通信层                                                  │   │
│  │  Event Broadcaster — AgentEvent → WebSocket 广播         │   │
│  │  Node Registry — 设备节点注册/心跳/能力发现              │   │
│  │  RPC Dispatcher — req/res 帧路由到具体 method handler    │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 RPC 方法分类

| 分类 | 方法 | 说明 |
| ---- | ---- | ---- |
| **Agent** | `agent`, `agent.wait`, `chat.abort` | 启动/等待/中止 Agent 运行 |
| **Chat** | `chat.send`, `chat.history`, `chat.sessions` | 会话消息管理 |
| **Sessions** | `sessions.list`, `sessions.preview`, `sessions.delete` | 会话管理 |
| **Channels** | `channels.status`, `channels.logout` | 渠道状态/登出 |
| **Config** | `config.get`, `config.set` | 配置读写 |
| **Cron** | `cron.list`, `cron.add`, `cron.remove` | 定时任务 |
| **Node** | `node.list`, `node.invoke`, `node.describe` | 设备节点 |
| **Device Pairing** | `device.pair.approve`, `device.pair.reject` | 设备配对 |
| **Node Pairing** | `node.pair.approve`, `node.pair.reject` | 节点配对 |

---

## 五、下游：Gateway 调度什么

### 5.1 Agent Runtime

Gateway 是 Agent Runtime 的**唯一调用者**：

```text
Gateway                              Agent Runtime
   │                                      │
   │  agent RPC → agentCommand()          │
   │ ────────────────────────────────────→ │
   │                                      │  runEmbeddedPiAgent()
   │                                      │  → LLM 交互
   │  ← AgentEvent (lifecycle/tool/text)  │  → 工具执行
   │ ←──────────────────────────────────── │  → 流式输出
   │                                      │
   │  agent.wait → waitForAgentJob()      │
   │ ────────────────────────────────────→ │
   │  ← { status, startedAt, endedAt }    │
   │ ←──────────────────────────────────── │
```

### 5.2 设备节点

```text
Gateway                              Device Node (macOS/iOS/Android)
   │                                      │
   │  ← node.register (能力声明)           │
   │ ←──────────────────────────────────── │
   │                                      │
   │  node.invoke (camera.snap)           │
   │ ────────────────────────────────────→ │
   │  ← { result: base64Image }           │
   │ ←──────────────────────────────────── │
```

### 5.3 事件广播

Gateway 订阅 Agent Runtime 的全局事件总线，将事件广播给所有连接的客户端：

```text
Agent Runtime                    Gateway                     客户端
     │                              │                           │
     │  emitAgentEvent()            │                           │
     │ ───────────────────────────→ │                           │
     │                              │  broadcast("chat", delta) │
     │                              │ ───────────────────────→  │  Web UI 更新
     │                              │                           │
     │                              │  broadcast("agent", evt)  │
     │                              │ ───────────────────────→  │  工具面板更新
     │                              │                           │
     │                              │  150ms 限频 (chat delta)  │
```

---

## 六、核心数据流

### 6.1 用户消息的完整旅程（通过 Gateway）

```text
用户在 Web UI 输入 "帮我看看 package.json"
    │
    ▼
① Web UI → WebSocket → Gateway
   帧: { type: "req", id: 1, method: "agent", params: { prompt: "..." } }
    │
    ▼
② Gateway RPC Dispatcher
   → 路由到 agent method handler
   → resolveAgentRoute() → 确定 agentId + sessionKey
   → agentCommand() → 创建运行上下文
    │
    ▼
③ Gateway → Agent Runtime
   → runEmbeddedPiAgent({ prompt, sessionKey, ... })
   → 双层队列入队 → 模型解析 → 认证 → runEmbeddedAttempt()
    │
    ▼
④ Agent Runtime ← LLM
   ← 流式 text_delta / tool_call / message_end
    │
    ▼
⑤ Agent Runtime → emitAgentEvent()
   → 全局事件总线 → Gateway 监听器
    │
    ▼
⑥ Gateway → WebSocket broadcast
   → { type: "event", stream: "chat", data: { state: "delta", text: "..." } }
   → 150ms 限频
    │
    ▼
⑦ Web UI 收到 → 实时更新聊天界面
```

### 6.2 消息渠道消息的旅程（通过 Gateway）

```text
用户在 Telegram 发送 "帮我看看代码"
    │
    ▼
① Telegram Bot API → Telegram Provider (在 Gateway 进程内)
   → createTelegramMessageProcessor()
   → 标准化为 MsgContext
    │
    ▼
② resolveAgentRoute(MsgContext)
   → 匹配 Agent 绑定 → 确定 agentId + sessionKey
    │
    ▼
③ dispatchInboundMessage()
   → 创建 ReplyDispatcher (含打字指示器)
   → dispatchReplyFromConfig()
    │
    ▼
④ getReplyFromConfig() → runEmbeddedPiAgent()
   → Agent Runtime 执行 (与上面 ③-⑤ 相同)
    │
    ▼
⑤ 回复流:
   ├── onBlockReply → ReplyDispatcher → Telegram sendMessage API
   ├── onToolResult → ReplyDispatcher → Telegram sendMessage API
   └── 最终回复 → ReplyDispatcher → Telegram sendMessage API
    │
    ▼
⑥ 用户在 Telegram 收到回复
```

### 6.3 Gateway 的关键角色对比

| 场景 | Gateway 做了什么 | 不经过 Gateway 的部分 |
| ---- | ---- | ---- |
| Web UI 对话 | 接收 WS 帧 → RPC 路由 → 调度 Runtime → 事件广播 | LLM 调用、工具执行 |
| Telegram 消息 | 管理 Bot 连接 → 消息标准化 → 路由 → 调度 → 回复投递 | LLM 调用、工具执行 |
| CLI 命令 | 接收 WS 帧 → RPC 路由 → 返回结果 | — |
| 定时任务 | Cron 触发 → 构造虚拟消息 → 调度 Runtime | LLM 调用、工具执行 |
| 设备控制 | 接收 node.invoke → 转发到设备节点 → 返回结果 | 设备端执行 |

---

## 七、Gateway 与其他子系统的边界

```text
渠道系统                     Gateway                     Agent Runtime
    │                            │                            │
    │  ──── 边界① ────→          │                            │
    │  原始消息格式 → MsgContext  │  ──── 边界② ────→          │
    │                            │  MsgContext → RunParams    │
    │                            │                            │
    │          ←──── 边界③ ────  │          ←──── 边界④ ────  │
    │  ReplyPayload → 平台 API  │  payloads → ReplyPayload   │
    │                            │                            │

客户端                       Gateway                     设备节点
    │                            │                            │
    │  ──── 边界⑤ ────→          │                            │
    │  JSON RPC 帧 → handler    │                            │
    │                            │                            │
    │          ←──── 边界⑥ ────  │                            │
    │  event broadcast → 展示   │  ──── 边界⑦ ────→          │
    │                            │  node.invoke → 本地执行    │
```

**7 个边界点说明**:

| 边界 | 转换内容 | 负责方 |
| ---- | ---- | ---- |
| ① 渠道 → Gateway | 平台原始格式 → `MsgContext` | 渠道 normalizer |
| ② Gateway → Runtime | `MsgContext` → `RunEmbeddedPiAgentParams` | Gateway agentCommand |
| ③ Gateway → 渠道 | `ReplyPayload` → 平台 API 调用 | 渠道 outbound adapter |
| ④ Runtime → Gateway | `assistantTexts` → `ReplyPayload` | `buildEmbeddedRunPayloads` |
| ⑤ 客户端 → Gateway | JSON RPC 帧 → method handler | RPC Dispatcher |
| ⑥ Gateway → 客户端 | `AgentEvent` → WebSocket event 帧 | Event Broadcaster |
| ⑦ Gateway → 设备 | `node.invoke` → 设备本地执行 | Node Registry |

---

## 八、专题文档索引

### Gateway 专题文档

| 文档 | 内容 | 适合回答的问题 |
| ---- | ---- | ---- |
| [GATEWAY-STARTUP.md](./GATEWAY-STARTUP.md) | 启动流程与组装 | Gateway 怎么启动? 组件怎么组装? |
| [GATEWAY-PROTOCOL.md](./GATEWAY-PROTOCOL.md) | WebSocket 协议 | 帧格式? RPC 方法? 事件系统? |
| [GATEWAY-AGENT-DISPATCH.md](./GATEWAY-AGENT-DISPATCH.md) | Agent 调度流程 | agent RPC 怎么处理? 事件怎么广播? |
| [GATEWAY-CLIENT.md](./GATEWAY-CLIENT.md) | 客户端通信 | 各客户端怎么连接? 认证怎么做? |
| [GATEWAY-CHANNEL-MANAGER.md](./GATEWAY-CHANNEL-MANAGER.md) | 渠道生命周期管理 | 渠道怎么启停? 账号怎么管理? 健康检查? QR 登录? |
| [GATEWAY-AUTH.md](./GATEWAY-AUTH.md) | 认证与配对 | token 验证? 设备配对? 角色权限? 网络安全? |
| [GATEWAY-NODE.md](./GATEWAY-NODE.md) | 设备节点管理 | 节点注册? 配对? 平台能力? invoke? 命令策略? |
| [GATEWAY-HTTP.md](./GATEWAY-HTTP.md) | HTTP 服务 | OpenAI API? Webhooks? Control UI? 插件路由? |

### 关联文档

| 文档 | 内容 |
| ---- | ---- |
| [../channel/CHANNELS-ARCHITECTURE.md](../channel/CHANNELS-ARCHITECTURE.md) | 消息渠道架构（入站/出站/路由/权限） |
| [../agent/AGENT-RUNTIME-v2.md](../agent/AGENT-RUNTIME-v2.md) | Agent Runtime 架构（Gateway 调度的下游） |
| [../agent/AGENT-RUNTIME-REPLY-DISPATCH.md](../agent/AGENT-RUNTIME-REPLY-DISPATCH.md) | 回复投递（从 Runtime 到用户的最后一公里） |
