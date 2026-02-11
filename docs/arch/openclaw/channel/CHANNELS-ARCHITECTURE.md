# 消息渠道架构

> 本文档详解 OpenClaw 的消息渠道（Channels）系统：多渠道统一接入架构、消息从各平台进入系统的入站流程、回复投递到各平台的出站流程、路由与会话映射、权限控制，以及渠道插件扩展机制。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、渠道统一架构](#二渠道统一架构)
- [三、入站消息流](#三入站消息流)
- [四、出站回复流](#四出站回复流)
- [五、路由与会话映射](#五路由与会话映射)
- [六、权限与准入控制](#六权限与准入控制)
- [七、各渠道特性对比](#七各渠道特性对比)
- [八、渠道插件机制](#八渠道插件机制)
- [九、健康监控与探针](#九健康监控与探针)
- [十、配置参考](#十配置参考)
- [十一、关键源文件索引](#十一关键源文件索引)

---

## 一、全局视角

### 1.1 渠道系统的角色

渠道系统是 OpenClaw 的**消息入口与出口**——它将来自不同即时通讯平台的消息统一为内部格式，交给 Agent Runtime 处理，然后将 Agent 的回复适配回各平台的原生格式送达用户。

```text
┌────────────────────────────────────────────────────────────────────────┐
│                     消息渠道在整体架构中的位置                           │
│                                                                        │
│  用户                                                                  │
│  ├── WhatsApp ──┐                                                      │
│  ├── Telegram ──┤                                                      │
│  ├── Discord  ──┤     ┌──────────────┐     ┌──────────────────────┐   │
│  ├── Slack    ──┼────→│  渠道系统     │────→│  Agent Runtime       │   │
│  ├── Signal   ──┤     │  (本文档)     │←────│  (处理 + 生成回复)    │   │
│  ├── iMessage ──┤     │              │     └──────────────────────┘   │
│  ├── Teams    ──┤     │  入站: 标准化  │                                │
│  ├── Matrix   ──┤     │  出站: 适配    │     ┌──────────────────────┐   │
│  └── Web UI   ──┘     │  路由: 映射    │────→│  Gateway WebSocket   │   │
│                        └──────────────┘     └──────────────────────┘   │
│                                                                        │
│  渠道系统 = 多平台 ←→ 统一格式 的双向翻译层                            │
└────────────────────────────────────────────────────────────────────────┘
```

### 1.2 支持的渠道

| 渠道 | 类型 | 接入方式 | 状态 |
| ---- | ---- | ---- | ---- |
| Telegram | 核心 | Bot API (webhook/polling) | 内置 |
| WhatsApp | 核心 | WhatsApp Web (Baileys) | 内置 |
| Discord | 核心 | Discord Gateway + REST | 内置 |
| Slack | 核心 | Socket Mode + Web API | 内置 |
| Signal | 核心 | signal-cli REST API | 内置 |
| iMessage | 核心 | AppleScript (macOS) | 内置 |
| MS Teams | 扩展 | Bot Framework | 插件 |
| Matrix | 扩展 | Matrix Client-Server API | 插件 |
| Zalo | 扩展 | Zalo OA API | 插件 |
| Voice Call | 扩展 | 电话 API | 插件 |
| Web UI | 特殊 | Gateway WebSocket | 内置 (不经渠道层) |

---

## 二、渠道统一架构

### 2.1 ChannelPlugin 接口

每个渠道（包括核心和扩展）都实现统一的 `ChannelPlugin` 接口：

> 源文件: `src/channels/plugins/types.plugin.ts`

```text
┌─────────────────────────────────────────────────────────────────┐
│  ChannelPlugin<ResolvedAccount>                                  │
│                                                                  │
│  ── 标识与元数据 ──────────────────────────────────────────────  │
│  id: ChannelId               (如 "telegram", "discord")          │
│  meta: ChannelMeta           (名称、文档路径、描述)               │
│  capabilities: ChannelCapabilities  (支持的功能声明)             │
│                                                                  │
│  ── 适配器 (模块化, 按需实现) ──────────────────────────────────  │
│  config         配置解析: 账号解析、allowFrom 格式化              │
│  setup?         设置向导: 账号配置流程                            │
│  pairing?       配对通知: 新用户配对码                            │
│  security?      安全策略: DM 策略、告警                          │
│  auth?          认证流程: 登录/登出                               │
│  messaging?     消息适配: 目标规范化、目录提示                    │
│  outbound?      出站投递: sendText/sendMedia/sendPayload          │
│  threading?     线程支持: 上下文构建、reply-to 模式               │
│  groups?        群组支持: 提及要求、工具策略                      │
│  mentions?      提及处理: 提及剥离模式                            │
│  status?        健康检查: 探针、审计、快照                        │
│  gateway?       Gateway 集成: 启停账号、QR 登录                  │
│  actions?       消息操作: 反应、按钮、卡片                        │
│  agentTools?    渠道工具: 渠道专属的 Agent 工具                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 MsgContext — 统一消息格式

所有渠道的原始消息都被**标准化**为 `MsgContext`：

```text
MsgContext {
  // ── 消息内容 ────────────────
  Body: string              ← 消息文本
  RawBody?: string          ← 原始文本 (含 Markdown/HTML)
  MediaUrl?: string         ← 附件媒体 URL
  MediaType?: string        ← 媒体 MIME 类型

  // ── 路由标识 ────────────────
  From: string              ← 发送者标识
  To: string                ← 接收者标识
  Provider: string          ← 渠道标识 (telegram/discord/...)
  Surface?: string          ← 消息表面 (可能与 Provider 不同)
  ChatType: string          ← dm / group / group-dm

  // ── 消息元数据 ──────────────
  MessageSid: string        ← 消息唯一 ID
  ReplyToId?: string        ← 回复的目标消息 ID
  MessageThreadId?: string  ← 线程/话题 ID

  // ── 发送者信息 ──────────────
  SenderId?: string
  SenderName?: string
  SenderUsername?: string
  SenderE164?: string       ← 电话号码 (E.164 格式)

  // ── 跨渠道 ─────────────────
  OriginatingChannel?: string   ← 消息原始来源渠道
  OriginatingTo?: string        ← 原始渠道的目标 ID
  AccountId?: string            ← 当前渠道的账号 ID
}
```

### 2.3 双向翻译流程

```text
入站 (用户 → Agent):
  平台原始格式 → 渠道 normalizer → MsgContext → resolveAgentRoute → Agent Runtime

出站 (Agent → 用户):
  ReplyPayload → routeReply/deliver → 渠道 outbound adapter → 平台 API → 用户
```

---

## 三、入站消息流

### 3.1 各渠道的接入方式

```text
┌────────────────────────────────────────────────────────────────────┐
│  各渠道的消息接入                                                  │
│                                                                    │
│  Telegram ─── Webhook / Long-polling ──→ Grammy bot ──→ ┐         │
│  Discord  ─── Gateway WebSocket ───────→ 事件处理 ───→  │         │
│  WhatsApp ─── Baileys (WA Web) ────────→ 消息处理 ───→  │         │
│  Slack    ─── Socket Mode ─────────────→ 事件处理 ───→  ├→ MsgContext
│  Signal   ─── SSE (signal-cli REST) ──→ 事件处理 ───→  │         │
│  iMessage ─── AppleScript 轮询 ────────→ 消息处理 ───→  │         │
│  扩展     ─── 各自的接入方式 ──────────→ 消息处理 ───→  ┘         │
│                                                                    │
│                          MsgContext                                │
│                             │                                      │
│                             ▼                                      │
│                    resolveAgentRoute()                             │
│                             │                                      │
│                             ▼                                      │
│                  dispatchInboundMessage()                          │
│                             │                                      │
│                             ▼                                      │
│                    Agent Runtime 执行                              │
└────────────────────────────────────────────────────────────────────┘
```

### 3.2 入站处理管线

每条入站消息经过的标准步骤：

```text
① 渠道原始消息到达 (webhook/WS/轮询)
    │
    ▼
② 渠道 Handler 处理
    ├── 过滤: 忽略自己发的消息、系统消息
    ├── 媒体: 提取图片/音频/视频附件
    ├── 线程: 提取线程/话题 ID
    └── 群组: 检测提及 (@mention) 要求
    │
    ▼
③ Normalizer 转换为 MsgContext
    ├── 统一发送者标识 (ID/用户名/电话号码)
    ├── 统一目标标识
    ├── 统一聊天类型 (dm/group)
    └── 提取引用/回复上下文
    │
    ▼
④ resolveAgentRoute() — 路由决策
    ├── 匹配 Agent 绑定 (peer/guild/team/account/channel)
    ├── 构建 sessionKey
    └── 确定 agentId
    │
    ▼
⑤ dispatchInboundMessage() — 调度
    ├── 创建 ReplyDispatcher
    ├── 配置打字指示器
    └── 调用 dispatchReplyFromConfig()
```

### 3.3 各渠道入站特点

| 渠道 | 接入技术 | 特殊处理 |
| ---- | ---- | ---- |
| Telegram | Grammy bot (webhook/polling) | 消息排序化（per chat+thread）、话题 ID 提取 |
| Discord | discord.js Gateway | Guild/Channel/Thread 层级解析、Slash 命令 |
| WhatsApp | Baileys (WA Web 协议) | E.164 号码格式化、群组 JID 解析 |
| Slack | @slack/socket-mode | Thread TS 解析、App Mention 事件、Blocks 解析 |
| Signal | signal-cli REST (SSE) | 信封格式解析、群组 ID 转换 |
| iMessage | AppleScript 轮询 | Messages.app 数据库查询、群组 GUID |

---

## 四、出站回复流

### 4.1 统一投递入口

> 源文件: `src/infra/outbound/deliver.ts`

```text
Agent Runtime 生成回复 (ReplyPayload)
    │
    ▼
deliverOutboundPayloads({
  channel, to, payloads,
  threadId, replyToId, ...
})
    │
    ├── ① 加载渠道适配器 (ChannelOutboundAdapter)
    │
    ├── ② 遍历 payloads:
    │      │
    │      ├── 有 channelData? → handler.sendPayload() (自定义)
    │      │
    │      ├── 有媒体? → handler.sendMedia(buffer, type, caption)
    │      │   └── 第一个媒体携带 caption，后续无 caption
    │      │
    │      └── 纯文本? → handler.chunker → handler.sendText()
    │          └── 按渠道限制分块 (2000-4096 字符)
    │
    └── ③ 返回投递结果列表
```

### 4.2 各渠道出站适配

| 渠道 | 投递模式 | 文本限制 | 分块器 | 格式 |
| ---- | ---- | ---- | ---- | ---- |
| Telegram | direct | 4000 字符 | Markdown→HTML | HTML |
| Discord | direct | 2000 字符 | — | Markdown |
| WhatsApp | gateway | 4000 字符 | 纯文本分块 | 纯文本 |
| Slack | direct | 4000 字符 | — | Blocks API |
| Signal | direct | — | Markdown→Signal styles | 带样式范围的纯文本 |
| iMessage | direct | — | — | 纯文本 + 附件 |

**投递模式**: `"direct"` = 直接调用平台 API；`"gateway"` = 通过 Gateway 中转（WhatsApp 需要维持 Web 连接）。

---

## 五、路由与会话映射

### 5.1 路由系统

> 源文件: `src/routing/resolve-route.ts`

路由决定**哪条消息由哪个 Agent 处理**：

```text
resolveAgentRoute(ctx)
    │
    ▼
查找 Agent 绑定 (从配置中读取), 按优先级匹配:
    │
    ├── ① peer 绑定 (DM/群组的精确匹配)
    │      → 特定用户/群组 → 特定 Agent
    │
    ├── ② peer.parent (线程的父会话)
    │      → 线程消息 → 匹配父频道的 Agent
    │
    ├── ③ guild 绑定 (Discord guild)
    │      → 整个服务器 → 特定 Agent
    │
    ├── ④ team 绑定 (Slack team)
    │      → 整个团队 → 特定 Agent
    │
    ├── ⑤ account 绑定 (按账号)
    │      → 该渠道账号的所有消息 → 特定 Agent
    │
    ├── ⑥ channel 绑定 (accountId: "*")
    │      → 该渠道类型的所有消息 → 特定 Agent
    │
    └── ⑦ default 绑定
         → 未匹配的消息 → 默认 Agent
```

### 5.2 会话键 (sessionKey)

会话键是渠道消息与 Agent 会话之间的**映射桥梁**：

```text
sessionKey 格式:
  agent:<agentId>:<channel>:<chatType>:<senderId>
  
示例:
  agent:default:telegram:dm:12345
  agent:default:discord:group:guild123:channel456
  agent:default:whatsapp:dm:+8613800138000
```

**DM 作用域** (`dmScope`):

| 模式 | sessionKey 粒度 | 效果 |
| ---- | ---- | ---- |
| `main` (默认) | 所有 DM 共享一个会话 | 跨渠道对话连续 |
| `per-peer` | 每个用户一个会话 | 用户间隔离 |
| `per-channel-peer` | 每个渠道+用户一个会话 | 渠道间也隔离 |

### 5.3 跨渠道路由

当消息来源与当前会话绑定的渠道不同时，系统会自动路由回复到原始渠道：

```text
用户在 Telegram 发消息 → Agent 会话绑定在 WhatsApp
    │
    ▼
MsgContext:
  Provider: "whatsapp"
  OriginatingChannel: "telegram"
  OriginatingTo: "12345"
    │
    ▼
shouldRouteToOriginating = true
→ 回复通过 routeReply() 发送到 Telegram (而非 WhatsApp)
```

---

## 六、权限与准入控制

### 6.1 DM 策略

| 策略 | 行为 |
| ---- | ---- |
| `pairing` (默认) | 未知用户收到配对码提示，配对后才能对话 |
| `allowlist` | 仅 `allowFrom` 列表中的用户可对话 |
| `open` | 所有用户可对话（需 `allowFrom: ["*"]`） |
| `disabled` | 禁用所有 DM |

### 6.2 群组策略

| 策略 | 行为 |
| ---- | ---- |
| `open` | 所有群组的消息都处理（配合 mention 要求） |
| `allowlist` | 仅 `groupAllowFrom` 列表中的群组/发送者 |
| `disabled` | 禁用群组消息 |

### 6.3 Allowlist 匹配

> 源文件: `src/auto-reply/command-auth.ts`

```text
resolveCommandAuthorization(ctx)
    │
    ├── 获取渠道的 allowFrom 列表
    ├── 规范化发送者标识:
    │   ├── 用户 ID
    │   ├── 用户名
    │   ├── E.164 电话号码
    │   └── slug / 显示名
    │
    └── 逐个匹配 allowFrom 条目:
        ├── "*" → 通配符，允许所有
        ├── 精确匹配 (ID/用户名/号码)
        └── 模式匹配 (通配符)
```

### 6.4 提升权限 (Elevated)

某些高风险工具（如 `exec` 提权）需要额外的 elevated allowlist：

```yaml
tools:
  elevated:
    allowFrom:
      telegram: ["admin_user"]
      discord: ["123456789"]
```

---

## 七、各渠道特性对比

| 特性 | Telegram | Discord | WhatsApp | Slack | Signal | iMessage |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| 聊天类型 | DM/群/频道/话题 | DM/群/频道/线程 | DM/群 | DM/频道/线程 | DM/群 | DM/群 |
| 线程支持 | 话题 (Forum) | 线程 | 无 | 线程 (ts) | 无 | 无 |
| 反应/表情 | 有 | 有 | 有 | 有 | 有 | 有 |
| 内联按钮 | Inline Keyboard | — | — | Blocks | — | — |
| 原生命令 | `/start`, `/help` | Slash Commands | — | Slash Commands | — | — |
| 媒体支持 | 图片/音频/视频/文件 | 文件附件 | 图片/音频/视频 | 文件上传 | 附件 | 附件 |
| 文本格式 | HTML | Markdown | 纯文本 | Blocks/mrkdwn | 样式范围 | 纯文本 |
| 接入方式 | Bot API | Gateway WS | Baileys | Socket Mode | signal-cli | AppleScript |
| 流式回复 | Block streaming | Block streaming | Block streaming | Block streaming | — | — |

---

## 八、渠道插件机制

### 8.1 扩展渠道的注册

扩展渠道通过插件 API 注册：

```typescript
// extensions/msteams/index.ts
export function register(api: PluginApi) {
  api.registerChannel({
    id: "msteams",
    meta: { label: "Microsoft Teams", ... },
    capabilities: { chatTypes: ["direct", "group", "channel"] },
    config: { ... },
    messaging: { ... },
    outbound: { ... },
    status: { ... },
    agentTools: (ctx) => [ /* Teams 专属工具 */ ],
  });
}
```

### 8.2 渠道能力声明

```typescript
type ChannelCapabilities = {
  chatTypes: ChatType[];         // 支持的聊天类型
  nativeCommands?: boolean;      // 支持原生命令
  blockStreaming?: boolean;      // 支持流式分块回复
  reactions?: boolean;           // 支持反应/表情
  threads?: boolean;             // 支持线程
  media?: boolean;               // 支持媒体附件
  typing?: boolean;              // 支持打字指示器
  polls?: boolean;               // 支持投票
};
```

### 8.3 渠道专属工具

渠道插件可以提供 Agent 可用的专属工具（如 `teams_send_card`、`slack_post_block`）：

```text
渠道插件注册 agentTools
    │
    ▼
listChannelAgentTools({ cfg })  → 收集所有渠道工具
    │
    ▼
合并到 createOpenClawCodingTools()  → Agent 工具集
```

---

## 九、健康监控与探针

### 9.1 状态快照

每个渠道账号有一个状态快照：

```text
ChannelAccountSnapshot {
  enabled: boolean          ← 是否启用
  configured: boolean       ← 是否已配置
  linked: boolean           ← 是否已连接
  running: boolean          ← 是否正在运行
  connected: boolean        ← 是否在线
  lastConnectedAt?: number  ← 最后连接时间
  lastDisconnect?: string   ← 最后断开原因
  lastError?: string        ← 最后错误
  reconnectAttempts?: number
}
```

### 9.2 探针系统

`openclaw channels status --probe` 执行主动健康检查：

| 渠道 | 探针方式 |
| ---- | ---- |
| Telegram | 调用 Bot API `/getMe` |
| Discord | 检查 Gateway 连接状态、Intents 配置 |
| WhatsApp | 检查 Web 连接状态 |
| Slack | 检查 Socket Mode 连接 |
| Signal | 调用 signal-cli REST API |
| iMessage | AppleScript 连通性测试 |

### 9.3 状态问题收集

每个渠道有专门的问题检测器，发现常见配置/连接问题：

- Telegram: 不可达的群组、bot 权限不足
- Discord: Gateway Intents 缺失、权限不足
- WhatsApp: 已登出、QR 过期
- 通用: 账号未配置、Token 无效

---

## 十、配置参考

### Telegram 配置

```yaml
channels:
  telegram:
    accounts:
      - botToken: "123456:ABC..."
        enabled: true
        dmPolicy: "pairing"
        allowFrom: ["user1", 12345]
        groupPolicy: "open"
        requireMention: true
        replyToMode: "first"
        groups:
          my-group:
            activation: "mention"
            tools:
              allow: ["read", "exec"]
```

### Discord 配置

```yaml
channels:
  discord:
    accounts:
      - token: "MTk..."
        enabled: true
        dm:
          policy: "pairing"
          allowFrom: ["user#1234"]
        guilds:
          my-server:
            channels: ["general", "bot-channel"]
            groupPolicy: "open"
        replyToMode: "all"
```

### WhatsApp 配置

```yaml
channels:
  whatsapp:
    accounts:
      - enabled: true
        allowFrom: ["+8613800138000"]
        groupPolicy: "allowlist"
        groupAllowFrom: ["Group Name"]
        requireMention: true
```

### 通用渠道配置项

```yaml
channels:
  <channel>:
    accounts:
      - enabled: boolean              # 是否启用
        allowFrom: string[]           # DM 白名单
        dmPolicy: string              # DM 策略
        groupPolicy: string           # 群组策略
        groupAllowFrom: string[]      # 群组白名单
        requireMention: boolean       # 群组是否需要 @mention
        replyToMode: string           # 线程回复模式
        historyLimit: number          # 群组历史上下文限制
```

---

## 十一、关键源文件索引

### 共享基础设施

| 文件 | 核心功能 |
| ---- | ---- |
| `src/channels/plugins/types.plugin.ts` | `ChannelPlugin` 接口定义 |
| `src/channels/plugins/types.adapters.ts` | 所有适配器接口定义 |
| `src/channels/dock.ts` | 轻量渠道元数据（无重依赖） |
| `src/channels/registry.ts` | 渠道 ID 规范化、元数据 |
| `src/channels/plugins/normalize/` | 各渠道消息标准化器 |
| `src/channels/plugins/outbound/` | 各渠道出站适配器 |
| `src/channels/plugins/status.ts` | 渠道状态快照构建 |
| `src/channels/allowlist-match.ts` | Allowlist 匹配工具 |

### 路由与调度

| 文件 | 核心功能 |
| ---- | ---- |
| `src/routing/resolve-route.ts` | `resolveAgentRoute`：路由决策 |
| `src/routing/session-key.ts` | 会话键构建 |
| `src/routing/bindings.ts` | Agent 绑定列表解析 |
| `src/auto-reply/dispatch.ts` | `dispatchInboundMessage`：入站调度入口 |
| `src/auto-reply/reply/dispatch-from-config.ts` | 回复调度主流程 |
| `src/auto-reply/command-auth.ts` | 命令授权与 allowlist |
| `src/infra/outbound/deliver.ts` | 统一出站投递入口 |

### 核心渠道

| 文件 | 核心功能 |
| ---- | ---- |
| `src/telegram/bot.ts` | Telegram bot 创建与处理 |
| `src/discord/monitor/provider.ts` | Discord Gateway 监控 |
| `src/web/inbound/monitor.ts` | WhatsApp Web 入站监控 |
| `src/slack/monitor.ts` | Slack Socket Mode 监控 |
| `src/signal/monitor/event-handler.ts` | Signal 事件处理 |
| `src/imessage/monitor/monitor-provider.ts` | iMessage AppleScript 监控 |
