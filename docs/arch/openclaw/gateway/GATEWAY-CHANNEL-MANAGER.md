# Gateway 渠道生命周期管理

> 本文档详解 Gateway 的 Channel Manager：如何发现、配置、启动、监控和停止消息渠道，以及渠道账号管理、健康检查和 QR 登录等机制。

---

## 目录

- [一、Channel Manager 的角色](#一channel-manager-的角色)
- [二、渠道生命周期](#二渠道生命周期)
- [三、账号管理](#三账号管理)
- [四、启动流程](#四启动流程)
- [五、停止与关闭](#五停止与关闭)
- [六、健康监控](#六健康监控)
- [七、QR 登录流程 (WhatsApp)](#七qr-登录流程-whatsapp)
- [八、Gateway RPC 方法](#八gateway-rpc-方法)
- [九、关键源文件索引](#九关键源文件索引)

---

## 一、Channel Manager 的角色

Channel Manager 是 Gateway 内部管理所有消息渠道生命周期的组件。它不处理消息内容——只负责渠道的**启停、状态追踪和健康监控**。

```text
Gateway 启动
    │
    ▼
createChannelManager()
    │
    ├── 发现所有注册的 ChannelPlugin
    ├── 解析每个渠道的账号配置
    ├── 按顺序启动所有已启用的渠道
    │
    ▼
运行期间:
    ├── 追踪每个账号的运行状态 (running/connected/error)
    ├── 定期健康检查 (60s 间隔)
    ├── 响应 RPC 请求 (channels.status / channels.logout)
    │
    ▼
Gateway 关闭
    │
    └── 逐个停止所有运行中的渠道
```

### Channel Manager 接口

> 源文件: `src/gateway/server-channels.ts`

```typescript
type ChannelManager = {
  getRuntimeSnapshot(): ChannelRuntimeSnapshot;       // 获取所有渠道状态
  startChannels(): Promise<void>;                     // 启动所有渠道
  startChannel(channel, accountId?): Promise<void>;   // 启动特定渠道/账号
  stopChannel(channel, accountId?): Promise<void>;    // 停止特定渠道/账号
  markChannelLoggedOut(channel, cleared, accountId?): void;  // 标记已登出
};
```

---

## 二、渠道生命周期

### 2.1 生命周期状态

```text
┌──────────┐  配置有效  ┌──────────┐  启动成功  ┌──────────┐  连接建立  ┌──────────┐
│ 已发现   │──────────→│ 已配置   │──────────→│ 运行中   │──────────→│ 已连接   │
│ discovered│          │configured│          │ running  │          │connected │
└──────────┘          └──────────┘          └────┬─────┘          └────┬─────┘
                           │                     │                     │
                      配置无效                 启动失败              连接断开
                           │                     │                     │
                           ▼                     ▼                     ▼
                      ┌──────────┐          ┌──────────┐          ┌──────────┐
                      │ 未配置   │          │ 错误     │          │ 重连中   │
                      │not config│          │  error   │          │reconnect │
                      └──────────┘          └──────────┘          └──────────┘
```

### 2.2 运行时状态追踪

每个渠道的每个账号都有独立的运行时状态：

```typescript
type ChannelAccountSnapshot = {
  accountId: string;
  running?: boolean;           // 进程是否在运行
  connected?: boolean;         // 是否已建立连接
  configured?: boolean;        // 是否已正确配置
  enabled?: boolean;           // 是否已启用
  lastError?: string | null;   // 最近的错误
  lastStartAt?: number;        // 最近启动时间
  lastStopAt?: number;         // 最近停止时间
  lastProbeAt?: number;        // 最近探针时间
  reconnectAttempts?: number;  // 重连尝试次数
};
```

### 2.3 运行时存储

```text
ChannelRuntimeStore (per channel)
    │
    ├── aborts: Map<accountId, AbortController>     ← 用于停止账号
    ├── tasks: Map<accountId, Promise<unknown>>      ← 运行中的任务
    └── runtimes: Map<accountId, AccountSnapshot>    ← 状态快照
```

---

## 三、账号管理

### 3.1 多账号支持

每个渠道支持多个账号（如多个 Telegram bot、多个 Discord bot）：

```yaml
channels:
  telegram:
    accounts:
      bot-main:
        botToken: "123:ABC..."
        enabled: true
      bot-backup:
        botToken: "456:DEF..."
        enabled: false
```

### 3.2 账号解析流程

```text
startChannel("telegram")
    │
    ▼
plugin.config.listAccountIds(cfg)
    → ["bot-main", "bot-backup"]
    │
    ▼
对每个 accountId:
    │
    ├── plugin.config.resolveAccount(cfg, accountId)
    │   → 解析为完整的账号配置对象
    │
    ├── plugin.config.isEnabled(account, cfg)
    │   → false? → 跳过，标记 "disabled"
    │
    ├── plugin.config.isConfigured(account, cfg)  [async]
    │   → false? → 跳过，标记 "not configured"
    │
    └── 通过检查 → 启动账号
```

### 3.3 默认账号

```typescript
// 当未指定 accountId 时
const defaultId = plugin.config.defaultAccountId?.(cfg);
// 或者使用 listAccountIds 返回的第一个
```

---

## 四、启动流程

### 4.1 Gateway 启动时的渠道初始化

> 源文件: `src/gateway/server-startup.ts`

```text
Gateway startGatewayServer()
    │
    ├── ... (配置加载、插件注册、WebSocket 服务器就绪)
    │
    ├── createChannelManager()  → Channel Manager 实例化
    │
    ├── startGatewaySidecars()  → 辅助服务启动
    │
    └── startChannels()  → 所有渠道启动
         │
         ├── 环境变量检查: OPENCLAW_SKIP_CHANNELS / OPENCLAW_SKIP_PROVIDERS
         │   → 设置时跳过所有渠道启动
         │
         └── 按 listChannelPlugins() 顺序逐个启动
              │
              ├── Telegram
              ├── WhatsApp
              ├── Discord
              ├── Slack
              ├── Signal
              ├── iMessage
              └── 扩展渠道 (按 meta.order 排序)
```

**启动顺序**: 按 `CHAT_CHANNEL_ORDER` 定义的顺序 + 扩展渠道按 `meta.order`。

**失败处理**: 单个渠道启动失败**不会阻塞**其他渠道的启动——错误被捕获、记录到日志、标记到运行时状态。

### 4.2 单个账号的启动

```text
startChannel("telegram", "bot-main")
    │
    ▼
① 检查是否已在运行
    store.tasks.has("bot-main")?
    → 是: 跳过 (幂等)
    │
    ▼
② 解析账号 + 检查 enabled + 检查 configured
    │
    ▼
③ 创建 AbortController
    store.aborts.set("bot-main", abort)
    │
    ▼
④ 更新状态: { running: true, lastStartAt: now, lastError: null }
    │
    ▼
⑤ 调用 plugin.gateway.startAccount({
      cfg, accountId, account,
      abortSignal: abort.signal,      ← 用于停止
      log: channelLog,                ← 渠道日志器
      getStatus: () => snapshot,      ← 读取当前状态
      setStatus: (patch) => update,   ← 更新状态
   })
    │
    ├── 成功: 渠道 monitor 开始运行 (长期 Promise)
    └── 失败: 记录错误, 标记 lastError
```

### 4.3 各渠道的 startAccount 实现

| 渠道 | 实现函数 | 行为 |
| ---- | ---- | ---- |
| Telegram | `monitorTelegramProvider()` | 启动 Grammy bot，进入 polling/webhook 循环 |
| Discord | `monitorDiscordProvider()` | 连接 Discord Gateway WebSocket |
| WhatsApp | `monitorWebChannel()` | 通过 Baileys 连接 WhatsApp Web |
| Slack | `monitorSlackProvider()` | 连接 Slack Socket Mode |
| Signal | `monitorSignalProvider()` | 连接 signal-cli REST API (SSE) |
| iMessage | `monitorIMessageProvider()` | 启动 AppleScript 轮询循环 |

所有 monitor 函数都是**长期运行的 Promise**——它们在渠道运行期间不会 resolve，只在停止或出错时结束。

---

## 五、停止与关闭

### 5.1 单个账号停止

```text
stopChannel("telegram", "bot-main")
    │
    ├── ① abort.abort()  → 触发 AbortSignal
    │      → monitor 内部收到信号，开始清理
    │
    ├── ② plugin.gateway.stopAccount() (如果有)
    │      → 渠道特定的清理逻辑
    │
    ├── ③ await task  → 等待 monitor Promise 结束
    │
    └── ④ 清理状态:
           store.aborts.delete("bot-main")
           store.tasks.delete("bot-main")
           setRuntime({ running: false, lastStopAt: now })
```

### 5.2 Gateway 关闭

> 源文件: `src/gateway/server-close.ts`

```text
Gateway 收到关闭信号 (SIGINT / SIGTERM / 手动)
    │
    ▼
遍历所有已注册的 ChannelPlugin:
    │
    ├── stopChannel("telegram")     → 停止所有 Telegram 账号
    ├── stopChannel("discord")      → 停止所有 Discord 账号
    ├── stopChannel("whatsapp")     → 停止所有 WhatsApp 账号
    ├── ... (所有渠道)
    │
    ▼
关闭 WebSocket 服务器
    │
    ▼
关闭 HTTP 服务器
```

---

## 六、健康监控

### 6.1 定期健康检查

> 源文件: `src/gateway/server-maintenance.ts`, `src/gateway/server/health-state.ts`

```text
每 60 秒:
    │
    ▼
refreshGatewayHealthSnapshot({ probe: true })
    │
    ├── 遍历所有渠道和账号
    ├── 合并 runtime snapshot (来自 Channel Manager)
    ├── 可选: 调用 plugin.status.probeAccount() (主动探测)
    ├── 可选: 调用 plugin.status.auditAccount() (配置审计)
    │
    ▼
广播健康更新给连接的客户端
```

### 6.2 探针 (Probe)

| 渠道 | 探针方式 | 检测内容 |
| ---- | ---- | ---- |
| Telegram | `getMe()` API 调用 | Bot token 是否有效、API 是否可达 |
| Discord | Gateway 连接状态检查 | WebSocket 连接、Intents 配置 |
| WhatsApp | Baileys 连接状态 | Web 会话是否存活 |
| Slack | Socket Mode 连接状态 | 连接是否活跃 |
| Signal | signal-cli REST 健康接口 | API 是否可达 |
| iMessage | AppleScript 连通性 | Messages.app 是否响应 |

### 6.3 重连策略

各渠道在 monitor 内部自行处理重连：

| 渠道 | 重连策略 |
| ---- | ---- |
| Telegram | 内置重试循环，指数退避 |
| Discord | Gateway 自动重连，`maxAttempts: Infinity` |
| WhatsApp | Baileys 内置重连 |
| Slack | Socket Mode 自动重连 |
| Signal | SSE 重连 |

Channel Manager 不直接参与重连——它只追踪 `reconnectAttempts` 计数和 `connected` 状态。

---

## 七、QR 登录流程 (WhatsApp)

WhatsApp 需要通过 QR 码扫描来建立 Web 会话连接，这是一个特殊的交互流程：

### 7.1 完整流程

```text
客户端                  Gateway                     WhatsApp Web
   │                       │                              │
   │  web.login.start      │                              │
   │ ────────────────────→ │                              │
   │                       │  ① 停止现有连接                │
   │                       │  stopChannel("whatsapp")     │
   │                       │                              │
   │                       │  ② 创建新 socket              │
   │                       │  createWaSocket()            │
   │                       │                              │
   │                       │  ③ 等待 QR 生成               │
   │                       │ ────────────────────────────→│
   │                       │ ←──── QR 数据 ───────────────│
   │                       │                              │
   │                       │  ④ 渲染 QR 为 base64 PNG     │
   │  ← { qrDataUrl }     │                              │
   │ ←──────────────────── │                              │
   │                       │                              │
   │  (用户手机扫码)        │                              │
   │                       │                              │
   │  web.login.wait       │                              │
   │ ────────────────────→ │                              │
   │                       │  ⑤ 等待连接确认               │
   │                       │ ────────────────────────────→│
   │                       │ ←──── connected ─────────────│
   │                       │                              │
   │                       │  ⑥ 启动渠道                   │
   │                       │  startChannel("whatsapp")    │
   │                       │                              │
   │  ← { connected: true }│                              │
   │ ←──────────────────── │                              │
```

### 7.2 活跃登录管理

- 使用 `activeLogins: Map<accountId, ActiveLogin>` 追踪进行中的登录
- TTL: 3 分钟——超时后需要重新发起
- 幂等: 如果 QR 还在有效期内，直接返回已有的 QR

### 7.3 错误处理

| 错误 | 处理 |
| ---- | ---- |
| Status 515 (重启请求) | 自动重试一次 |
| DisconnectReason.loggedOut | 清除认证缓存，需要全新扫码 |
| 超时 | 返回 "Still waiting for QR scan" |

---

## 八、Gateway RPC 方法

### 渠道相关的 RPC 方法

| 方法 | 参数 | 说明 |
| ---- | ---- | ---- |
| `channels.status` | `{ probe?: boolean, timeoutMs?: number }` | 获取所有渠道状态快照 |
| `channels.logout` | `{ channel, accountId? }` | 停止渠道并登出 |
| `web.login.start` | `{ force?, timeoutMs?, verbose? }` | 启动 WhatsApp QR 登录 |
| `web.login.wait` | `{ timeoutMs? }` | 等待 QR 扫码完成 |

### channels.status 响应结构

```text
{
  channels: {
    telegram: {
      accounts: [{
        accountId: "bot-main",
        running: true,
        connected: true,
        enabled: true,
        configured: true,
        lastStartAt: 1706000000000,
        lastError: null,
        probe: { ok: true, latencyMs: 120 },   // 仅 probe=true 时
        audit: { issues: [] },                  // 仅 probe=true 时
      }]
    },
    discord: { ... },
    whatsapp: { ... },
    ...
  }
}
```

---

## 九、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/gateway/server-channels.ts` | `createChannelManager`：Channel Manager 核心实现 |
| `src/gateway/server-startup.ts` | Gateway 启动时渠道初始化入口 |
| `src/gateway/server-close.ts` | Gateway 关闭时渠道停止 |
| `src/gateway/server-maintenance.ts` | 定期健康检查 (60s) |
| `src/gateway/server/health-state.ts` | 健康快照缓存与刷新 |
| `src/gateway/server-methods/channels.ts` | `channels.status` / `channels.logout` RPC |
| `src/gateway/server-methods/web.ts` | `web.login.start` / `web.login.wait` RPC |
| `src/web/login-qr.ts` | WhatsApp QR 登录实现 |
| `src/channels/plugins/types.adapters.ts` | 渠道适配器接口定义 |
| `src/channels/plugins/types.core.ts` | `ChannelAccountSnapshot` 类型 |
| `src/channels/plugins/status.ts` | `buildChannelAccountSnapshot` 状态构建 |
| `src/channels/plugins/index.ts` | `listChannelPlugins` 渠道枚举与排序 |
