# Gateway 启动与组装流程详解

> 本文档详细介绍 OpenClaw Gateway 的启动流程、组件组装和内部架构。

---

## 目录

1. [概述](#一概述)
2. [启动流程总览](#二启动流程总览)
3. [启动阶段详解](#三启动阶段详解)
4. [核心组件说明](#四核心组件说明)
5. [WebSocket 协议](#五websocket-协议)
6. [RPC 方法列表](#六rpc-方法列表)
7. [组装完成图](#七组装完成图)
8. [关键源码索引](#八关键源码索引)

---

## 一、概述

### 1.1 Gateway 是什么

Gateway 是 OpenClaw 的**控制平面 (Control Plane)**，负责：

- 接收和管理所有客户端 WebSocket 连接
- 管理消息渠道 (Telegram, Discord, Slack 等)
- 管理设备节点 (iOS, Android, macOS)
- 提供 HTTP 服务 (Control UI, OpenAI API, Webhooks)
- 调度 AI 代理执行任务

### 1.2 启动入口

```typescript
// 主入口文件: src/gateway/server.impl.ts
// 导出文件: src/gateway/server.ts

import { startGatewayServer } from "./server.impl.js";

// 启动 Gateway
const gateway = await startGatewayServer(18789, {
  bind: "loopback",  // 绑定模式
  controlUiEnabled: true,
  // ...其他选项
});

// 关闭 Gateway
await gateway.close();
```

---

## 二、启动流程总览

```
startGatewayServer(port=18789)
         │
         ▼
┌────────────────────────────────────────────────────────────────────┐
│  Phase 1: 配置准备                                                  │
│  ────────────────────────────────────────────────────────────────  │
│  • 读取配置文件 (~/.openclaw/config.yaml)                          │
│  • 迁移旧版配置                                                    │
│  • 验证配置有效性                                                  │
│  • 自动启用插件                                                    │
└────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────────┐
│  Phase 2: 核心组件初始化                                            │
│  ────────────────────────────────────────────────────────────────  │
│  • 加载插件 (loadGatewayPlugins)                                   │
│  • 解析运行时配置 (resolveGatewayRuntimeConfig)                    │
│  • 加载 TLS 配置                                                   │
│  • 创建依赖注入容器 (createDefaultDeps)                            │
└────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────────┐
│  Phase 3: 运行时状态创建 (createGatewayRuntimeState)                │
│  ────────────────────────────────────────────────────────────────  │
│  • 创建 Canvas Host (可选)                                         │
│  • 创建 HTTP Server                                                │
│  • 创建 WebSocket Server                                           │
│  • 创建广播器 (broadcaster)                                        │
│  • 创建状态管理容器                                                │
└────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────────┐
│  Phase 4: 服务组件创建                                              │
│  ────────────────────────────────────────────────────────────────  │
│  • NodeRegistry (设备节点注册表)                                    │
│  • ChannelManager (消息渠道管理器)                                  │
│  • CronScheduler (定时任务调度器)                                   │
│  • ExecApprovalManager (命令审批管理器)                             │
│  • Discovery Service (服务发现)                                    │
└────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────────┐
│  Phase 5: WebSocket 处理器挂载 (attachGatewayWsHandlers)            │
│  ────────────────────────────────────────────────────────────────  │
│  • 挂载连接处理器                                                  │
│  • 挂载消息处理器                                                  │
│  • 注册所有 RPC 方法                                               │
└────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────────┐
│  Phase 6: 启动服务                                                  │
│  ────────────────────────────────────────────────────────────────  │
│  • 启动消息渠道 (Telegram/Discord/Slack...)                        │
│  • 启动浏览器控制服务                                              │
│  • 启动定时任务                                                    │
│  • 启动心跳 runner                                                 │
│  • 启动配置热加载监听                                              │
└────────────────────────────────────────────────────────────────────┘
         │
         ▼
    Gateway 启动完成! 🚀
```

---

## 三、启动阶段详解

### 3.1 Phase 1: 配置准备

**源码位置**: `src/gateway/server.impl.ts` (Line 147-211)

```typescript
export async function startGatewayServer(
  port = 18789,
  opts: GatewayServerOptions = {},
): Promise<GatewayServer> {
  
  // Step 1: 设置环境变量
  process.env.OPENCLAW_GATEWAY_PORT = String(port);
  
  // Step 2: 读取配置快照
  let configSnapshot = await readConfigFileSnapshot();
  // configSnapshot = {
  //   path: "~/.openclaw/config.yaml",
  //   exists: true,
  //   valid: true,
  //   parsed: { ... },
  //   config: { ... },
  //   legacyIssues: [],
  //   issues: []
  // }
  
  // Step 3: 迁移旧版配置
  if (configSnapshot.legacyIssues.length > 0) {
    const { config: migrated, changes } = migrateLegacyConfig(configSnapshot.parsed);
    if (migrated) {
      await writeConfigFile(migrated);
    }
  }
  
  // Step 4: 验证配置有效性
  configSnapshot = await readConfigFileSnapshot();
  if (configSnapshot.exists && !configSnapshot.valid) {
    throw new Error(`Invalid config. Run "openclaw doctor" to repair.`);
  }
  
  // Step 5: 自动启用插件
  const autoEnable = applyPluginAutoEnable({ config: configSnapshot.config, env: process.env });
  if (autoEnable.changes.length > 0) {
    await writeConfigFile(autoEnable.config);
  }
  
  // Step 6: 最终加载配置
  const cfgAtStart = loadConfig();
}
```

**配置文件结构** (`~/.openclaw/config.yaml`):

```yaml
gateway:
  bind: loopback          # loopback | lan | tailnet | auto
  port: 18789
  auth:
    token: "xxx"
    password: "xxx"
  tls:
    enabled: false
  controlUi:
    enabled: true

agents:
  defaults:
    model:
      provider: anthropic
      model: claude-sonnet-4-20250514

telegram:
  enabled: true
  token: "xxx"

discord:
  enabled: true
  token: "xxx"

# ... 更多配置
```

### 3.2 Phase 2: 核心组件初始化

**源码位置**: `src/gateway/server.impl.ts` (Line 212-270)

```typescript
// Step 7: 诊断与重启策略
const diagnosticsEnabled = isDiagnosticsEnabled(cfgAtStart);
if (diagnosticsEnabled) {
  startDiagnosticHeartbeat();
}
setGatewaySigusr1RestartPolicy({ allowExternal: cfgAtStart.commands?.restart === true });

// Step 8: 初始化子代理注册表
initSubagentRegistry();

// Step 9: 解析默认代理信息
const defaultAgentId = resolveDefaultAgentId(cfgAtStart);         // "main"
const defaultWorkspaceDir = resolveAgentWorkspaceDir(cfgAtStart, defaultAgentId);

// Step 10: 加载插件
const baseMethods = listGatewayMethods();
const { pluginRegistry, gatewayMethods: baseGatewayMethods } = loadGatewayPlugins({
  cfg: cfgAtStart,
  workspaceDir: defaultWorkspaceDir,
  log,
  coreGatewayHandlers,
  baseMethods,
});

// Step 11: 初始化渠道日志
const channelLogs = Object.fromEntries(
  listChannelPlugins().map((plugin) => [plugin.id, logChannels.child(plugin.id)]),
);
// channelLogs = { telegram: Logger, discord: Logger, slack: Logger, ... }

// Step 12: 合并所有 Gateway 方法
const channelMethods = listChannelPlugins().flatMap((plugin) => plugin.gatewayMethods ?? []);
const gatewayMethods = Array.from(new Set([...baseGatewayMethods, ...channelMethods]));

// Step 13: 解析运行时配置
const runtimeConfig = await resolveGatewayRuntimeConfig({
  cfg: cfgAtStart,
  port,
  bind: opts.bind,
  host: opts.host,
  controlUiEnabled: opts.controlUiEnabled,
  auth: opts.auth,
  tailscale: opts.tailscale,
});

const {
  bindHost,                  // "127.0.0.1"
  controlUiEnabled,          // true
  controlUiBasePath,         // "/ui"
  resolvedAuth,              // { tokenHash, passwordHash, ... }
  tailscaleConfig,
  hooksConfig,
  canvasHostEnabled,
} = runtimeConfig;

// Step 14: 创建依赖注入容器
const deps = createDefaultDeps();

// Step 15: 加载 TLS 配置
const gatewayTls = await loadGatewayTlsRuntime(cfgAtStart.gateway?.tls);
```

### 3.3 Phase 3: 运行时状态创建

**源码位置**: `src/gateway/server-runtime-state.ts`

```typescript
export async function createGatewayRuntimeState(params) {
  
  // Step 1: 创建 Canvas Host (可选)
  let canvasHost: CanvasHostHandler | null = null;
  if (params.canvasHostEnabled) {
    canvasHost = await createCanvasHostHandler({
      runtime: params.canvasRuntime,
      rootDir: params.cfg.canvasHost?.root,
      basePath: "/canvas",
    });
  }
  
  // Step 2: 创建请求处理器
  const handleHooksRequest = createGatewayHooksRequestHandler({ ... });
  const handlePluginRequest = createGatewayPluginRequestHandler({ ... });
  
  // Step 3: 解析绑定地址
  const bindHosts = await resolveGatewayListenHosts(params.bindHost);
  
  // Step 4: 创建 HTTP Server
  const httpServers: HttpServer[] = [];
  for (const host of bindHosts) {
    const httpServer = createGatewayHttpServer({
      canvasHost,
      controlUiEnabled: params.controlUiEnabled,
      controlUiBasePath: params.controlUiBasePath,
      openAiChatCompletionsEnabled: params.openAiChatCompletionsEnabled,
      handleHooksRequest,
      handlePluginRequest,
      resolvedAuth: params.resolvedAuth,
      tlsOptions: params.gatewayTls?.tlsOptions,
    });
    
    await listenGatewayHttpServer({ httpServer, bindHost: host, port: params.port });
    httpServers.push(httpServer);
  }
  
  // Step 5: 创建 WebSocket Server
  const wss = new WebSocketServer({
    noServer: true,                    // 不独立监听，使用 HTTP upgrade
    maxPayload: MAX_PAYLOAD_BYTES,     // 25MB
  });
  
  // 挂载 upgrade 处理
  for (const server of httpServers) {
    attachGatewayUpgradeHandler({ httpServer: server, wss, canvasHost });
  }
  
  // Step 6: 创建客户端集合和广播器
  const clients = new Set<GatewayWsClient>();
  const broadcast = createGatewayBroadcaster(clients);
  
  // Step 7: 创建状态管理容器
  const agentRunSeq = new Map<string, number>();        // 代理运行序号
  const dedupe = new Map<string, DedupeEntry>();        // 请求去重
  const chatRunState = createChatRunState();            // 聊天运行状态
  const chatAbortControllers = new Map();               // 中止控制器
  
  return {
    canvasHost,
    httpServer: httpServers[0],
    httpServers,
    wss,
    clients,
    broadcast,
    agentRunSeq,
    dedupe,
    chatRunState,
    chatAbortControllers,
    // ...
  };
}
```

### 3.4 Phase 4: 服务组件创建

**源码位置**: `src/gateway/server.impl.ts` (Line 310-400)

```typescript
// Step 16: 创建节点注册表
const nodeRegistry = new NodeRegistry();
const nodeSubscriptions = createNodeSubscriptionManager();

// 节点事件发送函数
const nodeSendEvent = (opts) => {
  nodeRegistry.sendEvent(opts.nodeId, opts.event, payload);
};

const nodeSendToSession = (sessionKey, event, payload) =>
  nodeSubscriptions.sendToSession(sessionKey, event, payload, nodeSendEvent);

// Step 17: 应用队列并发限制
applyGatewayLaneConcurrency(cfgAtStart);

// Step 18: 创建定时任务服务
let cronState = buildGatewayCronService({
  cfg: cfgAtStart,
  deps,
  broadcast,
});
let { cron, storePath: cronStorePath } = cronState;

// Step 19: 创建渠道管理器
const channelManager = createChannelManager({
  loadConfig,
  channelLogs,
  channelRuntimeEnvs,
});

const {
  getRuntimeSnapshot,     // 获取渠道运行时快照
  startChannels,          // 启动所有渠道
  startChannel,           // 启动单个渠道
  stopChannel,            // 停止单个渠道
} = channelManager;

// Step 20: 启动服务发现
const discovery = await startGatewayDiscovery({
  machineDisplayName: await getMachineDisplayName(),
  port,
  gatewayTls: gatewayTls.enabled ? { enabled: true, fingerprintSha256 } : undefined,
  mdnsMode: cfgAtStart.discovery?.mdns?.mode,
});

// Step 21: 启动维护定时器
const { tickInterval, healthInterval, dedupeCleanup } = startGatewayMaintenanceTimers({
  broadcast,
  nodeSendToAllSubscribed,
  getPresenceVersion,
  getHealthVersion,
  refreshGatewayHealthSnapshot,
  dedupe,
  chatAbortControllers,
  // ...
});

// Step 22: 订阅代理事件
const agentUnsub = onAgentEvent(
  createAgentEventHandler({
    broadcast,
    nodeSendToSession,
    agentRunSeq,
    chatRunState,
  }),
);

// Step 23: 订阅心跳事件
const heartbeatUnsub = onHeartbeatEvent((evt) => {
  broadcast("heartbeat", evt, { dropIfSlow: true });
});

let heartbeatRunner = startHeartbeatRunner({ cfg: cfgAtStart });

// Step 24: 启动定时任务
void cron.start();

// Step 25: 创建命令审批管理器
const execApprovalManager = new ExecApprovalManager();
const execApprovalHandlers = createExecApprovalHandlers(execApprovalManager, { ... });
```

### 3.5 Phase 5: WebSocket 处理器挂载

**源码位置**: `src/gateway/server-ws-runtime.ts`, `src/gateway/server/ws-connection.ts`

```typescript
// Step 26: 挂载 WebSocket 处理器
attachGatewayWsHandlers({
  wss,                          // WebSocket Server
  clients,                      // 客户端集合
  port,
  gatewayHost: bindHost,
  canvasHostEnabled: Boolean(canvasHost),
  resolvedAuth,                 // 认证配置
  gatewayMethods,               // 所有 RPC 方法名
  events: GATEWAY_EVENTS,       // 所有事件名
  extraHandlers: {
    ...pluginRegistry.gatewayHandlers,
    ...execApprovalHandlers,
  },
  broadcast,
  context: {                    // 请求上下文 (所有依赖)
    deps,
    cron,
    cronStorePath,
    loadGatewayModelCatalog,
    broadcast,
    nodeSendToSession,
    nodeRegistry,
    agentRunSeq,
    chatAbortControllers,
    addChatRun,
    removeChatRun,
    dedupe,
    wizardSessions,
    getRuntimeSnapshot,
    startChannel,
    stopChannel,
    // ... 更多
  },
});
```

**WebSocket 连接处理**:

```typescript
// src/gateway/server/ws-connection.ts

wss.on("connection", (socket, upgradeReq) => {
  const connId = randomUUID();
  
  // 1. 发送挑战 nonce
  send({
    type: "event",
    event: "connect.challenge",
    payload: { nonce: connectNonce, ts: Date.now() },
  });
  
  // 2. 设置握手超时
  const handshakeTimer = setTimeout(() => {
    if (!client) close();
  }, handshakeTimeoutMs);
  
  // 3. 挂载消息处理器
  attachGatewayWsMessageHandler({
    socket,
    connId,
    connectNonce,
    resolvedAuth,
    gatewayMethods,
    events,
    extraHandlers,
    buildRequestContext,
    send,
    close,
    setClient: (next) => {
      client = next;
      clients.add(next);
    },
  });
});
```

### 3.6 Phase 6: 启动服务

**源码位置**: `src/gateway/server.impl.ts` (Line 480-590)

```typescript
// Step 27: 记录启动日志
logGatewayStartup({
  cfg: cfgAtStart,
  bindHost,
  port,
  tlsEnabled: gatewayTls.enabled,
});

// Step 28: 启动 Tailscale 暴露 (可选)
const tailscaleCleanup = await startGatewayTailscaleExposure({
  tailscaleMode,
  port,
  controlUiBasePath,
});

// Step 29: 启动边车服务 (浏览器控制、渠道等)
({ browserControl, pluginServices } = await startGatewaySidecars({
  cfg: cfgAtStart,
  pluginRegistry,
  defaultWorkspaceDir,
  deps,
  startChannels,          // 启动所有消息渠道
}));

// Step 30: 创建热加载处理器
const { applyHotReload, requestGatewayRestart } = createGatewayReloadHandlers({
  deps,
  broadcast,
  getState: () => ({ hooksConfig, heartbeatRunner, cronState, browserControl }),
  setState: (nextState) => { /* 更新状态 */ },
  startChannel,
  stopChannel,
});

// Step 31: 启动配置文件监听
const configReloader = startGatewayConfigReloader({
  initialConfig: cfgAtStart,
  readSnapshot: readConfigFileSnapshot,
  onHotReload: applyHotReload,
  onRestart: requestGatewayRestart,
  watchPath: CONFIG_PATH,           // ~/.openclaw/config.yaml
});

// Step 32: 创建关闭处理器
const close = createGatewayCloseHandler({
  bonjourStop,
  tailscaleCleanup,
  canvasHost,
  stopChannel,
  pluginServices,
  cron,
  heartbeatRunner,
  broadcast,
  clients,
  configReloader,
  browserControl,
  wss,
  httpServer,
});

// Step 33: 返回 Gateway 实例
return {
  close: async (opts) => {
    if (diagnosticsEnabled) stopDiagnosticHeartbeat();
    skillsChangeUnsub();
    await close(opts);
  },
};
```

---

## 四、核心组件说明

### 4.1 组件列表

| 组件 | 源码位置 | 作用 |
|------|----------|------|
| **WebSocket Server** | `server-ws-runtime.ts` | 管理所有 WebSocket 连接 |
| **HTTP Server** | `server-http.ts` | 提供 HTTP 服务 (Control UI, API) |
| **Channel Manager** | `server-channels.ts` | 管理消息渠道生命周期 |
| **Node Registry** | `node-registry.ts` | 管理设备节点注册和通信 |
| **Cron Scheduler** | `server-cron.ts` | 管理定时任务 |
| **Config Reloader** | `config-reload.ts` | 配置热加载 |
| **Health Monitor** | `server/health-state.ts` | 健康状态监控 |
| **Discovery** | `server-discovery.ts` | mDNS 服务发现 |
| **Broadcast** | `server-broadcast.ts` | 事件广播 |
| **Auth** | `auth.ts` | 认证授权 |

### 4.2 HTTP 服务端点

```
HTTP Server (port 18789)
├── GET  /ui/*              → Control UI (静态资源)
├── POST /v1/chat/completions → OpenAI 兼容 API
├── POST /v1/responses      → OpenResponses API
├── POST /webhook/*         → Hooks 端点
├── GET  /canvas/*          → Canvas Host
├── ANY  /plugins/*         → 插件 HTTP 端点
└── UPGRADE /               → WebSocket 升级
```

### 4.3 消息渠道

```typescript
// Channel Manager 管理的渠道
const channels = {
  telegram: TelegramBot,     // grammY
  discord: DiscordBot,       // discord.js
  slack: SlackApp,           // Bolt
  signal: SignalClient,      // signal-cli
  web: WhatsAppWeb,          // Baileys
  imessage: iMessageClient,  // AppleScript/BlueBubbles
  line: LineBot,             // LINE SDK
  // ... 更多渠道
};
```

---

## 五、WebSocket 协议

> 完整的协议格式、帧结构和交互流程，请参见 **[GATEWAY-PROTOCOL.md](./GATEWAY-PROTOCOL.md)**。以下仅列出要点摘要。

### 5.1 帧格式

```typescript
// 请求帧 (客户端 → Gateway)
interface RequestFrame {
  type: "req";
  id: string;          // 请求ID
  method: string;      // 方法名
  params?: unknown;    // 参数
}

// 响应帧 (Gateway → 客户端)
interface ResponseFrame {
  type: "res";
  id: string;          // 对应的请求ID
  ok: boolean;         // 是否成功
  payload?: unknown;   // 数据
  error?: { code: number; message: string };
}

// 事件帧 (Gateway → 客户端)
interface EventFrame {
  type: "event";
  event: string;       // 事件类型
  payload: unknown;    // 数据
  seq?: number;        // 序列号
}
```

### 5.2 连接流程

```
Client                              Gateway
  │                                    │
  │ ──────── WebSocket Connect ──────► │
  │                                    │
  │ ◄────── event:connect.challenge ── │
  │         { nonce, ts }              │
  │                                    │
  │ ──────── req:connect ────────────► │
  │         { minProtocol, maxProtocol,│
  │           client, auth, device }   │
  │                                    │
  │ ◄────── res:hello-ok ───────────── │
  │         { protocol, server,        │
  │           features, snapshot }     │
  │                                    │
  │ ═══════ 连接建立完成 ═════════════ │
  │                                    │
  │ ──────── req:agent ──────────────► │
  │         { message, idempotencyKey }│
  │                                    │
  │ ◄────── res:agent (ack) ────────── │
  │         { runId, status:"accepted"}│
  │                                    │
  │ ◄────── event:agent (delta) ────── │
  │         { runId, type:"delta", ... }│
  │                                    │
  │ ◄────── res:agent (final) ──────── │
  │         { runId, status:"completed"}│
```

---

## 六、RPC 方法列表

> 完整的方法参数、返回值和事件列表，请参见 **[GATEWAY-PROTOCOL.md](./GATEWAY-PROTOCOL.md)**。以下列出方法注册结构和分类摘要。

### 6.1 方法注册

**源码位置**: `src/gateway/server-methods.ts`

```typescript
export const coreGatewayHandlers: GatewayRequestHandlers = {
  ...connectHandlers,       // connect
  ...logsHandlers,          // logs.tail
  ...voicewakeHandlers,     // voicewake.*
  ...healthHandlers,        // health
  ...channelsHandlers,      // channels.*
  ...chatHandlers,          // chat.*
  ...cronHandlers,          // cron.*
  ...deviceHandlers,        // device.*
  ...execApprovalsHandlers, // exec.approvals.*
  ...webHandlers,           // web.*
  ...modelsHandlers,        // models.*
  ...configHandlers,        // config.*
  ...wizardHandlers,        // wizard.*
  ...talkHandlers,          // talk.*
  ...ttsHandlers,           // tts.*
  ...skillsHandlers,        // skills.*
  ...sessionsHandlers,      // sessions.*
  ...systemHandlers,        // system-presence, last-heartbeat
  ...updateHandlers,        // update.*
  ...nodeHandlers,          // node.*
  ...sendHandlers,          // send
  ...usageHandlers,         // usage.*
  ...agentHandlers,         // agent, agent.wait
  ...agentsHandlers,        // agents.list
  ...browserHandlers,       // browser.*
};
```

### 6.2 方法分类

| 分类 | 方法 | 描述 |
|------|------|------|
| **代理** | `agent`, `agent.wait`, `chat.abort` | 执行 AI 代理任务 |
| **消息** | `send`, `chat.send`, `chat.history` | 发送消息 |
| **节点** | `node.list`, `node.invoke`, `node.pair.*` | 设备节点控制 |
| **渠道** | `channels.status`, `channels.logout` | 渠道管理 |
| **会话** | `sessions.list`, `sessions.preview`, `sessions.delete` | 会话管理 |
| **定时** | `cron.list`, `cron.add`, `cron.run` | 定时任务 |
| **系统** | `health`, `config.*`, `update.*` | 系统管理 |
| **浏览器** | `browser.status`, `browser.navigate`, `browser.snapshot` | 浏览器控制 |

### 6.3 事件列表

```typescript
// src/gateway/server-methods-list.ts

export const GATEWAY_EVENTS = [
  "tick",                    // 心跳 (每30秒)
  "agent",                   // 代理事件 (delta, tool, lifecycle)
  "chat",                    // 聊天事件 (delta, final)
  "presence",                // 在线状态变化
  "health",                  // 健康状态变化
  "heartbeat",               // 心跳事件
  "shutdown",                // 关闭通知
  "voicewake.changed",       // 语音唤醒配置变化
  "connect.challenge",       // 连接挑战
  "talk.mode",               // 对话模式变化
  "cron",                    // 定时任务事件
  "node.pair.requested",     // 节点配对请求
  "node.pair.resolved",      // 节点配对完成
  "node.invoke.request",     // 节点命令调用
  "device.pair.requested",   // 设备配对请求
  "device.pair.resolved",    // 设备配对完成
  "exec.approval.requested", // 命令审批请求
  "exec.approval.resolved",  // 命令审批完成
  // ... 完整列表见 GATEWAY-PROTOCOL.md
];
```

---

## 七、组装完成图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GatewayServer                                   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  HTTP Layer (port 18789)                                            │   │
│  │                                                                     │   │
│  │  ├── GET  /ui/*              → Control UI                           │   │
│  │  ├── POST /v1/chat/completions → OpenAI API                         │   │
│  │  ├── POST /v1/responses      → OpenResponses API                    │   │
│  │  ├── POST /webhook/*         → Hooks                                │   │
│  │  ├── GET  /canvas/*          → Canvas Host                          │   │
│  │  ├── ANY  /plugins/*         → Plugin HTTP                          │   │
│  │  └── UPGRADE /               → WebSocket                            │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  WebSocket Layer                                                    │   │
│  │                                                                     │   │
│  │  wss (WebSocketServer)                                              │   │
│  │  ├── clients: Set<GatewayWsClient>                                  │   │
│  │  ├── broadcast: (event, payload) => void                            │   │
│  │  │                                                                  │   │
│  │  │  RPC Methods                                                     │   │
│  │  │  ├── connect, agent, send, health                                │   │
│  │  │  ├── node.*, channels.*, sessions.*                              │   │
│  │  │  ├── cron.*, config.*, browser.*                                 │   │
│  │  │  └── ... (50+ methods)                                           │   │
│  │  │                                                                  │   │
│  │  │  Events                                                          │   │
│  │  │  ├── tick, agent, presence, health                               │   │
│  │  │  ├── heartbeat, shutdown                                         │   │
│  │  │  └── connect.challenge                                           │   │
│  │  │                                                                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Service Components                                                 │   │
│  │                                                                     │   │
│  │  ├── nodeRegistry        - 设备节点注册表                           │   │
│  │  ├── channelManager      - 消息渠道管理器                           │   │
│  │  │   ├── telegram, discord, slack                                   │   │
│  │  │   ├── signal, web (WhatsApp), imessage                           │   │
│  │  │   └── line, matrix, msteams, ...                                 │   │
│  │  ├── cron                - 定时任务调度器                           │   │
│  │  ├── heartbeatRunner     - 心跳运行器                               │   │
│  │  ├── discovery           - 服务发现 (mDNS)                          │   │
│  │  ├── execApprovalManager - 命令审批管理器                           │   │
│  │  └── configReloader      - 配置热加载器                             │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  State Management                                                   │   │
│  │                                                                     │   │
│  │  ├── dedupe              - 请求去重 (幂等性)                        │   │
│  │  ├── agentRunSeq         - 代理运行序号                             │   │
│  │  ├── chatRunState        - 聊天运行状态                             │   │
│  │  ├── chatAbortControllers - 中止控制器                              │   │
│  │  └── wizardSessions      - 向导会话                                 │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Sidecar Services                                                   │   │
│  │                                                                     │   │
│  │  ├── browserControl      - 浏览器控制服务 (port 18790)              │   │
│  │  ├── canvasHost          - Canvas Host (port 18793)                 │   │
│  │  └── pluginServices      - 插件服务                                 │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  close() → 优雅关闭所有组件                                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 八、关键源码索引

### 8.1 启动相关

| 文件 | 描述 |
|------|------|
| `src/gateway/server.ts` | 导出入口 |
| `src/gateway/server.impl.ts` | 主启动函数 `startGatewayServer()` |
| `src/gateway/server-runtime-state.ts` | 运行时状态创建 |
| `src/gateway/server-runtime-config.ts` | 运行时配置解析 |
| `src/gateway/server-startup.ts` | 边车服务启动 |
| `src/gateway/server-startup-log.ts` | 启动日志 |

### 8.2 WebSocket 相关

| 文件 | 描述 |
|------|------|
| `src/gateway/server-ws-runtime.ts` | WS 处理器挂载 |
| `src/gateway/server/ws-connection.ts` | WS 连接处理 |
| `src/gateway/server/ws-connection/message-handler.ts` | WS 消息处理 |
| `src/gateway/server-broadcast.ts` | 事件广播 |

### 8.3 RPC 方法相关

| 文件 | 描述 |
|------|------|
| `src/gateway/server-methods.ts` | 核心处理器聚合 |
| `src/gateway/server-methods-list.ts` | 方法和事件列表 |
| `src/gateway/server-methods/agent.ts` | agent 方法 |
| `src/gateway/server-methods/nodes.ts` | node.* 方法 |
| `src/gateway/server-methods/sessions.ts` | sessions.* 方法 |

### 8.4 服务组件相关

| 文件 | 描述 |
|------|------|
| `src/gateway/server-channels.ts` | 渠道管理器 |
| `src/gateway/node-registry.ts` | 节点注册表 |
| `src/gateway/server-cron.ts` | 定时任务 |
| `src/gateway/server-discovery.ts` | 服务发现 |
| `src/gateway/config-reload.ts` | 配置热加载 |
| `src/gateway/auth.ts` | 认证授权 |

---

## 总结

Gateway 的启动遵循清晰的六阶段模式：

| 阶段 | 关键步骤 | 目的 |
|------|----------|------|
| **Phase 1** | 配置准备 | 读取、验证、迁移配置 |
| **Phase 2** | 核心组件初始化 | 加载插件、解析配置、创建依赖 |
| **Phase 3** | 运行时状态创建 | HTTP/WS 服务器、广播器、状态 |
| **Phase 4** | 服务组件创建 | 节点、渠道、定时、发现 |
| **Phase 5** | WS处理器挂载 | 注册 RPC 方法、事件处理 |
| **Phase 6** | 启动服务 | 启动渠道、热加载、返回实例 |

**关键设计原则：**

1. **依赖注入** - 通过 `context` 传递所有依赖
2. **组件化** - 每个功能独立成模块
3. **可插拔** - 插件可扩展方法和处理器
4. **热加载** - 配置变更不重启
5. **优雅关闭** - 按顺序清理资源
