# Gateway 客户端通信详解

> 本文档详细介绍 OpenClaw 各类客户端如何与 Gateway 建立连接并通信。

---

## 目录

1. [客户端类型概览](#一客户端类型概览)
2. [Node.js 客户端](#二nodejs-客户端)
3. [浏览器客户端](#三浏览器客户端)
4. [移动端客户端](#四移动端客户端)
5. [连接生命周期](#五连接生命周期)
6. [认证机制](#六认证机制)
7. [请求/响应模式](#七请求响应模式)
8. [事件订阅](#八事件订阅)
9. [工具中的 Gateway 调用](#九工具中的-gateway-调用)
10. [最佳实践](#十最佳实践)

---

## 一、客户端类型概览

### 1.1 客户端分类

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Gateway 客户端生态                                   │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────┐
                              │    Gateway      │
                              │    Server       │
                              │  :18789         │
                              └────────┬────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
          ▼                            ▼                            ▼
  ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
  │  Node.js      │          │   Browser     │          │    Mobile     │
  │  Clients      │          │   Clients     │          │    Clients    │
  ├───────────────┤          ├───────────────┤          ├───────────────┤
  │ • CLI         │          │ • Control UI  │          │ • iOS App     │
  │ • Node Host   │          │ • WebChat     │          │ • Android App │
  │ • Agent Tools │          │               │          │               │
  │ • ACP Server  │          │               │          │               │
  └───────────────┘          └───────────────┘          └───────────────┘
```

### 1.2 客户端标识

```typescript
// src/gateway/protocol/client-info.ts

const GATEWAY_CLIENT_IDS = {
  // UI 客户端
  WEBCHAT_UI: "webchat-ui",
  CONTROL_UI: "openclaw-control-ui",
  
  // CLI 客户端
  CLI: "cli",
  GATEWAY_CLIENT: "gateway-client",
  
  // 应用客户端
  MACOS_APP: "openclaw-macos",
  IOS_APP: "openclaw-ios",
  ANDROID_APP: "openclaw-android",
  
  // 设备节点
  NODE_HOST: "node-host",
  
  // 特殊用途
  TEST: "test",
  PROBE: "openclaw-probe",
  FINGERPRINT: "fingerprint",
};

const GATEWAY_CLIENT_MODES = {
  WEBCHAT: "webchat",    // WebChat 模式
  CLI: "cli",            // CLI 模式
  UI: "ui",              // 桌面/移动 App 模式
  BACKEND: "backend",    // 后端服务模式
  NODE: "node",          // 设备节点模式
  PROBE: "probe",        // 探针模式
  TEST: "test",          // 测试模式
};
```

### 1.3 关键源文件

| 文件 | 描述 |
|------|------|
| `src/gateway/client.ts` | Node.js GatewayClient 类 |
| `src/gateway/call.ts` | 单次调用辅助函数 |
| `ui/src/ui/gateway.ts` | 浏览器 GatewayBrowserClient |
| `apps/android/.../GatewaySession.kt` | Android 客户端 |
| `apps/ios/.../GatewaySession.swift` | iOS 客户端 |

---

## 二、Node.js 客户端

### 2.1 GatewayClient 类

`GatewayClient` 是 Node.js 环境下与 Gateway 通信的核心类：

```typescript
// src/gateway/client.ts

export class GatewayClient {
  private ws: WebSocket | null = null;
  private pending = new Map<string, Pending>();  // 等待响应的请求
  private backoffMs = 1000;                       // 重连退避时间
  private closed = false;                         // 是否已关闭
  private lastSeq: number | null = null;          // 最后收到的序列号
  private connectNonce: string | null = null;     // 连接挑战 nonce
  private connectSent = false;                    // 是否已发送 connect
  private lastTick: number | null = null;         // 最后 tick 时间
  
  constructor(opts: GatewayClientOptions) {
    this.opts = {
      ...opts,
      deviceIdentity: opts.deviceIdentity ?? loadOrCreateDeviceIdentity(),
    };
  }
  
  // 启动连接
  start(): void;
  
  // 停止连接
  stop(): void;
  
  // 发送请求
  request<T>(method: string, params?: unknown, opts?: { expectFinal?: boolean }): Promise<T>;
}
```

### 2.2 配置选项

```typescript
type GatewayClientOptions = {
  // 连接配置
  url?: string;                    // Gateway URL (默认 ws://127.0.0.1:18789)
  token?: string;                  // 认证 Token
  password?: string;               // 认证密码
  tlsFingerprint?: string;         // TLS 证书指纹 (用于自签名证书)
  
  // 客户端信息
  instanceId?: string;             // 实例 ID (区分多开)
  clientName?: GatewayClientName;  // 客户端标识
  clientDisplayName?: string;      // 显示名称
  clientVersion?: string;          // 版本号
  platform?: string;               // 平台 (darwin/linux/win32)
  mode?: GatewayClientMode;        // 运行模式
  
  // 角色与权限
  role?: string;                   // 角色 (operator/node)
  scopes?: string[];               // 权限范围
  
  // Node 特有
  caps?: string[];                 // 能力列表 (如 ["browser", "camera"])
  commands?: string[];             // 命令列表 (如 ["camera.snap"])
  permissions?: Record<string, boolean>;
  pathEnv?: string;                // PATH 环境变量
  
  // 协议版本
  minProtocol?: number;
  maxProtocol?: number;
  
  // 设备身份
  deviceIdentity?: DeviceIdentity;
  
  // 回调函数
  onEvent?: (evt: EventFrame) => void;       // 事件回调
  onHelloOk?: (hello: HelloOk) => void;      // 连接成功回调
  onConnectError?: (err: Error) => void;     // 连接错误回调
  onClose?: (code: number, reason: string) => void;  // 关闭回调
  onGap?: (info: { expected: number; received: number }) => void;  // 序列号间隙回调
};
```

### 2.3 使用示例

```typescript
import { GatewayClient } from "./gateway/client.js";

const client = new GatewayClient({
  url: "ws://127.0.0.1:18789",
  clientName: "cli",
  clientVersion: "2026.2.1",
  mode: "cli",
  role: "operator",
  scopes: ["operator.admin"],
  
  onHelloOk: (hello) => {
    console.log("Connected!", hello.server.version);
  },
  
  onEvent: (evt) => {
    if (evt.event === "agent") {
      console.log("Agent event:", evt.payload);
    }
  },
  
  onClose: (code, reason) => {
    console.log(`Disconnected: ${code} - ${reason}`);
  },
});

// 启动连接
client.start();

// 发送请求
try {
  const result = await client.request("health");
  console.log("Health:", result);
} catch (err) {
  console.error("Request failed:", err);
}

// 发送 agent 请求 (等待最终响应)
const agentResult = await client.request("agent", {
  message: "Hello, world!",
  idempotencyKey: "key-123",
}, { expectFinal: true });

// 停止连接
client.stop();
```

### 2.4 callGateway 辅助函数

对于单次调用场景，可使用 `callGateway` 函数：

```typescript
// src/gateway/call.ts

import { callGateway } from "./gateway/call.js";

// 简单调用
const health = await callGateway({
  method: "health",
});

// 带参数调用
const result = await callGateway({
  method: "agent",
  params: {
    message: "Write a hello world program",
    idempotencyKey: "idem-001",
  },
  expectFinal: true,      // 等待最终响应
  timeoutMs: 60_000,      // 超时时间
});

// 指定 URL 和认证
const remoteResult = await callGateway({
  url: "wss://gateway.example.com",
  token: "my-secret-token",
  method: "status",
  tlsFingerprint: "AA:BB:CC:...",  // 自签名证书指纹
});
```

---

## 三、浏览器客户端

### 3.1 GatewayBrowserClient 类

浏览器环境使用专门的客户端实现：

```typescript
// ui/src/ui/gateway.ts

export class GatewayBrowserClient {
  private ws: WebSocket | null = null;
  private pending = new Map<string, Pending>();
  private closed = false;
  private lastSeq: number | null = null;
  private connectNonce: string | null = null;
  private connectSent = false;
  private backoffMs = 800;
  
  constructor(private opts: GatewayBrowserClientOptions) {}
  
  // 启动连接
  start(): void;
  
  // 停止连接
  stop(): void;
  
  // 连接状态
  get connected(): boolean;
  
  // 发送请求
  request<T = unknown>(method: string, params?: unknown): Promise<T>;
}
```

### 3.2 浏览器特殊处理

```typescript
// 浏览器客户端的特殊考虑

// 1. 设备身份 - 需要 crypto.subtle (仅 HTTPS/localhost)
const isSecureContext = typeof crypto !== "undefined" && !!crypto.subtle;

if (isSecureContext) {
  // 使用 IndexedDB 存储设备密钥
  deviceIdentity = await loadOrCreateDeviceIdentity();
} else {
  // 不安全上下文，回退到 token-only 认证
  // 需要 gateway.controlUi.allowInsecureAuth=true
}

// 2. 重连退避 - 使用 window.setTimeout
window.setTimeout(() => this.connect(), delay);

// 3. 关闭代码 - 浏览器拒绝 1008，使用 4008
const CONNECT_FAILED_CLOSE_CODE = 4008;
```

### 3.3 使用示例

```typescript
import { GatewayBrowserClient } from "./gateway";

const client = new GatewayBrowserClient({
  url: `ws://${window.location.host}`,
  clientName: "openclaw-control-ui",
  clientVersion: "2026.2.1",
  mode: "webchat",
  
  onHello: (hello) => {
    console.log("Connected to Gateway");
    // 获取初始状态快照
    const snapshot = hello.snapshot;
  },
  
  onEvent: (evt) => {
    switch (evt.event) {
      case "agent":
        handleAgentEvent(evt.payload);
        break;
      case "presence":
        updatePresence(evt.payload);
        break;
      case "tick":
        // 心跳
        break;
    }
  },
  
  onClose: ({ code, reason }) => {
    showDisconnectedMessage(`${code}: ${reason}`);
  },
  
  onGap: ({ expected, received }) => {
    console.warn(`Event gap: expected ${expected}, got ${received}`);
  },
});

client.start();

// 发送聊天消息
async function sendChat(message: string) {
  const result = await client.request("chat.send", {
    message,
    sessionKey: "global",
    idempotencyKey: crypto.randomUUID(),
  });
  return result;
}
```

---

## 四、移动端客户端

### 4.1 Android 客户端 (Kotlin)

```kotlin
// apps/android/app/src/main/java/ai/openclaw/android/gateway/GatewaySession.kt

class GatewaySession(
  private val scope: CoroutineScope,
  private val identityStore: DeviceIdentityStore,
  private val deviceAuthStore: DeviceAuthStore,
  private val onConnected: (serverName: String?, remoteAddress: String?, mainSessionKey: String?) -> Unit,
  private val onDisconnected: (message: String) -> Unit,
  private val onEvent: (event: String, payloadJson: String?) -> Unit,
  private val onInvoke: (suspend (InvokeRequest) -> InvokeResult)? = null,
) {
  // 连接到 Gateway
  fun connect(
    endpoint: GatewayEndpoint,
    token: String?,
    password: String?,
    options: GatewayConnectOptions,
    tls: GatewayTlsParams? = null,
  )
  
  // 断开连接
  fun disconnect()
  
  // 重新连接
  fun reconnect()
  
  // 发送请求
  suspend fun request(method: String, paramsJson: String?, timeoutMs: Long = 15_000): String
  
  // 发送 Node 事件
  suspend fun sendNodeEvent(event: String, payloadJson: String?)
}
```

### 4.2 Android 使用示例

```kotlin
// 创建 Gateway 会话
val session = GatewaySession(
  scope = lifecycleScope,
  identityStore = deviceIdentityStore,
  deviceAuthStore = deviceAuthStore,
  onConnected = { serverName, remoteAddress, mainSessionKey ->
    Log.i("Gateway", "Connected to $serverName at $remoteAddress")
    updateConnectionStatus(true)
  },
  onDisconnected = { message ->
    Log.i("Gateway", "Disconnected: $message")
    updateConnectionStatus(false)
  },
  onEvent = { event, payloadJson ->
    when (event) {
      "agent" -> handleAgentEvent(payloadJson)
      "presence" -> handlePresenceEvent(payloadJson)
      "node.invoke.request" -> handleInvokeRequest(payloadJson)
    }
  },
  onInvoke = { request ->
    // 处理来自 Gateway 的调用请求
    when (request.command) {
      "camera.snap" -> handleCameraSnap(request)
      "location.get" -> handleLocationGet(request)
      else -> GatewaySession.InvokeResult.error("UNKNOWN_COMMAND", "Unknown: ${request.command}")
    }
  },
)

// 连接配置
val options = GatewayConnectOptions(
  role = "node",
  scopes = listOf("node.invoke"),
  caps = listOf("camera", "location", "notify"),
  commands = listOf("camera.snap", "location.get", "notify.show"),
  permissions = mapOf("camera" to true, "location" to true),
  client = GatewayClientInfo(
    id = "openclaw-android",
    displayName = Build.MODEL,
    version = BuildConfig.VERSION_NAME,
    platform = "android",
    mode = "node",
    instanceId = UUID.randomUUID().toString(),
    deviceFamily = "Android",
    modelIdentifier = Build.MODEL,
  ),
)

// 连接
session.connect(
  endpoint = GatewayEndpoint(host = "192.168.1.100", port = 18789),
  token = savedToken,
  password = null,
  options = options,
)

// 发送请求
lifecycleScope.launch {
  try {
    val result = session.request("health", null)
    Log.i("Gateway", "Health: $result")
  } catch (e: Exception) {
    Log.e("Gateway", "Request failed", e)
  }
}
```

### 4.3 iOS 客户端 (Swift)

iOS 客户端结构类似，使用 URLSessionWebSocketTask：

```swift
// apps/ios/Sources/Gateway/GatewaySession.swift (概念示例)

class GatewaySession: ObservableObject {
  @Published var isConnected = false
  @Published var serverVersion: String?
  
  private var webSocketTask: URLSessionWebSocketTask?
  private var pending: [String: CheckedContinuation<Data, Error>] = [:]
  
  func connect(to endpoint: GatewayEndpoint, options: GatewayConnectOptions) async throws
  func disconnect()
  func request<T: Decodable>(_ method: String, params: Encodable?) async throws -> T
}
```

---

## 五、连接生命周期

### 5.1 完整生命周期

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        连接生命周期                                          │
└─────────────────────────────────────────────────────────────────────────────┘

  Client                                                        Gateway
     │                                                             │
     │ ═══════════════ Phase 1: 建立 WebSocket ═════════════════  │
     │                                                             │
     │  ──── new WebSocket(url) ────────────────────────────────►  │
     │                                                             │
     │  ◄─── onopen ────────────────────────────────────────────  │
     │                                                             │
     │ ═══════════════ Phase 2: 握手挑战 ═══════════════════════  │
     │                                                             │
     │  ◄─── event: connect.challenge ──────────────────────────  │
     │       { nonce: "uuid-xxx", ts: 1706000000000 }              │
     │                                                             │
     │ ═══════════════ Phase 3: 发送 connect ═══════════════════  │
     │                                                             │
     │  ──── req: connect ──────────────────────────────────────►  │
     │       {                                                     │
     │         minProtocol: 13,                                    │
     │         maxProtocol: 13,                                    │
     │         client: { id, version, platform, mode },            │
     │         auth: { token, password },                          │
     │         device: { id, publicKey, signature, nonce },        │
     │         role: "operator",                                   │
     │         scopes: ["operator.admin"],                         │
     │       }                                                     │
     │                                                             │
     │ ═══════════════ Phase 4: 接收 hello-ok ══════════════════  │
     │                                                             │
     │  ◄─── res: hello-ok ─────────────────────────────────────  │
     │       {                                                     │
     │         protocol: 13,                                       │
     │         server: { version, connId },                        │
     │         features: { methods, events },                      │
     │         snapshot: { presence, health },                     │
     │         auth: { deviceToken, role, scopes },                │
     │         policy: { tickIntervalMs: 30000 },                  │
     │       }                                                     │
     │                                                             │
     │ ═══════════════ Phase 5: 正常通信 ═══════════════════════  │
     │                                                             │
     │  ──── req: method ───────────────────────────────────────►  │
     │  ◄─── res ───────────────────────────────────────────────  │
     │                                                             │
     │  ◄─── event: tick ───────────────────────────────────────  │
     │  ◄─── event: agent ──────────────────────────────────────  │
     │  ◄─── event: presence ───────────────────────────────────  │
     │                                                             │
     │ ═══════════════ Phase 6: 关闭/重连 ══════════════════════  │
     │                                                             │
     │  ◄─── onclose (code, reason) ────────────────────────────  │
     │                                                             │
     │  [backoff delay]                                            │
     │                                                             │
     │  ──── reconnect (回到 Phase 1) ──────────────────────────►  │
     │                                                             │
```

### 5.2 重连机制

```typescript
// GatewayClient 重连逻辑

private scheduleReconnect() {
  if (this.closed) return;
  
  // 清理 tick 监控
  if (this.tickTimer) {
    clearInterval(this.tickTimer);
    this.tickTimer = null;
  }
  
  const delay = this.backoffMs;
  
  // 指数退避: 1s → 2s → 4s → 8s → 16s → 30s (max)
  this.backoffMs = Math.min(this.backoffMs * 2, 30_000);
  
  // 延迟后重连
  setTimeout(() => this.start(), delay).unref();
}

// 连接成功后重置退避
private onConnectSuccess() {
  this.backoffMs = 1000;
}
```

### 5.3 心跳检测

```typescript
// Tick 超时检测

private startTickWatch() {
  if (this.tickTimer) {
    clearInterval(this.tickTimer);
  }
  
  const interval = Math.max(this.tickIntervalMs, 1000);
  
  this.tickTimer = setInterval(() => {
    if (this.closed || !this.lastTick) return;
    
    const gap = Date.now() - this.lastTick;
    
    // 如果超过 2 倍 tick 间隔没有收到 tick，认为连接已死
    if (gap > this.tickIntervalMs * 2) {
      this.ws?.close(4000, "tick timeout");
    }
  }, interval);
}

// 收到 tick 时更新时间
private handleTick() {
  this.lastTick = Date.now();
}
```

---

## 六、认证机制

### 6.1 认证方式

Gateway 支持多种认证方式：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           认证方式                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Token 认证                                                              │
│     • 配置: gateway.auth.token                                              │
│     • 环境变量: OPENCLAW_GATEWAY_TOKEN                                      │
│     • 适用: CLI、远程访问                                                   │
│                                                                             │
│  2. Password 认证                                                           │
│     • 配置: gateway.auth.password                                           │
│     • 环境变量: OPENCLAW_GATEWAY_PASSWORD                                   │
│     • 适用: 简单场景                                                        │
│                                                                             │
│  3. 设备身份认证 (Device Identity)                                          │
│     • 基于 Ed25519 密钥对                                                   │
│     • 设备 ID + 签名验证                                                    │
│     • 适用: App、Node Host                                                  │
│                                                                             │
│  4. 设备令牌 (Device Token)                                                 │
│     • 首次配对后签发                                                        │
│     • 存储在客户端，后续自动使用                                            │
│     • 适用: 已配对设备                                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 设备身份认证流程

```typescript
// 构建设备认证参数

const signedAtMs = Date.now();
const nonce = this.connectNonce;  // 从 connect.challenge 获取

// 构建待签名数据
const payload = buildDeviceAuthPayload({
  deviceId: deviceIdentity.deviceId,
  clientId: "openclaw-macos",
  clientMode: "ui",
  role: "operator",
  scopes: ["operator.admin"],
  signedAtMs,
  token: authToken ?? null,
  nonce,
});

// 使用私钥签名
const signature = signDevicePayload(deviceIdentity.privateKeyPem, payload);

// connect 参数中的 device 字段
const device = {
  id: deviceIdentity.deviceId,
  publicKey: publicKeyRawBase64UrlFromPem(deviceIdentity.publicKeyPem),
  signature,
  signedAt: signedAtMs,
  nonce,
};
```

### 6.3 设备令牌存储与使用

```typescript
// 首次连接成功后，存储返回的 deviceToken
const helloOk = await client.request("connect", params);

if (helloOk.auth?.deviceToken && deviceIdentity) {
  storeDeviceAuthToken({
    deviceId: deviceIdentity.deviceId,
    role: helloOk.auth.role ?? "operator",
    token: helloOk.auth.deviceToken,
    scopes: helloOk.auth.scopes ?? [],
  });
}

// 后续连接时，优先使用存储的 deviceToken
const storedToken = loadDeviceAuthToken({
  deviceId: deviceIdentity.deviceId,
  role: "operator",
})?.token;

const authToken = storedToken ?? opts.token;
```

---

## 七、请求/响应模式

### 7.1 请求发送

```typescript
// GatewayClient.request()

async request<T = Record<string, unknown>>(
  method: string,
  params?: unknown,
  opts?: { expectFinal?: boolean },
): Promise<T> {
  // 1. 检查连接状态
  if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
    throw new Error("gateway not connected");
  }
  
  // 2. 生成请求 ID
  const id = randomUUID();
  
  // 3. 构建请求帧
  const frame: RequestFrame = { type: "req", id, method, params };
  
  // 4. 验证帧格式
  if (!validateRequestFrame(frame)) {
    throw new Error("invalid request frame");
  }
  
  // 5. 创建 Promise 并存储
  const expectFinal = opts?.expectFinal === true;
  const p = new Promise<T>((resolve, reject) => {
    this.pending.set(id, {
      resolve: (value) => resolve(value as T),
      reject,
      expectFinal,
    });
  });
  
  // 6. 发送请求
  this.ws.send(JSON.stringify(frame));
  
  // 7. 返回 Promise
  return p;
}
```

### 7.2 响应处理

```typescript
// 处理响应帧

private handleMessage(raw: string) {
  const parsed = JSON.parse(raw);
  
  if (validateResponseFrame(parsed)) {
    const pending = this.pending.get(parsed.id);
    if (!pending) return;
    
    // 如果期望最终响应，且当前是 accepted 状态，继续等待
    const status = (parsed.payload as { status?: unknown })?.status;
    if (pending.expectFinal && status === "accepted") {
      return;  // 不删除 pending，继续等待 final 响应
    }
    
    // 删除 pending 并解析
    this.pending.delete(parsed.id);
    
    if (parsed.ok) {
      pending.resolve(parsed.payload);
    } else {
      pending.reject(new Error(parsed.error?.message ?? "unknown error"));
    }
  }
}
```

### 7.3 expectFinal 模式

对于 `agent` 等双响应方法，使用 `expectFinal: true`：

```typescript
// 普通模式: 收到第一个响应就返回 (accepted)
const ack = await client.request("agent", params);
// ack = { runId: "...", status: "accepted" }

// expectFinal 模式: 等待最终响应 (ok/error)
const result = await client.request("agent", params, { expectFinal: true });
// result = { runId: "...", status: "ok", result: {...} }
```

---

## 八、事件订阅

### 8.1 事件处理

```typescript
const client = new GatewayClient({
  url: "ws://127.0.0.1:18789",
  
  onEvent: (evt: EventFrame) => {
    console.log(`Event: ${evt.event}, seq: ${evt.seq}`);
    
    switch (evt.event) {
      case "tick":
        // 心跳事件
        break;
        
      case "agent":
        // Agent 执行事件
        const payload = evt.payload as AgentEventPayload;
        handleAgentEvent(payload);
        break;
        
      case "presence":
        // 在线状态变化
        updatePresence(evt.payload);
        break;
        
      case "chat":
        // 聊天消息
        handleChatEvent(evt.payload);
        break;
        
      case "health":
        // 健康状态变化
        updateHealth(evt.payload);
        break;
        
      case "shutdown":
        // Gateway 关闭通知
        const shutdown = evt.payload as { reason: string; restartExpectedMs?: number };
        console.log(`Gateway shutting down: ${shutdown.reason}`);
        break;
        
      case "node.invoke.request":
        // (Node 客户端) 接收调用请求
        handleInvokeRequest(evt.payload);
        break;
    }
  },
  
  onGap: ({ expected, received }) => {
    // 序列号间隙检测
    console.warn(`Event gap: expected ${expected}, got ${received}`);
    // 可能需要重新获取状态
  },
});
```

### 8.2 Agent 事件处理

```typescript
function handleAgentEvent(payload: AgentEventPayload) {
  const { runId, stream, seq, data, sessionKey } = payload;
  
  switch (stream) {
    case "assistant":
      // 文本增量
      if (data.text) {
        appendText(runId, data.text);
      }
      break;
      
    case "tool":
      // 工具执行
      if (data.type === "tool_start") {
        showToolStart(runId, data.tool, data.toolCallId);
      } else if (data.type === "tool_result") {
        showToolResult(runId, data.toolCallId, data.result);
      }
      break;
      
    case "lifecycle":
      // 生命周期
      if (data.phase === "start") {
        onAgentStart(runId, data.startedAt);
      } else if (data.phase === "end") {
        onAgentEnd(runId, data.endedAt, data.aborted);
      } else if (data.phase === "error") {
        onAgentError(runId, data.error);
      }
      break;
      
    case "reasoning":
      // 推理内容 (thinking)
      if (data.text) {
        showReasoning(runId, data.text);
      }
      break;
  }
}
```

---

## 九、工具中的 Gateway 调用

### 9.1 Agent Tool 调用 Gateway

Agent 执行时的工具可以通过 Gateway 与外部交互：

```typescript
// src/agents/tools/gateway.ts

import { callGateway } from "../../gateway/call.js";

export async function callGatewayTool<T = Record<string, unknown>>(
  method: string,
  opts: GatewayCallOptions,
  params?: unknown,
  extra?: { expectFinal?: boolean },
) {
  const gateway = resolveGatewayOptions(opts);
  
  return await callGateway<T>({
    url: gateway.url,
    token: gateway.token,
    method,
    params,
    timeoutMs: gateway.timeoutMs,
    expectFinal: extra?.expectFinal,
    clientName: GATEWAY_CLIENT_NAMES.GATEWAY_CLIENT,
    clientDisplayName: "agent",
    mode: GATEWAY_CLIENT_MODES.BACKEND,
  });
}
```

### 9.2 使用示例

```typescript
// 节点调用工具 (nodes tool)
const result = await callGatewayTool("node.invoke", gatewayOpts, {
  nodeId: "iphone-xxx",
  command: "camera.snap",
  params: { facing: "back" },
  idempotencyKey: randomUUID(),
});

// 发送消息工具 (send tool)
await callGatewayTool("send", gatewayOpts, {
  to: "telegram:123456",
  message: "Hello from agent!",
  idempotencyKey: randomUUID(),
});

// 定时任务工具 (cron tool)
await callGatewayTool("cron.add", gatewayOpts, {
  name: "daily-report",
  schedule: "0 9 * * *",
  message: "Generate daily report",
  idempotencyKey: randomUUID(),
});
```

---

## 十、最佳实践

### 10.1 连接管理

```typescript
// 1. 使用单例模式管理长连接
class GatewayManager {
  private static instance: GatewayClient | null = null;
  
  static getClient(): GatewayClient {
    if (!this.instance) {
      this.instance = new GatewayClient({
        url: "ws://127.0.0.1:18789",
        // ...
      });
      this.instance.start();
    }
    return this.instance;
  }
  
  static shutdown() {
    this.instance?.stop();
    this.instance = null;
  }
}

// 2. 对于一次性调用，使用 callGateway
const result = await callGateway({ method: "health" });
```

### 10.2 错误处理

```typescript
// 1. 捕获连接错误
const client = new GatewayClient({
  onConnectError: (err) => {
    if (err.message.includes("ECONNREFUSED")) {
      showNotification("Gateway not running");
    } else if (err.message.includes("authentication failed")) {
      showNotification("Invalid credentials");
    }
  },
  onClose: (code, reason) => {
    if (code === 1008) {
      // Policy violation - 可能是认证问题
    } else if (code === 1012) {
      // Service restart - 等待重连
    }
  },
});

// 2. 请求错误处理
try {
  const result = await client.request("agent", params, { expectFinal: true });
} catch (err) {
  if (err.message.includes("timeout")) {
    // 请求超时
  } else if (err.message.includes("gateway closed")) {
    // 连接已断开
  } else {
    // 其他错误
  }
}
```

### 10.3 幂等性

```typescript
// 对于有副作用的方法，始终提供 idempotencyKey
await client.request("agent", {
  message: "Do something",
  idempotencyKey: generateIdempotencyKey(),  // UUID
});

await client.request("send", {
  to: "telegram:123",
  message: "Hello",
  idempotencyKey: generateIdempotencyKey(),
});

// 幂等键生成
function generateIdempotencyKey(): string {
  return randomUUID();
}
```

### 10.4 超时设置

```typescript
// 根据操作类型设置合适的超时
const TIMEOUTS = {
  health: 5_000,       // 健康检查: 5s
  status: 10_000,      // 状态查询: 10s
  agent: 300_000,      // Agent 执行: 5min
  "node.invoke": 60_000,  // 节点调用: 1min
};

await callGateway({
  method: "agent",
  params,
  timeoutMs: TIMEOUTS.agent,
  expectFinal: true,
});
```

---

## 总结

### 客户端选择指南

| 场景 | 推荐 | 说明 |
|------|------|------|
| CLI 工具 | `callGateway` | 一次性调用，自动管理连接 |
| 后台服务 | `GatewayClient` | 长连接，支持事件订阅 |
| Web UI | `GatewayBrowserClient` | 浏览器环境，自动重连 |
| 移动 App | 原生实现 | Kotlin/Swift 原生 WebSocket |
| Node Host | `GatewayClient` (node mode) | 设备节点，接收调用请求 |

### 关键要点

| 要点 | 描述 |
|------|------|
| **协议版本** | 当前 PROTOCOL_VERSION = 13 |
| **默认端口** | 18789 |
| **认证** | Token / Password / Device Identity |
| **心跳** | tick 事件每 30s，超时自动重连 |
| **重连** | 指数退避 (1s → 30s max) |
| **幂等性** | 副作用方法需要 idempotencyKey |

### 相关文档

| 文档 | 描述 |
|------|------|
| [GATEWAY-PROTOCOL.md](./GATEWAY-PROTOCOL.md) | 协议格式详解 |
| [GATEWAY-STARTUP.md](./GATEWAY-STARTUP.md) | Gateway 启动流程 |
| [GATEWAY-AGENT-DISPATCH.md](./GATEWAY-AGENT-DISPATCH.md) | Agent 调度流程 |
| [GATEWAY-ARCHITECTURE.md](./GATEWAY-ARCHITECTURE.md) | Gateway 架构总览 |
