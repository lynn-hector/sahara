# Gateway WebSocket 协议详解

> 本文档详细介绍 OpenClaw Gateway 的 WebSocket 协议格式、帧结构和交互流程。

---

## 目录

1. [协议概述](#一协议概述)
2. [帧格式](#二帧格式)
3. [连接握手](#三连接握手)
4. [RPC 方法](#四rpc-方法)
5. [事件系统](#五事件系统)
6. [错误处理](#六错误处理)
7. [完整交互示例](#七完整交互示例)
8. [TypeBox Schema](#八typebox-schema)

---

## 一、协议概述

### 1.1 基本信息

| 属性 | 值 |
|------|-----|
| **传输层** | WebSocket (ws:// 或 wss://) |
| **默认端口** | 18789 |
| **帧格式** | JSON 文本帧 |
| **协议版本** | 13 (PROTOCOL_VERSION) |
| **最大负载** | 25MB (MAX_PAYLOAD_BYTES) |

### 1.2 协议特点

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Gateway 协议特点                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. 基于 JSON 的文本协议                                                    │
│     • 所有帧都是 JSON 对象                                                  │
│     • 使用 TypeBox 定义 Schema，Ajv 验证                                    │
│                                                                             │
│  2. 请求-响应模式 (RPC)                                                     │
│     • 客户端发送 req 帧，服务端返回 res 帧                                  │
│     • 通过 id 字段匹配请求和响应                                            │
│                                                                             │
│  3. 服务端推送 (Events)                                                     │
│     • 服务端主动发送 event 帧                                               │
│     • 支持序列号检测丢失                                                    │
│                                                                             │
│  4. 幂等性支持                                                              │
│     • 副作用方法需要 idempotencyKey                                         │
│     • 服务端保持短期去重缓存                                                │
│                                                                             │
│  5. 角色与权限                                                              │
│     • 支持 operator 和 node 两种角色                                        │
│     • 细粒度的 scope 权限控制                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 源码位置

| 文件 | 描述 |
|------|------|
| `src/gateway/protocol/index.ts` | 协议入口，验证器导出 |
| `src/gateway/protocol/schema/` | Schema 定义目录 |
| `src/gateway/protocol/schema/frames.ts` | 帧格式定义 |
| `src/gateway/protocol/schema/primitives.ts` | 基础类型 |
| `src/gateway/protocol/client-info.ts` | 客户端标识 |

---

## 二、帧格式

### 2.1 三种帧类型

Gateway 协议定义了三种帧类型，通过 `type` 字段区分：

```typescript
// 所有帧的联合类型
type GatewayFrame = RequestFrame | ResponseFrame | EventFrame;
```

### 2.2 请求帧 (RequestFrame)

**方向**: 客户端 → 服务端

```typescript
interface RequestFrame {
  type: "req";           // 固定值
  id: string;            // 请求ID (用于匹配响应，UUID 推荐)
  method: string;        // 方法名 (如 "agent", "send", "health")
  params?: unknown;      // 方法参数 (可选)
}
```

**示例**:

```json
{
  "type": "req",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "method": "agent",
  "params": {
    "message": "帮我写个Python脚本",
    "idempotencyKey": "idem-123456"
  }
}
```

### 2.3 响应帧 (ResponseFrame)

**方向**: 服务端 → 客户端

```typescript
interface ResponseFrame {
  type: "res";           // 固定值
  id: string;            // 对应的请求ID
  ok: boolean;           // 是否成功
  payload?: unknown;     // 成功时的数据
  error?: ErrorShape;    // 失败时的错误信息
}

interface ErrorShape {
  code: string;          // 错误码 (如 "INVALID_REQUEST")
  message: string;       // 错误消息
  details?: unknown;     // 详细信息 (可选)
  retryable?: boolean;   // 是否可重试
  retryAfterMs?: number; // 重试等待时间
}
```

**成功响应示例**:

```json
{
  "type": "res",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "ok": true,
  "payload": {
    "runId": "run-789",
    "status": "accepted"
  }
}
```

**失败响应示例**:

```json
{
  "type": "res",
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "ok": false,
  "error": {
    "code": "INVALID_REQUEST",
    "message": "missing required field: message",
    "retryable": false
  }
}
```

### 2.4 事件帧 (EventFrame)

**方向**: 服务端 → 客户端 (主动推送)

```typescript
interface EventFrame {
  type: "event";         // 固定值
  event: string;         // 事件类型 (如 "agent", "tick", "presence")
  payload?: unknown;     // 事件数据
  seq?: number;          // 序列号 (用于检测丢失)
  stateVersion?: {       // 状态版本 (可选)
    presence: number;
    health: number;
  };
}
```

**示例**:

```json
{
  "type": "event",
  "event": "agent",
  "payload": {
    "runId": "run-789",
    "seq": 1,
    "stream": "delta",
    "ts": 1706000000000,
    "data": {
      "type": "delta",
      "text": "我来帮你创建一个Python脚本..."
    }
  },
  "seq": 42
}
```

### 2.5 帧格式 Schema (TypeBox)

```typescript
// src/gateway/protocol/schema/frames.ts

import { Type } from "@sinclair/typebox";

// 请求帧
export const RequestFrameSchema = Type.Object({
  type: Type.Literal("req"),
  id: NonEmptyString,
  method: NonEmptyString,
  params: Type.Optional(Type.Unknown()),
}, { additionalProperties: false });

// 响应帧
export const ResponseFrameSchema = Type.Object({
  type: Type.Literal("res"),
  id: NonEmptyString,
  ok: Type.Boolean(),
  payload: Type.Optional(Type.Unknown()),
  error: Type.Optional(ErrorShapeSchema),
}, { additionalProperties: false });

// 事件帧
export const EventFrameSchema = Type.Object({
  type: Type.Literal("event"),
  event: NonEmptyString,
  payload: Type.Optional(Type.Unknown()),
  seq: Type.Optional(Type.Integer({ minimum: 0 })),
  stateVersion: Type.Optional(StateVersionSchema),
}, { additionalProperties: false });

// 联合类型 (带鉴别器)
export const GatewayFrameSchema = Type.Union(
  [RequestFrameSchema, ResponseFrameSchema, EventFrameSchema],
  { discriminator: "type" }
);
```

---

## 三、连接握手

### 3.1 握手流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            连接握手流程                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  Client                                                        Gateway
     │                                                             │
     │ ═══════════════ Step 1: WebSocket 连接 ══════════════════  │
     │                                                             │
     │  ────────────── WebSocket Connect ───────────────────────►  │
     │                 ws://127.0.0.1:18789                        │
     │                                                             │
     │ ═══════════════ Step 2: 挑战 ════════════════════════════  │
     │                                                             │
     │  ◄───────────── event:connect.challenge ─────────────────  │
     │                 {                                           │
     │                   "type": "event",                          │
     │                   "event": "connect.challenge",             │
     │                   "payload": {                              │
     │                     "nonce": "uuid-xxx",                    │
     │                     "ts": 1706000000000                     │
     │                   }                                         │
     │                 }                                           │
     │                                                             │
     │ ═══════════════ Step 3: 认证请求 ════════════════════════  │
     │                                                             │
     │  ────────────── req:connect ─────────────────────────────►  │
     │                 {                                           │
     │                   "type": "req",                            │
     │                   "id": "conn-1",                           │
     │                   "method": "connect",                      │
     │                   "params": {                               │
     │                     "minProtocol": 13,                      │
     │                     "maxProtocol": 13,                      │
     │                     "client": {                             │
     │                       "id": "openclaw-macos",               │
     │                       "version": "2026.2.1",                │
     │                       "platform": "darwin",                 │
     │                       "mode": "ui"                          │
     │                     },                                      │
     │                     "auth": { "token": "xxx" },             │
     │                     "device": { ... }                       │
     │                   }                                         │
     │                 }                                           │
     │                                                             │
     │ ═══════════════ Step 4: 认证响应 ════════════════════════  │
     │                                                             │
     │  ◄───────────── res:hello-ok ────────────────────────────  │
     │                 {                                           │
     │                   "type": "res",                            │
     │                   "id": "conn-1",                           │
     │                   "ok": true,                               │
     │                   "payload": {                              │
     │                     "type": "hello-ok",                     │
     │                     "protocol": 13,                         │
     │                     "server": { ... },                      │
     │                     "features": { ... },                    │
     │                     "snapshot": { ... }                     │
     │                   }                                         │
     │                 }                                           │
     │                                                             │
     │ ═══════════════ 连接建立完成! ═══════════════════════════  │
     │                                                             │
```

### 3.2 connect 参数详解

```typescript
// src/gateway/protocol/schema/frames.ts

interface ConnectParams {
  // 协议版本范围
  minProtocol: number;        // 最低支持版本
  maxProtocol: number;        // 最高支持版本
  
  // 客户端信息
  client: {
    id: GatewayClientId;      // 客户端标识
    displayName?: string;     // 显示名称
    version: string;          // 客户端版本
    platform: string;         // 平台 (darwin/linux/win32)
    deviceFamily?: string;    // 设备系列 (iPhone/iPad/Mac)
    modelIdentifier?: string; // 设备型号
    mode: GatewayClientMode;  // 运行模式
    instanceId?: string;      // 实例ID (区分多开)
  };
  
  // 能力声明 (用于 Node)
  caps?: string[];            // 能力列表 (如 ["browser", "camera"])
  commands?: string[];        // 命令列表 (如 ["camera.snap"])
  permissions?: Record<string, boolean>; // 权限声明
  pathEnv?: string;           // PATH 环境变量
  
  // 角色与权限
  role?: string;              // 角色 (operator/node)
  scopes?: string[];          // 权限范围
  
  // 设备配对 (可选)
  device?: {
    id: string;               // 设备ID
    publicKey: string;        // 公钥 (Base64)
    signature: string;        // 签名
    signedAt: number;         // 签名时间戳
    nonce?: string;           // 挑战 nonce
  };
  
  // 认证凭据 (可选)
  auth?: {
    token?: string;           // Token 认证
    password?: string;        // 密码认证
  };
  
  // 其他
  locale?: string;            // 语言区域
  userAgent?: string;         // User-Agent
}
```

### 3.3 客户端标识 (GatewayClientId)

```typescript
// src/gateway/protocol/client-info.ts

const GATEWAY_CLIENT_IDS = {
  WEBCHAT_UI: "webchat-ui",           // WebChat UI
  CONTROL_UI: "openclaw-control-ui",  // Control UI
  WEBCHAT: "webchat",                 // WebChat
  CLI: "cli",                         // CLI 客户端
  GATEWAY_CLIENT: "gateway-client",   // Gateway 内部客户端
  MACOS_APP: "openclaw-macos",        // macOS 应用
  IOS_APP: "openclaw-ios",            // iOS 应用
  ANDROID_APP: "openclaw-android",    // Android 应用
  NODE_HOST: "node-host",             // Node 主机
  TEST: "test",                       // 测试客户端
  FINGERPRINT: "fingerprint",         // 指纹客户端
  PROBE: "openclaw-probe",            // 探针客户端
};
```

### 3.4 客户端模式 (GatewayClientMode)

```typescript
const GATEWAY_CLIENT_MODES = {
  WEBCHAT: "webchat",     // WebChat 模式
  CLI: "cli",             // CLI 模式
  UI: "ui",               // UI 模式 (macOS/iOS/Android App)
  BACKEND: "backend",     // 后端模式 (内部调用)
  NODE: "node",           // Node 模式 (设备节点)
  PROBE: "probe",         // 探针模式
  TEST: "test",           // 测试模式
};
```

### 3.5 hello-ok 响应详解

```typescript
interface HelloOk {
  type: "hello-ok";
  
  // 协商的协议版本
  protocol: number;
  
  // 服务器信息
  server: {
    version: string;       // 服务器版本
    commit?: string;       // Git commit
    host?: string;         // 主机名
    connId: string;        // 连接ID
  };
  
  // 功能特性
  features: {
    methods: string[];     // 支持的方法列表
    events: string[];      // 支持的事件列表
  };
  
  // 初始状态快照
  snapshot: {
    presence: PresenceEntry[];  // 在线状态
    health: unknown;            // 健康状态
    stateVersion: {             // 状态版本
      presence: number;
      health: number;
    };
    uptimeMs: number;           // 运行时间
    configPath?: string;        // 配置文件路径
    stateDir?: string;          // 状态目录
    sessionDefaults?: {         // 会话默认值
      defaultAgentId: string;
      mainKey: string;
      mainSessionKey: string;
    };
  };
  
  // Canvas Host URL (可选)
  canvasHostUrl?: string;
  
  // 设备令牌 (配对后)
  auth?: {
    deviceToken: string;
    role: string;
    scopes: string[];
    issuedAtMs?: number;
  };
  
  // 协议策略
  policy: {
    maxPayload: number;        // 最大负载 (字节)
    maxBufferedBytes: number;  // 最大缓冲 (字节)
    tickIntervalMs: number;    // tick 间隔 (毫秒)
  };
}
```

---

## 四、RPC 方法

### 4.1 方法列表

```typescript
// src/gateway/server-methods-list.ts

const BASE_METHODS = [
  // 系统
  "health",
  "logs.tail",
  "status",
  "system-presence",
  "last-heartbeat",
  
  // 代理
  "agent",
  "agent.wait",
  "agent.identity.get",
  "agents.list",
  "wake",
  
  // 消息
  "send",
  "chat.history",
  "chat.send",
  "chat.abort",
  
  // 渠道
  "channels.status",
  "channels.logout",
  
  // 节点
  "node.list",
  "node.describe",
  "node.invoke",
  "node.invoke.result",
  "node.event",
  "node.pair.request",
  "node.pair.list",
  "node.pair.approve",
  "node.pair.reject",
  "node.pair.verify",
  "node.rename",
  
  // 设备配对
  "device.pair.list",
  "device.pair.approve",
  "device.pair.reject",
  "device.token.rotate",
  "device.token.revoke",
  
  // 会话
  "sessions.list",
  "sessions.preview",
  "sessions.patch",
  "sessions.reset",
  "sessions.delete",
  "sessions.compact",
  
  // 定时任务
  "cron.list",
  "cron.status",
  "cron.add",
  "cron.update",
  "cron.remove",
  "cron.run",
  "cron.runs",
  
  // 配置
  "config.get",
  "config.set",
  "config.apply",
  "config.patch",
  "config.schema",
  
  // 模型
  "models.list",
  
  // 技能
  "skills.status",
  "skills.bins",
  "skills.install",
  "skills.update",
  
  // TTS
  "tts.status",
  "tts.providers",
  "tts.enable",
  "tts.disable",
  "tts.convert",
  "tts.setProvider",
  
  // 使用量
  "usage.status",
  "usage.cost",
  
  // 向导
  "wizard.start",
  "wizard.next",
  "wizard.cancel",
  "wizard.status",
  
  // 命令审批
  "exec.approvals.get",
  "exec.approvals.set",
  "exec.approval.request",
  "exec.approval.resolve",
  
  // 浏览器
  "browser.request",
  
  // 语音唤醒
  "voicewake.get",
  "voicewake.set",
  
  // 更新
  "update.run",
  
  // Talk 模式
  "talk.mode",
];
```

### 4.2 核心方法详解

#### 4.2.1 agent - 执行代理任务

```typescript
// 请求
interface AgentParams {
  message: string;              // 用户消息 (必需)
  idempotencyKey: string;       // 幂等键 (必需)
  
  agentId?: string;             // 代理ID
  sessionId?: string;           // 会话ID
  sessionKey?: string;          // 会话Key
  to?: string;                  // 目标地址
  replyTo?: string;             // 回复地址
  thinking?: string;            // 思考级别 (off/low/medium/high)
  deliver?: boolean;            // 是否投递
  attachments?: unknown[];      // 附件
  channel?: string;             // 渠道
  accountId?: string;           // 账号ID
  threadId?: string;            // 线程ID
  groupId?: string;             // 群组ID
  timeout?: number;             // 超时时间
  lane?: string;                // 队列
  extraSystemPrompt?: string;   // 额外系统提示
  label?: string;               // 会话标签
  spawnedBy?: string;           // 创建者
}

// 响应 (确认)
{
  "runId": "run-xxx",
  "status": "accepted"
}

// 响应 (最终)
{
  "runId": "run-xxx",
  "status": "completed",
  "text": "我已经帮你创建了...",
  "summary": { ... }
}
```

**示例**:

```json
// 请求
{
  "type": "req",
  "id": "req-1",
  "method": "agent",
  "params": {
    "message": "帮我写一个排序算法",
    "idempotencyKey": "idem-abc123",
    "thinking": "medium"
  }
}

// 响应 (确认)
{
  "type": "res",
  "id": "req-1",
  "ok": true,
  "payload": {
    "runId": "run-789",
    "status": "accepted"
  }
}
```

#### 4.2.2 send - 发送消息

```typescript
interface SendParams {
  to: string;                   // 目标 (如 "telegram:123456")
  message: string;              // 消息内容
  idempotencyKey: string;       // 幂等键
  
  mediaUrl?: string;            // 媒体URL
  mediaUrls?: string[];         // 多个媒体URL
  gifPlayback?: boolean;        // GIF 播放
  channel?: string;             // 渠道
  accountId?: string;           // 账号ID
  sessionKey?: string;          // 会话Key (用于镜像)
}
```

**示例**:

```json
{
  "type": "req",
  "id": "req-2",
  "method": "send",
  "params": {
    "to": "telegram:123456",
    "message": "Hello, World!",
    "idempotencyKey": "idem-xyz789"
  }
}
```

#### 4.2.3 node.invoke - 调用节点命令

```typescript
interface NodeInvokeParams {
  nodeId: string;               // 节点ID
  command: string;              // 命令 (如 "camera.snap")
  idempotencyKey: string;       // 幂等键
  
  params?: unknown;             // 命令参数
  timeoutMs?: number;           // 超时时间
}
```

**示例**:

```json
{
  "type": "req",
  "id": "req-3",
  "method": "node.invoke",
  "params": {
    "nodeId": "iphone-xxx",
    "command": "camera.snap",
    "params": {
      "facing": "back",
      "quality": 0.8
    },
    "idempotencyKey": "idem-cam001"
  }
}
```

### 4.3 权限控制

```typescript
// src/gateway/server-methods.ts

const ADMIN_SCOPE = "operator.admin";
const READ_SCOPE = "operator.read";
const WRITE_SCOPE = "operator.write";
const APPROVALS_SCOPE = "operator.approvals";
const PAIRING_SCOPE = "operator.pairing";

// 只读方法 (需要 read 或 write scope)
const READ_METHODS = [
  "health", "channels.status", "models.list",
  "sessions.list", "cron.list", "node.list", ...
];

// 写入方法 (需要 write scope)
const WRITE_METHODS = [
  "send", "agent", "chat.send", "node.invoke", ...
];

// 配对方法 (需要 pairing scope)
const PAIRING_METHODS = [
  "node.pair.request", "node.pair.approve",
  "device.pair.approve", ...
];

// 管理方法 (需要 admin scope)
// config.*, wizard.*, cron.add, sessions.reset, ...
```

---

## 五、事件系统

### 5.1 事件列表

```typescript
// src/gateway/server-methods-list.ts

const GATEWAY_EVENTS = [
  "connect.challenge",      // 连接挑战
  "agent",                  // 代理事件
  "chat",                   // 聊天事件
  "presence",               // 在线状态
  "tick",                   // 心跳
  "talk.mode",              // Talk 模式变化
  "shutdown",               // 关闭通知
  "health",                 // 健康状态变化
  "heartbeat",              // 心跳事件
  "cron",                   // 定时任务事件
  "node.pair.requested",    // 节点配对请求
  "node.pair.resolved",     // 节点配对结果
  "node.invoke.request",    // 节点调用请求 (发给 Node)
  "device.pair.requested",  // 设备配对请求
  "device.pair.resolved",   // 设备配对结果
  "voicewake.changed",      // 语音唤醒变化
  "exec.approval.requested",// 命令审批请求
  "exec.approval.resolved", // 命令审批结果
];
```

### 5.2 核心事件详解

#### 5.2.1 tick - 心跳事件

```typescript
interface TickEvent {
  ts: number;  // 时间戳 (毫秒)
}
```

**示例**:

```json
{
  "type": "event",
  "event": "tick",
  "payload": {
    "ts": 1706000000000
  }
}
```

**说明**: Gateway 每 30 秒发送一次 tick，用于保持连接活跃和检测断连。

#### 5.2.2 agent - 代理事件

```typescript
interface AgentEvent {
  runId: string;      // 运行ID
  seq: number;        // 序列号
  stream: string;     // 流类型
  ts: number;         // 时间戳
  data: {             // 事件数据
    type: string;     // 数据类型
    // ... 其他字段
  };
}

// 流类型 (stream)
// - "delta"          文本增量
// - "tool_start"     工具开始
// - "tool_result"    工具结果
// - "reasoning"      推理内容
// - "final"          最终结果
```

**示例 (文本增量)**:

```json
{
  "type": "event",
  "event": "agent",
  "payload": {
    "runId": "run-789",
    "seq": 1,
    "stream": "delta",
    "ts": 1706000001000,
    "data": {
      "type": "delta",
      "text": "我来帮你实现"
    }
  }
}
```

**示例 (工具开始)**:

```json
{
  "type": "event",
  "event": "agent",
  "payload": {
    "runId": "run-789",
    "seq": 5,
    "stream": "tool",
    "ts": 1706000002000,
    "data": {
      "type": "tool_start",
      "tool": "write",
      "toolCallId": "call-123"
    }
  }
}
```

**示例 (工具结果)**:

```json
{
  "type": "event",
  "event": "agent",
  "payload": {
    "runId": "run-789",
    "seq": 6,
    "stream": "tool",
    "ts": 1706000003000,
    "data": {
      "type": "tool_result",
      "tool": "write",
      "toolCallId": "call-123",
      "result": "File written successfully"
    }
  }
}
```

#### 5.2.3 presence - 在线状态

```typescript
interface PresenceEntry {
  host?: string;           // 主机名
  ip?: string;             // IP 地址
  version?: string;        // 版本
  platform?: string;       // 平台
  deviceFamily?: string;   // 设备系列
  mode?: string;           // 模式
  lastInputSeconds?: number; // 上次输入距今秒数
  reason?: string;         // 原因 (connect/disconnect)
  tags?: string[];         // 标签
  text?: string;           // 文本
  ts: number;              // 时间戳
  deviceId?: string;       // 设备ID
  roles?: string[];        // 角色
  scopes?: string[];       // 权限
  instanceId?: string;     // 实例ID
}
```

**示例**:

```json
{
  "type": "event",
  "event": "presence",
  "payload": {
    "presence": [
      {
        "host": "MacBook-Pro",
        "platform": "darwin",
        "mode": "ui",
        "reason": "connect",
        "ts": 1706000000000,
        "deviceId": "device-abc"
      }
    ]
  },
  "stateVersion": {
    "presence": 5,
    "health": 3
  }
}
```

#### 5.2.4 shutdown - 关闭通知

```typescript
interface ShutdownEvent {
  reason: string;              // 关闭原因
  restartExpectedMs?: number;  // 预计重启时间 (毫秒)
}
```

**示例**:

```json
{
  "type": "event",
  "event": "shutdown",
  "payload": {
    "reason": "config-reload-restart",
    "restartExpectedMs": 5000
  }
}
```

#### 5.2.5 node.invoke.request - 节点调用请求

**方向**: Gateway → Node

```typescript
interface NodeInvokeRequestEvent {
  id: string;                 // 请求ID
  nodeId: string;             // 节点ID
  command: string;            // 命令
  paramsJSON?: string;        // 参数 (JSON 字符串)
  timeoutMs?: number;         // 超时
  idempotencyKey?: string;    // 幂等键
}
```

**示例**:

```json
{
  "type": "event",
  "event": "node.invoke.request",
  "payload": {
    "id": "invoke-123",
    "nodeId": "iphone-xxx",
    "command": "camera.snap",
    "paramsJSON": "{\"facing\":\"back\"}",
    "timeoutMs": 30000
  }
}
```

---

## 六、错误处理

### 6.1 错误码

```typescript
// src/gateway/protocol/schema/error-codes.ts

const ErrorCodes = {
  NOT_LINKED: "NOT_LINKED",           // 未链接
  NOT_PAIRED: "NOT_PAIRED",           // 未配对
  AGENT_TIMEOUT: "AGENT_TIMEOUT",     // 代理超时
  INVALID_REQUEST: "INVALID_REQUEST", // 无效请求
  UNAVAILABLE: "UNAVAILABLE",         // 服务不可用
};
```

### 6.2 错误响应格式

```typescript
interface ErrorShape {
  code: string;          // 错误码
  message: string;       // 错误消息
  details?: unknown;     // 详细信息
  retryable?: boolean;   // 是否可重试
  retryAfterMs?: number; // 重试等待时间
}
```

### 6.3 常见错误场景

| 错误码 | 场景 | 处理建议 |
|--------|------|----------|
| `INVALID_REQUEST` | 参数验证失败 | 检查请求参数 |
| `NOT_LINKED` | WhatsApp 未登录 | 引导用户登录 |
| `NOT_PAIRED` | 设备未配对 | 引导用户配对 |
| `AGENT_TIMEOUT` | 代理执行超时 | 重试或减少任务复杂度 |
| `UNAVAILABLE` | 服务暂时不可用 | 等待 retryAfterMs 后重试 |

**示例**:

```json
{
  "type": "res",
  "id": "req-1",
  "ok": false,
  "error": {
    "code": "AGENT_TIMEOUT",
    "message": "Agent execution timed out after 300000ms",
    "retryable": true,
    "retryAfterMs": 5000
  }
}
```

---

## 七、完整交互示例

### 7.1 执行代理任务的完整流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    完整交互示例: 执行代理任务                                │
└─────────────────────────────────────────────────────────────────────────────┘

  Client                                                        Gateway
     │                                                             │
     │ ═══════════════ 1. 连接握手 ═════════════════════════════  │
     │                                                             │
     │  ──── WebSocket Connect ──────────────────────────────────► │
     │                                                             │
     │  ◄─── event:connect.challenge ────────────────────────────  │
     │       { "nonce": "xxx", "ts": 1706000000000 }               │
     │                                                             │
     │  ──── req:connect ────────────────────────────────────────► │
     │       { "minProtocol": 13, "maxProtocol": 13, ... }         │
     │                                                             │
     │  ◄─── res (hello-ok) ─────────────────────────────────────  │
     │       { "protocol": 13, "features": {...}, ... }            │
     │                                                             │
     │ ═══════════════ 2. 发送代理请求 ═════════════════════════  │
     │                                                             │
     │  ──── req:agent ──────────────────────────────────────────► │
     │       {                                                     │
     │         "type": "req",                                      │
     │         "id": "req-1",                                      │
     │         "method": "agent",                                  │
     │         "params": {                                         │
     │           "message": "帮我写一个快速排序",                   │
     │           "idempotencyKey": "idem-001"                      │
     │         }                                                   │
     │       }                                                     │
     │                                                             │
     │ ═══════════════ 3. 接收确认响应 ═════════════════════════  │
     │                                                             │
     │  ◄─── res (ack) ──────────────────────────────────────────  │
     │       {                                                     │
     │         "type": "res",                                      │
     │         "id": "req-1",                                      │
     │         "ok": true,                                         │
     │         "payload": {                                        │
     │           "runId": "run-123",                               │
     │           "status": "accepted"                              │
     │         }                                                   │
     │       }                                                     │
     │                                                             │
     │ ═══════════════ 4. 接收流式事件 ═════════════════════════  │
     │                                                             │
     │  ◄─── event:agent (delta) ────────────────────────────────  │
     │       {                                                     │
     │         "runId": "run-123",                                 │
     │         "stream": "delta",                                  │
     │         "data": { "type": "delta", "text": "我来帮你" }     │
     │       }                                                     │
     │                                                             │
     │  ◄─── event:agent (delta) ────────────────────────────────  │
     │       { "data": { "type": "delta", "text": "实现快速排序" }}│
     │                                                             │
     │  ◄─── event:agent (tool_start) ───────────────────────────  │
     │       {                                                     │
     │         "data": {                                           │
     │           "type": "tool_start",                             │
     │           "tool": "write",                                  │
     │           "toolCallId": "call-001"                          │
     │         }                                                   │
     │       }                                                     │
     │                                                             │
     │  ◄─── event:agent (tool_result) ──────────────────────────  │
     │       {                                                     │
     │         "data": {                                           │
     │           "type": "tool_result",                            │
     │           "tool": "write",                                  │
     │           "result": "File written: quicksort.py"            │
     │         }                                                   │
     │       }                                                     │
     │                                                             │
     │  ◄─── event:agent (delta) ────────────────────────────────  │
     │       { "data": { "type": "delta", "text": "完成了!" }}     │
     │                                                             │
     │ ═══════════════ 5. 接收最终响应 ═════════════════════════  │
     │                                                             │
     │  ◄─── res (final) ────────────────────────────────────────  │
     │       {                                                     │
     │         "type": "res",                                      │
     │         "id": "req-1",                                      │
     │         "ok": true,                                         │
     │         "payload": {                                        │
     │           "runId": "run-123",                               │
     │           "status": "completed",                            │
     │           "text": "我来帮你实现快速排序...完成了!",          │
     │           "summary": {                                      │
     │             "toolCalls": 1,                                 │
     │             "durationMs": 5432                              │
     │           }                                                 │
     │         }                                                   │
     │       }                                                     │
     │                                                             │
     │ ═══════════════ 6. 保持连接 (心跳) ══════════════════════  │
     │                                                             │
     │  ◄─── event:tick ─────────────────────────────────────────  │
     │       { "ts": 1706000030000 }                               │
     │                                                             │
     │  ◄─── event:tick ─────────────────────────────────────────  │
     │       { "ts": 1706000060000 }                               │
     │                                                             │
```

---

## 八、TypeBox Schema

### 8.1 Schema 技术栈

Gateway 协议使用 [TypeBox](https://github.com/sinclairzx81/typebox) 定义 Schema：

```typescript
import { Type } from "@sinclair/typebox";

// 基础类型
const NonEmptyString = Type.String({ minLength: 1 });

// 复合类型
const RequestFrameSchema = Type.Object({
  type: Type.Literal("req"),
  id: NonEmptyString,
  method: NonEmptyString,
  params: Type.Optional(Type.Unknown()),
}, { additionalProperties: false });

// 使用 Ajv 编译验证器
import Ajv from "ajv";
const ajv = new Ajv({ allErrors: true, strict: false });
const validateRequestFrame = ajv.compile(RequestFrameSchema);
```

### 8.2 Schema 文件索引

| 文件 | 内容 |
|------|------|
| `schema/primitives.ts` | 基础类型 (NonEmptyString, GatewayClientId, ...) |
| `schema/frames.ts` | 帧格式 (RequestFrame, ResponseFrame, EventFrame) |
| `schema/agent.ts` | Agent 相关 (AgentParams, AgentEvent, ...) |
| `schema/nodes.ts` | Node 相关 (NodeInvokeParams, ...) |
| `schema/sessions.ts` | Session 相关 |
| `schema/channels.ts` | Channel 相关 |
| `schema/config.ts` | Config 相关 |
| `schema/cron.ts` | Cron 相关 |
| `schema/snapshot.ts` | Snapshot 相关 |
| `schema/error-codes.ts` | 错误码 |

### 8.3 验证示例

```typescript
import { validateRequestFrame, formatValidationErrors } from "./protocol/index.js";

function handleMessage(raw: string) {
  let frame: unknown;
  try {
    frame = JSON.parse(raw);
  } catch {
    return { error: "Invalid JSON" };
  }
  
  if (!validateRequestFrame(frame)) {
    const errors = formatValidationErrors(validateRequestFrame.errors);
    return { error: `Invalid frame: ${errors}` };
  }
  
  // frame 现在是类型安全的 RequestFrame
  const req = frame as RequestFrame;
  console.log(`Method: ${req.method}, ID: ${req.id}`);
}
```

---

## 总结

### 协议要点

| 要点 | 说明 |
|------|------|
| **传输** | WebSocket JSON 文本帧 |
| **帧类型** | req (请求) / res (响应) / event (事件) |
| **匹配** | 通过 `id` 字段匹配请求和响应 |
| **幂等** | 副作用方法需要 `idempotencyKey` |
| **认证** | 首帧必须是 connect，支持 token/password/设备配对 |
| **心跳** | tick 事件每 30 秒一次 |
| **验证** | TypeBox + Ajv 验证所有帧 |

### 关键文件

| 文件 | 描述 |
|------|------|
| `src/gateway/protocol/index.ts` | 协议入口 |
| `src/gateway/protocol/schema/frames.ts` | 帧格式定义 |
| `src/gateway/protocol/schema/agent.ts` | Agent 参数 |
| `src/gateway/server-methods-list.ts` | 方法和事件列表 |
| `docs/gateway/protocol.md` | 官方协议文档 |
