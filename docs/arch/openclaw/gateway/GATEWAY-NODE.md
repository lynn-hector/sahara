# Gateway 设备节点管理

> 本文档详解 Gateway 的设备节点（Node）系统：如何注册、配对和管理 macOS/iOS/Android 远程设备，以及 Agent 如何通过 Gateway 调用设备能力（拍照、录屏、系统命令等）。

---

## 目录

- [一、节点系统的角色](#一节点系统的角色)
- [二、节点注册与连接](#二节点注册与连接)
- [三、节点配对](#三节点配对)
- [四、平台与能力](#四平台与能力)
- [五、命令调用 (Invoke)](#五命令调用-invoke)
- [六、Agent 节点工具](#六agent-节点工具)
- [七、节点事件](#七节点事件)
- [八、多节点与选择](#八多节点与选择)
- [九、命令策略](#九命令策略)
- [十、配置参考](#十配置参考)
- [十一、关键源文件索引](#十一关键源文件索引)

---

## 一、节点系统的角色

节点系统让 Agent 能够**控制远程设备**——拍照、录屏、执行系统命令、获取位置等。每个设备运行一个节点客户端（macOS App / iOS App / Android App），通过 WebSocket 连接到 Gateway。

```text
Agent Runtime                Gateway                    设备节点
    │                           │                           │
    │  nodes tool               │                           │
    │  (camera_snap)            │                           │
    │ ────────────────────────→ │                           │
    │                           │  node.invoke.request      │
    │                           │ ────────────────────────→ │
    │                           │                           │  拍照
    │                           │                           │  ↓
    │                           │  node.invoke.result       │
    │                           │ ←──────────────────────── │
    │  ← { base64Image }       │                           │
    │ ←──────────────────────── │                           │
```

---

## 二、节点注册与连接

### 2.1 注册流程

节点在 WebSocket 握手时自动注册（`role: "node"`）：

```text
节点客户端 → WebSocket connect
    │
    ├── connect params:
    │   {
    │     client: { id, mode, version, platform },
    │     device: { id, publicKey, signature },  ← 设备身份
    │     role: "node",                          ← 标识为节点
    │     caps: ["canvas"],                       ← 能力声明
    │     commands: ["camera.snap", "system.run"], ← 支持的命令
    │     permissions: { camera: true, location: true },
    │     pathEnv: "/usr/bin:/usr/local/bin"
    │   }
    │
    ▼
Gateway:
    ├── 验证设备签名 + 配对状态
    ├── nodeRegistry.register(client, opts)
    │   → 创建 NodeSession
    │   → 存入 nodesById Map
    ├── 更新配对元数据 (lastConnectedAtMs)
    ├── 异步探测远程 bins
    └── 发送语音唤醒配置快照
```

### 2.2 NodeSession 数据结构

```typescript
type NodeSession = {
  nodeId: string;               // 唯一节点 ID
  connId: string;               // WebSocket 连接 ID
  client: GatewayWsClient;      // WebSocket 客户端引用
  displayName?: string;         // 显示名称
  platform?: string;            // 平台 (ios/android/macos/...)
  version?: string;             // 节点版本
  deviceFamily?: string;        // 设备系列
  modelIdentifier?: string;     // 硬件型号
  caps: string[];               // 能力列表
  commands: string[];           // 支持的命令列表
  permissions?: Record<string, boolean>;  // 权限标志
  pathEnv?: string;             // PATH 环境变量
  connectedAtMs: number;        // 连接时间戳
};
```

### 2.3 断线与重连

- **断线检测**: WebSocket `close` 事件触发 `nodeRegistry.unregister(connId)`
- **待处理请求**: 断线时所有 pending invoke 被拒绝（"node disconnected"）
- **重连**: 节点重连时替换旧的 NodeSession（同 nodeId 覆盖）
- **配对持久化**: 配对元数据跨断线保留，`lastConnectedAtMs` 重连时更新

---

## 三、节点配对

### 3.1 配对流程

> 源文件: `src/infra/node-pairing.ts`

```text
新节点首次连接
    │
    ▼
① node.pair.request → Gateway
    │  元数据: nodeId, displayName, platform, caps, commands
    │
    ▼
② Gateway 存储请求到 pending.json (TTL: 5 分钟)
    │  广播 node.pair.requested 事件
    │
    ▼
③ 管理员操作: node.pair.approve
    │  生成节点 Token
    │  存储到 paired.json
    │
    ▼
④ 节点使用 Token 重连 → 认证成功
```

### 3.2 节点配对 vs 设备配对

| 维度 | 节点配对 | 设备配对 |
| ---- | ---- | ---- |
| 存储路径 | `~/.openclaw/nodes/` | `~/.openclaw/devices/` |
| 角色 | `role: "node"` | `role: "operator"` |
| 用途 | 远程设备执行能力 | 客户端管理控制 |
| RPC 前缀 | `node.pair.*` | `device.pair.*` |

### 3.3 配对 RPC 方法

| 方法 | 说明 |
| ---- | ---- |
| `node.pair.request` | 请求配对 |
| `node.pair.list` | 列出待处理/已配对节点 |
| `node.pair.approve` | 批准配对请求 |
| `node.pair.reject` | 拒绝配对请求 |
| `node.pair.verify` | 验证节点 Token |
| `node.rename` | 重命名已配对节点 |

---

## 四、平台与能力

### 4.1 支持的平台

| 平台 | 标识 | 设备 | 典型能力 |
| ---- | ---- | ---- | ---- |
| macOS | `macos` | Mac 电脑 | camera, screen, system.run, browser.proxy |
| iOS | `ios` | iPhone/iPad | camera, screen, location, canvas |
| Android | `android` | Android 设备 | camera, screen, location, sms.send, canvas |
| Linux | `linux` | Linux 主机 | system.run |
| Windows | `windows` | Windows PC | system.run |

### 4.2 平台默认命令

> 源文件: `src/gateway/node-command-policy.ts`

| 命令类别 | iOS | Android | macOS | Linux/Windows |
| ---- | ---- | ---- | ---- | ---- |
| `canvas.*` (UI 呈现) | 是 | 是 | 是 | 否 |
| `camera.list/snap/clip` | 是 | 是 | 是 | 否 |
| `screen.record` | 是 | 是 | 是 | 否 |
| `location.get` | 是 | 是 | 是 | 否 |
| `system.run/which/notify` | 否 | 否 | 是 | 是 |
| `system.execApprovals.*` | 否 | 否 | 是 | 否 |
| `browser.proxy` | 否 | 否 | 是 | 否 |
| `sms.send` | 否 | 是 | 否 | 否 |

### 4.3 能力声明

节点在连接时通过 `caps` 和 `commands` 两个字段声明能力：

- **`caps`**: 高层能力标签（如 `["canvas"]`），用于节点选择
- **`commands`**: 具体命令列表（如 `["camera.snap", "system.run"]`），用于调用验证

命令调用时会**同时检查**: 命令在节点的 `commands` 中 **且** 在平台策略允许列表中。

---

## 五、命令调用 (Invoke)

### 5.1 调用流程

> 源文件: `src/gateway/node-registry.ts`

```text
Agent 或客户端发起: node.invoke RPC
    │
    ▼
① Gateway 验证:
    ├── 节点是否在线? → 否: NOT_CONNECTED
    ├── 命令是否在节点 commands 中? → 否: COMMAND_NOT_SUPPORTED
    └── 命令是否通过策略检查? → 否: COMMAND_DENIED
    │
    ▼
② 创建 PendingInvoke:
    { requestId: UUID, resolve, reject, timeoutMs }
    存入 pendingInvokes Map
    │
    ▼
③ 发送到节点:
    node.invoke.request 事件 via WebSocket
    { id, command, paramsJSON }
    │
    ▼
④ 节点执行命令 (拍照/录屏/shell等)
    │
    ▼
⑤ 节点返回: node.invoke.result RPC
    { requestId, ok, payloadJSON, error }
    │
    ▼
⑥ Gateway resolve PendingInvoke
    → 返回给调用方
```

### 5.2 超时机制

- 默认超时: 30 秒
- 可通过 `timeoutMs` 参数覆盖
- 超时后自动 reject PendingInvoke
- 节点断线时立即 reject 所有 pending

### 5.3 幂等性

`idempotencyKey` 参数支持请求去重——相同 key 的重复调用不会再次执行。

---

## 六、Agent 节点工具

### 6.1 `nodes` 工具

> 源文件: `src/agents/tools/nodes-tool.ts`

LLM 通过 `nodes` 工具与设备节点交互，支持 12 种 action：

| Action | 对应命令 | 说明 |
| ---- | ---- | ---- |
| `status` | `node.list` | 列出所有节点 |
| `describe` | `node.describe` | 获取节点详情 |
| `pending` | `node.pair.list` | 查看待配对节点 |
| `approve` | `node.pair.approve` | 批准配对 |
| `reject` | `node.pair.reject` | 拒绝配对 |
| `notify` | `system.notify` | 发送系统通知 |
| `camera_snap` | `camera.snap` | 拍照 |
| `camera_list` | `camera.list` | 列出摄像头 |
| `camera_clip` | `camera.clip` | 录制视频片段 |
| `screen_record` | `screen.record` | 录制屏幕 |
| `location_get` | `location.get` | 获取设备位置 |
| `run` | `system.run` | 执行系统命令 |

### 6.2 LLM 使用示例

```text
LLM: 我需要看看用户的桌面截图

→ nodes({ action: "screen_record", nodeId: "mac-1", durationSeconds: 5 })

→ Gateway → node.invoke.request → macOS 节点
→ 节点录屏 5 秒 → 返回视频数据
→ LLM 收到结果
```

### 6.3 节点选择

工具调用时的 `nodeId` 解析：

```text
resolveNodeId(input)
    │
    ├── 精确匹配 nodeId
    ├── 匹配 displayName
    ├── 匹配 remoteIp
    ├── 前缀匹配
    │
    └── 未指定 → pickDefaultNode()
         ├── 优先: 有 canvas 能力的已连接节点
         ├── 偏好: macOS 节点 (mac- 前缀)
         └── 回退: 任意已连接 canvas 节点
```

---

## 七、节点事件

### 7.1 事件系统

> 源文件: `src/gateway/server-node-events.ts`

节点通过 `node.event` RPC 向 Gateway 发送事件：

| 事件类型 | 说明 | 触发的 Gateway 动作 |
| ---- | ---- | ---- |
| `voice.transcript` | 语音转录文本 | 触发 Agent 运行 |
| `agent.request` | Agent 深链接请求 | 触发 Agent 运行 (支持投递) |
| `chat.subscribe` | 订阅会话事件 | 注册节点到会话 |
| `chat.unsubscribe` | 取消订阅 | 取消注册 |
| `exec.started` | 命令执行开始 | 记录系统事件 + 唤醒心跳 |
| `exec.finished` | 命令执行完成 | 记录系统事件 + 唤醒心跳 |
| `exec.denied` | 命令执行被拒 | 记录系统事件 + 唤醒心跳 |

### 7.2 会话订阅

节点可以订阅特定会话的聊天事件：

```text
节点 → chat.subscribe({ sessionKey: "agent:default:main" })
    → Gateway 注册到 NodeSubscriptionManager
    → Agent 运行时产生的 chat 事件会转发到该节点

节点 → chat.unsubscribe({ sessionKey: "..." })
    → 取消注册
```

多个节点可以订阅同一会话；每个节点独立管理订阅集合。

---

## 八、多节点与选择

### 8.1 多节点支持

Gateway 支持**同时连接多个节点**：

```text
┌───────────────┐
│  Gateway      │
│               │
│  nodesById:   │
│  ├── mac-1 ──────→ MacBook Pro (macOS)
│  ├── iphone-1 ───→ iPhone 15 (iOS)
│  └── pixel-1 ────→ Pixel 8 (Android)
└───────────────┘
```

### 8.2 节点发现

`node.list` RPC 合并两个来源：

```text
listDevicePairing()          ← 已配对节点 (持久化, 可能离线)
nodeRegistry.listConnected() ← 当前连接节点 (实时)
    │
    ▼
合并 (nodeId 去重, 实时数据优先)
    │
    ▼
返回: [{ nodeId, displayName, platform, connected, caps, commands, ... }]
```

### 8.3 默认节点选择

当 `nodeId` 未指定时，自动选择：

1. 优先选择有 `canvas` 能力的已连接节点
2. 偏好 macOS 节点
3. 回退到任意已连接节点

---

## 九、命令策略

### 9.1 策略解析

> 源文件: `src/gateway/node-command-policy.ts`

命令必须通过三重检查才能执行：

```text
命令: "camera.snap"
    │
    ├── ① 节点声明了此命令?
    │   → 检查 node.commands.includes("camera.snap")
    │
    ├── ② 平台默认允许?
    │   → 检查 platformDefaults[platform].includes("camera.snap")
    │
    └── ③ 配置未拒绝?
        → 不在 gateway.nodes.denyCommands 中
        → 或在 gateway.nodes.allowCommands 中 (覆盖)
```

### 9.2 策略叠加

```text
最终允许列表 = (平台默认 + allowCommands) - denyCommands

实际可执行 = 最终允许列表 ∩ 节点声明的 commands
```

---

## 十、配置参考

```yaml
gateway:
  nodes:
    browser:
      mode: "auto"          # "auto" | "manual" | "off"
      node: "mac-1"         # 固定到指定节点

    allowCommands:           # 额外允许的命令
      - "sms.send"

    denyCommands:            # 拒绝的命令
      - "system.run"
```

| 配置项 | 说明 | 默认值 |
| ---- | ---- | ---- |
| `browser.mode` | 浏览器路由模式 | `"auto"` |
| `browser.node` | 固定浏览器节点 | (无, 自动选择) |
| `allowCommands` | 额外允许的命令 | `[]` |
| `denyCommands` | 拒绝的命令 | `[]` |

---

## 十一、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/gateway/node-registry.ts` | NodeRegistry：注册、注销、invoke、结果处理 |
| `src/gateway/server-methods/nodes.ts` | 节点 RPC 方法（invoke/pair/list/describe） |
| `src/gateway/server-node-events.ts` | 节点事件处理（voice/agent/exec/chat） |
| `src/gateway/server-node-events-types.ts` | 节点事件类型定义 |
| `src/gateway/server-node-subscriptions.ts` | 会话订阅管理 |
| `src/gateway/node-command-policy.ts` | 命令策略：平台默认 + allow/deny |
| `src/infra/node-pairing.ts` | 节点配对持久化 |
| `src/agents/tools/nodes-tool.ts` | `nodes` Agent 工具（12 种 action） |
| `src/agents/tools/nodes-utils.ts` | 节点解析辅助（resolveNodeId, pickDefaultNode） |
| `src/config/types.gateway.ts` | 节点配置类型 (`GatewayNodesConfig`) |
