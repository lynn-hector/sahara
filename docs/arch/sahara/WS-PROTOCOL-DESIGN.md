# WebSocket 协议设计（C 端多实例版）

> Client ↔ Gateway 之间的实时通信协议。
> 面向 C 端多用户、多 Gateway 实例、断线恢复等场景设计，采用 JSON RPC 帧格式 + 双响应模式 + 幂等键 + 序列号。
>
> 关联文档：
> - [gRPC 协议设计](./GRPC-PROTOCOL-DESIGN.md) — Gateway ↔ Runtime 内部通信（D1）
> - [Gateway 架构设计](./GATEWAY-ARCHITECTURE-DESIGN.md) — Go Gateway 实现蓝图（D3）
> - [Runtime 架构设计](./RUNTIME-ARCHITECTURE-DESIGN.md) — Python Runtime 实现蓝图（D4）
> - [Event Bus 架构设计](./EVENT-BUS-DESIGN.md) — 事件从 Runtime 传递到 Gateway 的异步路径（D5）

---

## 目录

1. [概述与设计目标](#一概述与设计目标)
2. [设计决策与取舍](#二设计决策与取舍)
3. [帧格式](#三帧格式)
4. [连接生命周期](#四连接生命周期)
5. [认证协议](#五认证协议)
6. [RPC 方法定义](#六rpc-方法定义)
7. [事件定义](#七事件定义)
8. [双响应模式](#八双响应模式)
9. [断线恢复与事件回放](#九断线恢复与事件回放)
10. [多实例与连接路由](#十多实例与连接路由)
11. [限频与背压](#十一限频与背压)
12. [错误处理](#十二错误处理)
13. [心跳与超时](#十三心跳与超时)
14. [安全](#十四安全)
15. [客户端 SDK 指南](#十五客户端-sdk-指南)
16. [性能指标](#十六性能指标)
17. [附录](#附录)
    - [A. WS 关闭码](#附录-a-ws-关闭码)
    - [B. 与工程计划的任务映射](#附录-b-与工程计划的任务映射)
    - [C. OpenClaw 参考与设计对比](#附录-c-openclaw-参考与设计对比)

---

## 一、概述与设计目标

### 1.1 协议在架构中的位置

```text
C 端用户 (Web/App/小程序)
    │
    │ ★ WebSocket (本文档)
    │
    ▼
┌────────────┐       gRPC        ┌─────────────┐
│  Gateway   │──────────────────▶│  Runtime    │
│  (Go)      │◀── Event Bus ────│  Worker (Py)│
└────────────┘                   └─────────────┘
```

### 1.2 设计目标

| # | 目标 | 说明 |
| --- | --- | --- |
| G1 | **C 端就绪** | JWT 多用户认证；用户级权限隔离；无管理面暴露 |
| G2 | **多实例透明** | 客户端不感知后端有多少 Gateway 实例；LB 断连后重连到任意实例均可恢复 |
| G3 | **断线恢复** | 携带 `resumeToken` 重连后，Gateway 从 Event Bus 回放错过的事件 |
| G4 | **简洁成熟的帧格式** | req/res/event 三种帧类型、双响应模式、幂等键、序列号 |
| G5 | **客户端极简** | 方法集聚焦 C 端用户操作，不暴露运维/配置/节点管理 |

### 1.3 基本参数

| 属性 | 值 |
| --- | --- |
| 传输层 | WebSocket (wss://) |
| 帧格式 | JSON 文本帧 |
| 协议版本 | 1 (`SAHARA_PROTOCOL_VERSION`) |
| 最大帧大小 | 1MB (`MAX_FRAME_BYTES`) |
| 心跳间隔 | 30s (服务端 `tick` 事件) |
| 编码 | UTF-8 |

---

## 二、设计决策与取舍

Sahara WS 协议面向 C 端高并发云部署场景，以下是关键设计决策：

| 维度 | 设计选择 | 理由 |
| --- | --- | --- |
| **认证** | JWT (短期 15min) + Refresh Token | C 端多用户，无状态鉴权，支持水平扩展 |
| **部署模式** | 多实例云端 `wss://` + LB | 水平扩展，单实例故障不影响其他用户 |
| **方法集** | ~15 个（纯用户操作） | C 端不暴露管理面，最小攻击面 |
| **事件传递** | Event Bus (Redis Streams) 异步投递 | Gateway 与 Runtime 跨进程解耦 |
| **握手方式** | HTTP Upgrade 阶段 JWT 验证 | 一个 RTT 完成认证，简洁高效 |
| **断线恢复** | `resumeToken` + 事件回放 | 网络波动不丢事件，用户体验连续 |
| **序列号** | 按 session 级别的 seq | 多 session 互不干扰，回放范围精确 |
| **限频** | 三层（连接/用户/全局） | C 端防滥用，保护后端资源 |

---

## 三、帧格式

### 3.1 三种帧类型

协议定义三种帧类型，通过 `type` 字段区分：

```text
┌────────────────────────────────────────────────────────────────┐
│  帧类型                                                        │
│                                                                │
│  req   ────────▶  Client → Server  请求                       │
│  res   ◀────────  Server → Client  响应                       │
│  event ◀────────  Server → Client  服务端推送事件              │
└────────────────────────────────────────────────────────────────┘
```

**`res` 与 `event` 的职责区分**：

| 帧类型 | `type` 值 | 是否携带 `id` | 来源 | 用途 |
| --- | --- | --- | --- | --- |
| 响应帧 | `"res"` | 是，与请求 `id` 一一对应 | Gateway 直接回复 | 确认客户端请求已受理或已完成 |
| 事件帧 | `"event"` | 否 | Event Bus → Gateway 转发 | 服务端主动推送异步事件 |

以 `agent.submit` 的完整生命周期为例：

```text
Client ──── req (agent.submit) ────────────────────▶ Gateway
Client ◀─── res (code:200, status:"accepted") ───── Gateway   ← type: "res"（确认收到）

Client ◀─── event: agent.run_start ──────────────── Gateway   ← type: "event"
Client ◀─── event: agent.delta ──────────────────── Gateway   ← type: "event"
Client ◀─── event: agent.delta ──────────────────── Gateway   ← type: "event"
Client ◀─── event: agent.run_complete ───────────── Gateway   ← type: "event"

Client ◀─── res (code:200, status:"final") ──────── Gateway   ← type: "res"（最终确认）
```

> **关键原则**：`res` 表达"你的请求我处理了"，`event` 表达"有新事情发生了"。
> 一次异步 RPC 调用会产生 **2 个 res**（accepted + final）和 **N 个 event**（执行过程流）。
> 客户端通过 `id` 匹配 res，通过 `sessionKey` + `runId` + `seq` 匹配 event。

### 3.2 请求帧 (Request Frame)

**方向**：Client → Server

```json
{
  "type": "req",
  "id": "req_01JKXYZ...",
  "method": "agent.submit",
  "params": {
    "sessionKey": "sess_abc123",
    "message": "帮我写个排序算法",
    "idempotencyKey": "idem_01JKXYZ..."
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `type` | `"req"` | 是 | 固定值 |
| `id` | string | 是 | 请求唯一 ID，ULID 推荐，用于匹配响应 |
| `method` | string | 是 | RPC 方法名 |
| `params` | object | 否 | 方法参数 |

> **关于来源标识**：请求帧**不携带**客户端来源字段（web/ios/api/desktop 等）。
> 客户端身份在 **WS 连接建立时** 通过 HTTP Upgrade 头一次性确定：
>
> ```text
> GET /ws HTTP/1.1
> X-Client-Id: sahara-web          ← 客户端标识 (web/ios/android/desktop/api)
> X-Client-Version: 1.0.0          ← 版本号
> X-Client-Platform: web           ← 平台 (web/ios/android/macos/windows/linux)
> ```
>
> Gateway 将这些信息绑定在连接对象上，后续每个 req 自动继承——WS 是长连接，
> 没有必要每帧重传。如果特定请求需要影响行为（如移动端选择轻量模型），
> 应在该方法的 `params.options` 中传递，而非帧层面的通用字段。

### 3.3 响应帧 (Response Frame)

**方向**：Server → Client

```json
{
  "type": "res",
  "id": "req_01JKXYZ...",
  "code": 200,
  "status": "accepted",
  "payload": {
    "runId": "run_01JKXYZ...",
    "taskId": "task_01JKXYZ..."
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `type` | `"res"` | 是 | 固定值 |
| `id` | string | 是 | 对应的请求 ID |
| `code` | integer | 是 | 状态码（见下表） |
| `status` | string | 是 | 可读状态：`"ok"` / `"accepted"` / `"final"` / `"error"` |
| `payload` | object | 否 | 成功时 (`code == 200`) 的响应数据 |
| `error` | ErrorShape | 否 | 失败时 (`code >= 400`) 的错误信息 |

**状态码约定**：

| code | status | 含义 | 客户端判断 |
| --- | --- | --- | --- |
| `200` | `"ok"` | 同步方法成功（如 session.list, ping） | `code == 200` = 成功 |
| `200` | `"accepted"` | 异步任务已接受（agent.submit 第一个响应） | `status` 区分阶段 |
| `200` | `"final"` | 异步任务完成（agent.submit 最终响应） | `status` 区分阶段 |
| `400` | `"error"` | 客户端错误（参数无效、未知方法等） | `code >= 400` = 失败 |
| `403` | `"error"` | 权限不足 | |
| `404` | `"error"` | 资源不存在 (session/task) | |
| `409` | `"error"` | 冲突（session 锁、幂等键冲突） | |
| `429` | `"error"` | 限频 | `error.retryAfterMs` 提示重试时间 |
| `500` | `"error"` | 服务端内部错误 | |
| `503` | `"error"` | 系统繁忙（所有 Worker 满载） | |

> **设计说明**：
>
> 1. **成功统一用 `200`**，由 `status` 区分语义阶段（`ok` / `accepted` / `final`）。
>    这是 WS 帧协议而非 HTTP API，不需要 201/202/204 这类细分码。
>    客户端判断极简：`code === 200` = 成功，`code >= 400` = 失败。
> 2. **错误码保留 HTTP 语义**（400/403/404/429/500/503），前端开发者零学习成本。
> 3. **`status` 是业务层状态**，`code` 是传输层状态。双响应模式中客户端通过
>    `status === "accepted"` 知道"任务还在跑"，`status === "final"` 知道"完成了"。

**ErrorShape**：

```json
{
  "reason": "RATE_LIMITED",
  "message": "请求过于频繁，请稍后再试",
  "retryable": true,
  "retryAfterMs": 5000,
  "details": {}
}
```

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `reason` | string | 是 | 业务错误原因码（如 `RATE_LIMITED`、`SESSION_LOCKED`） |
| `message` | string | 是 | 人类可读错误描述 |
| `retryable` | boolean | 否 | 是否建议客户端重试 |
| `retryAfterMs` | integer | 否 | 建议重试等待时间 (ms) |
| `details` | object | 否 | 附加信息（如校验失败的字段名） |

> **命名说明**：ErrorShape 的业务错误原因字段命名为 `reason`（字符串），
> 与响应帧顶层的 `code`（数字）在名称、类型、语义上完全正交。
> 外层 `code` 用于快速判断成功/失败，内层 `reason` 用于业务错误分类处理。
> `error.reason` 读起来最自然——"错误的原因是 RATE_LIMITED"，且不与帧层的 `type` 字段冲突。

### 3.4 事件帧 (Event Frame)

**方向**：Server → Client（主动推送）

```json
{
  "type": "event",
  "event": "agent.delta",
  "sessionKey": "sess_abc123",
  "runId": "run_01JKXYZ...",
  "seq": 42,
  "ts": 1706000001000,
  "payload": {
    "text": "好的，我来帮你实现"
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `type` | `"event"` | 是 | 固定值 |
| `event` | string | 是 | 事件名称 |
| `sessionKey` | string | 否 | 关联的 session（agent 事件必填） |
| `runId` | string | 否 | 关联的执行 ID（agent 事件必填） |
| `seq` | integer | 否 | session 内递增序列号 |
| `ts` | integer | 否 | 事件时间戳 (ms) |
| `payload` | object | 否 | 事件数据 |

#### 3.4.1 多模态内容模型

LLM 的输出不仅是文本，还可能包含图片生成、文件产出、代码块等。协议通过**分层设计**支持多模态：

```text
┌─────────────────────────────────────────────────────────────────────┐
│  多模态内容传输设计                                                  │
│                                                                     │
│  agent.delta          ← 纯文本流式增量 (热路径, 极简)               │
│  agent.content        ← 非文本内容块 (图片/文件/代码块, 离散推送)   │
│  agent.run_complete   ← 完整 Content Block 列表 (最终结果)          │
│                                                                     │
│  原则:                                                              │
│  • delta 只传文本片段 (保持高频推送的极致精简)                       │
│  • 非文本内容用 URL 引用，不在 WS 帧中传 binary                    │
│  • 客户端按需通过 HTTP GET 下载实际内容                              │
└─────────────────────────────────────────────────────────────────────┘
```

**agent.delta** — 文本流式增量（不变，保持极简）：

```json
{
  "event": "agent.delta",
  "payload": {
    "text": "好的，我来帮你实现",
    "stream": "assistant"
  }
}
```

**agent.content** — 非文本内容块（新增事件）：

```json
{
  "event": "agent.content",
  "payload": {
    "blockId": "blk_01JK...",
    "blockType": "image",
    "mimeType": "image/png",
    "url": "https://files.sahara.com/runs/run_01JK/output-chart.png",
    "metadata": {
      "width": 800,
      "height": 600,
      "alt": "排序算法时间复杂度对比图",
      "sizeBytes": 45200
    }
  }
}
```

支持的 `blockType`：

| blockType | 说明 | payload 关键字段 |
| --- | --- | --- |
| `image` | 生成的图片 | `url`, `mimeType`, `width`, `height`, `alt` |
| `file` | 产出的文件 | `url`, `filename`, `mimeType`, `sizeBytes` |
| `code` | 结构化代码块 | `language`, `code`, `filename` |
| `table` | 结构化表格 | `headers`, `rows` |
| `audio` | 音频（TTS 等） | `url`, `mimeType`, `durationMs` |
| `card` | 富文本卡片 | `title`, `description`, `imageUrl`, `actions` |

**agent.run_complete** — 最终结果包含完整 Content Block 列表：

```json
{
  "event": "agent.run_complete",
  "payload": {
    "content": [
      { "type": "text", "text": "我已经帮你实现了快速排序算法，并生成了性能对比图：" },
      { "type": "image", "url": "https://files.sahara.com/runs/run_01JK/chart.png",
        "mimeType": "image/png", "alt": "性能对比" },
      { "type": "file", "url": "https://files.sahara.com/runs/run_01JK/quicksort.py",
        "filename": "quicksort.py", "mimeType": "text/x-python" }
    ],
    "iterations": 3,
    "durationMs": 12500
  }
}
```

> **核心原则：WS 帧永远不传 binary 数据，所有富媒体内容通过 URL 引用。**
>
> 这条原则贯穿事件帧、响应帧和历史消息：
>
> | 场景 | 传输方式 | 示例 |
> | --- | --- | --- |
> | `agent.content` 事件推送 | URL 引用 | `"url": "https://files.sahara.com/..."` |
> | `agent.run_complete` 最终结果 | URL 引用 | `content[].url` |
> | `session.get` 历史消息中的图片/文件 | URL 引用 | `history[].attachments[].url` |
> | 用户上传附件 | 先 HTTP 上传获取 URL，再在 WS `params` 中传 URL | `params.attachments[].url` |
>
> **理由**：
>
> 1. WS 帧大小上限 1MB，一张图片就可能超限
> 2. base64 编码膨胀 33%，浪费带宽和客户端解码开销
> 3. 客户端可按需加载（懒加载图片/延迟下载文件），减少初始传输量
> 4. URL 可设置 CDN 加速、缓存策略、签名访问权限（临时签名 URL，过期自动失效）
> 5. 前端框架原生支持 `<img src="...">` / `<video src="...">`，无需额外 blob 处理

---

## 四、连接生命周期

### 4.1 完整流程

```text
Client                             Load Balancer              Gateway-N
  │                                      │                        │
  │ ══════════ Phase 1: HTTP Upgrade + JWT 验证 ════════════════ │
  │                                      │                        │
  │  GET /ws                             │                        │
  │  Upgrade: websocket                  │                        │
  │  Authorization: Bearer <JWT>         │                        │
  │ ────────────────────────────────────▶│                        │
  │                                      │  路由到某个 Gateway     │
  │                                      │ ──────────────────────▶│
  │                                      │                        │ JWT 验证
  │                                      │                        │ 提取 user_id
  │  101 Switching Protocols             │                        │
  │ ◀────────────────────────────────────│◀───────────────────── │
  │                                      │                        │
  │ ══════════ Phase 2: 协议握手 ═══════════════════════════════ │
  │                                      │                        │
  │  ◀────────── event: welcome ──────────────────────────────── │
  │  {                                                            │
  │    "type": "event",                                           │
  │    "event": "welcome",                                        │
  │    "payload": {                                               │
  │      "protocol": 1,                                           │
  │      "connId": "gw2_conn_01JK...",                            │
  │      "userId": "user_abc",                                    │
  │      "serverVersion": "0.1.0",                                │
  │      "resumeToken": "rt_01JKXYZ...",                          │
  │      "features": {                                            │
  │        "methods": ["agent.submit", "agent.abort", ...],       │
  │        "events": ["agent.delta", "agent.run_start", ...]      │
  │      },                                                       │
  │      "policy": {                                              │
  │        "maxFrameBytes": 1048576,                              │
  │        "tickIntervalMs": 30000,                               │
  │        "maxConcurrentSubmits": 3                               │
  │      }                                                        │
  │    }                                                          │
  │  }                                                            │
  │                                                               │
  │ ══════════ Phase 3: 正常通信 ════════════════════════════════ │
  │                                                               │
  │  ──── req: agent.submit ────────────────────────────────────▶ │
  │  ◀──── res (accepted) ──────────────────────────────────────  │
  │  ◀──── event: agent.run_start ──────────────────────────────  │
  │  ◀──── event: agent.delta ──────────────────────────────────  │
  │  ◀──── event: agent.delta ──────────────────────────────────  │
  │  ◀──── event: agent.run_complete ───────────────────────────  │
  │  ◀──── res (final) ────────────────────────────────────────   │
  │                                                               │
  │  ◀──── event: tick ─────────────────────────────────────────  │
  │                                                               │
  │ ══════════ Phase 4: 断线 / 重连 ═════════════════════════════ │
  │                                                               │
  │  连接断开 (网络波动 / Gateway 滚动更新)                       │
  │                                                               │
  │  [指数退避 1s → 2s → 4s → ... → 30s max]                     │
  │                                                               │
  │  GET /ws?resumeToken=rt_01JKXYZ...&lastSeq=42                │
  │  Authorization: Bearer <JWT>                                  │
  │ ────────────────────────────────────▶│                        │
  │                                      │ 可能路由到不同 Gateway │
  │                                      │ ──────────────────────▶│
  │                                      │                        │ JWT 验证
  │                                      │                        │ 加载 resume 上下文
  │  101 Switching Protocols             │                        │
  │ ◀────────────────────────────────────│◀───────────────────── │
  │                                      │                        │
  │  ◀──── event: welcome (resumed) ─────────────────────────── │
  │  ◀──── event: agent.delta (回放 seq=43) ────────────────── │
  │  ◀──── event: agent.delta (回放 seq=44) ────────────────── │
  │  ◀──── event: agent.run_complete (回放 seq=45) ─────────── │
  │  继续正常通信...                                              │
```

### 4.2 连接状态机

```text
                    ┌───────────────┐
                    │  DISCONNECTED │
                    └───────┬───────┘
                            │ connect()
                            ▼
                    ┌───────────────┐
                    │  CONNECTING   │
                    └───────┬───────┘
                            │ 101 Switching Protocols
                            ▼
                    ┌───────────────┐
                    │  HANDSHAKING  │─── 收到 welcome 事件前
                    └───────┬───────┘    超时(5s) → DISCONNECTED
                            │ 收到 welcome 事件
                            ▼
                    ┌───────────────┐
              ┌────▶│  CONNECTED    │◀─── 正常通信状态
              │     └───────┬───────┘
              │             │ 连接断开
              │             ▼
              │     ┌───────────────┐
              │     │  RECONNECTING │──── 指数退避
              │     └───────┬───────┘
              │             │ 建立新连接
              └─────────────┘
                            │ stop() 或超过最大重试
                            ▼
                    ┌───────────────┐
                    │   CLOSED      │
                    └───────────────┘
```

---

## 五、认证协议

### 5.1 认证流程

Sahara 采用 **HTTP Upgrade 阶段 JWT 验证**——握手仅需一个 RTT 即可完成认证，无需额外的帧内握手步骤。

```text
Client                                           Gateway
  │                                                  │
  │  GET /ws HTTP/1.1                                │
  │  Host: api.sahara.com                            │
  │  Upgrade: websocket                              │
  │  Connection: Upgrade                             │
  │  Authorization: Bearer eyJhbGciOi...             │
  │  X-Client-Id: sahara-web                         │
  │  X-Client-Version: 1.0.0                         │
  │ ────────────────────────────────────────────────▶│
  │                                                  │
  │                                                  │ 1. 验证 JWT 签名 (RS256)
  │                                                  │ 2. 检查 exp (过期时间)
  │                                                  │ 3. 提取 user_id, roles
  │                                                  │ 4. 检查 Rate Limit
  │                                                  │ 5. 分配 connId
  │                                                  │ 6. 注册到连接表 (Redis)
  │                                                  │
  │  HTTP/1.1 101 Switching Protocols                │
  │ ◀────────────────────────────────────────────────│
  │                                                  │
  │  ◀─── event: welcome ───────────────────────────│
  │                                                  │
```

### 5.2 JWT Claims

```json
{
  "sub": "user_abc123",
  "iss": "sahara",
  "aud": "sahara-gateway",
  "exp": 1706000900,
  "iat": 1706000000,
  "roles": ["user"],
  "quota": {
    "maxConcurrentSessions": 5,
    "maxSubmitsPerMinute": 20
  }
}
```

| Claim | 说明 |
| --- | --- |
| `sub` | 用户唯一 ID |
| `roles` | 角色列表 (`user` / `premium` / `admin`) |
| `quota` | 用户级配额（覆盖系统默认值） |
| `exp` | 过期时间 (15 分钟) |

### 5.3 Token 过期处理

JWT 即将过期时，Gateway **不断开连接**，而是提前 60 秒推送 `auth.expiring` 事件。客户端收到后用 Refresh Token 通过 HTTP API 获取新 JWT，然后在 WS 连接上发送 `auth.refresh` 方法续期。

```text
Client                                         Gateway
  │                                                │
  │  ◀─── event: auth.expiring ───────────────────│  JWT 还有 60s 过期
  │                                                │
  │  POST /api/auth/refresh (HTTP)                 │
  │  { "refreshToken": "rt_xxx" }                  │
  │ ──────────────────────────────────────────────▶│
  │  ◀── { "accessToken": "新JWT" } ──────────────│
  │                                                │
  │  req: auth.refresh                             │
  │  { "token": "新JWT" }                          │
  │ ──────────────────────────────────────────────▶│
  │                                                │  验证新 JWT
  │  ◀─── res (code:200, status:"ok") ───────────│  更新连接上的认证信息
  │                                                │
  │  继续正常通信，无需断连重连                      │
```

### 5.4 认证失败

| 场景 | HTTP 状态码 | WS 关闭码 | 说明 |
| --- | --- | --- | --- |
| 无 Authorization header | 401 | — | HTTP 阶段拒绝 |
| JWT 签名无效 | 401 | — | HTTP 阶段拒绝 |
| JWT 已过期 | 401 | — | HTTP 阶段拒绝（需刷新） |
| JWT 过期（连接中） | — | 不关闭 | 推送 `auth.expiring`，等待客户端刷新 |
| JWT 过期 + 未刷新 >5min | — | 4001 | 强制关闭 |
| 用户被封禁 | 403 | 4003 | 拒绝连接 |
| 连接数超限 | 429 | 4029 | 同一用户连接数超过上限 |

---

## 六、RPC 方法定义

### 6.1 方法总览

C 端仅暴露用户操作所需的最小方法集（~15 个），不暴露管理/配置/渠道/节点类方法。

```text
┌────────────────────────────────────────────────────────────────┐
│  C 端 RPC 方法                                                 │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Agent 操作                                                    │
│  ├── agent.submit          提交 Agent 任务         Phase 1     │
│  ├── agent.abort           中止执行中的任务         Phase 1     │
│  └── agent.status          查询任务状态             Phase 2     │
│                                                                │
│  Session 操作                                                  │
│  ├── session.list          列出用户的会话           Phase 1     │
│  ├── session.get           获取会话详情和历史        Phase 1     │
│  ├── session.create        创建新会话               Phase 1     │
│  ├── session.delete        删除会话                 Phase 1     │
│  └── session.rename        重命名会话               Phase 2     │
│                                                                │
│  认证                                                          │
│  └── auth.refresh          刷新 JWT（不断连续期）   Phase 1     │
│                                                                │
│  用户                                                          │
│  ├── user.profile          获取用户信息             Phase 2     │
│  └── user.usage            查询用量/配额            Phase 2     │
│                                                                │
│  系统                                                          │
│  └── ping                  心跳探测                 Phase 1     │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### 6.2 agent.submit — 提交 Agent 任务

**请求**：

```json
{
  "type": "req",
  "id": "req_01JK...",
  "method": "agent.submit",
  "params": {
    "sessionKey": "sess_abc123",
    "message": "帮我写一个快速排序",
    "idempotencyKey": "idem_01JK...",
    "options": {
      "model": "claude-sonnet",
      "thinking": "medium",
      "maxIterations": 20
    },
    "attachments": [
      {
        "filename": "data.csv",
        "mimeType": "text/csv",
        "url": "https://upload.sahara.com/tmp/xxx.csv"
      }
    ]
  }
}
```

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `sessionKey` | string | 是 | 会话标识（由 `session.create` 返回） |
| `message` | string | 是 | 用户消息文本 |
| `idempotencyKey` | string | 是 | 幂等键（ULID 推荐） |
| `options.model` | string | 否 | 覆盖默认模型 |
| `options.thinking` | string | 否 | 思考级别：`off` / `low` / `medium` / `high` |
| `options.maxIterations` | integer | 否 | 最大工具调用轮数 |
| `attachments` | array | 否 | 附件列表（已上传到临时存储的 URL） |

**accepted 响应**（立即返回，通常 <50ms）：

```json
{
  "type": "res",
  "id": "req_01JK...",
  "code": 200,
  "status": "accepted",
  "payload": {
    "taskId": "task_01JK...",
    "runId": "run_01JK...",
    "position": 0
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `taskId` | 任务 ID（全局唯一） |
| `runId` | 执行 ID（重试时会变） |
| `position` | 排队位置（0 = 立即执行，>0 = 排队中） |

**final 响应**（任务结束时返回）：

```json
{
  "type": "res",
  "id": "req_01JK...",
  "code": 200,
  "status": "final",
  "payload": {
    "taskId": "task_01JK...",
    "runId": "run_01JK...",
    "state": "completed",
    "summary": {
      "text": "我已经帮你实现了快速排序算法...",
      "iterations": 3,
      "toolCalls": 2,
      "durationMs": 12500,
      "tokensUsed": 2400
    }
  }
}
```

> **`final` 响应 vs `agent.run_complete` 事件**：
> `agent.run_complete`（event 帧）在任务执行完毕时由 Event Bus 推送，携带完整的多模态 `content` 列表，用于前端实时渲染。
> `final`（res 帧）随后由 Gateway 发出，仅提供 `summary` 纪要信息，用于客户端 Promise 归结。
> 两者互补——流式 UI 监听 event，程序化调用 await final。

### 6.3 agent.abort — 中止任务

```json
{
  "type": "req",
  "id": "req_02...",
  "method": "agent.abort",
  "params": {
    "taskId": "task_01JK...",
    "runId": "run_01JK..."
  }
}
```

**响应**：

```json
{
  "type": "res",
  "id": "req_02...",
  "code": 200,
  "status": "ok",
  "payload": {
    "aborted": true,
    "previousState": "running"
  }
}
```

### 6.4 session.list — 列出会话

```json
{
  "type": "req",
  "id": "req_03...",
  "method": "session.list",
  "params": {
    "limit": 20,
    "cursor": "cur_xxx",
    "agentId": "main"
  }
}
```

**响应**：

```json
{
  "type": "res",
  "id": "req_03...",
  "code": 200,
  "status": "ok",
  "payload": {
    "sessions": [
      {
        "sessionKey": "sess_abc123",
        "title": "快速排序算法",
        "agentId": "main",
        "lastMessageAt": 1706000000000,
        "messageCount": 12,
        "isActive": false
      }
    ],
    "nextCursor": "cur_yyy",
    "hasMore": true
  }
}
```

### 6.5 session.get — 获取会话详情

```json
{
  "type": "req",
  "id": "req_04...",
  "method": "session.get",
  "params": {
    "sessionKey": "sess_abc123",
    "includeHistory": true,
    "historyLimit": 50
  }
}
```

**响应**：

```json
{
  "type": "res",
  "id": "req_04...",
  "code": 200,
  "status": "ok",
  "payload": {
    "session": {
      "sessionKey": "sess_abc123",
      "title": "快速排序算法",
      "agentId": "main",
      "createdAt": 1706000000000,
      "lastMessageAt": 1706000000000
    },
    "history": [
      {
        "role": "user",
        "content": "帮我写一个快速排序",
        "ts": 1706000000000
      },
      {
        "role": "assistant",
        "content": "好的，我来帮你实现...",
        "ts": 1706000001000,
        "toolCalls": [
          { "name": "write", "file": "quicksort.py" }
        ]
      }
    ]
  }
}
```

### 6.6 session.create — 创建会话

```json
{
  "type": "req",
  "id": "req_05...",
  "method": "session.create",
  "params": {
    "agentId": "main",
    "title": "新会话"
  }
}
```

**响应**：

```json
{
  "type": "res",
  "id": "req_05...",
  "code": 200,
  "status": "ok",
  "payload": {
    "sessionKey": "sess_def456"
  }
}
```

### 6.7 auth.refresh — 在连接上刷新 JWT

```json
{
  "type": "req",
  "id": "req_06...",
  "method": "auth.refresh",
  "params": {
    "token": "eyJhbGciOi..."
  }
}
```

**响应**：

```json
{
  "type": "res",
  "id": "req_06...",
  "code": 200,
  "status": "ok",
  "payload": {
    "expiresAt": 1706001800000
  }
}
```

### 6.8 其他方法（Phase 2 或语义简单）

以下方法语义明确，遵循统一的 req/res 帧格式，在 Phase 2 或后续迭代中实现：

| 方法 | 说明 | Phase | 备注 |
| --- | --- | --- | --- |
| `session.delete` | 删除会话及其历史 | Phase 1 | `params: { sessionKey }` |
| `session.rename` | 重命名会话标题 | Phase 2 | `params: { sessionKey, title }` |
| `agent.status` | 查询任务当前状态 | Phase 2 | `params: { taskId }` |
| `user.profile` | 获取当前用户信息 | Phase 2 | 无参数，返回昵称、头像、角色等 |
| `user.usage` | 查询用量与配额 | Phase 2 | 返回已用 token、剩余配额等 |
| `ping` | 应用层心跳探测 | Phase 1 | 无参数，返回 `{ ts }` |

> 详细的请求/响应字段定义在实现 Phase 到来时补充。以上方法均为同步方法（`status: "ok"`），无需双响应模式。

---

## 七、事件定义

### 7.1 事件总览

```text
┌────────────────────────────────────────────────────────────────┐
│  C 端事件                                                      │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  连接生命周期                                                  │
│  ├── welcome              连接建立 + 协议握手完成              │
│  └── goodbye              服务端主动关闭前通知                 │
│                                                                │
│  认证                                                          │
│  └── auth.expiring        JWT 即将过期（提前 60s 通知）        │
│                                                                │
│  Agent 事件 (来自 Event Bus)                                   │
│  ├── agent.run_start      Agent 执行开始                       │
│  ├── agent.delta          LLM 流式文本片段 (纯文本增量)        │
│  ├── agent.content        非文本内容块 (图片/文件/代码/音频)   │
│  ├── agent.thinking       模型思考内容                         │
│  ├── agent.tool_start     工具开始执行                         │
│  ├── agent.tool_result    工具执行结果                         │
│  ├── agent.run_complete   Agent 执行完成 (含完整 Content 列表) │
│  ├── agent.run_error      Agent 执行出错                       │
│  ├── agent.run_abort      Agent 执行被中止                     │
│  └── agent.usage          Token 用量统计                       │
│                                                                │
│  系统                                                          │
│  ├── tick                 心跳 (每 30s)                        │
│  └── system.maintenance   系统维护通知                         │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### 7.2 welcome — 连接建立

连接升级成功后，服务端**立即**发送的第一个帧。客户端在收到 `welcome` 之前不应发送任何 `req`。

```json
{
  "type": "event",
  "event": "welcome",
  "payload": {
    "protocol": 1,
    "connId": "gw2_conn_01JK...",
    "userId": "user_abc",
    "serverVersion": "0.1.0",
    "resumeToken": "rt_01JKXYZ...",
    "resumed": false,
    "features": {
      "methods": ["agent.submit", "agent.abort", "session.list", "..."],
      "events": ["agent.delta", "agent.run_start", "..."]
    },
    "policy": {
      "maxFrameBytes": 1048576,
      "tickIntervalMs": 30000,
      "maxConcurrentSubmits": 3,
      "rateLimit": {
        "submitsPerMinute": 20,
        "framesPerSecond": 10
      }
    }
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `connId` | 全局唯一连接 ID，格式 `{gatewayId}_{localId}` |
| `resumeToken` | 断线恢复令牌，客户端应持久化 |
| `resumed` | 是否为恢复连接（true 时后续会回放错过的事件） |
| `policy` | 协议策略参数，客户端应遵守 |

### 7.3 Agent 事件详解

所有 Agent 事件共享以下顶层字段：

```json
{
  "type": "event",
  "event": "agent.<event_type>",
  "sessionKey": "sess_abc123",
  "runId": "run_01JK...",
  "seq": 42,
  "ts": 1706000001000,
  "payload": { ... }
}
```

#### agent.delta — 流式文本

```json
{
  "event": "agent.delta",
  "payload": {
    "text": "好的，我来帮你",
    "stream": "assistant"
  }
}
```

`stream` 值：`"assistant"` (正文) / `"thinking"` (思考)。

> **150ms 聚合**：Gateway 会将 150ms 窗口内的多个 delta 合并为一个帧推送，减少客户端帧频。客户端不需要处理这个逻辑。

#### agent.tool_start — 工具开始

```json
{
  "event": "agent.tool_start",
  "payload": {
    "toolCallId": "call_01JK...",
    "toolName": "write",
    "input": {
      "path": "quicksort.py",
      "content": "def quicksort(arr): ..."
    }
  }
}
```

#### agent.tool_result — 工具结果

```json
{
  "event": "agent.tool_result",
  "payload": {
    "toolCallId": "call_01JK...",
    "toolName": "write",
    "success": true,
    "output": "文件已创建: quicksort.py",
    "durationMs": 120
  }
}
```

#### agent.run_complete — 执行完成

```json
{
  "event": "agent.run_complete",
  "payload": {
    "finalText": "我已经帮你实现了快速排序算法...",
    "content": [
      { "type": "text", "text": "我已经帮你实现了快速排序算法，并生成了性能对比图：" },
      { "type": "image", "url": "https://files.sahara.com/runs/run_01JK/chart.png",
        "mimeType": "image/png", "alt": "性能对比" },
      { "type": "file", "url": "https://files.sahara.com/runs/run_01JK/quicksort.py",
        "filename": "quicksort.py", "mimeType": "text/x-python" }
    ],
    "iterations": 3,
    "toolCalls": 2,
    "durationMs": 12500,
    "tokensUsed": 2400
  }
}
```

> `finalText` 是纯文本摘要（向后兼容），`content` 是完整的多模态 Content Block 列表（参见 §3.4.1）。
> 客户端应优先使用 `content` 渲染最终结果；若 `content` 为空，回退到 `finalText`。

#### agent.thinking — 模型思考

当 `options.thinking` 开启时，模型的思考过程通过独立事件推送。

```json
{
  "event": "agent.thinking",
  "payload": {
    "text": "用户需要快速排序，我应该先考虑时间复杂度...",
    "isFinal": false
  }
}
```

> **与 `agent.delta` 中 `stream: "thinking"` 的关系**：
> `agent.delta` 的 `stream` 字段用于标记文本流的来源（`"assistant"` 正文 / `"thinking"` 思考），适用于将思考内容混入流式文本的轻量场景。
> `agent.thinking` 是独立事件，适用于思考内容需要独立渲染（如折叠面板）的场景。
> Runtime 根据 `options.thinking` 级别决定走哪条路径——`low` 用 delta stream，`medium` / `high` 用独立事件。

#### agent.run_abort — 执行被中止

用户调用 `agent.abort` 后，Runtime 确认中止时推送此事件。

```json
{
  "event": "agent.run_abort",
  "payload": {
    "abortedBy": "user",
    "partialText": "我已经开始编写快速排序...",
    "iterationsCompleted": 1,
    "durationMs": 3200
  }
}
```

#### agent.run_error — 执行出错

```json
{
  "event": "agent.run_error",
  "payload": {
    "reason": "LLM_PROVIDER_ERROR",
    "message": "模型服务暂时不可用",
    "retryable": true
  }
}
```

#### agent.usage — Token 用量

```json
{
  "event": "agent.usage",
  "payload": {
    "model": "claude-sonnet-4-20250514",
    "inputTokens": 1200,
    "outputTokens": 850,
    "iteration": 2
  }
}
```

### 7.4 tick — 心跳

```json
{
  "type": "event",
  "event": "tick",
  "ts": 1706000030000
}
```

### 7.5 goodbye — 优雅关闭

```json
{
  "type": "event",
  "event": "goodbye",
  "payload": {
    "reason": "gateway_restart",
    "message": "服务升级中，请稍候自动重连",
    "reconnectAfterMs": 1000,
    "silent": true
  }
}
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `reason` | string | 关闭原因：`gateway_restart` / `idle_timeout` / `user_banned` / `auth_expired` |
| `reconnectAfterMs` | integer | 建议客户端等待多久后重连 |
| `silent` | boolean | **静默模式**：`true` 时客户端不应显示任何断连 UI（参见 §15.4） |

> **`silent: true` 的含义**：这是服务端主动、预期的断连（如滚动更新），客户端应在后台静默重连，用户不应感知到任何中断。
> `silent: false` 或缺省时，表示异常关闭，客户端应显示重连状态提示。

### 7.6 system.maintenance — 系统维护通知

当系统即将进入维护窗口时推送，客户端可据此展示 UI 提示。与 `goodbye` 不同，`system.maintenance` 不意味着立即断连，而是提前通知。

```json
{
  "type": "event",
  "event": "system.maintenance",
  "payload": {
    "scheduledAt": 1706004000000,
    "estimatedDurationMin": 30,
    "message": "系统将于 02:00 进行维护升级，预计 30 分钟",
    "allowNewSubmits": false
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `scheduledAt` | 维护开始时间 (ms) |
| `estimatedDurationMin` | 预计维护时长（分钟） |
| `allowNewSubmits` | 维护期间是否仍接受新任务 |

---

## 八、双响应模式

`agent.submit` 采用双响应（Dual Response）模式——一个请求返回两次响应：

```text
Client                                         Gateway
  │                                                │
  │  req: agent.submit                             │
  │ ──────────────────────────────────────────────▶│
  │                                                │ gRPC SubmitTask → Runtime
  │  res (status="accepted")                       │
  │ ◀──────────────────────────────────────────────│ ~50ms
  │                                                │
  │  event: agent.run_start                        │
  │ ◀──────────────────────────────────────────────│
  │  event: agent.delta ...                        │
  │ ◀──────────────────────────────────────────────│ Event Bus → Gateway
  │  event: agent.delta ...                        │
  │ ◀──────────────────────────────────────────────│
  │  event: agent.run_complete                     │
  │ ◀──────────────────────────────────────────────│
  │                                                │
  │  res (status="final")                          │
  │ ◀──────────────────────────────────────────────│ 任务结束
  │                                                │
```

**客户端处理策略**：

| 模式 | 行为 | 适用场景 |
| --- | --- | --- |
| **Fire-and-forget** | 收到 `accepted` 即认为成功；通过事件获取流式结果 | 聊天 UI（实时显示流式输出） |
| **Await final** | 等待 `final` 响应后才 resolve Promise | 程序化调用（需要完整结果） |

---

## 九、断线恢复与事件回放

### 9.1 机制概述

Sahara 支持基于 `resumeToken` 的断线恢复与事件回放，确保网络波动期间不丢失任何事件。

```text
┌────────────────────────────────────────────────────────────────┐
│  断线恢复流程                                                  │
│                                                                │
│  1. 首次连接 → welcome 中返回 resumeToken                     │
│  2. 客户端持久化 resumeToken 和 lastSeq (per session)         │
│  3. 断线后 → 携带 resumeToken + lastSeq 重连                  │
│  4. 新 Gateway 验证 resumeToken                                │
│  5. 从 Event Bus (Redis Streams) 中读取断线期间的事件          │
│  6. 按 seq 顺序回放给客户端                                    │
│  7. 回放完毕 → 切换到实时推送模式                              │
└────────────────────────────────────────────────────────────────┘
```

### 9.2 客户端重连请求

```text
GET /ws?resumeToken=rt_01JKXYZ...&lastSeqs=sess_abc:42,sess_def:18
Authorization: Bearer <JWT>
```

| 参数 | 说明 |
| --- | --- |
| `resumeToken` | 上次 `welcome` 中返回的令牌 |
| `lastSeqs` | 每个活跃 session 最后收到的 seq，逗号分隔 |

### 9.3 resumed welcome

```json
{
  "type": "event",
  "event": "welcome",
  "payload": {
    "protocol": 1,
    "connId": "gw3_conn_02AB...",
    "resumed": true,
    "resumeToken": "rt_02ABCDE...",
    "replayingSessions": ["sess_abc123"],
    "missedEvents": 3
  }
}
```

回放事件紧随 welcome 之后，保持原始 seq 顺序。回放完毕后发送一个 `replay.complete` 事件：

```json
{
  "type": "event",
  "event": "replay.complete",
  "payload": {
    "sessionsReplayed": ["sess_abc123"],
    "eventsReplayed": 3
  }
}
```

### 9.4 恢复限制

| 限制 | 值 | 说明 |
| --- | --- | --- |
| resumeToken 有效期 | 5 分钟 | 超过后视为新连接 |
| 最大回放事件数 | 1000 | 超过后截断，通知客户端刷新 session |
| 回放超时 | 10 秒 | 超过后放弃回放，切换到实时模式 |

---

## 十、多实例与连接路由

### 10.1 连接 ID 全局唯一

每个连接的 `connId` 格式为 `{gatewayInstanceId}_{localConnId}`，确保多实例不冲突：

```text
Gateway-1 上的连接: gw1_conn_01JKXYZ...
Gateway-2 上的连接: gw2_conn_01JKABC...
```

### 10.2 事件路由

用户可能连接在 Gateway-1，但事件来自 Runtime → Event Bus。所有 Gateway 实例都订阅 Event Bus，收到事件后检查"该 session 的用户是否连接在我这里"：

```text
Event Bus 发出事件: sessionKey=sess_abc123
         │
    ┌────┴────┐
    ▼         ▼
Gateway-1   Gateway-2
检查: 该用户  检查: 该用户
连在我这?    连在我这?
  │ 否         │ 是
  │ 丢弃       │ 推送给客户端
```

**优化**：Gateway 在 Redis 中维护 `session→gateway` 路由表。Event Bus 可以用 session key 做 topic 级别路由，只投递给目标 Gateway，避免无效扇出。

### 10.3 多设备同步

同一用户可能从 Web 和 App 同时在线。同一 session 的事件会推送给**该用户的所有连接**：

```text
User-A 的连接:
  ├── Gateway-1: Web 浏览器
  └── Gateway-2: iOS App

sess_abc123 的事件 → 同时推送到两个连接
```

---

## 十一、限频与背压

### 11.1 三层限频

```text
┌────────────────────────────────────────────────────────────────┐
│  Layer 1: 连接级限频                                           │
│  ─────────────────                                             │
│  单连接每秒最多 N 个 req 帧 (默认 10)                          │
│  超过 → res 返回 RATE_LIMITED 错误                             │
│                                                                │
│  Layer 2: 用户级限频                                           │
│  ─────────────────                                             │
│  每用户每分钟最多 M 个 agent.submit (默认 20)                  │
│  跨连接累计 (Redis 滑动窗口)                                   │
│  超过 → res 返回 RATE_LIMITED + retryAfterMs                   │
│                                                                │
│  Layer 3: 全局级限频                                           │
│  ─────────────────                                             │
│  系统每秒最多 P 个 agent.submit (默认 100)                     │
│  超过 → res 返回 SYSTEM_BUSY + 排队位置                        │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### 11.2 慢客户端保护

如果客户端消费事件太慢（WS 写缓冲区积压），Gateway 采取以下策略：

```text
缓冲区大小
  │
  ├── < 64KB    正常推送
  ├── 64-256KB  标记为慢客户端，delta 事件聚合窗口从 150ms 增大到 500ms
  ├── 256KB-1MB 丢弃 delta 事件，只保留 lifecycle 事件 (run_start/complete/error)
  └── > 1MB     强制断开连接 (code 4008: buffer overflow)
```

### 11.3 事件聚合

Gateway 对下行 `agent.delta` 事件做 150ms 窗口聚合：

```text
Runtime 发出:  T+0ms  "好"
               T+30ms "的"
               T+60ms "，"
               T+90ms "我"
               T+120ms "来"

Gateway 聚合: T+150ms "好的，我来"  → 一个 delta 事件推给客户端
```

---

## 十二、错误处理

### 12.1 错误码表

| 错误码 | 说明 | retryable | 触发场景 |
| --- | --- | --- | --- |
| `INVALID_REQUEST` | 请求参数无效 | false | 帧格式错误、缺少必填字段 |
| `UNKNOWN_METHOD` | 未知方法名 | false | method 不在支持列表中 |
| `UNAUTHORIZED` | 认证失败 | false | JWT 无效/过期且未刷新 |
| `FORBIDDEN` | 权限不足 | false | 用户角色不允许该操作 |
| `NOT_FOUND` | 资源不存在 | false | session/task 不存在 |
| `RATE_LIMITED` | 请求频率超限 | true | 触发任一层限频 |
| `SYSTEM_BUSY` | 系统繁忙 | true | 所有 Worker 满载 |
| `AGENT_TIMEOUT` | Agent 执行超时 | true | 任务执行超过上限 |
| `SESSION_LOCKED` | Session 被占用 | true | 同一 session 已有任务在执行 |
| `IDEMPOTENCY_CONFLICT` | 幂等键冲突 | false | 相同 key 不同参数 |
| `INTERNAL_ERROR` | 内部错误 | false | 服务端未预期的异常 |

### 12.2 错误响应示例

```json
{
  "type": "res",
  "id": "req_01JK...",
  "code": 429,
  "status": "error",
  "error": {
    "reason": "RATE_LIMITED",
    "message": "请求过于频繁，请稍后再试",
    "retryable": true,
    "retryAfterMs": 5000
  }
}
```

### 12.3 无效帧处理

| 情况 | 服务端行为 |
| --- | --- |
| 帧不是合法 JSON | 发送 error res（id="unknown"），递增错误计数器 |
| 缺少 `type` 字段 | 发送 error res |
| `type` 不是 `req` | 忽略（客户端不应发送 res/event） |
| 帧大小超过 `maxFrameBytes` | 关闭连接 (code 4009: frame too large) |
| 连续 5 个无效帧 | 关闭连接 (code 4010: too many errors) |

---

## 十三、心跳与超时

### 13.1 双层心跳

```text
Layer 1: WebSocket Ping/Pong (传输层)
  ├── Gateway 每 30s 发送 WS ping 帧
  ├── 客户端自动回复 pong
  └── 10s 内无 pong → 关闭连接

Layer 2: tick 事件 (应用层)
  ├── Gateway 每 30s 发送 tick 事件
  ├── 客户端用于检测"应用层活跃"
  └── 2 倍间隔 (60s) 无 tick → 客户端认为连接死亡，主动重连
```

### 13.2 超时配置

| 超时 | 值 | 说明 |
| --- | --- | --- |
| 握手超时 | 5s | 连接建立后 5s 内未收到 welcome → 断开 |
| 请求超时（默认） | 30s | 客户端发送 req 后 30s 无 res → 超时 |
| agent.submit 超时 | 5min | 等待 final 响应的超时 |
| idle 超时 | 10min | 10 分钟无任何 req → 服务端关闭连接（节省资源） |
| auth 刷新宽限期 | 5min | JWT 过期后 5 分钟内未刷新 → 强制关闭 |

---

## 十四、安全

### 14.1 传输安全

| 要求 | 说明 |
| --- | --- |
| TLS 强制 | 生产环境只允许 `wss://`，HTTP 重定向到 HTTPS |
| TLS 版本 | ≥ TLS 1.2 |
| 证书 | Let's Encrypt / 商业证书 |

### 14.2 输入校验

所有客户端 `req` 帧必须通过 JSON Schema 验证：

```go
// Go Gateway 帧验证伪代码
func (h *Hub) handleFrame(conn *Conn, raw []byte) {
    // 1. 大小检查
    if len(raw) > maxFrameBytes {
        conn.Close(4009, "frame too large")
        return
    }

    // 2. JSON 解析
    var frame Frame
    if err := json.Unmarshal(raw, &frame); err != nil {
        conn.SendError("unknown", "INVALID_REQUEST", "invalid JSON")
        return
    }

    // 3. 帧类型检查
    if frame.Type != "req" {
        return // 忽略非 req 帧
    }

    // 4. 方法级参数校验
    if err := validateParams(frame.Method, frame.Params); err != nil {
        conn.SendError(frame.ID, "INVALID_REQUEST", err.Error())
        return
    }

    // 5. Rate Limit 检查
    if !h.rateLimiter.Allow(conn) {
        conn.SendError(frame.ID, "RATE_LIMITED", "too many requests")
        return
    }

    // 6. 路由到 handler
    h.dispatch(conn, frame)
}
```

### 14.3 注入防护

- 用户消息（`message` 字段）直接传递给 Runtime，不在 Gateway 层执行
- Session key 格式校验：只允许 `[a-zA-Z0-9_-]`，禁止路径穿越
- 帧大小上限 1MB，防止内存耗尽攻击

---

## 十五、客户端 SDK 指南

### 15.1 最小实现清单

| # | 功能 | 优先级 | 说明 |
| --- | --- | --- | --- |
| 1 | WS 连接建立 | 必须 | 支持 `wss://` + Authorization header |
| 2 | 等待 `welcome` 事件 | 必须 | 存储 `resumeToken` 和 `policy` |
| 3 | req/res 请求匹配 | 必须 | 基于 `id` 字段匹配 |
| 4 | 双响应处理 | 必须 | `accepted` vs `final` 区分 |
| 5 | 事件监听 | 必须 | 按 `event` 字段分发 |
| 6 | seq 跟踪 | 必须 | 每个 session 的 seq 递增检查 |
| 7 | 自动重连 | 必须 | 指数退避 (1s→30s)，携带 `resumeToken` |
| 8 | **静默重连** | 必须 | goodbye `silent:true` 时不显示任何断连 UI（§15.4） |
| 9 | tick 超时检测 | 必须 | 2 × tickInterval 无 tick → 重连 |
| 10 | JWT 自动刷新 | 必须 | 收到 `auth.expiring` → HTTP 刷新 → WS `auth.refresh` |
| 11 | 幂等键生成 | 必须 | 每个有副作用的 req 生成唯一 key |

### 15.2 重连伪代码

```text
function reconnect():
    delay = min(baseDelay * 2^attempt, 30s)
    delay += random(0, delay * 0.2)   // jitter 防惊群
    sleep(delay)

    url = "wss://api.sahara.com/ws"
    if resumeToken:
        url += "?resumeToken={resumeToken}&lastSeqs={buildLastSeqs()}"

    jwt = getValidJWT()  // 如果过期，先 HTTP refresh
    ws = new WebSocket(url, headers={"Authorization": "Bearer " + jwt})

    ws.onopen:
        // 等待 welcome 事件
        startHandshakeTimeout(5s)

    ws.onmessage(frame):
        if frame.event == "welcome":
            clearHandshakeTimeout()
            resumeToken = frame.payload.resumeToken
            attempt = 0    // 重置退避
            onConnected()
        elif frame.event == "replay.complete":
            onResumeComplete()
        elif frame.type == "event":
            trackSeq(frame.sessionKey, frame.seq)
            onEvent(frame)
        elif frame.type == "res":
            resolveRequest(frame.id, frame)

    ws.onclose(code, reason):
        if code == 4001:  // auth expired
            jwt = refreshJWT()  // 必须刷新后再重连
        if code != 1000:  // 非正常关闭
            attempt++
            reconnect()
```

### 15.3 seq 间隙处理

```text
function trackSeq(sessionKey, seq):
    expected = lastSeqs[sessionKey] + 1

    if seq == expected:
        lastSeqs[sessionKey] = seq       // 正常
    elif seq > expected:
        gap = seq - expected
        log.warn("event gap: missing {gap} events for {sessionKey}")
        lastSeqs[sessionKey] = seq       // 跳过间隙
        // 客户端可选择: 忽略 / 请求 session.get 刷新状态
    elif seq <= lastSeqs[sessionKey]:
        log.debug("duplicate event, ignoring")  // 重复事件（回放场景）
```

### 15.4 静默重连（零感知部署）

服务端滚动更新时会发送 `goodbye` 事件并携带 `silent: true`。客户端必须区分**静默重连**和**异常重连**，确保用户在服务发版时完全无感：

```text
state:
    silentMode = false
    silentTimer = null

// -------- 收到 goodbye 事件 --------
onGoodbye(event):
    if event.payload.silent:
        // 服务端主动关闭（滚动更新），进入静默模式
        silentMode = true

        // 立即准备重连，不等连接断开
        scheduleReconnect(delay = event.payload.reconnectAfterMs)

        // 静默超时保护: 超过 3 秒未恢复则降级显示 UI
        silentTimer = setTimeout(3000, () => {
            silentMode = false
            showReconnectingUI()
        })
    else:
        // 非静默关闭（如被封禁、idle 超时），正常提示用户
        showDisconnectReason(event.payload.reason, event.payload.message)

// -------- 连接断开 --------
onDisconnect(code, reason):
    if silentMode:
        // 预期内的断开，不做任何 UI 变化
        return
    if code == 4001:
        refreshJWT()
    if code != 1000:
        showDisconnectedUI()
        startReconnectWithBackoff()

// -------- 重连成功 --------
onReconnected(welcomeEvent):
    clearTimeout(silentTimer)
    if silentMode:
        silentMode = false
        // 静默恢复完成，不显示任何提示
        log.debug("silent reconnect succeeded")
    else:
        showToast("已恢复连接")

    // 通用逻辑: 更新 resumeToken，等待事件回放
    resumeToken = welcomeEvent.payload.resumeToken
```

**静默重连的关键行为约束**：

| 行为 | 静默重连 (`silent: true`) | 异常重连 |
| --- | --- | --- |
| 显示"连接已断开" | **禁止** | 显示 |
| 显示"正在重连..." | **禁止**（3 秒内） | 显示 |
| 显示 loading 遮罩 | **禁止** | 可选 |
| 聊天界面闪烁 | **禁止** | 可接受 |
| 正在打字的输入框 | 保持不变 | 保持不变 |
| 正在流式显示的文字 | 暂停，重连后续上 | 暂停，重连后续上 |
| 静默超时 (>3s) | 降级为异常重连 UI | — |

> **详见**：[Gateway 架构设计 §14](./GATEWAY-ARCHITECTURE-DESIGN.md) — 零感知部署的完整服务端实现。

---

## 十六、性能指标

### 16.1 目标基线

| 指标 | 目标 | 说明 |
| --- | --- | --- |
| 连接建立 (含握手) | < 200ms | TLS + Upgrade + welcome |
| req → accepted res | < 50ms | 不含 Runtime 执行时间 |
| 事件延迟 (Runtime → Client) | < 200ms | 含 Event Bus 传输 + 150ms 聚合 |
| 单 Gateway 连接数 | ≥ 50,000 | goroutine per conn |
| 内存/连接 | < 16KB | 含读写缓冲区和连接元数据 |
| tick 间隔 | 30s | 可配置 |

### 16.2 帧大小优化

| 数据类型 | 策略 |
| --- | --- |
| delta 事件 | 150ms 窗口聚合，减少帧数 |
| 工具结果 | 截断到 10KB（完整结果通过 session.get 获取） |
| 附件 | 通过 HTTP 上传，WS 只传 URL |
| 历史消息 | 分页返回，每页最多 50 条 |

---

## 附录

### 附录 A. WS 关闭码

| 码 | 说明 |
| --- | --- |
| 1000 | 正常关闭 |
| 1001 | 服务端关闭 (going away) |
| 1008 | 协议违规 |
| 1009 | 帧过大 |
| 1011 | 服务端内部错误 |
| **4001** | JWT 过期且未在宽限期内刷新 |
| **4003** | 用户被封禁 |
| **4008** | 客户端写缓冲溢出 (慢客户端) |
| **4009** | 帧大小超限 |
| **4010** | 连续无效帧过多 |
| **4029** | 连接数超限 |
| **4100** | Gateway 滚动更新（客户端应立即重连） |

### 附录 B. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| 第三章 帧格式 | P1-2 WS 帧解析 | Phase 1 |
| 第四章 连接生命周期 | P1-1 WS 连接管理 | Phase 1 |
| 第五章 JWT 认证 | P2-1 JWT 无状态认证 | Phase 2 |
| 第六章 RPC 方法 | P1-3 RPC 路由表 | Phase 1 |
| 第七章 事件定义 | P1-9 Gateway 事件消费 | Phase 1 |
| 第八章 双响应模式 | P1-9 + P1-10 | Phase 1 |
| 第九章 断线恢复 | P2-4 Gateway 多实例 | Phase 2 |
| 第十章 多实例路由 | P2-4 Gateway 多实例 | Phase 2 |
| 第十一章 限频 | P1-5 + P2-3 Rate Limiting | Phase 1-2 |
| 第十二章 错误处理 | P1-2 WS 帧解析 + P1-3 RPC 路由表 | Phase 1 |
| 第十三章 心跳与超时 | P1-1 WS 连接管理 | Phase 1 |
| 第十四章 安全 | P2-1 JWT 认证 + P1-5 Rate Limiting | Phase 1-2 |
| 第十五章 客户端 SDK | — (前端团队自行实现) | Phase 1 |
| 第十六章 性能指标 | — (全阶段持续度量) | Phase 1+ |

### 附录 C. OpenClaw 参考与设计对比

> Sahara WS 协议在设计过程中参考了 OpenClaw 项目的 Gateway 协议。以下记录两者的设计差异和字段映射，供团队追溯设计决策。

**参考文档**：

| 文档 | 路径 | 参考价值 |
| --- | --- | --- |
| Gateway 协议详解 | [GATEWAY-PROTOCOL.md](../openclaw/gateway/GATEWAY-PROTOCOL.md) | 帧格式 (req/res/event)、双响应模式、序列号设计 |
| Gateway 认证与配对 | [GATEWAY-AUTH.md](../openclaw/gateway/GATEWAY-AUTH.md) | 多层认证体系、设备配对流程（Sahara 改为 JWT） |
| Gateway 客户端通信 | [GATEWAY-CLIENT.md](../openclaw/gateway/GATEWAY-CLIENT.md) | 客户端生命周期、重连机制、心跳检测 |

**核心设计差异**：

| 维度 | OpenClaw | Sahara | 变化原因 |
| --- | --- | --- | --- |
| 定位 | 单用户本地部署 | 多用户云端部署 | C 端场景 |
| 认证 | 静态 Token / Password / 设备配对 | JWT + Refresh Token | 多用户无状态鉴权 |
| 握手 | 帧内 connect 方法 (4 步) | HTTP Upgrade + JWT (2 步) | 减少 RTT |
| 方法集 | ~80 个（含管理/配置/节点/渠道） | ~15 个（纯用户操作） | 最小攻击面 |
| 事件传递 | 进程内回调 (emitAgentEvent) | Event Bus (Redis Streams) | 跨进程解耦 |
| 事件命名 | 通用 `event: "agent"` + payload.stream | 具体 `event: "agent.delta"` 等 | 一级分发更清晰 |
| 断线恢复 | 重连获取新 snapshot | resumeToken + 事件回放 | 不丢事件 |
| 序列号 | 全局 seq | session 级 seq | 多 session 隔离 |
| 限频 | 无 | 三层 (连接/用户/全局) | C 端防滥用 |
| 多模态 | 无 | agent.content 事件 + URL 引用 | LLM 多模态输出 |

**帧字段映射**：

| OpenClaw 字段 | Sahara 字段 | 说明 |
| --- | --- | --- |
| `type: "req"` | `type: "req"` | 保持 |
| `id` | `id` | 保持 |
| `method: "agent"` | `method: "agent.submit"` | 方法名更明确 |
| `params.message` | `params.message` | 保持 |
| `params.idempotencyKey` | `params.idempotencyKey` | 保持 |
| `params.thinking` | `params.options.thinking` | 移入 options 子对象 |
| — | `params.options.model` | 新增：客户端可选模型 |
| `type: "res"` + `ok: boolean` | `type: "res"` + `code: integer` | boolean 改为数字状态码 |
| — | `status: "ok"/"accepted"/"final"/"error"` | 新增：业务阶段状态 |
| `error.code` (string) | `error.reason` (string) | 重命名，避免与外层 code 冲突 |
| `event: "agent"` + `payload.stream` | `event: "agent.delta"` 等具体事件名 | 拆分为一级事件 |
| `event: "tick"` | `event: "tick"` | 保持 |
| `event: "connect.challenge"` | 移除 | HTTP Upgrade 阶段已完成认证 |
| `payload.stateVersion` | 移除 | C 端不需要全局状态版本 |
| — | `welcome` 中 `resumeToken` | 新增：断线恢复支持 |
| — | `event: "auth.expiring"` | 新增：JWT 过期提醒 |
| — | `event: "agent.content"` | 新增：多模态内容块 |
