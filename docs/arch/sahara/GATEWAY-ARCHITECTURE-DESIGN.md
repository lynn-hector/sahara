# Gateway 架构设计

> Go Gateway 的内部模块划分、并发模型、数据流与核心接口定义。
> 本文档是 Go 开发组的实现蓝图，覆盖从接收第一个 WS 连接到推送最后一个事件的完整数据路径。
>
> 关联文档：
> - [gRPC 协议设计](./GRPC-PROTOCOL-DESIGN.md) — Gateway 作为 gRPC Client 调用 Runtime（D1）
> - [WebSocket 协议设计](./WS-PROTOCOL-DESIGN.md) — Gateway 作为 WS Server 面向客户端（D2）
> - [API Service 设计](./API-SERVICE-DESIGN.md) — C 端 RESTful API 服务（D7，用户/会话/文件等 HTTP 接口）
> - [异步事件传输协议](./EVENT-BUS-DESIGN.md) — Runtime ↔ Gateway 事件传输规范（D5）
> - [技术方案 §三](./TECH-PROPOSAL-C-END-REFACTOR.md) — Gateway 七大职责与技术选型

---

## 目录

1. [模块全景](#一模块全景)
2. [Go 包结构](#二go-包结构)
3. [核心类型与接口](#三核心类型与接口)
4. [连接管理 (Hub + Conn)](#四连接管理-hub--conn)
5. [帧处理管线](#五帧处理管线)
6. [RPC 路由器](#六rpc-路由器)
7. [Agent 调度器 (Dispatcher)](#七agent-调度器-dispatcher)
8. [事件广播器 (Broadcaster)](#八事件广播器-broadcaster)
9. [认证模块](#九认证模块)
10. [Rate Limiter](#十rate-limiter)
11. [Session Router](#十一session-router)
12. [Worker Registry](#十二worker-registry)
13. [HTTP 服务](#十三http-服务)
14. [优雅关闭与零感知部署](#十四优雅关闭与零感知部署)
15. [配置管理](#十五配置管理)
16. [可观测性](#十六可观测性)
17. [并发模型总览](#十七并发模型总览)

---

## 一、模块全景

### 1.1 四层架构

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  Gateway 内部架构 (Go)                                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌───────────────────── 接入层 ──────────────────────────────────────┐ │
│  │                                                                    │ │
│  │  ┌──────────────┐   ┌──────────────┐   ┌───────────────────────┐ │ │
│  │  │  WS Listener │   │ HTTP Minimal │   │  Channel Adapters     │ │ │
│  │  │  nhooyr/ws   │   │  /ws 升级    │   │  (Phase 2)            │ │ │
│  │  │              │   │  /v1/* SSE   │   │                       │ │ │
│  │  │              │   │  /healthz    │   │                       │ │ │
│  │  └──────┬───────┘   └──────┬───────┘   └───────────┬───────────┘ │ │
│  │         │                  │                        │             │ │
│  └─────────┼──────────────────┼────────────────────────┼─────────────┘ │
│            │                  │                        │               │
│  ┌─────────┼──────────── 管线层 ──────────────────────┼───────────┐   │
│  │         ▼                  ▼                        ▼           │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │   │
│  │  │  Auth        │  │  RateLimiter │  │  Frame Validator     │ │   │
│  │  │  (JWT)       │  │  (3 层)      │  │  (JSON Schema)       │ │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘ │   │
│  │         └─────────────────┼──────────────────────┘             │   │
│  │                           ▼                                    │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │   │
│  │  │  RPC Router  │  │  Dedup       │  │  Idempotency Cache   │ │   │
│  │  │  method →    │  │  (Redis)     │  │  (Redis)             │ │   │
│  │  │  handler     │  └──────────────┘  └──────────────────────┘ │   │
│  │  └──────┬───────┘                                              │   │
│  └─────────┼──────────────────────────────────────────────────────┘   │
│            │                                                           │
│  ┌─────────┼──────────── 业务层 ──────────────────────────────────┐   │
│  │         ▼                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │   │
│  │  │  Dispatcher  │  │  Session     │  │  Worker Registry     │ │   │
│  │  │  (gRPC 调度) │  │  Manager     │  │  (服务发现)          │ │   │
│  │  └──────────────┘  └──────────────┘  └──────────────────────┘ │   │
│  │                                                                │   │
│  │  ┌──────────────────────────────────────────────────────────┐ │   │
│  │  │  Broadcaster (事件广播)                                    │ │   │
│  │  │  Event Bus Consumer → Session Route → Aggregate → Push   │ │   │
│  │  └──────────────────────────────────────────────────────────┘ │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────── 基础设施层 ────────────────────────────────────┐ │
│  │  Redis Client │ gRPC Client Pool │ Config │ Logger │ Metrics      │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 模块职责速查

| 模块 | 包路径 | 核心职责 | 依赖 |
| --- | --- | --- | --- |
| **Hub** | `internal/ws` | WS 连接生命周期、读写 goroutine 调度 | — |
| **FramePipeline** | `internal/ws` | 帧解析 → 校验 → Auth → RateLimit → 路由 | Hub |
| **Router** | `internal/router` | method → Handler 映射、参数校验 | — |
| **Dispatcher** | `internal/dispatch` | gRPC 调用 Runtime Worker、负载均衡、重试 | WorkerRegistry |
| **Broadcaster** | `internal/broadcast` | Event Bus 消费 → session 路由 → 聚合 → WS 推送 | Hub, SessionRouter |
| **Auth** | `internal/auth` | JWT 验证、Token 刷新、权限检查 | Redis |
| **RateLimiter** | `internal/ratelimit` | 三层限频（连接/用户/全局） | Redis |
| **SessionRouter** | `internal/session` | session ↔ connId 映射表、Resume Token | Redis |
| **WorkerRegistry** | `internal/registry` | Worker 列表、健康检查、负载信息 | gRPC, Redis |
| **HTTPServer** | `internal/httpapi` | WS 升级入口、OpenAI 兼容 SSE、运维端点（业务 API 在 sahara-api） | Dispatcher |
| **Config** | `internal/config` | 配置加载、校验、热更新 | — |

---

## 二、Go 包结构

```text
gateway/
├── cmd/
│   └── sahara-gw/
│       └── main.go                 # 入口：初始化 → 启动 → 优雅关闭
├── internal/                       # 内部包（不对外暴露）
│   ├── ws/                         # WebSocket 核心
│   │   ├── hub.go                  # 连接注册表 (Hub)
│   │   ├── conn.go                 # 单连接抽象 (Conn)
│   │   ├── frame.go                # 帧定义与序列化
│   │   ├── pipeline.go             # 入站帧处理管线
│   │   ├── writer.go               # 出站帧写入（channel 串行化）
│   │   └── upgrader.go             # HTTP → WS 升级 + JWT 验证
│   ├── router/                     # RPC 路由
│   │   ├── router.go               # method → Handler 注册表
│   │   └── handlers.go             # 各 method handler 实现
│   ├── dispatch/                   # Agent 调度
│   │   ├── dispatcher.go           # gRPC 调度 + 负载均衡
│   │   └── retry.go                # Worker 轮转重试
│   ├── broadcast/                  # 事件广播
│   │   ├── consumer.go             # Event Bus 消费者 (Redis Streams)
│   │   ├── aggregator.go           # 150ms delta 聚合
│   │   └── broadcaster.go          # session → conns 推送
│   ├── auth/                       # 认证
│   │   ├── jwt.go                  # JWT 验证/解析
│   │   └── middleware.go           # 认证中间件
│   ├── ratelimit/                  # 限频
│   │   ├── limiter.go              # 三层限频接口
│   │   ├── conn_limiter.go         # 连接级 (本地令牌桶)
│   │   ├── user_limiter.go         # 用户级 (Redis 滑动窗口)
│   │   └── global_limiter.go       # 全局级 (Redis 滑动窗口)
│   ├── session/                    # Session 管理
│   │   ├── router.go               # session → conns 路由表
│   │   └── resume.go               # Resume Token 管理
│   ├── registry/                   # Worker 注册
│   │   ├── registry.go             # Worker 列表与状态
│   │   └── health.go               # 健康检查轮询
│   ├── httpapi/                    # HTTP 端点 (仅 GW 自身，业务 API 在 sahara-api)
│   │   ├── server.go               # 路由注册 (/ws, /v1/*, /healthz, /metrics)
│   │   ├── openai_compat.go        # /v1/chat/completions (SSE)
│   │   └── health.go               # /healthz, /readyz (含 draining 状态)
│   ├── config/                     # 配置
│   │   ├── config.go               # 配置结构体 + 默认值
│   │   └── loader.go               # 环境变量 + 文件加载
│   └── observe/                    # 可观测性
│       ├── metrics.go              # Prometheus 指标定义
│       ├── logger.go               # slog 配置
│       └── tracing.go              # OpenTelemetry 配置
├── gen/                            # Proto 生成的 Go 代码
│   └── sahara/
│       ├── agent/v1/
│       ├── worker/v1/
│       ├── common/v1/
│       └── event/v1/
├── go.mod
├── go.sum
└── Dockerfile
```

**包依赖规则**：

```text
cmd/sahara-gw  →  所有 internal/* (组装)
internal/ws    →  internal/router, internal/auth, internal/ratelimit
internal/router →  internal/dispatch, internal/session
internal/dispatch → internal/registry, gen/ (gRPC)
internal/broadcast → internal/ws (Hub), internal/session
internal/httpapi → internal/dispatch, internal/auth  (仅 /v1/* SSE + /ws 升级)
```

**禁止循环依赖**：`ws` 不依赖 `broadcast`，`broadcast` 不依赖 `router`。模块间通过接口解耦。

---

## 三、核心类型与接口

本章定义 Gateway 内部所有核心类型（struct）和接口（interface）的职责、字段含义、以及它们之间的协作关系。

### 3.0 类型与接口全景图

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Gateway 核心类型与接口关系                                                     │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐               │
│  │                         外部世界                             │               │
│  │  Client (WS)        Runtime (gRPC)      Event Bus (Redis)   │               │
│  └──────┬──────────────────┬───────────────────┬───────────────┘               │
│         │                  │                   │                                │
│         ▼                  ▼                   ▼                                │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────┐                        │
│  │  Upgrader   │   │   gRPC Pool  │   │  EventSub    │                        │
│  │  (HTTP→WS)  │   │  (连接池)    │   │  (消费者)    │                        │
│  └──────┬──────┘   └──────┬───────┘   └──────┬───────┘                        │
│         │                  │                   │                                │
│         ▼                  │                   ▼                                │
│  ┌─────────────┐          │            ┌──────────────┐                        │
│  │    Conn     │◄─────────┼────────────│ Broadcaster  │                        │
│  │  (WS 连接)  │          │            │ (事件分发)   │                        │
│  └──────┬──────┘          │            └──────┬───────┘                        │
│         │ register/       │                   │ ConnLookup                      │
│         │ unregister      │                   │ 接口                            │
│         ▼                  │                   ▼                                │
│  ┌──────────────────────────────────────────────────────┐                      │
│  │                      Hub                              │                      │
│  │              (连接注册表, 中央协调器)                  │                      │
│  │                                                       │                      │
│  │  实现接口: ConnLookup                                 │                      │
│  │  依赖接口: SessionRouter, Metrics                     │                      │
│  └──────────────────────┬────────────────────────────────┘                      │
│                         │                                                       │
│         ┌───────────────┼───────────────┐                                      │
│         ▼               ▼               ▼                                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────────┐                           │
│  │SessionRouter│  │  Router    │  │ TaskDispatcher │                           │
│  │  (接口)    │  │ (方法路由) │  │   (接口)       │                           │
│  │  ↓ impl   │  │            │  │   ↓ impl       │                           │
│  │  Redis     │  │ method →   │  │   gRPCDispatch │                           │
│  │  实现      │  │  Handler   │  │   er           │                           │
│  └────────────┘  └─────┬──────┘  └────────────────┘                           │
│                        │                                                        │
│                        ▼                                                        │
│                 ┌────────────┐                                                  │
│                 │  Handler   │                                                  │
│                 │  (接口)    │  ← agent.submit / agent.abort / ...             │
│                 │  ↓ impl   │                                                  │
│                 │  各业务    │                                                  │
│                 │  Handler   │                                                  │
│                 └────────────┘                                                  │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────┐                      │
│  │  支撑接口                                             │                      │
│  │  WorkerRegistry    RateLimiter     IdempotencyStore  │                      │
│  │  (Worker 发现)     (限频)           (幂等去重)        │                      │
│  └──────────────────────────────────────────────────────┘                      │
│                                                                                 │
│  设计原则:                                                                      │
│  • struct 持有状态和行为, interface 定义跨模块契约                              │
│  • 上层依赖接口, 不依赖实现 (DIP)                                              │
│  • Hub 是唯一的可变共享状态, 其他模块通过接口访问                               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Conn — 单个 WS 连接

**职责**：封装一个客户端 WebSocket 连接的全部状态和行为。每个 Conn 是一个独立的生命周期单元——从 Upgrade 成功到连接关闭。

**与其他类型的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| 被管理于 | `Hub` | Hub.conns 持有 Conn 的引用，Conn 通过 hub 字段反向引用 Hub |
| 驱动 | `FramePipeline` | readPump 读到的帧交给 pipeline 处理 |
| 被写入 | `Broadcaster` / `Handler` | 通过 `SendEvent()` 往 writeCh 投递帧 |

```go
// internal/ws/conn.go

type Conn struct {
    // ── 身份信息 (Upgrade 时从 JWT 填充, 只读, 无需锁) ──
    ID          string     // 全局唯一: {gatewayID}_{ulid}
    UserID      string     // JWT claims.sub — 用户 ID
    Roles       []string   // JWT claims.roles — 权限角色
    RemoteAddr  string     // 客户端 IP (X-Forwarded-For 优先)
    ClientInfo  ClientInfo // 客户端在 connect 帧中上报的信息
    ConnectedAt time.Time  // 连接建立时间

    // ── 底层 WS 连接 (私有, 仅 readPump/writePump 访问) ──
    ws        *websocket.Conn // nhooyr/websocket 连接实例
    writeCh   chan []byte      // 写通道: 所有要发送的帧先入此 channel
                               //   → writePump 从中取出并写入 WS
                               //   → 缓冲 256, 避免发送方阻塞
    done      chan struct{}    // 关闭信号: close(done) 通知所有 goroutine 退出
    closeOnce sync.Once       // 保证 Close() 幂等, 只执行一次

    // ── 运行时状态 (原子操作, 无锁并发安全) ──
    lastActivity atomic.Int64  // 最后活跃时间 (unix ms)
                               //   → readPump 每收到帧更新
                               //   → 空闲检测 timer 读取判断超时
    frameCount   atomic.Uint64 // 接收帧计数 (单位时间)
                               //   → 连接级限频使用
    slowCount    atomic.Int32  // 连续写满次数
                               //   → ≥ 3 次判定慢客户端, 踢下线
    seq          atomic.Uint64 // 出站事件序列号 (递增)
                               //   → 用于断线恢复: 客户端用 lastSeq 告知断点

    // ── 引用 (构造时注入, 只读) ──
    hub      *Hub           // 所属 Hub, 用于注销自身和 metrics 上报
    pipeline *FramePipeline // 帧处理管线, readPump 将帧交给它
    config   *ConnConfig    // 连接级配置 (超时, 缓冲大小等)
}

type ClientInfo struct {
    ID       string `json:"id"`       // 客户端标识: sahara-web, sahara-ios, ...
    Version  string `json:"version"`  // 客户端版本: 1.2.0
    Platform string `json:"platform"` // 平台: web, ios, android
}

type ConnConfig struct {
    WriteBufferSize int           // writeCh 缓冲大小 (默认 256)
    MaxFrameBytes   int64         // 读帧大小上限 (默认 1MB)
    IdleTimeout     time.Duration // 空闲超时 (默认 5 分钟)
    WriteTimeout    time.Duration // 单帧写入超时 (默认 5 秒)
}
```

**Conn 的生命周期**：

```text
  Upgrader.ServeHTTP
       │
       ▼
  NewConn(ws, claims, hub, pipeline)   ← 构造, 填充身份信息
       │
       ├── conn.SendEvent("welcome")   ← 发送欢迎帧
       ├── hub.register <- conn        ← 注册到 Hub
       ├── go conn.readPump()          ← 启动读 goroutine
       └── go conn.writePump()         ← 启动写 goroutine
                │
                ▼
       ┌────────────────────┐
       │  活跃状态           │ ← readPump 持续读帧
       │  • 接收请求帧       │   writePump 持续写帧
       │  • 发送事件帧       │
       │  • 心跳 tick        │
       └────────┬───────────┘
                │ 触发关闭 (断开/超时/踢下线/服务关闭)
                ▼
       conn.Close()
       │  closeOnce.Do:
       │    1. close(done)              ← 通知 readPump/writePump 退出
       │    2. close(writeCh)           ← writePump 自动退出
       │    3. ws.Close(code, reason)   ← 底层 WS 关闭
       │    4. hub.unregister <- conn   ← 从 Hub 注销
       ▼
  [Conn 被 GC 回收]
```

### 3.2 Hub — 连接注册表

**职责**：Gateway 中所有 WS 连接的**中央登记处**和**协调器**。它是整个 Gateway 中唯一的可变共享状态持有者，所有需要查找、遍历连接的模块都通过 Hub（或其暴露的 `ConnLookup` 接口）访问。

**与其他类型的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| 管理 | `Conn` | 持有所有 Conn 的引用，管理其注册和注销 |
| 实现 | `ConnLookup` 接口 | 对外暴露只读查询能力，隐藏内部实现 |
| 依赖 | `SessionRouter` 接口 | 注册/注销时同步更新 Redis 路由表 |
| 被依赖 | `Broadcaster` | 通过 ConnLookup 查找目标连接推送事件 |
| 被依赖 | `Upgrader` | 注册新连接、检查连接数 |

```go
// internal/ws/hub.go

type Hub struct {
    // ── 连接存储 (受 mu 保护) ──
    conns        map[string]*Conn    // connID → Conn     全量索引
    userConns    map[string][]*Conn  // userID → [Conn]   同一用户多设备/多 Tab
    sessionConns map[string][]*Conn  // sessionKey → [Conn]  供 Broadcaster 本地优先查找
    mu           sync.RWMutex        // 读多写少: 读用 RLock, 写走 channel 串行

    // ── 写操作通道 (串行化注册/注销) ──
    register   chan *Conn // Upgrader → Hub.Run(): 新连接注册
    unregister chan *Conn // Conn.Close() → Hub.Run(): 连接注销

    // ── 快速计数 (原子操作, 供 Upgrader 无锁读取) ──
    connCount atomic.Int64 // 当前总连接数

    // ── 依赖 (构造时注入) ──
    sessionRouter SessionRouter   // Redis 路由表, 注册/注销时同步
    metrics       *observe.Metrics // Prometheus 指标
    config        *HubConfig       // 连接上限等配置
}

type HubConfig struct {
    MaxConnsTotal   int // 全局最大连接数 (默认 100,000)
    MaxConnsPerUser int // 单用户最大连接数 (默认 5)
    RegisterChSize  int // register channel 缓冲 (默认 256)
    UnregisterChSize int // unregister channel 缓冲 (默认 256)
}
```

**ConnLookup 接口 — Hub 的只读视图**：

```go
// internal/ws/lookup.go

// ConnLookup 是 Hub 对外暴露的只读查询接口。
// Broadcaster、Handler 等模块依赖此接口而非 Hub 实现,
// 实现了依赖倒置 (DIP), 也便于测试时 mock。
type ConnLookup interface {
    // ConnsByUser 返回指定用户的所有活跃连接 (多设备/多 Tab)
    // 用途: Broadcaster 向用户的所有设备推送事件
    ConnsByUser(userID string) []*Conn

    // ConnByID 返回指定 ID 的连接, 不存在返回 nil
    // 用途: SessionRouter 查找到 connID 后定位具体连接
    ConnByID(connID string) *Conn

    // ConnsBySession 返回监听指定 session 的所有本地连接
    // 用途: Broadcaster 本地优先查找, 避免 Redis IO (详见 §8.2)
    ConnsBySession(sessionKey string) []*Conn

    // ConnCount 返回当前总连接数
    // 用途: Upgrader 阶段 1 全局限制检查, metrics 上报
    ConnCount() int

    // UserConnCount 返回指定用户的连接数
    // 用途: Upgrader 阶段 1 用户限制检查
    UserConnCount(userID string) int
}
```

> **为什么要分离 ConnLookup 接口？** Hub 内部有 register/unregister channel、RWMutex 等复杂并发原语。如果让 Broadcaster 直接依赖 Hub struct，会暴露过多内部细节，也无法在单测中用简单的 mock 替代。ConnLookup 只暴露"查找"能力，是最小接口原则（ISP）的体现。

### 3.3 FramePipeline — 帧处理管线

**职责**：对入站 WS 帧进行链式处理——解析、校验、限频、路由。类似 HTTP 中间件栈，但针对 WS 帧设计。

**与其他类型的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| 被调用 | `Conn.readPump` | 每收到一帧调用 `pipeline.Process()` |
| 调用 | `Router` | 管线末端将校验通过的请求帧路由到 Handler |
| 调用 | `RateLimiter` | 管线中的限频阶段 |
| 调用 | `IdempotencyStore` | 管线中的幂等检查阶段 |

```go
// internal/pipeline/pipeline.go

// Stage 是管线中的一个处理阶段
// 返回 error 表示拒绝该帧, 管线短路返回错误帧给客户端
type Stage interface {
    // Name 返回阶段名称, 用于 tracing 和 metrics
    Name() string
    // Process 处理帧, 返回 (可能修改的帧, error)
    // error 非 nil 时管线短路
    Process(ctx *FrameContext, raw json.RawMessage) (json.RawMessage, error)
}

// FrameContext 在管线各阶段之间传递上下文
type FrameContext struct {
    Conn      *ws.Conn        // 来源连接
    RequestID string          // 请求 ID (从帧中提取)
    Method    string          // RPC 方法名 (从帧中提取)
    TraceCtx  context.Context // OpenTelemetry span 上下文
    StartTime time.Time       // 用于计算管线延迟
}

// FramePipeline 将多个 Stage 串联为处理链。
// 分为同步阶段 (readPump goroutine 中, 无 IO) 和异步阶段 (独立 goroutine, 有 Redis IO)。
// 完整实现详见 §5.4。
type FramePipeline struct {
    syncStages  []Stage        // 同步阶段: Parse, AuthQuickCheck, ConnRateLimit, RouteLookup
    asyncStages []Stage        // 异步阶段: AuthBlacklist, UserRateLimit, Idempotency, Validate
    router      *Router        // 管线末端: 路由到具体 Handler
    metrics     *PipelineMetrics
}

// Process 是管线入口, 由 Conn.readPump 同步调用。
// 同步阶段在 readPump 中执行 (~8μs, 不阻塞); 异步阶段切换到独立 goroutine。
func (p *FramePipeline) Process(conn *ws.Conn, raw []byte) {
    ctx := &FrameContext{Conn: conn, StartTime: time.Now()}
    var msg json.RawMessage = raw

    // 同步阶段 (无 IO, 快)
    for _, stage := range p.syncStages {
        var err error
        msg, err = stage.Process(ctx, msg)
        if err != nil {
            conn.SendError(ctx.RequestID, err)
            return
        }
    }

    // 异步阶段 + Handler 执行 (有 IO, 在独立 goroutine 中)
    go p.executeAsync(conn, ctx, msg)
}
```

**内置阶段**：

| 顺序 | Stage 实现 | 职责 | 短路条件 |
| --- | --- | --- | --- |
| 1 | `ParseStage` | JSON 解析, 提取 type/id/method/params | 无效 JSON, 非 `"req"` 类型 |
| 2 | `AuthStage` | 校验连接是否仍已认证 (JWT 过期检查) | Token 已过期或已加入黑名单 |
| 3 | `RateLimitStage` | 连接级 + 用户级限频检查 | 超频 |
| 4 | `IdempotencyStage` | 幂等 key 去重 | 重复请求 (返回缓存结果) |
| 5 | `ValidateStage` | 请求参数校验 (必填字段、格式) | 参数校验失败 |

### 3.4 Router & Handler — RPC 方法路由与处理

**Router 职责**：将请求帧中的 `method` 字段映射到具体的 Handler 实例。类似 HTTP 路由器将 URL path 映射到 handler。

**Handler 职责**：处理一种具体的 RPC 方法（如 `agent.submit`、`session.list`）。每个 Handler 专注于自己的业务逻辑，不关心帧解析、认证、限频等横切关注点（这些由 Pipeline 处理）。

**与其他类型的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| Router 被调用 | `FramePipeline` | 管线末端调用 `router.Dispatch()` |
| Router 持有 | `Handler` (多个) | method → Handler 映射表 |
| Handler 依赖 | `TaskDispatcher` 接口 | agent.submit Handler 调用 Dispatcher 提交任务 |
| Handler 依赖 | `ConnLookup` 接口 | 部分 Handler 需要查找连接信息 |

```go
// internal/router/router.go

type Router struct {
    handlers map[string]Handler        // method → Handler 映射
    metas    map[string]*MethodMeta    // method → 方法元数据 (权限、限频策略等)
    metrics  *RouterMetrics
}

// MethodMeta 描述一个 RPC 方法的元数据
type MethodMeta struct {
    Method      string   // 方法名: "agent.submit"
    Category    string   // 分类: "agent" / "session" / "auth" / "system"
    RequireAuth bool     // 是否需要认证
    Roles       []string // 允许的角色 (空=全部角色可用)
    WriteOp     bool     // 是否为写操作 (true: 需要幂等去重和用户级限频)
}

// Register 注册 RPC 方法处理器
func (r *Router) Register(method string, h Handler, meta *MethodMeta) {
    r.handlers[method] = h
    r.metas[method] = meta
}

// Dispatch 根据 method 分发到对应 Handler (完整实现详见 §6.2)
func (r *Router) Dispatch(ctx *FrameContext, parsed *RequestFrame) {
    h, meta, ok := r.Lookup(parsed.Method)
    if !ok {
        ctx.Conn.SendErrorRes(parsed.ID, 404, "METHOD_NOT_FOUND", "unknown method")
        return
    }

    // RBAC 权限检查
    if meta.RequireAuth && len(meta.Roles) > 0 {
        if !hasAnyRole(ctx.Conn.Roles, meta.Roles) {
            ctx.Conn.SendErrorRes(parsed.ID, 403, "FORBIDDEN", "insufficient roles")
            return
        }
    }

    hctx := &HandlerContext{
        Conn: ctx.Conn, UserID: ctx.Conn.UserID, Roles: ctx.Conn.Roles,
        RequestID: parsed.ID, Method: parsed.Method, TraceCtx: ctx.TraceCtx,
    }

    result, err := h.Handle(hctx, parsed.Params)
    if err != nil {
        if errors.Is(err, ErrAlreadyResponded) { return }
        code, reason, msg := errorToResponse(err)
        ctx.Conn.SendErrorRes(parsed.ID, code, reason, msg)
        return
    }
    ctx.Conn.SendResponse(parsed.ID, 200, "ok", result)
}

// ── HandlerContext: 传递给 Handler 的上下文 ──

// HandlerContext 携带本次请求的全部上下文信息,
// Handler 无需自行提取认证信息或连接信息。
type HandlerContext struct {
    Conn      *ws.Conn        // 来源连接 (用于发送事件、获取用户信息)
    UserID    string          // 当前用户 ID (从 Conn 提取, 便于使用)
    Roles     []string        // 用户角色 (从 Conn 提取)
    RequestID string          // 请求 ID (用于关联响应帧)
    TraceCtx  context.Context // OpenTelemetry 追踪上下文
}

// ── Handler 接口 ──

// Handler 是所有 RPC 方法的统一接口。
// 每个具体方法实现此接口: AgentSubmitHandler, AgentAbortHandler, ...
type Handler interface {
    Handle(ctx *HandlerContext, params json.RawMessage) (any, error)
}

// HandlerFunc 函数适配器, 允许用普通函数注册为 Handler
type HandlerFunc func(ctx *HandlerContext, params json.RawMessage) (any, error)

func (f HandlerFunc) Handle(ctx *HandlerContext, params json.RawMessage) (any, error) {
    return f(ctx, params)
}
```

**已注册的 Handler 一览**：

| Method | Handler | 依赖接口 | 写操作 | 说明 |
| --- | --- | --- | --- | --- |
| `agent.submit` | `AgentSubmitHandler` | `TaskDispatcher` | ✓ | 提交 Agent 任务 |
| `agent.abort` | `AgentAbortHandler` | `TaskDispatcher` | ✓ | 中止正在执行的任务 |
| `agent.input` | `AgentInputHandler` | `Dispatcher` | ✓ | 人机交互回源（强制亲和, §7.4）|
| `agent.status` | `AgentStatusHandler` | `TaskDispatcher` | — | 查询任务状态 |
| `auth.refresh` | `AuthRefreshHandler` | `AuthModule` | — | WS 内 Token 续期（§9.6）|
| `ping` | `PingHandler` | — | — | 应用层心跳应答 |

> 随着业务扩展，新增 RPC 方法只需实现 Handler 接口并 `router.Register(method, handler)` 即可，无需修改管线或路由器代码。

### 3.5 Broadcaster — 事件分发器

**职责**：从 Event Bus 消费到的事件，分发到正确的 WS 连接。是 Gateway 中 **QPS 最高的模块**（每秒几万次推送），也是性能优化的重点。

**与其他类型的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| 依赖（优先） | `ConnLookup` 接口 | **本地优先**：先通过 `ConnsBySession` 在 Hub 内存中查找（零 IO） |
| 依赖（回退） | `SessionRouter` 接口 | **Redis 回退**：本地未命中时才查 Redis 路由表 |
| 依赖 | `Dispatcher` | 联动亲和状态（收到 input_required 时升级为 Sticky） |
| 被驱动 | `EventSub` | EventSub 消费 Redis Streams，回调 Broadcaster |

```go
// internal/broadcast/broadcaster.go

type Broadcaster struct {
    lookup        ConnLookup     // Hub 的只读视图 (本地内存)
    sessionRouter SessionRouter  // Redis 路由查询 (远程, 有 IO 开销)
    dispatcher    *Dispatcher    // 亲和状态联动
    metrics       *observe.Metrics
}

// Broadcast 将一个事件推送到目标 session 的所有连接。
// 采用 "本地优先" 策略: 先查 Hub 内存, 命中则零 IO; 未命中才查 Redis。
func (b *Broadcaster) Broadcast(event *EventMessage) {
    // ── 第一步: 本地 Hub 查找 (零 IO, RLock 读内存) ──
    conns := b.lookup.ConnsBySession(event.SessionKey)
    if len(conns) > 0 {
        // 本地命中 → 直接推送, 无需 Redis 调用
        for _, conn := range conns {
            seq := conn.NextSeq()
            conn.SendEvent(event.Event, event.Payload, seq)
        }
        b.metrics.BroadcastLocalHit.Inc()
        return
    }

    // ── 第二步: Redis SessionRouter 查找 (有 IO, 仅在本地未命中时) ──
    // 场景: Consumer Group fan-out 模式下, 事件可能被分发到
    //       不持有该 session 连接的 Gateway 实例
    connIDs := b.sessionRouter.LookupConns(event.SessionKey)
    if len(connIDs) == 0 {
        b.metrics.BroadcastMiss.Inc()
        return // 该 session 的用户确实不在本 Gateway
    }

    // connID 来自 Redis, 需要在本地 Hub 查找对应的 Conn 对象
    for _, connID := range connIDs {
        conn := b.lookup.ConnByID(connID)
        if conn == nil {
            continue // 连接已断开或在其他 Gateway
        }
        seq := conn.NextSeq()
        conn.SendEvent(event.Event, event.Payload, seq)
    }
    b.metrics.BroadcastRedisHit.Inc()
}
```

> **本地优先策略的性能收益**：Hub 内存查找是纯 RLock + map 读取，延迟 < 1μs；而 Redis 查询即使在同机房也需要 ~0.5ms 的网络 IO。在大多数场景下（单实例、或事件恰好路由到持有连接的 Gateway），本地命中率可达 90% 以上，大幅减少 Redis 调用量。

### 3.6 核心接口定义

以下接口定义在各自的包中，跨包通过接口引用，不直接依赖具体实现。这是 Gateway 实现**依赖倒置**的关键——上层模块定义接口、下层模块提供实现。

```text
接口依赖关系:

  Hub ────────────────┐
    │ 实现             │ 依赖
    ▼                  ▼
  ConnLookup      SessionRouter ◄──── Broadcaster
  (查找连接)      (路由表)             (事件分发)
    ▲                  ▲
    │ 依赖             │ 依赖
    │                  │
  Router          Upgrader
  Handler

  AgentSubmitHandler ───依赖──▶ TaskDispatcher ◄──── gRPCDispatcher (实现)
                                (任务下发)

  RateLimitStage ───依赖──▶ RateLimiter ◄──── RedisRateLimiter (实现)
                             (限频)

  IdempotencyStage ──依赖──▶ IdempotencyStore ◄──── RedisIdempotencyStore (实现)
                              (幂等去重)

  Hub ───依赖──▶ WorkerRegistry ◄──── RedisWorkerRegistry (实现)
                  (Worker 发现)
```

#### TaskDispatcher — 任务下发

```go
// internal/dispatch/dispatcher.go

// TaskDispatcher 定义向 Agent Runtime 下发任务的能力。
// 实现: gRPCDispatcher (通过 gRPC 连接池调用 Runtime)
//
// 调用者: AgentSubmitHandler, AgentAbortHandler, AgentStatusHandler
type TaskDispatcher interface {
    // Submit 提交一个新的 Agent 任务
    // 返回 taskID 和 runID, 后续事件通过 Event Bus 异步推送
    Submit(ctx context.Context, req *SubmitRequest) (*SubmitResponse, error)

    // Abort 中止一个正在执行的任务
    // 幂等: 对已完成的任务调用 Abort 不会报错
    Abort(ctx context.Context, taskID, runID string) error

    // TaskStatus 查询任务当前状态
    // 返回 pending / running / completed / failed / aborted
    TaskStatus(ctx context.Context, taskID string) (*TaskStatus, error)
}

type SubmitRequest struct {
    SessionID string            // 会话 ID
    AgentID   string            // Agent 模板 ID
    Input     string            // 用户输入
    Attachments []Attachment    // 附件 (URL 引用)
    Config    map[string]any    // Agent 运行配置覆盖
}

type SubmitResponse struct {
    TaskID string // 任务 ID (全局唯一)
    RunID  string // 运行 ID (同一 task 可能多次 run)
}

type TaskStatus struct {
    TaskID  string    // 任务 ID
    RunID   string    // 当前运行 ID
    Status  string    // pending / running / completed / failed / aborted
    StartAt time.Time
    EndAt   *time.Time
}
```

#### SessionRouter — 会话路由

```go
// internal/session/router.go

// SessionRouter 管理 "session ↔ Gateway 连接" 的映射关系。
// 解决多实例部署时 "Event Bus 的事件应该推送给哪个 Gateway 的哪个连接" 的问题。
// 实现: RedisSessionRouter (使用 Redis Hash 存储)
//
// 调用者:
//   写操作 — Hub.Run() 注册/注销时调用
//   读操作 — Broadcaster 推送事件时调用
//   Token  — Upgrader 断线恢复时调用
type SessionRouter interface {
    // Register 注册连接与 session 的关系
    // 一个连接可能监听多个 session (同时打开多个会话)
    Register(sessionKey, userID, connID string) error

    // Unregister 注销连接的所有 session 关系
    // 在 Hub.Run() 处理 unregister 时调用
    Unregister(connID string) error

    // LookupConns 查找某个 session 在本实例的所有连接 ID
    // Broadcaster 消费到事件后调用此方法找到推送目标
    LookupConns(sessionKey string) []string

    // CreateResumeToken 为断线恢复创建 token
    // 包含用户当前所有活跃 session 和最后序列号
    CreateResumeToken(userID string, activeSessions []string) (string, error)

    // ValidateResumeToken 校验并消费 token (一次性)
    // 返回恢复上下文: 用户 ID, 活跃 session 列表, 各 session 的最后序列号
    ValidateResumeToken(token string) (*ResumeContext, error)
}

type ResumeContext struct {
    UserID          string
    ActiveSessions  []string          // 恢复时需要重新订阅的 session
    LastSeqs        map[string]uint64 // sessionKey → 客户端收到的最后序列号
}
```

#### WorkerRegistry — Runtime Worker 发现

```go
// internal/registry/registry.go

// WorkerRegistry 管理所有 Agent Runtime Worker 的注册与发现。
// Gateway 通过此接口获取可用 Worker 列表，用于 gRPC 负载均衡。
// 实现: RedisWorkerRegistry (Worker 心跳写入 Redis, Gateway 读取)
//
// 调用者: gRPCDispatcher (选择目标 Worker 发送任务)
type WorkerRegistry interface {
    // ReadyWorkers 返回所有健康且就绪的 Worker
    // gRPCDispatcher 从中选择一个进行任务下发
    ReadyWorkers() []*WorkerInfo

    // MarkUnhealthy 将 Worker 标记为不健康 (gRPC 调用失败时)
    // 下次 ReadyWorkers 不会返回该 Worker, 直到心跳恢复
    MarkUnhealthy(workerID string)

    // Worker 获取特定 Worker 的信息 (用于 metrics/调试)
    Worker(workerID string) *WorkerInfo
}

type WorkerInfo struct {
    ID         string    // Worker 唯一标识
    Addr       string    // gRPC 地址 host:port
    Tags       []string  // 能力标签: ["code-exec", "web-browse", ...]
    MaxTasks   int       // 最大并发任务数
    ActiveTasks int      // 当前活跃任务数
    LastHeartbeat time.Time // 最后心跳时间
}
```

#### RateLimiter & IdempotencyStore — 限频与幂等

```go
// internal/ratelimit/limiter.go

// RateLimiter 提供多层限频能力。
// 实现: 本地 Token Bucket (连接级) + Redis (用户级/全局级)
//
// 调用者: FramePipeline 中的 RateLimitStage
type RateLimiter interface {
    // Allow 检查是否允许通过
    // key 格式: "conn:{connID}" / "user:{userID}" / "global:{method}"
    // 返回: 是否允许, 剩余配额, 重置时间
    Allow(ctx context.Context, key string, limit Rate) (*AllowResult, error)
}

type Rate struct {
    Count  int           // 允许次数
    Window time.Duration // 时间窗口
}

type AllowResult struct {
    Allowed   bool      // 是否放行
    Remaining int       // 剩余配额
    ResetAt   time.Time // 配额重置时间
    RetryAfter time.Duration // 建议重试间隔 (被限频时)
}

// internal/idempotency/store.go

// IdempotencyStore 提供请求幂等去重能力。
// 实现: RedisIdempotencyStore (SET NX + TTL)
//
// 调用者: FramePipeline 中的 IdempotencyStage
type IdempotencyStore interface {
    // Check 检查幂等 key 是否已处理过
    // 返回 (是否重复, 缓存的响应)
    Check(ctx context.Context, key string) (isDuplicate bool, cachedResult []byte, err error)

    // Store 存储请求处理结果 (供后续重复请求直接返回)
    Store(ctx context.Context, key string, result []byte, ttl time.Duration) error
}
```

### 3.7 类型与接口设计原则总结

| 原则 | 体现 |
| --- | --- |
| **单一职责 (SRP)** | Conn 只管连接生命周期，Hub 只管注册表，Handler 只管业务逻辑 |
| **接口隔离 (ISP)** | Hub 暴露最小的 ConnLookup 接口，而非整个 Hub struct |
| **依赖倒置 (DIP)** | Handler 依赖 TaskDispatcher 接口，不依赖 gRPCDispatcher 实现 |
| **开闭原则 (OCP)** | 新增 RPC 方法只需实现 Handler 接口并注册，无需修改管线/路由器 |
| **可测试性** | 所有跨模块依赖都是接口，单测时可用 mock 替代 Redis/gRPC |

---

## 四、连接管理 (Hub + Conn)

### 4.1 Goroutine 模型

每个 WS 连接产生 **2 个 goroutine**——readPump 读、writePump 写。写通过 channel 串行化，避免并发写锁。

```text
                        ┌──────────────────────────────┐
                        │          Conn                 │
                        │                               │
  Client ──WS Frame──▶ │  readPump goroutine           │
                        │  ├── JSON 解析                │
                        │  ├── Pipeline 处理            │
                        │  └── 路由到 Handler            │
                        │                               │
                        │  writePump goroutine          │ ──WS Frame──▶ Client
                        │  ├── 从 writeCh 读取          │
                        │  ├── 序列化 JSON              │
                        │  └── ws.Write()               │
                        │                               │
                        │  writeCh chan []byte           │
                        │  (缓冲: 256 条)               │
                        └──────────────────────────────┘
```

```go
// internal/ws/conn.go

func (c *Conn) readPump() {
    defer c.Close()
    c.ws.SetReadLimit(maxFrameBytes)

    for {
        _, msg, err := c.ws.Read(c.done)
        if err != nil {
            return // 连接断开
        }
        c.lastActivity.Store(time.Now().UnixMilli())
        c.frameCount.Add(1)

        // 交给管线处理 (不阻塞 readPump)
        c.pipeline.Process(c, msg)
    }
}

func (c *Conn) writePump() {
    ticker := time.NewTicker(30 * time.Second) // tick 心跳
    defer ticker.Stop()
    defer c.Close()

    for {
        select {
        case msg, ok := <-c.writeCh:
            if !ok {
                return
            }
            // 批量写入: 将 writeCh 中积压的帧一次性写出
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            err := c.ws.Write(ctx, websocket.MessageText, msg)
            cancel()
            if err != nil {
                return
            }
            // 尝试非阻塞读取更多待发送帧
            for i := 0; i < len(c.writeCh); i++ {
                additional := <-c.writeCh
                ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
                c.ws.Write(ctx2, websocket.MessageText, additional)
                cancel2()
            }

        case <-ticker.C:
            c.SendEvent("tick", map[string]any{"ts": time.Now().UnixMilli()})

        case <-c.done:
            return
        }
    }
}

// SendEvent 通过 writeCh 发送事件帧 (并发安全)
func (c *Conn) SendEvent(event string, payload any) error {
    frame := EventFrame{Type: "event", Event: event, Payload: payload, Ts: time.Now().UnixMilli()}
    data, err := json.Marshal(frame)
    if err != nil {
        return err
    }
    select {
    case c.writeCh <- data:
        return nil
    default:
        // 写缓冲满 → 慢客户端
        c.hub.metrics.SlowClientInc()
        return ErrWriteBufferFull
    }
}
```

### 4.2 Hub 主循环

Hub 是 Gateway 管理所有 WS 连接的**中央登记处**。它的核心作用是将并发的连接注册/注销操作**串行化**，避免 map 并发读写冲突。

**为什么需要 Hub 主循环**：

Gateway 可能在同一时刻发生以下并发操作——用户 A 连入、用户 B 也连入、用户 C 断开、Broadcaster 要查找连接推送事件。这些操作分别在不同的 goroutine 中执行，都需要访问同一个 `conns` map。Go 的 map 不是线程安全的，并发写会直接 panic。

Hub 采用 **channel 事件循环 + RWMutex 读锁** 的组合方案解决这个问题：

```text
┌────────────────────────────────────────────────────────────────────────┐
│  Hub 并发模型                                                          │
│                                                                        │
│  写操作 (低频, 几十次/秒):                                             │
│  ┌───────────────┐    register     ┌──────────────┐                   │
│  │  WS Upgrader  │ ──── chan ────▶ │              │                   │
│  └───────────────┘                 │  Hub.Run()   │ ← 单 goroutine   │
│  ┌───────────────┐    unregister   │  串行处理    │    串行写 map     │
│  │  Conn.Close   │ ──── chan ────▶ │              │                   │
│  └───────────────┘                 └──────────────┘                   │
│                                           │                            │
│                                     写入 conns map                     │
│                                     写入 Redis 路由表                  │
│                                     更新 metrics                       │
│                                                                        │
│  读操作 (高频, 几万次/秒):                                             │
│  ┌───────────────┐                 ┌──────────────┐                   │
│  │  Broadcaster  │ ── RLock ─────▶ │  conns map   │ ← 多 goroutine   │
│  │  (事件推送)   │    读取         │  (共享读)    │    并发读         │
│  └───────────────┘                 └──────────────┘                   │
│  ┌───────────────┐                        ▲                            │
│  │  Handler      │ ── RLock ──────────────┘                           │
│  │  (广播)       │    读取                                             │
│  └───────────────┘                                                     │
│                                                                        │
│  为什么不全走 channel?                                                 │
│  读操作每秒几万次，如果也排队到 channel 串行处理，                     │
│  Hub.Run 就会成为瓶颈。RLock 允许多个读并发执行，零阻塞。              │
│                                                                        │
│  为什么写不直接加锁?                                                   │
│  写操作除了改 map，还要做 Redis IO (路由注册) 和 metrics 更新。       │
│  channel 串行化把这些副作用收敛到一个 goroutine，逻辑更清晰、          │
│  不用担心锁内做 IO 导致持锁时间过长。                                  │
└────────────────────────────────────────────────────────────────────────┘
```

| 操作 | 路径 | 并发安全方式 | 频率 |
| --- | --- | --- | --- |
| 注册连接 | `register` channel → Hub.Run 串行处理 | channel 串行 | 低频（几十/秒） |
| 注销连接 | `unregister` channel → Hub.Run 串行处理 | channel 串行 | 低频 |
| 查找连接（`GetConn`） | 直接 `RLock` 读 `conns` map | RWMutex 读锁 | 高频（几万/秒） |
| 广播（`BroadcastAll`） | 直接 `RLock` 遍历 `conns` map | RWMutex 读锁 | 中频 |

> **设计来源**：这是 Go 中经典的 Hub 模式（gorilla/websocket 官方示例同样采用此模式），兼顾并发安全和性能。

**实现**：

```go
func (h *Hub) Run(ctx context.Context) {
    for {
        select {
        case conn := <-h.register:
            h.mu.Lock()
            h.conns[conn.ID] = conn
            h.userConns[conn.UserID] = append(h.userConns[conn.UserID], conn)
            h.mu.Unlock()

            h.metrics.ConnectionsGauge.Inc()
            h.sessionRouter.Register(/* ... */)
            slog.Info("conn registered", "connId", conn.ID, "userId", conn.UserID)

        case conn := <-h.unregister:
            h.mu.Lock()
            delete(h.conns, conn.ID)
            h.removeUserConn(conn.UserID, conn.ID)
            h.mu.Unlock()

            h.metrics.ConnectionsGauge.Dec()
            h.sessionRouter.Unregister(conn.ID)
            slog.Info("conn unregistered", "connId", conn.ID)

        case <-ctx.Done():
            return
        }
    }
}

// 读操作: 用 RLock，不走 channel，支持高并发
func (h *Hub) GetConn(connID string) *Conn {
    h.mu.RLock()
    defer h.mu.RUnlock()
    return h.conns[connID]
}

func (h *Hub) BroadcastAll(frame EventFrame) {
    h.mu.RLock()
    defer h.mu.RUnlock()
    for _, conn := range h.conns {
        conn.SendEvent(frame)
    }
}
```

### 4.3 Hub 性能分析与分片演进

#### 4.3.1 当前方案的性能边界

Hub.Run() 只处理低频的连接注册/注销写操作，高频的读操作（查找连接、推送事件）通过 RLock 并发执行，完全绕过 channel：

```text
操作频率分布（单实例 100K 连接）：

  频率 ▲
       │
  万/s │  ██████████  事件推送 GetConn / BroadcastAll → RLock 并发读
       │
  千/s │  ████  消息收发 readPump / writePump → 每连接独立 goroutine
       │
  十/s │  █  连接注册/注销 → Hub.Run() channel 串行
       │
       └──────────────────────────────────────────────────────▶ 操作类型
```

| 指标 | 估算值 | 计算依据 |
| --- | --- | --- |
| 在线连接数 | 100,000 | 单 Gateway 实例上限 |
| 平均会话时长 | 30 分钟 | C 端用户典型行为 |
| 注册 + 注销 QPS | ~55 次/秒 | 100K / 1800s × 2 |
| 单次处理耗时 | ~1-2ms | map 写入 + Redis SessionRouter IO |
| Hub.Run 利用率 | **~8%** | 55 × 1.5ms / 1000ms |

> **结论**：在 10 万连接规模下，Hub.Run() 利用率不到 10%，远不是瓶颈。

**真正的性能热点**：

| 热点 | 原因 | 应对方案 |
| --- | --- | --- |
| Broadcaster 事件推送 | 每个 Agent 事件要查找目标连接并写入 writeCh，QPS 最高 | 分片 Hub、批量写入 |
| writePump 帧序列化 | 每条消息 JSON 序列化 + WS 写入，CPU 密集 | 预序列化复用、writeBuffer 池化 |
| Event Bus 消费 | Redis Streams XREADGROUP 吞吐上限 | Consumer Group 多消费者、Pipeline 批读 |

#### 4.3.2 分片演进路线

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  Phase 1 — 单 Hub (当前方案, ≤ 10 万连接)                               │
│                                                                         │
│  ┌──────────────────────────────────────────────────────┐               │
│  │            Hub (单 goroutine + 单 RWMutex)           │               │
│  │  register ──▶ Run() 串行写 map                       │               │
│  │  GetConn  ──▶ RLock 并发读                           │               │
│  └──────────────────────────────────────────────────────┘               │
│  优点: 实现简单, 调试方便, 10 万连接绰绰有余                           │
│  触发演进条件: RLock 遍历延迟 > 1ms 或 CPU 锁竞争 > 5%                 │
├─────────────────────────────────────────────────────────────────────────┤
│  Phase 2 — 分片 Hub (10 万 ~ 50 万连接)                                │
│                                                                         │
│  ShardedHub 按 userID 哈希分片, 每个分片独立运行:                       │
│                                                                         │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐           │
│  │ Shard[0]  │  │ Shard[1]  │  │ Shard[2]  │  │ Shard[N]  │           │
│  │ Run()     │  │ Run()     │  │ Run()     │  │ Run()     │           │
│  │ RWMutex   │  │ RWMutex   │  │ RWMutex   │  │ RWMutex   │           │
│  │ conns map │  │ conns map │  │ conns map │  │ conns map │           │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘           │
│       ▲ hash(userID) % N 路由到对应分片                                 │
│                                                                         │
│  优点: 锁粒度缩小 N 倍, 遍历范围缩小 N 倍, 写并行度提升 N 倍          │
│  N 推荐值: CPU 核数 (通常 16~64), 每分片管理 ~5000 连接                │
│  触发演进条件: 需要单实例百万连接或亚毫秒级遍历                         │
├─────────────────────────────────────────────────────────────────────────┤
│  Phase 3 — 无锁结构 (50 万+, 极端场景)                                 │
│                                                                         │
│  使用 sync.Map 或定制的 lock-free concurrent map:                       │
│  ┌──────────────────────────────────────────────────────┐               │
│  │            sync.Map / lock-free map                  │               │
│  │  Store() ──▶ 无锁写入                                │               │
│  │  Load()  ──▶ 无锁读取                                │               │
│  │  Range() ──▶ 无锁遍历 (快照语义)                     │               │
│  └──────────────────────────────────────────────────────┘               │
│  注意: sync.Map 在写多读少时性能反而更差, 仅在                         │
│  读远多于写的 Hub 场景下才有优势 (正好符合)                             │
│  适用于: 百万级连接, 对 p99 延迟有极致要求                              │
└─────────────────────────────────────────────────────────────────────────┘
```

**Phase 2 分片 Hub 核心实现**：

```go
const shardCount = 32 // 推荐 CPU 核数, 2 的幂便于位运算

type ShardedHub struct {
    shards [shardCount]*HubShard
}

type HubShard struct {
    mu        sync.RWMutex
    conns     map[string]*Conn
    userConns map[string][]*Conn
    register  chan *Conn
    unregister chan *Conn
}

// 按 userID 哈希选择分片
func (sh *ShardedHub) getShard(userID string) *HubShard {
    h := fnv.New32a()
    h.Write([]byte(userID))
    return sh.shards[h.Sum32()%shardCount]
}

// 注册: 路由到对应分片的 channel
func (sh *ShardedHub) Register(conn *Conn) {
    sh.getShard(conn.UserID).register <- conn
}

// 查找: 直接在对应分片上 RLock 读取
func (sh *ShardedHub) GetConn(userID, connID string) *Conn {
    shard := sh.getShard(userID)
    shard.mu.RLock()
    defer shard.mu.RUnlock()
    return shard.conns[connID]
}

// 广播: 并行遍历所有分片
func (sh *ShardedHub) BroadcastAll(frame EventFrame) {
    var wg sync.WaitGroup
    for _, shard := range sh.shards {
        wg.Add(1)
        go func(s *HubShard) {
            defer wg.Done()
            s.mu.RLock()
            defer s.mu.RUnlock()
            for _, conn := range s.conns {
                conn.SendEvent(frame)
            }
        }(shard)
    }
    wg.Wait()
}

// 每个分片独立运行自己的事件循环
func (s *HubShard) Run(ctx context.Context) {
    for {
        select {
        case conn := <-s.register:
            s.mu.Lock()
            s.conns[conn.ID] = conn
            s.userConns[conn.UserID] = append(s.userConns[conn.UserID], conn)
            s.mu.Unlock()
        case conn := <-s.unregister:
            s.mu.Lock()
            delete(s.conns, conn.ID)
            s.removeUserConn(conn.UserID, conn.ID)
            s.mu.Unlock()
        case <-ctx.Done():
            return
        }
    }
}
```

**分片方案的关键收益**：

| 指标 | 单 Hub (Phase 1) | 分片 Hub (Phase 2, N=32) | 提升 |
| --- | --- | --- | --- |
| 写并行度 | 1 (串行) | 32 (32 个独立 channel) | 32× |
| 读锁粒度 | 10 万连接/锁 | ~3125 连接/锁 | 锁持有时间缩短 32× |
| BroadcastAll 耗时 | 串行遍历 10 万 | 32 路并行各遍历 3125 | 接近 32× |
| GetConn 锁竞争 | 全局竞争 | 仅同分片竞争 | 32× |
| 代码复杂度 | 低 | 中等（需要 hash 路由） | — |

> **Phase 1 是充分够用的**。分片演进仅在监控指标明确触发时才执行，避免过早优化。触发条件建议埋入 Prometheus 指标监控：`hub_rlock_duration_seconds`（RLock 持有时间）、`hub_channel_pending`（channel 积压深度）。

### 4.4 Hub 数据持久性与重启恢复

Hub 的 `conns` map 是纯内存结构，进程重启后全部丢失。但这是 **有意为之的设计**——WebSocket 连接本质是 TCP file descriptor，随进程消亡而消亡，即使将 map 持久化也无法"恢复"一个已断开的 TCP 连接。

**状态分层：什么在内存，什么在 Redis**

```text
┌──────────────────────────────────────────────────────────────────┐
│  Gateway 进程 (内存, 临时)              进程死 → 全部丢失        │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Hub.conns:      connID → *Conn (TCP fd)                  │  │
│  │  Hub.userConns:  userID → []*Conn                         │  │
│  │  writeCh:        待发送帧队列                              │  │
│  └────────────────────────────────────────────────────────────┘  │
│  这些数据都是 TCP 连接的内存索引，不可序列化、不可恢复。        │
├──────────────────────────────────────────────────────────────────┤
│  Redis (持久化, 跨重启存活)             进程死 → 全部存活        │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  SessionRouter:  sess:{sid} → gatewayID:connID   (TTL)    │  │
│  │  ResumeToken:    resume:{token} → {seq, sid}     (TTL)    │  │
│  │  Event Bus:      stream:session:{sid} → 事件流   (持久化) │  │
│  │  Auth Blacklist: blacklist:{jti} → 1             (TTL)    │  │
│  │  Rate Limiter:   rl:user:{uid} → counter         (TTL)    │  │
│  └────────────────────────────────────────────────────────────┘  │
│  所有需要跨重启存活的数据都在 Redis/PostgreSQL 中。              │
└──────────────────────────────────────────────────────────────────┘
```

> **设计原则**：Hub 是临时索引层，Redis 是持久状态层。Gateway 进程是无状态的——任何实例都可以通过 Redis 中的 resumeToken 和 Event Bus 重建用户会话。

#### 4.4.1 优雅重启恢复流程（正常部署）

结合 `§14 优雅关闭与零感知部署` 的机制，用户完全无感知：

```text
时间线:
  t0   Gateway-A 收到 SIGTERM
  t1   停止接受新连接, 等待进行中 Agent 任务完成
  t2   向所有客户端发送 goodbye(silent:true)     ← 客户端收到预告
  t3   关闭所有 WS 连接
  t4   进程退出
       ─── SessionRouter 中 Gateway-A 的路由记录等待 TTL 过期 ───
  t5   客户端静默重连 → 连到 Gateway-B (新实例)
  t6   Gateway-B 在自己的 Hub 中注册新连接         ← Hub 索引在新实例重建
  t7   Gateway-B 用 resumeToken 从 Redis 读取断点
  t8   Gateway-B 从 Event Bus 重放遗漏事件
  t9   客户端恢复正常, 用户完全无感知

  丢失数据: 无
  用户感知: 无 (静默重连窗口 < 3 秒)
```

#### 4.4.2 异常崩溃恢复流程（进程 crash / OOM Kill）

没有机会发送 `goodbye`，但客户端仍能自动恢复：

```text
时间线:
  t0   Gateway-A 崩溃, 进程消亡
       ─── Hub 内存丢失, 所有 TCP 连接断开 ───
  t1   客户端检测到连接断开 (ping/pong 超时, 通常 30s 内)
  t2   客户端进入重连退避 (exponential backoff)
  t3   客户端带 resumeToken 连到 Gateway-B
  t4   Gateway-B 校验 resumeToken (Redis 中仍有效, TTL 5min)
  t5   Gateway-B 在自己的 Hub 中注册新连接
  t6   Gateway-B 从 Event Bus 重放 seq 断点之后的事件
  t7   客户端恢复正常

  丢失数据: 无 (事件持久化在 Redis Streams)
  用户感知: 短暂断连 (< 30s), 然后自动恢复, 不丢消息
```

#### 4.4.3 SessionRouter 脏数据清理

Gateway 崩溃时，Redis 中 SessionRouter 仍指向已不存在的实例。需要多层清理机制：

| 策略 | 机制 | 生效时间 | 适用场景 |
| --- | --- | --- | --- |
| **重连覆盖** | 客户端重连到新实例时，新注册直接覆盖旧 key | 即时 | 最常见，用户主动重连 |
| **心跳续期** | Gateway 每 60s 对自己的路由记录 `EXPIRE` 续期；崩溃后续期停止 | ~60s | 缩短脏数据窗口 |
| **TTL 兜底** | 每条路由记录设 TTL（默认 5 分钟） | ≤5min | 兜底，防止孤儿记录 |
| **启动自清理** | Gateway 启动时删除指向自身旧实例 ID 的残留记录 | 启动时 | 原地重启 (同 Pod) |

> **推荐组合**：重连覆盖（即时生效）+ 心跳续期（缩短脏窗口到 60s）+ TTL 兜底（防止永久孤儿）。

```text
SessionRouter 脏数据清理时序 (崩溃场景):

  t0    Gateway-A crash
        Redis: sess:123 → "gateway-A:conn-456"       ← 脏数据

  t30   客户端重连到 Gateway-B
        Redis: sess:123 → "gateway-B:conn-789"       ← 覆盖, 立即修复 ✓

  (若客户端未重连的孤儿 session):
  t60   Gateway-A 心跳续期停止, TTL 开始倒计时
  t120  TTL 过期, Redis 自动删除 sess:orphan-session  ← 兜底清理 ✓
```

#### 4.4.4 总结

| 关注点 | 结论 |
| --- | --- |
| Hub 重启丢数据吗？ | 内存索引丢失，但 TCP 连接本身也已断开，无需也无法恢复 |
| 用户消息会丢吗？ | 不会。事件持久化在 Event Bus (Redis Streams)，重连后重放 |
| 用户状态会丢吗？ | 不会。resumeToken、会话数据全在 Redis / PostgreSQL |
| 用户感知到吗？ | 优雅重启：无感。崩溃：短暂断连后自动恢复 |
| 需要额外持久化 Hub 吗？ | **不需要**——TCP 连接不可序列化，Hub 天然是临时索引 |

### 4.5 连接限制

#### 4.5.1 限制参数一览

| 限制 | 默认值 | 可配置 | 说明 |
| --- | --- | --- | --- |
| 单用户最大连接数 | 5 | `WS_MAX_CONNS_PER_USER` | 超过则拒绝新连接 (WS Close 4029) |
| 全局最大连接数 | 100,000 | `WS_MAX_CONNS_TOTAL` | 超过则拒绝 (HTTP 503) |
| writeCh 缓冲 | 256 | `WS_WRITE_BUFFER_SIZE` | 满了标记为慢客户端并断开 |
| 读帧大小上限 | 1MB | `WS_MAX_FRAME_BYTES` | 超过断开连接 |
| 单连接空闲超时 | 5 分钟 | `WS_IDLE_TIMEOUT` | 无任何读写活动则断开 |

#### 4.5.2 检查时机与处理流程

连接限制在三个阶段分别检查，越早拒绝越好，避免浪费资源：

```text
客户端发起连接
  │
  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 阶段 1: HTTP Upgrade 前 (最廉价, 不消耗 WS 资源)                   │
│                                                                     │
│  ┌───────────────────┐   不通过    ┌───────────────────────────┐   │
│  │ 全局连接数 ≥ 上限? │ ─────────▶ │ HTTP 503 + Retry-After   │   │
│  └────────┬──────────┘            └───────────────────────────┘   │
│           │ 通过                                                    │
│  ┌───────────────────┐   不通过    ┌───────────────────────────┐   │
│  │ JWT 验证           │ ─────────▶ │ HTTP 401 Unauthorized     │   │
│  └────────┬──────────┘            └───────────────────────────┘   │
│           │ 通过                                                    │
│  ┌───────────────────┐   不通过    ┌───────────────────────────┐   │
│  │ 用户连接数 ≥ 上限? │ ─────────▶ │ HTTP 429 Too Many Conns   │   │
│  └────────┬──────────┘            └───────────────────────────┘   │
│           │ 通过                                                    │
│           ▼                                                         │
│  WebSocket Upgrade 握手                                             │
└─────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 阶段 2: 连接存活期 (运行时保护)                                     │
│                                                                     │
│  readPump:                                                          │
│  ┌───────────────────┐   超限      ┌───────────────────────────┐   │
│  │ 帧大小 > 1MB?     │ ─────────▶ │ WS Close 1009 (Too Large) │   │
│  └───────────────────┘            └───────────────────────────┘   │
│  ┌───────────────────┐   超时      ┌───────────────────────────┐   │
│  │ 空闲 > 5 分钟?    │ ─────────▶ │ WS Close 4000 (Idle)      │   │
│  └───────────────────┘            └───────────────────────────┘   │
│                                                                     │
│  writePump:                                                         │
│  ┌───────────────────┐   缓冲满    ┌───────────────────────────┐   │
│  │ writeCh 写不进去?  │ ─────────▶ │ 标记慢客户端 → 踢下线     │   │
│  └───────────────────┘            └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 阶段 3: Hub 注册时 (二次确认, 防止并发穿透)                        │
│                                                                     │
│  Hub.Run() 串行处理 register 时再次检查全局/用户连接数，            │
│  防止多个 Upgrade 同时通过阶段 1 检查后超限。                       │
│  不通过 → 发送 error 帧后 Close                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 4.5.3 核心实现

**Upgrader 阶段检查（阶段 1）**：

```go
// internal/ws/upgrader.go

func (u *Upgrader) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // ── 阶段 1a: 全局连接数检查 (原子操作, 无锁) ──
    currentTotal := u.hub.ConnCount()  // atomic.LoadInt64
    if currentTotal >= u.config.MaxConnsTotal {
        w.Header().Set("Retry-After", "5")
        http.Error(w, "service at capacity", http.StatusServiceUnavailable) // 503
        u.metrics.ConnRejectedTotal.WithLabelValues("global_limit").Inc()
        return
    }

    // ── 阶段 1b: JWT 验证 ──
    token := extractToken(r)
    if token == "" {
        http.Error(w, "missing authorization", http.StatusUnauthorized)
        return
    }
    claims, err := u.auth.Validate(token)
    if err != nil {
        http.Error(w, "invalid token", http.StatusUnauthorized)
        return
    }

    // ── 阶段 1c: 单用户连接数检查 (RLock 读取) ──
    userCount := u.hub.UserConnCount(claims.UserID)  // RLock 读
    if userCount >= u.config.MaxConnsPerUser {
        http.Error(w, "too many connections", http.StatusTooManyRequests) // 429
        u.metrics.ConnRejectedTotal.WithLabelValues("user_limit").Inc()
        return
    }

    // ── 升级为 WebSocket ──
    wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
        Subprotocols: []string{"sahara-v1"},
    })
    if err != nil {
        return
    }

    conn := NewConn(wsConn, claims, u.hub, u.pipeline)
    // ... (welcome, resume, register — 同 §9.7)
}
```

**Hub 注册时二次确认（阶段 3）**：

```go
// internal/ws/hub.go — Hub.Run() 中 register 分支

case conn := <-h.register:
    // ── 阶段 3: 二次确认, 防止并发穿透 ──
    // 多个 Upgrade 可能同时通过阶段 1 的检查, 此处串行化后再次校验

    // 3a: 全局连接数
    if len(h.conns) >= h.config.MaxConnsTotal {
        conn.SendErrorAndClose(4029, "global_limit", "server at capacity")
        break
    }

    // 3b: 单用户连接数
    if len(h.userConns[conn.UserID]) >= h.config.MaxConnsPerUser {
        conn.SendErrorAndClose(4029, "user_limit",
            fmt.Sprintf("max %d connections per user", h.config.MaxConnsPerUser))
        break
    }

    // ── 正式注册 ──
    h.conns[conn.ID] = conn
    h.userConns[conn.UserID] = append(h.userConns[conn.UserID], conn)
    h.connCount.Add(1) // atomic, 供阶段 1 快速读取

    h.metrics.ConnectionsGauge.Inc()
    h.sessionRouter.Register(conn.ID, conn.UserID, conn.SessionIDs)
    slog.Info("conn registered", "connId", conn.ID, "userId", conn.UserID,
        "userConns", len(h.userConns[conn.UserID]),
        "totalConns", len(h.conns))
```

**慢客户端检测与踢下线（阶段 2）**：

```go
// internal/ws/conn.go

// SendEvent 发送事件帧, 如果写缓冲满则标记慢客户端
func (c *Conn) SendEvent(event string, payload any) error {
    data, err := json.Marshal(EventFrame{
        Type: "event", Event: event, Payload: payload,
        Ts: time.Now().UnixMilli(),
    })
    if err != nil {
        return err
    }

    select {
    case c.writeCh <- data:
        return nil
    default:
        // writeCh 缓冲已满 → 客户端消费速度跟不上
        c.slowCount.Add(1)
        c.hub.metrics.SlowClientInc()

        if c.slowCount.Load() >= 3 {
            // 连续 3 次写满 → 判定为慢客户端, 主动断开
            slog.Warn("kicking slow client",
                "connId", c.ID,
                "userId", c.UserID,
                "pending", len(c.writeCh))
            c.CloseWithCode(4001, "slow_client")
        }
        return ErrWriteBufferFull
    }
}

// 空闲超时检测 (在 readPump 中)
func (c *Conn) readPump() {
    defer c.Close()
    c.ws.SetReadLimit(maxFrameBytes) // 1MB 上限

    idleTimer := time.NewTimer(c.config.IdleTimeout) // 5 分钟
    defer idleTimer.Stop()

    for {
        select {
        case <-idleTimer.C:
            // 空闲超时, 无任何读活动
            slog.Info("idle timeout", "connId", c.ID, "userId", c.UserID)
            c.CloseWithCode(4000, "idle_timeout")
            return
        case <-c.done:
            return
        default:
        }

        _, msg, err := c.ws.Read(c.done)
        if err != nil {
            return // 连接断开 或 帧超限 (Read 内部检查 ReadLimit)
        }

        // 有活动 → 重置空闲计时器
        idleTimer.Reset(c.config.IdleTimeout)
        c.lastActivity.Store(time.Now().UnixMilli())
        c.slowCount.Store(0) // 收到帧说明客户端活跃, 重置慢计数

        c.pipeline.Process(c, msg)
    }
}
```

**ConnCount 原子读取（供阶段 1 无锁快速判断）**：

```go
// Hub 提供原子读取, Upgrader 不需要获取 RLock 就能快速判断
func (h *Hub) ConnCount() int64 {
    return h.connCount.Load() // atomic.Int64
}

func (h *Hub) UserConnCount(userID string) int {
    h.mu.RLock()
    defer h.mu.RUnlock()
    return len(h.userConns[userID])
}
```

#### 4.5.4 客户端收到的拒绝响应

**HTTP 阶段拒绝（Upgrade 前）**：

```text
# 全局满载
HTTP/1.1 503 Service Unavailable
Retry-After: 5
Content-Type: text/plain

service at capacity

# 用户连接数超限
HTTP/1.1 429 Too Many Requests
Content-Type: text/plain

too many connections
```

**WS 阶段拒绝（Upgrade 后）**：

```json
// Hub 二次确认失败 → 发送 error 帧后关闭
{
  "type": "event",
  "event": "error",
  "payload": {
    "code": 4029,
    "reason": "user_limit",
    "message": "max 5 connections per user"
  }
}
// 紧接 WS Close: code=4029, reason="user_limit"
```

```json
// 慢客户端踢下线
{
  "type": "event",
  "event": "error",
  "payload": {
    "code": 4001,
    "reason": "slow_client",
    "message": "write buffer full, connection terminated"
  }
}
// 紧接 WS Close: code=4001, reason="slow_client"
```

```json
// 空闲超时
{
  "type": "event",
  "event": "goodbye",
  "payload": {
    "reason": "idle_timeout",
    "message": "no activity for 5 minutes",
    "silent": false
  }
}
// 紧接 WS Close: code=4000, reason="idle_timeout"
```

#### 4.5.5 为什么需要两阶段检查

```text
并发穿透场景:

  t0  用户已有 4 个连接 (上限 5)
  t1  用户同时发起 3 个新连接请求 (浏览器多 tab)

  阶段 1 (并发执行, 各自独立):
    连接 A: UserConnCount=4, 4 < 5 → 通过 ✓
    连接 B: UserConnCount=4, 4 < 5 → 通过 ✓  ← 还没注册, 看到的还是 4
    连接 C: UserConnCount=4, 4 < 5 → 通过 ✓

  如果只有阶段 1 → 3 个都通过 → 用户有 7 个连接, 超限!

  阶段 3 (Hub.Run 串行执行):
    连接 A: len(userConns)=4, 4 < 5 → 注册 ✓ → 变成 5
    连接 B: len(userConns)=5, 5 ≥ 5 → 拒绝 ✗
    连接 C: len(userConns)=5, 5 ≥ 5 → 拒绝 ✗

  最终: 用户恰好 5 个连接, 严格符合上限 ✓
```

> **阶段 1 是乐观检查**（快速过滤绝大多数超限请求，避免浪费 Upgrade 资源）；**阶段 3 是悲观确认**（Hub.Run 串行执行，保证绝对准确）。两阶段配合实现了 "高性能 + 严格准确" 的连接限制。

---

## 五、帧处理管线

### 5.1 职责与定位

帧处理管线（FramePipeline）是 Gateway 中**连接所有核心组件的关键路径**。每个客户端发来的 WS 帧都经过管线的链式处理，管线将解析、安全、限频、去重、路由等横切关注点与业务逻辑解耦，是 Gateway 的**请求主动脉**。

```text
帧处理管线在 Gateway 中的位置:

  Client
    │
    │  WS 帧 (bytes)
    ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  Conn.readPump                                                    │
  │  (每个连接独立 goroutine)                                         │
  │       │                                                           │
  │       │  pipeline.Process(conn, raw)                              │
  │       ▼                                                           │
  │  ┌────────────────────────────────────────────────────────────┐  │
  │  │  FramePipeline (本章)                                      │  │
  │  │                                                            │  │
  │  │  Stage 1     Stage 2     Stage 3     Stage 4     Stage 5  │  │
  │  │  ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐  │  │
  │  │  │Parse │──▶│Auth  │──▶│Rate  │──▶│Idemp │──▶│Valid │  │  │
  │  │  │      │   │      │   │Limit │   │otency│   │ate   │  │  │
  │  │  └──────┘   └──────┘   └──────┘   └──────┘   └──────┘  │  │
  │  │      │           │          │          │          │       │  │
  │  │      │           │          │          │          │       │  │
  │  │   短路?        短路?      短路?      短路?      短路?    │  │
  │  │   (错误帧)    (错误帧)   (错误帧)   (缓存响应)  (错误帧) │  │
  │  │                                                  │       │  │
  │  │                                            全部通过       │  │
  │  │                                                  ▼       │  │
  │  │                                           ┌──────────┐  │  │
  │  │                                           │  Router   │  │  │
  │  │                                           │ Dispatch  │  │  │
  │  │                                           └────┬─────┘  │  │
  │  └────────────────────────────────────────────────┼────────┘  │
  │                                                    │           │
  │       ┌────────────────────────────────────────────┘           │
  │       ▼                                                        │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                    │
  │  │ Handler  │  │Dispatcher│  │  Redis   │                    │
  │  │ (业务)   │─▶│ (gRPC)   │  │ (状态)  │                    │
  │  └──────────┘  └──────────┘  └──────────┘                    │
  │       │                                                        │
  │       ▼                                                        │
  │  Conn.writeCh → writePump → Client                            │
  └──────────────────────────────────────────────────────────────────┘
```

**管线的 5 大设计目标**：

| # | 目标 | 实现方式 |
| --- | --- | --- |
| ① | **关注点分离** | 每个 Stage 只做一件事，Handler 专注业务逻辑 |
| ② | **Fail-fast** | 越早拒绝无效请求越好，节省后续阶段的 CPU 和 IO |
| ③| **可扩展** | 新增 Stage 只需实现 Stage 接口并插入管线 |
| ④ | **可观测** | 每个 Stage 独立上报延迟和拒绝次数 |
| ⑤ | **不阻塞读** | readPump 中的快检查同步执行，慢检查和 Handler 异步执行 |

**与各模块的交互关系**：

| 管线阶段 | 交互模块 | 交互方式 | IO 类型 |
| --- | --- | --- | --- |
| ParseStage | — | 纯 CPU (JSON 解析) | 无 IO |
| AuthStage | `Blacklist` (Redis + LRU) | 本地缓存优先, 未命中查 Redis | 可能有 IO |
| RateLimitStage | `RateLimiter` (本地 + Redis) | 连接级本地, 用户级 Redis | 写操作有 IO |
| IdempotencyStage | `IdempotencyStore` (Redis) | Redis GET/SET | 有 IO |
| ValidateStage | — | 纯 CPU (字段校验) | 无 IO |
| Router.Dispatch | `Handler` → `Dispatcher` → gRPC | 业务逻辑 + 远程调用 | 有 IO |

### 5.2 同步/异步分界设计

管线中的阶段并非全部在 readPump goroutine 中同步执行。关键设计点是**在 IO 操作前切换到独立 goroutine**，避免阻塞 readPump 读取后续帧：

```text
readPump goroutine (同步, 不能阻塞)         独立 goroutine (异步, 可阻塞)
─────────────────────────────────────       ──────────────────────────────────
                                            
  ┌──────────────────────┐                  
  │ Stage 1: Parse       │ 纯 CPU, <1μs    
  │ Stage 2: Auth 快检查 │ 本地 exp 检查    
  │ Stage 3: 连接级限频  │ 本地 Token Bucket
  │ Stage 4: 方法存在性  │ map 查找         
  └──────────┬───────────┘                  
             │                              
             │  go pipeline.executeAsync()  ← goroutine 切换点
             │                              
             └─────────────────────────────▶ ┌──────────────────────┐
                                             │ Auth 黑名单 (Redis)  │
                                             │ 用户级限频 (Redis)   │
                                             │ 幂等去重 (Redis)     │
                                             │ 参数校验             │
                                             │ Router.Dispatch      │
                                             │ Handler 执行         │
                                             └──────────────────────┘
                                            
  为什么这样分?                             
  • readPump 必须尽快读完帧, 否则 WS 读缓冲满会阻塞对端发送
  • 前 4 个阶段全是本地操作, 总耗时 < 10μs, 同步执行无问题
  • 从 Redis IO 开始切换到异步, 避免一个慢请求卡住整个连接
```

### 5.3 入站管线全流程

```text
入站 WS 帧 (bytes)
  │
  ▼
┌───────────────────────────────────────────────────────────────────┐
│ Stage 1: ParseStage (同步)                                        │
│ • JSON 反序列化 → RequestFrame { type, id, method, params }      │
│ • 校验 type == "req", 非 req 帧直接丢弃 (不响应)                │
│ • 提取 id 和 method 填入 FrameContext                             │
│ • 创建 OpenTelemetry span                                        │
│                                                                   │
│ 短路: 无效 JSON → 400 INVALID_REQUEST                            │
│       缺少必填字段 (id/method) → 400 MISSING_FIELD               │
│       type 非 "req" → 静默丢弃                                   │
└────────┬──────────────────────────────────────────────────────────┘
         ▼
┌───────────────────────────────────────────────────────────────────┐
│ Stage 2: AuthStage — 快检查 (同步)                                │
│ • 检查 Conn.Claims.ExpiresAt 是否已过 (纯内存, 无 IO)           │
│ • 距过期 < 60s → 推送 auth.expiring 事件 (仅一次)               │
│                                                                   │
│ 短路: JWT 已过期 → 401 TOKEN_EXPIRED + 断开连接                  │
│                                                                   │
│ 注: 黑名单检查 (Redis) 在异步阶段执行, 避免阻塞 readPump        │
└────────┬──────────────────────────────────────────────────────────┘
         ▼
┌───────────────────────────────────────────────────────────────────┐
│ Stage 3: ConnRateLimitStage (同步)                                │
│ • 本地 Token Bucket 限频 (每连接独立, 无锁竞争)                  │
│ • 默认: 100 帧/秒/连接                                           │
│                                                                   │
│ 短路: 超限 → 429 RATE_LIMITED { retryAfterMs }                   │
└────────┬──────────────────────────────────────────────────────────┘
         ▼
┌───────────────────────────────────────────────────────────────────┐
│ Stage 4: RouteLookupStage (同步)                                  │
│ • Router.Lookup(method) → Handler + MethodMeta                   │
│ • 将 Handler 和 MethodMeta 写入 FrameContext 供后续阶段使用      │
│                                                                   │
│ 短路: 未知 method → 404 METHOD_NOT_FOUND                         │
└────────┬──────────────────────────────────────────────────────────┘
         │
         │  ──── goroutine 切换点 (go pipeline.executeAsync) ────
         │
         ▼  (以下在独立 goroutine 中执行)
┌───────────────────────────────────────────────────────────────────┐
│ Stage 5: AuthBlacklistStage (异步)                                │
│ • 检查 JWT jti 是否在 Redis 黑名单中 (本地 LRU 缓存优先)        │
│ • 详见 §9.4 Token 黑名单                                         │
│                                                                   │
│ 短路: jti 在黑名单 → 401 TOKEN_REVOKED + 断开连接               │
└────────┬──────────────────────────────────────────────────────────┘
         ▼
┌───────────────────────────────────────────────────────────────────┐
│ Stage 6: UserRateLimitStage (异步, 仅写操作)                      │
│ • 检查 MethodMeta.WriteOp, 读操作跳过                            │
│ • Redis 滑动窗口限频 (用户级)                                     │
│ • 限频 key: "user:{userID}:{method}" (从 MethodMeta.RateKey 生成)│
│ • 详见 §10 Rate Limiter                                          │
│                                                                   │
│ 短路: 超限 → 429 RATE_LIMITED { retryAfterMs, remaining: 0 }    │
│ 跳过: MethodMeta.WriteOp == false → 直接通过                     │
└────────┬──────────────────────────────────────────────────────────┘
         ▼
┌───────────────────────────────────────────────────────────────────┐
│ Stage 7: IdempotencyStage (异步, 仅写操作)                        │
│ • 检查 MethodMeta.WriteOp, 读操作跳过                            │
│ • 提取 params.idempotencyKey                                     │
│ • Redis GET: 该 key 是否已有缓存结果                              │
│ • 命中 → 直接返回缓存响应 (短路, 但不是"错误")                   │
│ • 未命中 → 通过, Handler 执行后存储结果                           │
│                                                                   │
│ 短路: 重复请求 → 返回之前的响应 (相当于幂等成功)                 │
│ 跳过: MethodMeta.WriteOp == false → 直接通过                     │
│       params 中无 idempotencyKey → 直接通过                       │
└────────┬──────────────────────────────────────────────────────────┘
         ▼
┌───────────────────────────────────────────────────────────────────┐
│ Stage 8: ValidateStage (异步)                                     │
│ • 根据 method 查找对应的参数校验规则                              │
│ • 校验必填字段、类型、长度、格式                                  │
│ • 如 agent.submit: sessionKey required, message max=100000       │
│                                                                   │
│ 短路: 校验失败 → 400 INVALID_PARAMS { field, message }          │
└────────┬──────────────────────────────────────────────────────────┘
         ▼
┌───────────────────────────────────────────────────────────────────┐
│ Router.Dispatch (异步)                                            │
│ • RBAC 权限检查                                                   │
│ • 构建 HandlerContext                                             │
│ • 执行 Handler                                                    │
│ • 封装响应帧                                                      │
│ • 详见 §6 RPC 路由器                                              │
└───────────────────────────────────────────────────────────────────┘
         │
         ▼
  出站响应帧 → Conn.writeCh → writePump → Client
```

### 5.4 核心实现

```go
// internal/pipeline/pipeline.go

type FramePipeline struct {
    // ── 同步阶段 (在 readPump goroutine 中执行) ──
    syncStages []Stage

    // ── 异步阶段 (在独立 goroutine 中执行) ──
    asyncStages []Stage

    // ── 末端路由 ──
    router  *router.Router

    metrics *PipelineMetrics
}

// NewPipeline 构造管线, 注入所有依赖
func NewPipeline(
    router *router.Router,
    auth *auth.AuthModule,
    limiter *ratelimit.RateLimiter,
    dedup *dedup.IdempotencyStore,
    metrics *PipelineMetrics,
) *FramePipeline {
    return &FramePipeline{
        syncStages: []Stage{
            NewParseStage(),                        // 1. JSON 解析
            NewAuthQuickCheckStage(auth),            // 2. JWT 过期快检查
            NewConnRateLimitStage(limiter),           // 3. 连接级限频
            NewRouteLookupStage(router),              // 4. 方法存在性检查
        },
        asyncStages: []Stage{
            NewAuthBlacklistStage(auth.Blacklist()),  // 5. Token 黑名单
            NewUserRateLimitStage(limiter),            // 6. 用户级限频
            NewIdempotencyStage(dedup),                // 7. 幂等去重
            NewValidateStage(),                        // 8. 参数校验
        },
        router:  router,
        metrics: metrics,
    }
}

// Process 是管线入口, 由 Conn.readPump 同步调用
func (p *FramePipeline) Process(conn *Conn, raw []byte) {
    start := time.Now()

    // ── 同步阶段: 在 readPump goroutine 中执行 (快, 无 IO) ──
    ctx := &FrameContext{
        Conn:      conn,
        StartTime: start,
        TraceCtx:  context.Background(),
    }

    var msg json.RawMessage = raw
    var err error

    for _, stage := range p.syncStages {
        stageStart := time.Now()
        msg, err = stage.Process(ctx, msg)
        p.metrics.StageDuration.WithLabelValues(stage.Name()).Observe(time.Since(stageStart).Seconds())

        if err != nil {
            p.handleStageError(conn, ctx, stage.Name(), err)
            return
        }
    }

    // ── 切换到异步: 避免 readPump 阻塞 ──
    go p.executeAsync(conn, ctx, msg)
}

// executeAsync 在独立 goroutine 中执行异步阶段和 Handler
func (p *FramePipeline) executeAsync(conn *Conn, ctx *FrameContext, msg json.RawMessage) {
    // 创建 tracing span
    spanCtx, span := otel.Tracer("pipeline").Start(ctx.TraceCtx, "pipeline.async",
        trace.WithAttributes(
            attribute.String("method", ctx.Method),
            attribute.String("connId", conn.ID),
        ))
    defer span.End()
    ctx.TraceCtx = spanCtx

    // ── 异步阶段: 可能有 Redis IO ──
    var err error
    for _, stage := range p.asyncStages {
        stageStart := time.Now()
        msg, err = stage.Process(ctx, msg)
        p.metrics.StageDuration.WithLabelValues(stage.Name()).Observe(time.Since(stageStart).Seconds())

        if err != nil {
            p.handleStageError(conn, ctx, stage.Name(), err)
            return
        }
    }

    // ── 全部通过 → 路由到 Handler ──
    p.router.Dispatch(ctx, ctx.ParsedFrame)

    // 上报管线总耗时
    p.metrics.TotalDuration.WithLabelValues(ctx.Method).Observe(time.Since(ctx.StartTime).Seconds())
}

// handleStageError 统一处理管线阶段的拒绝
func (p *FramePipeline) handleStageError(conn *Conn, ctx *FrameContext, stageName string, err error) {
    p.metrics.StageRejected.WithLabelValues(stageName).Inc()

    var fe *FrameError
    if errors.As(err, &fe) {
        conn.SendErrorRes(ctx.RequestID, fe.Code, fe.Reason, fe.Message)
    } else {
        conn.SendErrorRes(ctx.RequestID, 500, "INTERNAL_ERROR", "pipeline error")
    }
}
```

### 5.5 各 Stage 实现

#### ParseStage — JSON 解析与帧提取

```go
// internal/pipeline/stage_parse.go

type ParseStage struct{}

func (s *ParseStage) Name() string { return "parse" }

func (s *ParseStage) Process(ctx *FrameContext, raw json.RawMessage) (json.RawMessage, error) {
    var frame RequestFrame
    if err := json.Unmarshal(raw, &frame); err != nil {
        return nil, &FrameError{Code: 400, Reason: "INVALID_JSON", Message: "malformed JSON"}
    }

    // 非 req 帧静默丢弃 (不返回错误)
    if frame.Type != "req" {
        return nil, &SilentDrop{} // 特殊 error 类型, handleStageError 不发送响应
    }

    // 必填字段检查
    if frame.ID == "" {
        return nil, &FrameError{Code: 400, Reason: "MISSING_FIELD", Message: "field 'id' is required"}
    }
    if frame.Method == "" {
        return nil, &FrameError{Code: 400, Reason: "MISSING_FIELD", Message: "field 'method' is required"}
    }

    // 填充 FrameContext
    ctx.RequestID = frame.ID
    ctx.Method = frame.Method
    ctx.ParsedFrame = &frame

    return frame.Params, nil // 后续阶段只需要 params 部分
}
```

#### ConnRateLimitStage — 连接级限频

```go
// internal/pipeline/stage_conn_ratelimit.go

type ConnRateLimitStage struct {
    // 连接级用本地 Token Bucket, 不需要 Redis
    // 每个 Conn 内嵌一个 rate.Limiter (golang.org/x/time/rate)
    defaultRate rate.Limit // 默认 100 帧/秒
    burst       int        // 突发容量 20
}

func (s *ConnRateLimitStage) Name() string { return "conn_ratelimit" }

func (s *ConnRateLimitStage) Process(ctx *FrameContext, raw json.RawMessage) (json.RawMessage, error) {
    limiter := ctx.Conn.RateLimiter() // 每个 Conn 内嵌的 rate.Limiter
    if !limiter.Allow() {
        return nil, &FrameError{
            Code:    429,
            Reason:  "RATE_LIMITED",
            Message: "connection rate limit exceeded",
            Extra:   map[string]any{"retryAfterMs": 1000},
        }
    }
    return raw, nil
}
```

#### UserRateLimitStage — 用户级限频 (异步)

```go
// internal/pipeline/stage_user_ratelimit.go

type UserRateLimitStage struct {
    limiter RateLimiter // Redis 实现
}

func (s *UserRateLimitStage) Name() string { return "user_ratelimit" }

func (s *UserRateLimitStage) Process(ctx *FrameContext, raw json.RawMessage) (json.RawMessage, error) {
    meta := ctx.MethodMeta
    if meta == nil || !meta.WriteOp {
        return raw, nil // 读操作跳过用户级限频
    }

    // 构造限频 key: "user:{userID}:agent.submit"
    key := fmt.Sprintf("user:%s:%s", ctx.Conn.UserID, ctx.Method)

    result, err := s.limiter.Allow(ctx.TraceCtx, key, meta.RateLimit())
    if err != nil {
        // Redis 不可用 → fail-open (放行)
        slog.Warn("user ratelimit check failed, fail-open", "err", err)
        return raw, nil
    }

    if !result.Allowed {
        return nil, &FrameError{
            Code:    429,
            Reason:  "RATE_LIMITED",
            Message: fmt.Sprintf("user rate limit exceeded for %s", ctx.Method),
            Extra: map[string]any{
                "retryAfterMs": result.RetryAfter.Milliseconds(),
                "remaining":    0,
            },
        }
    }

    return raw, nil
}
```

#### IdempotencyStage — 幂等去重 (异步)

```go
// internal/pipeline/stage_idempotency.go

type IdempotencyStage struct {
    store IdempotencyStore // Redis 实现
}

func (s *IdempotencyStage) Name() string { return "idempotency" }

func (s *IdempotencyStage) Process(ctx *FrameContext, raw json.RawMessage) (json.RawMessage, error) {
    meta := ctx.MethodMeta
    if meta == nil || !meta.WriteOp {
        return raw, nil // 读操作不需要幂等
    }

    // 从 params 中提取 idempotencyKey
    key := ctx.ParsedFrame.IdempotencyKey()
    if key == "" {
        return raw, nil // 未提供幂等 key → 跳过 (不强制)
    }

    // 查询是否已有缓存结果
    isDuplicate, cachedResult, err := s.store.Check(ctx.TraceCtx, key)
    if err != nil {
        // Redis 不可用 → fail-open
        slog.Warn("idempotency check failed, fail-open", "err", err)
        return raw, nil
    }

    if isDuplicate {
        // 重复请求 → 直接返回缓存的响应帧
        ctx.Conn.WriteRaw(cachedResult) // 原样发送之前的响应
        return nil, &SilentDrop{}       // 管线短路, 但不是错误
    }

    // 首次请求 → 标记 ctx, Handler 执行完后存储结果
    ctx.IdempotencyKey = key
    return raw, nil
}
```

### 5.6 FrameContext — 管线上下文

FrameContext 在管线各阶段之间传递上下文和中间结果，是各 Stage 共享数据的唯一通道：

```go
// internal/pipeline/context.go

type FrameContext struct {
    // ── 来源连接 ──
    Conn *ws.Conn

    // ── 帧信息 (ParseStage 填充) ──
    RequestID   string        // 请求 ID
    Method      string        // RPC 方法名
    ParsedFrame *RequestFrame // 完整解析后的帧

    // ── 方法元数据 (RouteLookupStage 填充) ──
    MethodMeta *router.MethodMeta // 方法的权限、限频配置等
    Handler    router.Handler     // 匹配到的 Handler

    // ── 幂等 (IdempotencyStage 填充) ──
    IdempotencyKey string // 非空时, Handler 执行完后需要缓存结果

    // ── 追踪 ──
    TraceCtx  context.Context // OpenTelemetry span 上下文
    StartTime time.Time       // 管线入口时间
}
```

```text
FrameContext 数据流:

  ParseStage ──────▶ 填充 RequestID, Method, ParsedFrame
                          │
  AuthQuickCheck ──▶ 读取 Conn.Claims.ExpiresAt
                          │
  ConnRateLimit ───▶ 读取 Conn.RateLimiter()
                          │
  RouteLookup ─────▶ 填充 MethodMeta, Handler
                          │
                    ─── goroutine 切换 ───
                          │
  AuthBlacklist ───▶ 读取 Conn.Claims.ID (jti)
                          │
  UserRateLimit ───▶ 读取 MethodMeta.WriteOp, MethodMeta.RateLimit()
                          │
  Idempotency ────▶ 读取 ParsedFrame.IdempotencyKey()
                    填充 IdempotencyKey
                          │
  Validate ────────▶ 读取 Method, ParsedFrame.Params
                          │
  Router.Dispatch ─▶ 读取 Handler, MethodMeta, 全部字段
```

### 5.7 错误类型与短路机制

管线中的"短路"并非都是错误。有两种短路：

```go
// internal/pipeline/errors.go

// FrameError — 需要向客户端返回错误帧的短路
type FrameError struct {
    Code    int               // WS 响应 code: 400/401/403/404/429/500/503
    Reason  string            // 机器可读: "RATE_LIMITED", "TOKEN_EXPIRED", ...
    Message string            // 人类可读: "rate limit exceeded"
    Extra   map[string]any    // 额外信息: retryAfterMs, remaining, ...
}

func (e *FrameError) Error() string { return fmt.Sprintf("[%d] %s: %s", e.Code, e.Reason, e.Message) }

// SilentDrop — 静默丢弃, 不向客户端发送任何响应
// 用途: 非 req 帧 (type != "req"), 幂等命中已返回缓存
type SilentDrop struct{}

func (e *SilentDrop) Error() string { return "silent drop" }
```

```text
短路处理矩阵:

  Stage 返回值            管线行为
  ─────────────────────   ─────────────────────────────────────────
  (msg, nil)              正常 → 继续下一个 Stage
  (nil, *FrameError)      错误短路 → 发送错误帧给客户端, 终止管线
  (nil, *SilentDrop)      静默短路 → 不发送任何响应, 终止管线
  (msg, *SkipStage)       跳过 → 本 Stage 不处理, 继续下一个 (未来扩展)
```

### 5.8 管线性能指标

```go
type PipelineMetrics struct {
    // 每个 Stage 的处理延迟
    StageDuration *prometheus.HistogramVec // labels: stage
    // 每个 Stage 的拒绝次数
    StageRejected *prometheus.CounterVec   // labels: stage
    // 管线总延迟 (从 readPump 收到帧到 Handler 开始执行)
    TotalDuration *prometheus.HistogramVec // labels: method
    // 帧吞吐量
    FramesProcessed *prometheus.CounterVec // labels: method, result (ok/rejected/dropped)
    // 同步/异步阶段分布
    SyncPhaseDuration  prometheus.Histogram // 同步阶段总耗时
    AsyncPhaseDuration prometheus.Histogram // 异步阶段总耗时
}
```

**典型延迟分布（单帧）**：

| 阶段 | 延迟 | IO | 说明 |
| --- | --- | --- | --- |
| ParseStage | ~5μs | 无 | JSON 解析 |
| AuthQuickCheck | ~1μs | 无 | 内存时间比较 |
| ConnRateLimit | ~1μs | 无 | 本地 Token Bucket |
| RouteLookup | ~1μs | 无 | map 查找 |
| **同步阶段合计** | **~8μs** | **无** | **readPump 阻塞时间** |
| goroutine 切换 | ~1μs | — | Go 调度器 |
| AuthBlacklist | ~5μs (缓存命中) / ~500μs (Redis) | 可能 | LRU 缓存命中率 > 99% |
| UserRateLimit | ~500μs | Redis | 滑动窗口计算 |
| IdempotencyCheck | ~500μs | Redis | GET 操作 |
| ValidateStage | ~10μs | 无 | 字段校验 |
| **异步阶段合计** | **~1ms** | **Redis** | **在独立 goroutine 中** |
| Router.Dispatch | 取决于 Handler | gRPC | agent.submit ~5ms |

> **关键数字**：readPump 每帧同步阻塞时间仅 ~8μs，意味着单连接可支撑 **12.5 万帧/秒** 的理论吞吐（远超实际需求）。真正的延迟在异步阶段的 Redis IO 中，但不影响 readPump 读取后续帧。

### 5.9 管线扩展指南

新增管线阶段只需三步：

```text
1. 实现 Stage 接口:

   type MyStage struct { /* 依赖注入 */ }
   func (s *MyStage) Name() string { return "my_stage" }
   func (s *MyStage) Process(ctx *FrameContext, raw json.RawMessage) (json.RawMessage, error) {
       // 你的逻辑
       return raw, nil
   }

2. 决定同步还是异步:
   • 无 IO (纯 CPU) → 加入 syncStages
   • 有 IO (Redis/HTTP/gRPC) → 加入 asyncStages

3. 决定插入位置 (越早拒绝越好):
   • 安全类 (Auth) → 靠前
   • 限频类 (Rate) → 安全之后
   • 业务类 (Validate) → 靠后
```

**已规划的 Phase 2+ Stage**：

| Stage | Phase | 位置 | 说明 |
| --- | --- | --- | --- |
| `QuotaStage` | Phase 2 | 异步, UserRateLimit 后 | 检查用户配额（每日 submit 次数、Token 用量） |
| `ABTestStage` | Phase 2 | 异步, Validate 后 | A/B 实验分流，修改 params 中的 Agent 配置 |
| `AuditStage` | Phase 3 | 异步, Router 前 | 审计日志，记录所有写操作到审计表 |

---

## 六、RPC 路由器

### 6.1 职责与定位

RPC 路由器是 Gateway 处理客户端请求的**业务入口**。它将 WS 帧中的 `method` 字段映射到具体的 Handler 执行，类似 HTTP 框架中的 URL 路由器。

```text
RPC 路由器在请求链路中的位置:

  Client
    │
    │  WS 帧: { "type":"req", "id":"r1", "method":"agent.submit", "params":{...} }
    ▼
  ┌──────────────────────────────────────────────────────────────────────┐
  │  FramePipeline (§5)                                                  │
  │  JSON 解析 → 帧类型检查 → 连接级限频 → 方法存在性 → 参数校验 →     │
  │  用户级限频 → 幂等去重 →                                             │
  │         │                                                            │
  │         ▼                                                            │
  │  ┌─────────────────────────────────────────────────────────────┐    │
  │  │  RPC Router (§6)                                            │    │
  │  │                                                             │    │
  │  │  "agent.submit"   → AgentSubmitHandler  → Dispatcher       │    │
  │  │  "agent.abort"    → AgentAbortHandler   → Dispatcher       │    │
  │  │  "agent.input"    → AgentInputHandler   → Dispatcher       │    │
  │  │  "agent.status"   → AgentStatusHandler  → Dispatcher       │    │
  │  │  "auth.refresh"   → AuthRefreshHandler  → AuthModule       │    │
  │  │  "ping"           → PingHandler         → (直接响应)       │    │
  │  │  未知 method       → UNKNOWN_METHOD 错误帧                 │    │
  │  └─────────────────────────────────────────────────────────────┘    │
  └──────────────────────────────────────────────────────────────────────┘
    │
    ▼
  WS 帧: { "type":"res", "id":"r1", "code":200, "status":"accepted", "data":{...} }
```

**Router 的 4 项职责**：

| # | 职责 | 说明 |
| --- | --- | --- |
| ① | **方法路由** | `method` 字段 → Handler 映射查找 |
| ② | **上下文构建** | 从 Conn 提取认证信息，构造 `HandlerContext` |
| ③ | **响应封装** | 将 Handler 返回值统一封装为 WS 响应帧 |
| ④ | **错误转换** | 将 Handler 返回的 error 转换为标准错误帧 |

**与其他模块的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| 被调用 | `FramePipeline` | 管线末端调用 `router.Dispatch()` |
| 管理 | `Handler` (多个) | 持有 method → Handler 映射表 |
| 依赖 | `Dispatcher` | Agent 类 Handler 通过 Dispatcher 调度任务 |
| 依赖 | `SessionManager` | Session 类 Handler 通过 SessionManager 操作会话 |
| 依赖 | `AuthModule` | auth.refresh Handler 通过 AuthModule 刷新 Token |

### 6.2 核心结构与 Dispatch 流程

```go
// internal/router/router.go

type Router struct {
    handlers map[string]Handler    // method → Handler 映射
    metas    map[string]*MethodMeta // method → 方法元数据
    metrics  *RouterMetrics
}

// MethodMeta 描述一个 RPC 方法的元数据, 用于权限检查、限频策略选择等
type MethodMeta struct {
    Method      string   // 方法名: "agent.submit"
    Category    string   // 分类: "agent" / "session" / "auth" / "system"
    RequireAuth bool     // 是否需要认证 (true: 大部分方法; false: ping)
    Roles       []string // 允许的角色 (空=全部角色可用)
    WriteOp     bool     // 是否为写操作 (true: 需要幂等去重和用户级限频)
    RateKey     string   // 限频 key 模板 (如 "user:{userID}:agent.submit")
}

func New() *Router {
    return &Router{
        handlers: make(map[string]Handler),
        metas:    make(map[string]*MethodMeta),
    }
}

func (r *Router) Register(method string, h Handler, meta *MethodMeta) {
    r.handlers[method] = h
    r.metas[method] = meta
}

func (r *Router) Lookup(method string) (Handler, *MethodMeta, bool) {
    h, ok := r.handlers[method]
    if !ok {
        return nil, nil, false
    }
    return h, r.metas[method], true
}
```

**Dispatch 完整流程**：

```go
// Dispatch 是 Pipeline 调用的入口, 接收已通过管线校验的请求帧
func (r *Router) Dispatch(ctx *FrameContext, frame *RequestFrame) {
    start := time.Now()

    // 1. 查找 Handler
    handler, meta, ok := r.Lookup(frame.Method)
    if !ok {
        ctx.Conn.SendErrorRes(frame.ID, 404, "METHOD_NOT_FOUND",
            fmt.Sprintf("unknown method: %s", frame.Method))
        r.metrics.MethodNotFoundInc(frame.Method)
        return
    }

    // 2. 权限检查 (RBAC)
    if meta.RequireAuth && len(meta.Roles) > 0 {
        if !hasAnyRole(ctx.Conn.Roles, meta.Roles) {
            ctx.Conn.SendErrorRes(frame.ID, 403, "FORBIDDEN",
                fmt.Sprintf("method %s requires roles: %v", frame.Method, meta.Roles))
            return
        }
    }

    // 3. 构建 HandlerContext
    hctx := &HandlerContext{
        Conn:      ctx.Conn,
        UserID:    ctx.Conn.UserID,
        Roles:     ctx.Conn.Roles,
        RequestID: frame.ID,
        Method:    frame.Method,
        TraceCtx:  ctx.TraceCtx,
    }

    // 4. 执行 Handler
    result, err := handler.Handle(hctx, frame.Params)

    // 5. 处理结果
    duration := time.Since(start)
    r.metrics.MethodDuration.WithLabelValues(frame.Method).Observe(duration.Seconds())

    if err != nil {
        // 已手动响应 (如 agent.submit 的 accepted + 后续 final)
        if errors.Is(err, ErrAlreadyResponded) {
            return
        }
        // 转换为标准错误帧
        code, reason, message := errorToResponse(err)
        ctx.Conn.SendErrorRes(frame.ID, code, reason, message)
        r.metrics.MethodErrorInc(frame.Method, reason)
        return
    }

    // 6. 返回成功响应帧
    ctx.Conn.SendResponse(frame.ID, 200, "ok", result)
    r.metrics.MethodSuccessInc(frame.Method)
}

// errorToResponse 将 Handler 返回的 error 转换为响应三元组 (code, reason, message)
func errorToResponse(err error) (int, string, string) {
    var ve *ValidationError
    var fe *ForbiddenError
    var ne *NotFoundError
    var de *DispatchError

    switch {
    case errors.As(err, &ve):
        return 400, "INVALID_PARAMS", ve.Error()
    case errors.As(err, &fe):
        return 403, "FORBIDDEN", fe.Error()
    case errors.As(err, &ne):
        return 404, "NOT_FOUND", ne.Error()
    case errors.As(err, &de):
        if de.AllBusy {
            return 503, "SERVICE_BUSY", "all workers are busy, please retry later"
        }
        return 502, "DISPATCH_FAILED", de.Error()
    default:
        return 500, "INTERNAL_ERROR", "internal server error"
    }
}
```

### 6.3 方法注册表（Phase 1）

```go
// cmd/sahara-gw/main.go — 启动时注册所有 RPC 方法

func registerRoutes(r *router.Router, deps *Dependencies) {
    // ── Agent 操作 (核心业务) ──
    r.Register("agent.submit", handlers.NewAgentSubmit(deps.Dispatcher, deps.SessionMgr),
        &router.MethodMeta{
            Category: "agent", RequireAuth: true, WriteOp: true,
            RateKey: "user:{userID}:agent.submit",
        })
    r.Register("agent.abort", handlers.NewAgentAbort(deps.Dispatcher),
        &router.MethodMeta{
            Category: "agent", RequireAuth: true, WriteOp: true,
        })
    r.Register("agent.input", handlers.NewAgentInput(deps.Dispatcher),
        &router.MethodMeta{
            Category: "agent", RequireAuth: true, WriteOp: true,
        })
    r.Register("agent.status", handlers.NewAgentStatus(deps.Dispatcher),
        &router.MethodMeta{
            Category: "agent", RequireAuth: true, WriteOp: false,
        })

    // ── 认证 ──
    r.Register("auth.refresh", handlers.NewAuthRefresh(deps.Auth),
        &router.MethodMeta{
            Category: "auth", RequireAuth: true, WriteOp: false,
        })

    // ── 系统 ──
    r.Register("ping", router.HandlerFunc(
        func(ctx *router.HandlerContext, _ json.RawMessage) (any, error) {
            return map[string]any{"pong": time.Now().UnixMilli()}, nil
        }),
        &router.MethodMeta{
            Category: "system", RequireAuth: false, WriteOp: false,
        })
}
```

**完整方法注册表**：

| Method | Handler | 分类 | 写操作 | 需认证 | 需限频 | 需幂等 | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `agent.submit` | `AgentSubmitHandler` | agent | ✓ | ✓ | ✓ | ✓ | 提交 Agent 任务 |
| `agent.abort` | `AgentAbortHandler` | agent | ✓ | ✓ | ✓ | ✓ | 中止正在执行的任务 |
| `agent.input` | `AgentInputHandler` | agent | ✓ | ✓ | ✓ | ✓ | 人机交互回复（回源到同一 Worker） |
| `agent.status` | `AgentStatusHandler` | agent | — | ✓ | ✓ | — | 查询任务状态 |
| `auth.refresh` | `AuthRefreshHandler` | auth | — | ✓ | ✓ | — | WS 内 Token 续期 |
| `ping` | `PingHandler` | system | — | — | — | — | 应用层心跳 |

> Phase 2+ 可根据业务扩展新增方法（如 `agent.tool_confirm`、`agent.file_upload`），只需实现 Handler 并注册，无需修改 Router 或 Pipeline。

### 6.4 请求-响应生命周期

不同的 RPC 方法有不同的响应模式。Router 需要支持三种响应模式：

```text
模式 1: 同步响应 (最简单)
─────────────────────────────────────────
  方法: ping, agent.status, auth.refresh
  Handler 执行完毕 → 直接返回 result → Router 封装为 res 帧

  Client                    Router              Handler
    │  req: ping              │                     │
    │ ─────────────────────▶  │  Handle()           │
    │                         │ ──────────────────▶  │
    │                         │  ◀── result ──────  │
    │  ◀── res: {pong: ...} ──│                     │
    │                         │                     │

模式 2: 异步 Accepted (Agent 任务)
─────────────────────────────────────────
  方法: agent.submit
  Handler 先返回 accepted 响应 → 后续结果通过 Event Bus 异步推送

  Client                    Router           Handler          Dispatcher
    │  req: agent.submit      │                │                  │
    │ ─────────────────────▶  │  Handle()      │                  │
    │                         │ ─────────────▶ │  Submit()        │
    │                         │                │ ───────────────▶ │
    │                         │                │ ◀── ok ───────── │
    │  ◀── res: accepted      │ ◀── Responded  │                  │
    │    {taskId, runId}      │                │                  │
    │                         │                │                  │
    │  ... (Event Bus 异步推送 agent.delta, agent.run_complete) ...
    │                         │                │                  │

模式 3: 强制回源 (人机交互)
─────────────────────────────────────────
  方法: agent.input, agent.abort
  Handler 通过 Dispatcher 强制亲和路由到同一 Worker

  Client                    Router           Handler          Dispatcher
    │  req: agent.input       │                │                  │
    │  {taskId, input}        │                │                  │
    │ ─────────────────────▶  │  Handle()      │                  │
    │                         │ ─────────────▶ │  SendInput()     │
    │                         │                │ ───────────────▶ │
    │                         │                │     (亲和查找     │
    │                         │                │      taskId →    │
    │                         │                │      Worker-2)   │
    │                         │                │ ◀── ok ───────── │
    │  ◀── res: ok            │ ◀── result ──  │                  │
```

### 6.5 Handler 实现示例

#### agent.submit — 异步任务提交

```go
// internal/router/handlers/agent_submit.go

type AgentSubmitHandler struct {
    dispatcher TaskDispatcher
    sessions   SessionManager
}

type AgentSubmitParams struct {
    SessionKey     string          `json:"sessionKey" validate:"required"`
    Message        string          `json:"message" validate:"required,max=100000"`
    IdempotencyKey string          `json:"idempotencyKey" validate:"required"`
    Options        *SubmitOptions  `json:"options,omitempty"`
    Attachments    []Attachment    `json:"attachments,omitempty"`
}

func (h *AgentSubmitHandler) Handle(ctx *HandlerContext, raw json.RawMessage) (any, error) {
    // 1. 参数解析与校验
    var params AgentSubmitParams
    if err := json.Unmarshal(raw, &params); err != nil {
        return nil, &ValidationError{Field: "params", Message: err.Error()}
    }

    // 2. 验证 session 归属 (防止越权访问他人会话)
    if !h.sessions.UserOwnsSession(ctx.UserID, params.SessionKey) {
        return nil, &ForbiddenError{Message: "session not owned by user"}
    }

    // 3. 构建 gRPC 请求
    submitReq := &SubmitRequest{
        TaskID:         ulid.Make().String(),
        SessionKey:     params.SessionKey,
        UserMessage:    params.Message,
        IdempotencyKey: params.IdempotencyKey,
        UserID:         ctx.UserID,
        GatewayID:      config.GatewayID,
    }

    // 4. 调度到 Runtime Worker (含亲和策略, 详见 §7)
    resp, err := h.dispatcher.Submit(ctx.TraceCtx, submitReq)
    if err != nil {
        return nil, &DispatchError{Cause: err, AllBusy: errors.Is(err, ErrAllWorkersBusy)}
    }

    // 5. 立即返回 accepted 响应 (不等任务完成)
    ctx.Conn.SendResponse(ctx.RequestID, 200, "accepted", map[string]any{
        "taskId": submitReq.TaskID,
        "runId":  resp.RunID,
    })

    // 6. 注册 session → Hub 索引 (供 Broadcaster 本地优先查找, §8.3)
    ctx.Conn.Hub().RegisterSession(params.SessionKey, ctx.Conn)

    return nil, ErrAlreadyResponded
}
```

#### agent.input — 人机交互回源

```go
// internal/router/handlers/agent_input.go

type AgentInputHandler struct {
    dispatcher *Dispatcher
}

type AgentInputParams struct {
    TaskID string `json:"taskId" validate:"required"`
    RunID  string `json:"runId" validate:"required"`
    Action string `json:"action" validate:"required,oneof=approve reject input"`
    Input  string `json:"input,omitempty"` // action=input 时必填
}

func (h *AgentInputHandler) Handle(ctx *HandlerContext, raw json.RawMessage) (any, error) {
    var params AgentInputParams
    if err := json.Unmarshal(raw, &params); err != nil {
        return nil, &ValidationError{Field: "params", Message: err.Error()}
    }

    // 通过 Dispatcher 强制亲和路由到同一 Worker (详见 §7.4)
    err := h.dispatcher.SendInput(ctx.TraceCtx, &SendInputRequest{
        TaskID: params.TaskID,
        RunID:  params.RunID,
        Input:  params.Input,
        Action: params.Action,
    })
    if err != nil {
        return nil, err
    }

    return map[string]any{
        "taskId": params.TaskID,
        "status": "input_delivered",
    }, nil
}
```

#### agent.abort — 任务中止

```go
// internal/router/handlers/agent_abort.go

type AgentAbortHandler struct {
    dispatcher TaskDispatcher
}

type AgentAbortParams struct {
    TaskID string `json:"taskId" validate:"required"`
    Reason string `json:"reason,omitempty"` // 可选: 用户取消原因
}

func (h *AgentAbortHandler) Handle(ctx *HandlerContext, raw json.RawMessage) (any, error) {
    var params AgentAbortParams
    if err := json.Unmarshal(raw, &params); err != nil {
        return nil, &ValidationError{Field: "params", Message: err.Error()}
    }

    // 通过 Dispatcher 路由到持有任务的 Worker (亲和路由)
    err := h.dispatcher.Abort(ctx.TraceCtx, params.TaskID, params.Reason)
    if err != nil {
        return nil, err
    }

    return map[string]any{
        "taskId": params.TaskID,
        "status": "abort_requested",
    }, nil
}
```

### 6.6 错误类型体系

Handler 返回的 error 由 Router 统一转换为标准 WS 错误帧。使用类型化 error 确保错误码和 reason 的一致性：

```go
// internal/router/errors.go

// ValidationError — 参数校验失败 (400)
type ValidationError struct {
    Field   string `json:"field"`
    Message string `json:"message"`
}
func (e *ValidationError) Error() string { return fmt.Sprintf("validation: %s - %s", e.Field, e.Message) }

// ForbiddenError — 权限不足 (403)
type ForbiddenError struct {
    Message string `json:"message"`
}
func (e *ForbiddenError) Error() string { return "forbidden: " + e.Message }

// NotFoundError — 资源不存在 (404)
type NotFoundError struct {
    Resource string `json:"resource"` // "session" / "task"
    ID       string `json:"id"`
}
func (e *NotFoundError) Error() string { return fmt.Sprintf("%s not found: %s", e.Resource, e.ID) }

// DispatchError — 调度失败 (502/503)
type DispatchError struct {
    Cause   error
    AllBusy bool // true → 503, false → 502
}
func (e *DispatchError) Error() string { return "dispatch: " + e.Cause.Error() }
func (e *DispatchError) Unwrap() error { return e.Cause }

// ErrAlreadyResponded — 标记 Handler 已手动发送响应, Router 跳过自动响应
var ErrAlreadyResponded = errors.New("already responded")
```

**错误码与 WS 帧映射**：

| Handler Error | WS 帧 code | reason | 场景 |
| --- | --- | --- | --- |
| `ValidationError` | 400 | `INVALID_PARAMS` | 参数格式错误、必填字段缺失 |
| `ForbiddenError` | 403 | `FORBIDDEN` | 访问他人 session、角色不匹配 |
| `NotFoundError` | 404 | `NOT_FOUND` | session/task 不存在 |
| `DispatchError` (非 AllBusy) | 502 | `DISPATCH_FAILED` | gRPC 调用 Worker 失败 |
| `DispatchError` (AllBusy) | 503 | `SERVICE_BUSY` | 所有 Worker 满载 |
| 未知 error | 500 | `INTERNAL_ERROR` | 未预期的内部错误 |

### 6.7 监控指标

```go
type RouterMetrics struct {
    // 方法调用延迟 (按 method 分组)
    MethodDuration *prometheus.HistogramVec  // labels: method
    // 方法调用成功/失败计数
    MethodTotal    *prometheus.CounterVec    // labels: method, result (ok/error)
    // 错误分布
    ErrorTotal     *prometheus.CounterVec    // labels: method, reason
    // 未知方法调用 (可能是恶意探测)
    MethodNotFound *prometheus.CounterVec    // labels: method
}
```

| 告警规则 | 条件 | 级别 |
| --- | --- | --- |
| Handler 延迟过高 | `MethodDuration{method="agent.submit"} P99 > 5s` | Warning |
| 错误率突增 | `ErrorTotal / MethodTotal > 5%` 持续 1min | Warning |
| 未知方法频繁调用 | `MethodNotFound > 100/min` | Warning (可能被探测) |
| SERVICE_BUSY 频繁 | `ErrorTotal{reason="SERVICE_BUSY"} > 10/min` | Critical |

---

## 七、Agent 调度器 (Dispatcher)

### 7.1 定位与职责

Dispatcher 是 Gateway 与 Agent Runtime 之间的**调度枢纽**，负责将用户的 Agent 请求路由到合适的 Runtime Worker，并管理任务生命周期中需要回源（sticky）的后续交互。

```text
架构位置:

  Client ──WS──▶ Gateway                    Agent Runtime
                   │                         ┌──────────┐
  ┌────────────┐   │   ┌──────────────┐      │ Worker-1 │
  │  Handler   │───┼──▶│  Dispatcher  │─gRPC─│ (Python) │
  │ agent.submit│  │   │              │      └──────────┘
  │ agent.abort │  │   │  ┌────────┐  │      ┌──────────┐
  │ agent.input │──┼──▶│  │亲和路由│  │─gRPC─│ Worker-2 │
  └────────────┘   │   │  └────────┘  │      │ (Python) │
                   │   │              │      └──────────┘
                   │   │  ┌────────┐  │      ┌──────────┐
                   │   │  │重试引擎│  │─gRPC─│ Worker-N │
                   │   │  └────────┘  │      │ (Python) │
                   │   └──────────────┘      └──────────┘
                   │           │
                   │           ▼
                   │   WorkerRegistry (服务发现)
                   │   Redis/gRPC 健康检查

  Dispatcher 的 5 大职责:
  ① 选择 Worker (负载均衡 + 亲和路由)
  ② 发送 gRPC 请求 (Submit / Abort / SendInput)
  ③ 失败重试 (RESOURCE_EXHAUSTED / UNAVAILABLE 自动换 Worker)
  ④ 亲和关系管理 (session/task → Worker 绑定)
  ⑤ 熔断保护 (标记不健康 Worker, 避免雪崩)
```

### 7.2 核心结构

```go
// internal/dispatch/dispatcher.go

type Dispatcher struct {
    registry WorkerRegistry     // Worker 发现与健康管理
    pool     *grpcpool.Pool     // gRPC 连接池 (Worker addr → *grpc.ClientConn)
    next     atomic.Uint64      // Round-Robin 轮询索引
    metrics  *observe.Metrics

    // ── 亲和路由 ──
    affinity *AffinityManager   // session/task → Worker 绑定管理
}

// AffinityManager 管理请求与 Worker 之间的亲和关系。
// 确保同一 session 的后续交互（特别是人机交互回源）路由到同一 Worker。
type AffinityManager struct {
    // sessionKey → 亲和记录
    sessions sync.Map  // key: string, value: *AffinityRecord

    // taskID → workerID (任务级别, 更强的绑定)
    tasks    sync.Map  // key: string, value: *AffinityRecord

    config   AffinityConfig
}

type AffinityRecord struct {
    WorkerID   string    // 绑定的 Worker
    SessionKey string    // 关联的 session
    TaskID     string    // 关联的任务 (如有)
    RunID      string    // 当前运行 ID
    CreatedAt  time.Time // 绑定时间
    LastUsedAt time.Time // 最后命中时间
    HitCount   int64     // 命中次数 (metrics)
    Sticky     bool      // 是否为强制绑定 (人机交互场景)
}

type AffinityConfig struct {
    SessionTTL time.Duration // session 亲和过期时间 (默认 30 分钟)
    TaskTTL    time.Duration // task 亲和过期时间 (默认 10 分钟)
    CleanupInterval time.Duration // 过期清理周期 (默认 1 分钟)
}
```

### 7.3 调度策略

Dispatcher 支持三种调度策略，按优先级依次尝试：

```text
调度策略优先级:

  Submit(req) 进入
        │
        ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 策略 1: 强制亲和 (Sticky Affinity)                          │
  │ 条件: req 携带 taskID 且 affinityManager.tasks 中有绑定     │
  │ 场景: 人机交互回源 (agent.input → 同一 Worker)              │
  │                                                             │
  │   task:abc123 → Worker-2 (Sticky=true)                     │
  │   ✓ Worker-2 健康且有余量 → 直接使用                        │
  │   ✗ Worker-2 不健康 → 返回错误 (不降级, 任务状态在 Worker)  │
  └──────────┬──────────────────────────────────────────────────┘
             │ 未命中
             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 策略 2: Session 亲和 (Soft Affinity)                        │
  │ 条件: affinityManager.sessions 中有该 sessionKey 的绑定     │
  │ 场景: 同一会话的连续提交 (复用沙箱、上下文缓存)             │
  │                                                             │
  │   sess:xyz → Worker-1 (Sticky=false)                       │
  │   ✓ Worker-1 健康且有余量 → 优先使用                        │
  │   ✗ Worker-1 满载/不健康 → 降级到策略 3                     │
  └──────────┬──────────────────────────────────────────────────┘
             │ 未命中 或 降级
             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 策略 3: 负载均衡 (Load Balance)                             │
  │ 方式: Round-Robin (Phase 1) → 最少活跃 (Phase 2)           │
  │                                                             │
  │   从 registry.ReadyWorkers() 中选择:                        │
  │   Phase 1: 轮询索引 next++ % len(workers)                  │
  │   Phase 2: 选择 ActiveTasks 最少的 Worker                   │
  └─────────────────────────────────────────────────────────────┘
```

| 策略 | 绑定强度 | 降级行为 | 典型场景 |
| --- | --- | --- | --- |
| **强制亲和** (Sticky) | 强：必须回到同一 Worker | 不降级，返回错误 | 人机交互回源、工具审批、用户输入 |
| **Session 亲和** (Soft) | 弱：优先同一 Worker | 降级到负载均衡 | 连续对话、复用沙箱缓存 |
| **负载均衡** (Balance) | 无绑定 | 遍历所有 Worker 重试 | 新 session 首次请求 |

### 7.4 人机交互回源 — 为什么必须强制亲和

Agent 执行过程中存在需要**暂停等待用户输入**的场景。此时 Agent 的运行状态（LLM 上下文、工具调用栈、沙箱环境）全部在某个 Worker 的内存中。用户的回复**必须**路由回同一个 Worker，否则任务无法继续。

```text
人机交互全流程:

  Client              Gateway              Worker-2 (Runtime)
    │                    │                       │
    │  agent.submit      │                       │
    │ ─────────────────▶ │  gRPC SubmitTask      │
    │                    │ ─────────────────────▶ │
    │                    │                       │  LLM 决定调用工具
    │                    │                       │  工具需要用户确认
    │                    │                       │
    │  event: agent.     │  Event Bus            │  发送 input_required
    │  input_required    │ ◀──────────────────── │  事件
    │ ◀──────────────── │                       │
    │                    │                       │  Agent 挂起 (await)
    │  ┌──────────────────────────────┐         │  ┌────────────────┐
    │  │ 用户看到:                     │         │  │ Worker-2 内存:  │
    │  │ "Agent 想执行 rm -rf /tmp/*  │         │  │ • LLM 上下文   │
    │  │  是否允许？ [允许] [拒绝]"    │         │  │ • 工具调用栈   │
    │  └──────────────────────────────┘         │  │ • 沙箱 session │
    │                    │                       │  │ • 等待 channel │
    │  用户点击 [允许]    │                       │  └────────────────┘
    │                    │                       │
    │  agent.input       │                       │
    │  (taskID=abc123)   │                       │
    │ ─────────────────▶ │                       │
    │                    │  ┌──────────────────┐ │
    │                    │  │ Dispatcher 查找:  │ │
    │                    │  │ task:abc123       │ │
    │                    │  │ → Worker-2       │ │
    │                    │  │ (Sticky=true)    │ │
    │                    │  └──────────────────┘ │
    │                    │  gRPC SendInput       │
    │                    │ ─────────────────────▶ │  唤醒 Agent
    │                    │                       │  继续执行工具
    │  event: agent.     │  Event Bus            │
    │  tool_result       │ ◀──────────────────── │
    │ ◀──────────────── │                       │
    │                    │                       │
```

**如果路由到了错误的 Worker 会怎样？**

```text
  Worker-3 收到 SendInput(taskID=abc123)
    → 本地找不到 taskID=abc123 的运行上下文
    → 返回 gRPC NOT_FOUND
    → Gateway 收到错误, 返回客户端:
      { "type":"res", "code": 404, "status":"not_found",
        "error": { "reason":"TASK_NOT_FOUND",
                   "message":"任务已结束或不在此节点" } }
```

**需要强制亲和的操作类型**：

| RPC 方法 | 说明 | 为什么必须回源 |
| --- | --- | --- |
| `agent.input` | 用户回复 Agent 的输入请求 | Agent 的 await 挂起在 Worker 内存中 |
| `agent.abort` | 用户中止正在执行的任务 | 需要在同一 Worker 上取消 goroutine/asyncio task |
| `agent.tool_confirm` | 用户确认/拒绝工具执行 | 工具审批的 channel 在 Worker 内存中 |
| `agent.file_upload` | 任务执行中追加文件 | 文件需要写入同一 Worker 的沙箱 |

**不需要亲和的操作**：

| RPC 方法 | 说明 | 为什么不需要 |
| --- | --- | --- |
| `agent.submit` (新任务) | 新会话的首次提交 | 任何 Worker 都可以处理 |
| `agent.status` | 查询任务状态 | 状态在 Redis/DB 中，任何节点可读 |

### 7.5 亲和管理生命周期

```text
亲和记录的创建、使用、失效:

  ┌───────────────────────────────────────────────────────────────────┐
  │  1. 创建亲和                                                      │
  │                                                                   │
  │  Submit 成功:                                                     │
  │    sessions[sess:xyz] = { Worker-2, Sticky:false, TTL:30min }    │
  │    tasks[task:abc123] = { Worker-2, Sticky:true,  TTL:10min }    │
  │                                                                   │
  │  人机交互事件到达 Gateway:                                        │
  │    Event Bus → agent.input_required { taskID, workerID }         │
  │    tasks[task:abc123].Sticky = true   ← 标记为强制亲和            │
  │    tasks[task:abc123].LastUsedAt = now                            │
  ├───────────────────────────────────────────────────────────────────┤
  │  2. 使用亲和                                                      │
  │                                                                   │
  │  agent.input(taskID=abc123):                                      │
  │    查找 tasks[task:abc123] → Worker-2 (Sticky)                   │
  │    检查 Worker-2 健康? → 是 → 路由到 Worker-2                    │
  │                         → 否 → 返回错误 (不降级)                  │
  │                                                                   │
  │  agent.submit(sessionKey=sess:xyz):                               │
  │    查找 sessions[sess:xyz] → Worker-2 (Soft)                     │
  │    检查 Worker-2 健康且有余量? → 是 → 路由到 Worker-2            │
  │                                → 否 → 降级到 Round-Robin         │
  ├───────────────────────────────────────────────────────────────────┤
  │  3. 亲和失效                                                      │
  │                                                                   │
  │  自然过期:                                                        │
  │    Session 亲和 30min 无新请求 → 自动清理                         │
  │    Task 亲和 10min 无交互 → 自动清理                              │
  │                                                                   │
  │  主动清除:                                                        │
  │    Event Bus → agent.run_complete / agent.run_error               │
  │    → 删除 tasks[task:abc123]                                      │
  │    → sessions[sess:xyz].Sticky = false (降级为软亲和)             │
  │                                                                   │
  │  Worker 下线:                                                     │
  │    WorkerRegistry → Worker-2 标记为 unhealthy                    │
  │    → 清除所有指向 Worker-2 的 task 亲和 (Sticky)                 │
  │    → 清除所有指向 Worker-2 的 session 亲和                       │
  │    → 受影响的进行中任务: Event Bus 发送 agent.run_error           │
  └───────────────────────────────────────────────────────────────────┘
```

### 7.6 完整调度流程

```text
Submit(req) / SendInput(req) / Abort(req)
  │
  ▼
┌──────────────────────────────────────────────────────────────────┐
│ 1. 亲和查找                                                      │
│                                                                  │
│    携带 taskID?  ──是──▶ 查 tasks[taskID]                        │
│         │                    │                                   │
│         │               找到且 Sticky?                           │
│         │                    │                                   │
│         │              ┌─是──┴──否─┐                             │
│         │              ▼          ▼                              │
│         │         强制路由    当作 session 亲和                   │
│         │         到该 Worker  走下面的流程                       │
│         │              │                                         │
│         │         Worker 健康?                                   │
│         │         ├─是→ 直接调用 ✓                               │
│         │         └─否→ 返回 ErrWorkerUnavailable ✗              │
│         │                                                        │
│         否                                                       │
│         ▼                                                        │
│    携带 sessionKey? ──是──▶ 查 sessions[sessionKey]              │
│         │                       │                                │
│         │                  找到且 Worker 健康有余量?              │
│         │                  ├─是→ 优先使用该 Worker                │
│         │                  └─否→ 降级到负载均衡                   │
│         │                                                        │
│         否 (或降级)                                               │
│         ▼                                                        │
│    负载均衡选择 Worker                                           │
│    Phase 1: Round-Robin (next++ % N)                             │
│    Phase 2: Least-Active (最少活跃任务)                          │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ 2. gRPC 调用                                                     │
│                                                                  │
│    callCtx = context.WithTimeout(ctx, 5s)                        │
│    resp, err = worker.Client.SubmitTask(callCtx, grpcReq)        │
│         │                                                        │
│    ┌────┴────┐                                                   │
│    │ 成功    │                                                   │
│    │         │                                                   │
│    │  记录亲和:                                                  │
│    │  sessions[sessionKey] = { worker, Sticky:false }            │
│    │  tasks[taskID] = { worker, Sticky:false }                   │
│    │  (Sticky 后续由 input_required 事件升级)                    │
│    │                                                             │
│    │  return SubmitResponse ✓                                    │
│    └─────────                                                    │
│                                                                  │
│    ┌─────────┐                                                   │
│    │ 失败    │                                                   │
│    │         │                                                   │
│    │  RESOURCE_EXHAUSTED → 记录 metrics, 尝试下一个 Worker       │
│    │  UNAVAILABLE        → 标记 Worker 不健康, 尝试下一个        │
│    │  NOT_FOUND          → 清除亲和记录, 返回错误                │
│    │  其他               → 直接返回错误 (不重试)                 │
│    └─────────                                                    │
│                                                                  │
│    全部 Worker 失败 → ErrAllWorkersBusy                          │
└──────────────────────────────────────────────────────────────────┘
```

### 7.7 核心实现

```go
// internal/dispatch/dispatcher.go

func (d *Dispatcher) Submit(ctx context.Context, req *SubmitRequest) (*SubmitResponse, error) {
    // ── 策略 1: 强制亲和 (taskID 绑定) ──
    if req.TaskID != "" {
        if record := d.affinity.GetTask(req.TaskID); record != nil && record.Sticky {
            return d.callWorkerStrict(ctx, record.WorkerID, req)
        }
    }

    // ── 策略 2: Session 亲和 (软绑定) ──
    if record := d.affinity.GetSession(req.SessionKey); record != nil {
        worker := d.registry.Worker(record.WorkerID)
        if worker != nil && worker.IsHealthy() && worker.HasCapacity() {
            resp, err := d.callWorker(ctx, worker, req)
            if err == nil {
                d.affinity.BindTask(req.TaskID, worker.ID, req.SessionKey)
                return resp, nil
            }
            // 软亲和失败 → 降级到负载均衡 (不 return)
            d.metrics.AffinityMissInc("session_fallback")
        }
    }

    // ── 策略 3: 负载均衡 + 遍历重试 ──
    workers := d.registry.ReadyWorkers()
    if len(workers) == 0 {
        return nil, ErrNoWorkersAvailable
    }

    startIdx := int(d.next.Add(1))
    var lastErr error

    for attempt := 0; attempt < len(workers); attempt++ {
        w := workers[(startIdx+attempt)%len(workers)]

        resp, err := d.callWorker(ctx, w, req)
        if err == nil {
            // 成功 → 建立亲和
            d.affinity.BindSession(req.SessionKey, w.ID)
            d.affinity.BindTask(req.TaskID, w.ID, req.SessionKey)
            return resp, nil
        }

        lastErr = err
        st := status.Code(err)
        switch st {
        case codes.ResourceExhausted, codes.Unavailable:
            d.metrics.DispatchRetryInc(w.ID, st.String())
            if st == codes.Unavailable {
                d.registry.MarkUnhealthy(w.ID)
            }
            continue
        default:
            return nil, err
        }
    }
    return nil, fmt.Errorf("all workers busy: %w", lastErr)
}

// callWorkerStrict 强制亲和调用: 不降级, Worker 不可用则直接报错
func (d *Dispatcher) callWorkerStrict(ctx context.Context, workerID string, req *SubmitRequest) (*SubmitResponse, error) {
    worker := d.registry.Worker(workerID)
    if worker == nil || !worker.IsHealthy() {
        d.affinity.RemoveTask(req.TaskID) // 清除失效亲和
        return nil, fmt.Errorf("affinity worker %s unavailable: %w", workerID, ErrWorkerUnavailable)
    }

    resp, err := d.callWorker(ctx, worker, req)
    if err != nil {
        st := status.Code(err)
        if st == codes.Unavailable {
            d.registry.MarkUnhealthy(workerID)
            d.affinity.RemoveTask(req.TaskID)
        }
        return nil, err
    }
    return resp, nil
}

// SendInput 人机交互回源: 将用户输入发送到任务所在的 Worker
// 这是一个强制亲和操作, 必须路由到同一 Worker
func (d *Dispatcher) SendInput(ctx context.Context, req *SendInputRequest) error {
    record := d.affinity.GetTask(req.TaskID)
    if record == nil || !record.Sticky {
        return fmt.Errorf("task %s has no active affinity: %w", req.TaskID, ErrAffinityNotFound)
    }

    worker := d.registry.Worker(record.WorkerID)
    if worker == nil || !worker.IsHealthy() {
        return fmt.Errorf("affinity worker %s for task %s unavailable: %w",
            record.WorkerID, req.TaskID, ErrWorkerUnavailable)
    }

    callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    _, err := worker.Client.SendInput(callCtx, &agentv1.SendInputRequest{
        TaskId: req.TaskID,
        RunId:  record.RunID,
        Input:  req.Input,
        Action: req.Action, // "approve" / "reject" / "input"
    })

    if err == nil {
        record.LastUsedAt = time.Now()
        record.HitCount++
        d.metrics.AffinityHitInc("task_sticky")
    }
    return err
}

// callWorker 执行实际的 gRPC 调用
func (d *Dispatcher) callWorker(ctx context.Context, w *WorkerInfo, req *SubmitRequest) (*SubmitResponse, error) {
    callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    resp, err := w.Client.SubmitTask(callCtx, &agentv1.SubmitTaskRequest{
        TaskId:         req.TaskID,
        SessionKey:     req.SessionKey,
        UserMessage:    &agentv1.UserMessage{Text: req.UserMessage},
        IdempotencyKey: req.IdempotencyKey,
        Metadata:       map[string]string{"user_id": req.UserID, "gateway_id": req.GatewayID},
    })
    if err != nil {
        return nil, err
    }
    return &SubmitResponse{RunID: resp.RunId, WorkerID: resp.WorkerId}, nil
}
```

### 7.8 亲和管理器实现

```go
// internal/dispatch/affinity.go

type AffinityManager struct {
    sessions sync.Map // sessionKey → *AffinityRecord
    tasks    sync.Map // taskID → *AffinityRecord
    config   AffinityConfig
}

// BindSession 建立/更新 session 软亲和
func (am *AffinityManager) BindSession(sessionKey, workerID string) {
    record := &AffinityRecord{
        WorkerID:   workerID,
        SessionKey: sessionKey,
        CreatedAt:  time.Now(),
        LastUsedAt: time.Now(),
        Sticky:     false, // session 亲和始终是软绑定
    }
    am.sessions.Store(sessionKey, record)
}

// BindTask 建立 task 亲和 (初始为软绑定, 等 input_required 事件升级为 Sticky)
func (am *AffinityManager) BindTask(taskID, workerID, sessionKey string) {
    if taskID == "" {
        return
    }
    record := &AffinityRecord{
        WorkerID:   workerID,
        SessionKey: sessionKey,
        TaskID:     taskID,
        CreatedAt:  time.Now(),
        LastUsedAt: time.Now(),
        Sticky:     false,
    }
    am.tasks.Store(taskID, record)
}

// MarkSticky 将 task 亲和升级为强制绑定
// 由 Broadcaster 在收到 agent.input_required 事件时调用
func (am *AffinityManager) MarkSticky(taskID, runID string) {
    if v, ok := am.tasks.Load(taskID); ok {
        record := v.(*AffinityRecord)
        record.Sticky = true
        record.RunID = runID
        record.LastUsedAt = time.Now()
    }
}

// OnTaskComplete 任务完成时清理 task 亲和, 保留 session 软亲和
// 由 Broadcaster 在收到 agent.run_complete / agent.run_error 事件时调用
func (am *AffinityManager) OnTaskComplete(taskID string) {
    am.tasks.Delete(taskID)
}

// OnWorkerDown 当 Worker 下线时, 批量清除指向该 Worker 的所有亲和
func (am *AffinityManager) OnWorkerDown(workerID string) {
    am.tasks.Range(func(key, value any) bool {
        if value.(*AffinityRecord).WorkerID == workerID {
            am.tasks.Delete(key)
        }
        return true
    })
    am.sessions.Range(func(key, value any) bool {
        if value.(*AffinityRecord).WorkerID == workerID {
            am.sessions.Delete(key)
        }
        return true
    })
}

// Cleanup 定期清理过期的亲和记录 (由 background goroutine 调用)
func (am *AffinityManager) Cleanup() {
    now := time.Now()
    am.sessions.Range(func(key, value any) bool {
        r := value.(*AffinityRecord)
        if now.Sub(r.LastUsedAt) > am.config.SessionTTL {
            am.sessions.Delete(key)
        }
        return true
    })
    am.tasks.Range(func(key, value any) bool {
        r := value.(*AffinityRecord)
        if now.Sub(r.LastUsedAt) > am.config.TaskTTL {
            am.tasks.Delete(key)
        }
        return true
    })
}
```

### 7.9 亲和与 Event Bus 的联动

Broadcaster 消费到特定事件时，需要同步更新 Dispatcher 的亲和状态：

```text
Event Bus 事件              Dispatcher 亲和操作
──────────────────────────────────────────────────────────────
agent.run_start             BindTask(taskID, workerID) — 软绑定
                            BindSession(sessionKey, workerID)

agent.input_required        MarkSticky(taskID, runID) — 升级为强制
                            (Agent 挂起等待用户输入)

agent.tool_confirm_required MarkSticky(taskID, runID) — 升级为强制
                            (工具执行需要用户审批)

agent.run_complete          OnTaskComplete(taskID) — 删除 task 亲和
                            (session 亲和保留, 供后续请求复用 Worker)

agent.run_error             OnTaskComplete(taskID) — 同上
agent.run_abort             OnTaskComplete(taskID) — 同上

WorkerRegistry 事件:
worker.unhealthy            OnWorkerDown(workerID) — 批量清除
```

```go
// internal/broadcast/broadcaster.go — 事件消费回调中

func (b *Broadcaster) onEvent(event *EventMessage) {
    // ... 推送给客户端的逻辑 ...

    // 同步更新 Dispatcher 亲和状态
    switch event.Event {
    case "agent.run_start":
        b.dispatcher.Affinity().BindTask(event.TaskID, event.WorkerID, event.SessionKey)
        b.dispatcher.Affinity().BindSession(event.SessionKey, event.WorkerID)

    case "agent.input_required", "agent.tool_confirm_required":
        b.dispatcher.Affinity().MarkSticky(event.TaskID, event.RunID)

    case "agent.run_complete", "agent.run_error", "agent.run_abort":
        b.dispatcher.Affinity().OnTaskComplete(event.TaskID)
    }
}
```

### 7.10 多实例部署下的亲和一致性

亲和记录存储在 Gateway 进程内存中。多实例部署时，需要保证同一用户的请求始终路由到同一个 Gateway（由 SessionRouter 保证），因此亲和记录也始终在同一个 Gateway 上读写，不存在一致性问题。

```text
多实例亲和一致性:

  Client ──WS──▶ Gateway-1 (持有该用户的 WS 连接)
                     │
                     │ Hub 中有该用户的 Conn
                     │ Dispatcher 中有该用户的亲和记录
                     │
                     ├── agent.submit → Worker-2 (建立亲和)
                     ├── agent.input  → Worker-2 (亲和命中, 强制回源)
                     └── agent.abort  → Worker-2 (亲和命中)

  Gateway-2 (不持有该用户的 WS 连接)
                     │
                     │ 不会收到该用户的请求
                     │ 不需要该用户的亲和记录
                     └── 无关

  关键保证: 一个用户的所有 WS 请求都在同一个 Gateway 处理
           (由 WS 长连接天然保证), 因此亲和记录不需要跨实例共享。

  边界场景: 用户断线重连到 Gateway-2?
    → 旧亲和在 Gateway-1 (已丢失)
    → 但 agent.run_complete 事件会在重连前到达, 任务已结束
    → 新请求走负载均衡建立新亲和, 不影响正确性
    → 如果任务仍在执行中 (未结束), Event Bus 中的
      input_required 事件会在 Gateway-2 重新触发 MarkSticky
```

### 7.11 监控指标

```go
// Dispatcher 的 Prometheus 指标
type DispatcherMetrics struct {
    // 调度延迟
    DispatchDuration *prometheus.HistogramVec // labels: worker, strategy
    // 亲和命中
    AffinityHitTotal  *prometheus.CounterVec  // labels: type (task_sticky/session_soft)
    AffinityMissTotal *prometheus.CounterVec  // labels: reason (expired/worker_down/fallback)
    // 活跃亲和数
    AffinityActiveGauge *prometheus.GaugeVec  // labels: type (session/task)
    // 重试
    DispatchRetryTotal *prometheus.CounterVec  // labels: worker, grpc_code
    // 错误
    DispatchErrorTotal *prometheus.CounterVec  // labels: error_type
}
```

| 告警规则 | 条件 | 级别 |
| --- | --- | --- |
| Sticky 亲和失败率高 | `AffinityMissTotal{type="task_sticky"}` > 5/min | Critical |
| 全部 Worker 繁忙 | `DispatchErrorTotal{error_type="all_workers_busy"}` > 0 | Warning |
| 调度延迟过高 | `DispatchDuration P99 > 3s` | Warning |
| 亲和记录堆积 | `AffinityActiveGauge{type="task"} > 10000` | Warning |

---

## 八、事件广播器 (Broadcaster)

### 8.1 职责

从 Event Bus（Redis Streams）消费 Agent 事件 → 查找目标连接 → 聚合 delta → 推送给客户端。

### 8.2 架构 — 本地优先路由

Broadcaster 采用**本地优先（local-first）**的连接查找策略：先查本地 Hub 内存（零 IO），命中直接推送；未命中时才回退到 Redis SessionRouter 查询。

```text
Redis Streams                      Broadcaster                       Client
events:{session} ─── XREADGROUP ──▶ Consumer Loop                      │
                                    │                                   │
                                    ▼                                   │
                              ┌─────────────┐                          │
                              │ Deserialize  │ protobuf → AgentEvent   │
                              └──────┬──────┘                          │
                                     ▼                                   │
                              ┌─────────────────────────────────┐      │
                              │ Route (本地优先)                  │      │
                              │                                   │      │
                              │  ① Hub 内存查找 (RLock, <1μs)   │      │
                              │     sessionKey → []*Conn          │      │
                              │     命中? ─── 是 → 直接推送 ────────────▶
                              │      │                            │      │
                              │      否 (本地无此 session)        │      │
                              │      ▼                            │      │
                              │  ② Redis SessionRouter (~0.5ms)  │      │
                              │     sessionKey → []connID         │      │
                              │     命中? → connID→Hub→Conn→推送 ───────▶
                              │      │                            │      │
                              │      否 → 丢弃 (该 session       │      │
                              │           不在本 Gateway)         │      │
                              └─────────────────────────────────┘      │
                                     ▼                                   │
                              ┌─────────────┐                          │
                              │ Aggregate    │ delta 事件 150ms 窗口   │
                              │ (per session)│                          │
                              └──────┬──────┘                          │
                                     ▼                                   │
                              ┌─────────────┐                          │
                              │ Push         │ → Conn.SendEvent()      │
                              └──────┬──────┘                          │
                                     └──────────────────────────────────▶
```

**为什么本地优先？**

| 对比 | Hub 内存查找 | Redis SessionRouter 查找 |
| --- | --- | --- |
| 延迟 | < 1μs (RLock + map 读取) | ~0.5ms (网络 IO + 序列化) |
| 吞吐 | 百万次/秒 | ~10 万次/秒 (受网络和 Redis 限制) |
| 命中场景 | 事件的 session 连接在本 Gateway | 需要确认连接是否在本 Gateway |
| 适用阶段 | 所有阶段 | 仅 Phase 2+（多 Gateway 实例） |

在大多数场景下（单实例部署、或事件恰好路由到持有连接的 Gateway），**本地命中率可达 90% 以上**，大幅减少 Redis 调用量。

### 8.3 ConnLookup 接口扩展

为了支持本地优先路由，`ConnLookup` 需要提供按 sessionKey 查找连接的能力。Hub 内部维护一个 `sessionConns` 索引：

```go
// internal/ws/hub.go — Hub 的 sessionConns 索引（完整 Hub 结构见 §3.2）

// Hub 在原有 conns / userConns 之外, 增加 sessionConns 索引:
//   sessionConns map[string][]*Conn  // sessionKey → [Conn]
// 在 Register/Unregister 时同步维护, 供 Broadcaster 本地优先查找使用。

// ConnsBySession 返回监听指定 session 的所有本地连接
// Broadcaster 本地优先查找时调用
func (h *Hub) ConnsBySession(sessionKey string) []*Conn {
    h.mu.RLock()
    defer h.mu.RUnlock()
    return h.sessionConns[sessionKey]
}

// RegisterSession 将连接注册到 session 索引
// 在 Upgrader 阶段或客户端订阅 session 时调用
func (h *Hub) RegisterSession(sessionKey string, conn *Conn) {
    // 此方法在 Hub.Run() 串行执行, 无需额外加锁
    h.sessionConns[sessionKey] = append(h.sessionConns[sessionKey], conn)
}

// UnregisterSession 从 session 索引中移除连接
func (h *Hub) UnregisterSession(sessionKey string, connID string) {
    conns := h.sessionConns[sessionKey]
    for i, c := range conns {
        if c.ID == connID {
            h.sessionConns[sessionKey] = append(conns[:i], conns[i+1:]...)
            break
        }
    }
    if len(h.sessionConns[sessionKey]) == 0 {
        delete(h.sessionConns, sessionKey)
    }
}
```

完整的 `ConnLookup` 接口定义见 §3.2，其中 `ConnsBySession` 方法即为本地优先路由所需。

### 8.4 消费者循环

```go
// internal/broadcast/consumer.go

type EventConsumer struct {
    redis      *redis.Client
    hub        ConnLookup      // 本地 Hub 查找 (优先)
    sessRouter SessionRouter   // Redis 路由 (回退)
    dispatcher *Dispatcher     // 亲和状态联动
    agg        *Aggregator
    group      string          // 消费组名 = gatewayID
    consumer   string          // 消费者名 = gatewayID
    metrics    *observe.Metrics
}

func (c *EventConsumer) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        default:
        }

        streams, err := c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
            Group:    c.group,
            Consumer: c.consumer,
            Streams:  c.activeStreams(),
            Count:    100,
            Block:    1 * time.Second,
        }).Result()

        if err != nil {
            if errors.Is(err, redis.Nil) {
                continue
            }
            slog.Error("xreadgroup error", "err", err)
            time.Sleep(100 * time.Millisecond)
            continue
        }

        for _, stream := range streams {
            for _, msg := range stream.Messages {
                c.processMessage(ctx, stream.Stream, msg)
            }
        }
    }
}

func (c *EventConsumer) processMessage(ctx context.Context, stream string, msg redis.XMessage) {
    // 1. 反序列化事件
    event, err := deserializeEvent(msg)
    if err != nil {
        slog.Warn("invalid event", "stream", stream, "err", err)
        return
    }

    // 2. 本地优先查找目标连接
    conns := c.resolveConns(event.SessionKey)
    if len(conns) == 0 {
        return // 该 session 的用户不在本 Gateway
    }

    // 3. 联动 Dispatcher 亲和状态 (§7.9)
    c.syncAffinityState(event)

    // 4. delta 事件走聚合器，其他事件直接推送
    if event.Type == eventv1.EVENT_TYPE_DELTA {
        c.agg.Add(event, conns)
    } else {
        c.pushToConns(conns, event)
    }

    // 5. 终态事件触发 final 响应
    if isTerminalEvent(event.Type) {
        c.sendFinalResponse(event)
    }
}

// resolveConns 本地优先的连接查找
func (c *EventConsumer) resolveConns(sessionKey string) []*Conn {
    // ── 第一步: 本地 Hub 查找 (零 IO) ──
    conns := c.hub.ConnsBySession(sessionKey)
    if len(conns) > 0 {
        c.metrics.BroadcastLocalHit.Inc()
        return conns
    }

    // ── 第二步: Redis SessionRouter 查找 ──
    connIDs := c.sessRouter.LookupConns(sessionKey)
    if len(connIDs) == 0 {
        c.metrics.BroadcastMiss.Inc()
        return nil
    }

    // connID 来自 Redis, 需要在本地 Hub 定位 Conn 对象
    result := make([]*Conn, 0, len(connIDs))
    for _, connID := range connIDs {
        conn := c.hub.ConnByID(connID)
        if conn != nil {
            result = append(result, conn)
        }
    }

    if len(result) > 0 {
        c.metrics.BroadcastRedisHit.Inc()
    } else {
        c.metrics.BroadcastMiss.Inc()
    }
    return result
}

// pushToConns 将事件推送到所有目标连接
func (c *EventConsumer) pushToConns(conns []*Conn, event *AgentEvent) {
    frame := buildEventFrame(event)
    for _, conn := range conns {
        seq := conn.NextSeq()
        frame.Seq = seq
        conn.SendEvent(event.EventName(), frame)
    }
}
```

**本地优先路由在不同部署场景下的表现**：

```text
场景 1: 单 Gateway 实例 (Phase 1)
─────────────────────────────────────────
  所有连接都在本实例 → 100% 本地命中
  resolveConns: Hub.ConnsBySession → 命中 → return
  Redis 调用次数: 0

场景 2: 多 Gateway, 按 session 分配 Stream (Phase 2)
─────────────────────────────────────────
  Event Bus 按 sessionKey 哈希分配 Stream 给特定 Gateway
  大部分事件路由到持有连接的 Gateway → ~95% 本地命中
  少量路由偏差 (hash 不均等) → 回退到 Redis

场景 3: 多 Gateway, Consumer Group 广播 (Phase 1 多实例)
─────────────────────────────────────────
  所有 Gateway 都消费所有 Stream
  每个事件在所有 Gateway 上触发 resolveConns
  只有持有连接的 Gateway 本地命中 → 其他 Gateway 查 Redis 后丢弃
  本地命中率 = 1/N (N=Gateway 数)
  → 这就是为什么 Phase 2 需要按 session 分配 Stream
```

### 8.5 Delta 聚合器

```go
// internal/broadcast/aggregator.go

type Aggregator struct {
    buckets sync.Map // sessionKey → *AggBucket
}

type AggBucket struct {
    mu       sync.Mutex
    texts    []string
    timer    *time.Timer
    connIDs  []string
    lastSeq  int32
    runID    string
}

func (a *Aggregator) Add(event *AgentEvent) {
    key := event.SessionKey
    bucket, _ := a.buckets.LoadOrStore(key, &AggBucket{})
    b := bucket.(*AggBucket)

    b.mu.Lock()
    defer b.mu.Unlock()

    b.texts = append(b.texts, event.GetDelta().Text)
    b.lastSeq = event.Seq
    b.runID = event.RunId

    // 首次进入窗口 → 启动 150ms 定时器
    if b.timer == nil {
        b.timer = time.AfterFunc(150*time.Millisecond, func() {
            a.flush(key)
        })
    }
}

func (a *Aggregator) flush(sessionKey string) {
    raw, ok := a.buckets.LoadAndDelete(sessionKey)
    if !ok {
        return
    }
    b := raw.(*AggBucket)

    b.mu.Lock()
    merged := strings.Join(b.texts, "")
    seq := b.lastSeq
    runID := b.runID
    b.timer = nil
    b.mu.Unlock()

    // 构建聚合后的 delta 事件帧，推送给所有连接
    // ...
}
```

---

## 九、认证模块

### 9.1 职责与定位

认证模块是 Gateway 的**安全门卫**，负责验证每一个进入系统的客户端身份，并在连接生命周期内持续保障认证状态的有效性。

```text
认证模块在请求链路中的位置:

  Client
    │
    │  GET /ws (携带 JWT)
    ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  Gateway                                                      │
  │                                                                │
  │  ┌────────────────────────────────────────────────────┐       │
  │  │  认证模块 (internal/auth)                          │       │
  │  │                                                    │       │
  │  │  ① Upgrade 认证 — 连接建立时一次性校验 JWT         │       │
  │  │     • 签名验证 (RS256 公钥)                        │       │
  │  │     • Claims 提取 (userID, roles, quota)           │       │
  │  │     • 黑名单检查 (jti 是否已被撤销)                │       │
  │  │     • Token 提取 (Header 或 Query Param)           │       │
  │  │                                                    │       │
  │  │  ② 连接内认证 — 存活期间持续保护                   │       │
  │  │     • Pipeline AuthStage 每帧检查过期时间          │       │
  │  │     • JWT 即将过期时推送 auth.expiring 事件         │       │
  │  │     • 客户端用新 Token 续期 (auth.refresh 请求)     │       │
  │  │     • Token 被撤销时主动断开连接                    │       │
  │  │                                                    │       │
  │  │  ③ 黑名单管理 — Token 撤销机制                     │       │
  │  │     • Redis SET 存储被撤销的 jti                   │       │
  │  │     • sahara-api 登出/改密时写入黑名单              │       │
  │  │     • Gateway 在 Upgrade 和 Pipeline 中检查         │       │
  │  └────────────────────────────────────────────────────┘       │
  │       │ 认证通过                                               │
  │       ▼                                                        │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
  │  │   Hub    │  │ Pipeline │  │  Router  │  │Dispatcher│     │
  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘     │
  └──────────────────────────────────────────────────────────────┘
```

**认证模块的 5 项职责**：

| # | 职责 | 触发时机 | 失败处理 |
| --- | --- | --- | --- |
| ① | **JWT 签名验证** | HTTP Upgrade 阶段 | HTTP 401 拒绝连接 |
| ② | **Claims 提取与填充** | Upgrade 成功后填充到 Conn | — |
| ③ | **Token 黑名单检查** | Upgrade + 每帧 AuthStage | Upgrade: HTTP 401; 帧: error 帧 + 断开 |
| ④ | **Token 过期预警与续期** | 连接存活期间持续检测 | 推送 auth.expiring 事件，客户端续期 |
| ⑤ | **Token 刷新** | 客户端发送 auth.refresh 请求 | 返回新 Token 或拒绝 |

**与其他模块的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| 被调用 | `Upgrader` | Upgrade 阶段调用 `Validate()` |
| 被调用 | `FramePipeline.AuthStage` | 每帧调用 `IsExpired()` / `IsBlacklisted()` |
| 被调用 | `auth.refresh Handler` | Token 续期请求调用 `RefreshToken()` |
| 依赖 | Redis (黑名单) | 检查 `blacklist:{jti}` key 是否存在 |
| 被通知 | `sahara-api` | 用户登出/改密时，api 写入 Redis 黑名单，Gateway 即时感知 |

### 9.2 核心结构

```go
// internal/auth/auth.go

type AuthModule struct {
    validator  *JWTValidator   // JWT 签名与 Claims 验证
    blacklist  *Blacklist      // Token 撤销黑名单 (Redis)
    refresher  *TokenRefresher // Token 续期 (可选, 或由 sahara-api 处理)
    config     AuthConfig
    metrics    *AuthMetrics
}

type AuthConfig struct {
    Issuer          string        // JWT issuer (必须匹配)
    Audience        string        // JWT audience (必须匹配)
    PublicKeyPath   string        // RS256 公钥路径 (或 JWKS URL)
    BlacklistPrefix string        // Redis 黑名单 key 前缀 (默认 "blacklist:")
    ExpiryWarning   time.Duration // 过期预警提前量 (默认 60s)
    RefreshEnabled  bool          // 是否允许 Gateway 内 Token 刷新
}

type AuthMetrics struct {
    ValidateTotal    *prometheus.CounterVec  // labels: result (ok/expired/invalid_sig/blacklisted)
    BlacklistChecks  prometheus.Counter      // 黑名单检查次数
    BlacklistHits    prometheus.Counter      // 黑名单命中次数
    RefreshTotal     *prometheus.CounterVec  // labels: result (ok/rejected)
    ExpiringPushed   prometheus.Counter      // auth.expiring 事件推送次数
}
```

### 9.3 JWT 验证器

```go
// internal/auth/jwt.go

type JWTValidator struct {
    publicKeys map[string]*rsa.PublicKey // kid → RS256 public key
    issuer     string
    audience   string
}

// Claims 是 JWT payload 中携带的业务信息。
// sahara-api 签发 Token 时填充这些字段, Gateway 只读不写。
type Claims struct {
    UserID string   `json:"sub"`              // 用户唯一 ID
    Roles  []string `json:"roles"`            // 权限角色: ["user", "premium", "admin"]
    Quota  *Quota   `json:"quota,omitempty"`  // 用量配额 (嵌入 Token 避免每次查 DB)
    jwt.RegisteredClaims                       // exp, iat, jti, iss, aud
}

type Quota struct {
    MaxSubmitsPerDay int `json:"maxSubmitsPerDay"` // 每日最大提交数
    MaxTokensPerDay  int `json:"maxTokensPerDay"`  // 每日最大 Token 消耗
    Tier             string `json:"tier"`           // free / pro / enterprise
}

func (v *JWTValidator) Validate(tokenString string) (*Claims, error) {
    token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(t *jwt.Token) (any, error) {
        // 1. 校验签名算法 (防止 alg=none 攻击)
        if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
            return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
        }

        // 2. 提取 kid, 查找对应公钥
        kid, ok := t.Header["kid"].(string)
        if !ok {
            return nil, errors.New("missing kid in token header")
        }
        key, ok := v.publicKeys[kid]
        if !ok {
            return nil, fmt.Errorf("unknown kid: %s", kid)
        }
        return key, nil
    })
    if err != nil {
        return nil, err
    }

    claims := token.Claims.(*Claims)

    // 3. 额外校验 (jwt 库已校验 exp, 这里校验 issuer 和 audience)
    if claims.Issuer != v.issuer {
        return nil, fmt.Errorf("invalid issuer: got %s, want %s", claims.Issuer, v.issuer)
    }
    if !claims.VerifyAudience(v.audience, true) {
        return nil, fmt.Errorf("invalid audience")
    }

    return claims, nil
}
```

### 9.4 Token 黑名单

当用户在 `sahara-api` 执行登出、修改密码、被封禁等操作时，即使 JWT 尚未过期也需要立即失效。通过 Redis 黑名单实现 Token 撤销：

```text
Token 撤销流程:

  sahara-api                    Redis                      Gateway
      │                          │                            │
      │  用户点击 [退出登录]     │                            │
      │                          │                            │
      │  SET blacklist:{jti} 1   │                            │
      │  EXPIRE {jwt剩余有效期}  │                            │
      │ ───────────────────────▶ │                            │
      │                          │                            │
      │                          │  (下次 AuthStage 检查)     │
      │                          │ ◀────────────────────────  │
      │                          │  EXISTS blacklist:{jti}    │
      │                          │ ────────────────────────▶  │
      │                          │                            │
      │                          │  命中! Token 已撤销        │
      │                          │                            │
      │                          │          event: error      │
      │                          │          (auth_revoked)    │
      │                          │            + WS Close      │
      │                          │                       ───▶ Client
```

```go
// internal/auth/blacklist.go

type Blacklist struct {
    redis  *redis.Client
    prefix string // "blacklist:"

    // 本地缓存: 避免每帧都查 Redis
    // LRU 缓存最近检查过的 jti → 是否在黑名单中
    cache  *lru.Cache[string, bool]  // 容量 10,000, TTL 30s
}

// IsBlacklisted 检查 Token 的 jti 是否已被撤销
// 先查本地缓存, 未命中才查 Redis
func (bl *Blacklist) IsBlacklisted(ctx context.Context, jti string) (bool, error) {
    // 1. 本地缓存查找 (零 IO)
    if blocked, ok := bl.cache.Get(jti); ok {
        return blocked, nil
    }

    // 2. Redis 查找
    exists, err := bl.redis.Exists(ctx, bl.prefix+jti).Result()
    if err != nil {
        // Redis 不可用时放行 (fail-open), 依赖 JWT 自身 exp 兜底
        slog.Warn("blacklist check failed, fail-open", "jti", jti, "err", err)
        return false, nil
    }

    blocked := exists > 0
    bl.cache.Add(jti, blocked) // 缓存结果 30s
    return blocked, nil
}
```

> **Fail-open vs Fail-close**：黑名单检查采用 fail-open 策略（Redis 不可用时放行）。原因是 JWT 本身有 exp 过期时间兜底，黑名单只是"提前撤销"的加速机制。如果 Redis 不可用就拒绝所有连接（fail-close），会导致全站不可用，代价远大于短暂放行一个已撤销的 Token。

### 9.5 连接内认证 — Pipeline AuthStage

JWT 验证不仅在 Upgrade 时执行一次，还在连接存活期间**每帧持续检查**。这确保 Token 过期或被撤销后，用户无法继续发送请求。

```go
// internal/pipeline/auth_stage.go

type AuthStage struct {
    blacklist *Blacklist
    config    AuthConfig
}

func (s *AuthStage) Name() string { return "auth" }

func (s *AuthStage) Process(ctx *FrameContext, raw json.RawMessage) (json.RawMessage, error) {
    conn := ctx.Conn
    now := time.Now()

    // 1. 检查 JWT 是否已过期
    if conn.Claims.ExpiresAt != nil && now.After(conn.Claims.ExpiresAt.Time) {
        return nil, &FrameError{
            Code:    401,
            Reason:  "TOKEN_EXPIRED",
            Message: "authentication token has expired, please reconnect",
        }
    }

    // 2. 检查 Token 是否被撤销 (黑名单)
    if conn.Claims.ID != "" { // jti 字段
        blocked, _ := s.blacklist.IsBlacklisted(ctx.TraceCtx, conn.Claims.ID)
        if blocked {
            // 异步关闭连接 (发送 error 事件后断开)
            go conn.CloseWithEvent("error", map[string]any{
                "code":    401,
                "reason":  "TOKEN_REVOKED",
                "message": "token has been revoked",
            }, 4001)
            return nil, &FrameError{
                Code:    401,
                Reason:  "TOKEN_REVOKED",
                Message: "token has been revoked, please re-authenticate",
            }
        }
    }

    // 3. 过期预警: 距离过期不足 60s 时推送 auth.expiring 事件
    //    (仅推送一次, 用 Conn 上的标志位防重复)
    if conn.Claims.ExpiresAt != nil && !conn.ExpiryWarned() {
        remaining := time.Until(conn.Claims.ExpiresAt.Time)
        if remaining > 0 && remaining <= s.config.ExpiryWarning {
            conn.MarkExpiryWarned()
            conn.SendEvent("auth.expiring", map[string]any{
                "expiresAt":  conn.Claims.ExpiresAt.Time.UnixMilli(),
                "remainingMs": remaining.Milliseconds(),
                "message":    "token will expire soon, please refresh",
            })
        }
    }

    return raw, nil // 认证通过, 帧继续流入下一个 Stage
}
```

### 9.6 Token 续期

客户端收到 `auth.expiring` 事件后，可以通过 `auth.refresh` 请求在不断开连接的情况下更新 Token：

```text
Token 续期时序:

  Client                      Gateway                    sahara-api
    │                            │                            │
    │ ◀── event: auth.expiring ──│                            │
    │    (距过期还有 55s)         │                            │
    │                            │                            │
    │  方式 A: 通过 sahara-api 刷新 (推荐)                    │
    │                            │                            │
    │  POST /api/v1/auth/refresh ────────────────────────────▶│
    │  { refreshToken: "..." }   │                            │
    │                            │                 签发新 JWT │
    │  ◀──────────────────────── { accessToken: "..." } ──── │
    │                            │                            │
    │  req: auth.refresh         │                            │
    │  { token: "新JWT" }        │                            │
    │ ──────────────────────────▶│                            │
    │                            │  验证新 JWT                │
    │                            │  更新 Conn.Claims          │
    │  ◀── res: ok ──────────── │                            │
    │                            │                            │
    │  方式 B: Gateway 直接刷新 (简化, Phase 1)               │
    │                            │                            │
    │  req: auth.refresh         │                            │
    │  { refreshToken: "..." }   │                            │
    │ ──────────────────────────▶│                            │
    │                            │  调用 sahara-api 内部接口  │
    │                            │ ──────────────────────────▶│
    │                            │  ◀── 新 JWT ────────────── │
    │                            │  更新 Conn.Claims          │
    │  ◀── res: { token }────── │                            │
```

```go
// internal/router/handlers/auth_refresh.go

type AuthRefreshHandler struct {
    validator *JWTValidator
    metrics   *AuthMetrics
}

func (h *AuthRefreshHandler) Handle(ctx *HandlerContext, params json.RawMessage) (any, error) {
    var req struct {
        Token string `json:"token"` // 客户端从 sahara-api 获取的新 JWT
    }
    if err := json.Unmarshal(params, &req); err != nil {
        return nil, &FrameError{Code: 400, Reason: "INVALID_PARAMS"}
    }

    // 1. 验证新 Token
    newClaims, err := h.validator.Validate(req.Token)
    if err != nil {
        h.metrics.RefreshTotal.WithLabelValues("rejected").Inc()
        return nil, &FrameError{Code: 401, Reason: "INVALID_TOKEN", Message: "new token is invalid"}
    }

    // 2. 确保是同一个用户 (防止 Token 替换攻击)
    if newClaims.UserID != ctx.UserID {
        return nil, &FrameError{Code: 403, Reason: "USER_MISMATCH", Message: "token user mismatch"}
    }

    // 3. 更新 Conn 上的认证信息
    ctx.Conn.UpdateClaims(newClaims)
    ctx.Conn.ResetExpiryWarned() // 重置过期预警标志

    h.metrics.RefreshTotal.WithLabelValues("ok").Inc()

    return map[string]any{
        "expiresAt": newClaims.ExpiresAt.Time.UnixMilli(),
        "message":   "token refreshed successfully",
    }, nil
}
```

### 9.7 WS Upgrade 认证（完整流程）

Upgrade 阶段是认证的主入口，整合了 JWT 验证、黑名单检查、连接限制、断线恢复等多项检查：

```go
// internal/ws/upgrader.go

func (u *Upgrader) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // ── 1. 提取 JWT ──
    // 支持两种方式: Authorization header (优先) 或 query param (WebSocket 限制)
    token := extractToken(r)
    if token == "" {
        http.Error(w, "missing authorization", http.StatusUnauthorized)
        u.metrics.ValidateTotal.WithLabelValues("missing").Inc()
        return
    }

    // ── 2. 验证 JWT (签名 + Claims + 过期时间) ──
    claims, err := u.auth.Validate(token)
    if err != nil {
        http.Error(w, "invalid token", http.StatusUnauthorized)
        u.metrics.ValidateTotal.WithLabelValues("invalid").Inc()
        return
    }

    // ── 3. 黑名单检查 (Token 是否被撤销) ──
    if claims.ID != "" {
        blocked, _ := u.auth.Blacklist().IsBlacklisted(r.Context(), claims.ID)
        if blocked {
            http.Error(w, "token revoked", http.StatusUnauthorized)
            u.metrics.ValidateTotal.WithLabelValues("blacklisted").Inc()
            return
        }
    }

    // ── 4. 连接数检查 (详见 §4.5) ──
    if u.hub.ConnCount() >= u.config.MaxConnsTotal {
        w.Header().Set("Retry-After", "5")
        http.Error(w, "service at capacity", http.StatusServiceUnavailable)
        return
    }
    if u.hub.UserConnCount(claims.UserID) >= u.config.MaxConnsPerUser {
        http.Error(w, "too many connections", http.StatusTooManyRequests)
        return
    }

    u.metrics.ValidateTotal.WithLabelValues("ok").Inc()

    // ── 5. 升级为 WebSocket ──
    wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
        Subprotocols: []string{"sahara-v1"},
    })
    if err != nil {
        return
    }

    // ── 6. 处理 resumeToken (断线恢复) ──
    resumeToken := r.URL.Query().Get("resumeToken")
    lastSeqs := r.URL.Query().Get("lastSeqs")

    // ── 7. 创建 Conn 并填充认证信息 ──
    conn := NewConn(wsConn, claims, u.hub, u.pipeline)

    // ── 8. 发送 welcome 事件 ──
    conn.SendEvent("welcome", buildWelcome(conn, resumeToken != ""))

    // ── 9. 如果是恢复连接, 回放错过的事件 ──
    if resumeToken != "" {
        go u.replayEvents(conn, resumeToken, lastSeqs)
    }

    // ── 10. 注册到 Hub 并启动读写循环 ──
    u.hub.register <- conn
    go conn.readPump()
    go conn.writePump()
}

// extractToken 从请求中提取 JWT
// 优先级: Authorization header > query param "token"
func extractToken(r *http.Request) string {
    // 方式 1: Authorization: Bearer <token>
    if auth := r.Header.Get("Authorization"); auth != "" {
        if strings.HasPrefix(auth, "Bearer ") {
            return strings.TrimPrefix(auth, "Bearer ")
        }
    }
    // 方式 2: ?token=<jwt> (WebSocket 在浏览器中无法自定义 header)
    if token := r.URL.Query().Get("token"); token != "" {
        return token
    }
    return ""
}
```

### 9.8 认证安全策略总结

| 安全措施 | 实现方式 | 防御目标 |
| --- | --- | --- |
| **RS256 非对称签名** | sahara-api 持有私钥签发, Gateway 只持公钥验证 | 防止 Token 伪造 |
| **alg 校验** | 显式检查 `t.Method.(*jwt.SigningMethodRSA)` | 防止 alg=none 攻击 |
| **kid 轮换** | 多公钥支持, 按 kid 查找 | 支持密钥无缝轮换 |
| **Token 黑名单** | Redis + 本地 LRU 缓存 | 登出/改密后即时撤销 |
| **连接内持续验证** | Pipeline AuthStage 每帧检查 | 防止过期 Token 继续使用 |
| **过期预警** | auth.expiring 事件, 提前 60s | 客户端无缝续期, 不断连 |
| **用户匹配校验** | refresh 时检查 `newClaims.UserID == ctx.UserID` | 防止 Token 替换攻击 |
| **Fail-open 黑名单** | Redis 不可用时放行, JWT exp 兜底 | 避免单点故障导致全站不可用 |
| **query param Token** | 仅在无法设置 header 时使用 (浏览器 WS) | 兼容 WebSocket 限制 |

---

## 十、Rate Limiter

### 10.1 Rate Limiter 的作用

> Gateway 直接暴露在公网，面对的是**不可信的 C 端用户**。没有限频意味着：
> - 一个恶意脚本就能打满 Gateway 的 CPU（帧洪水）
> - 一个用户狂点就能耗尽所有 Runtime Worker（Agent 过载）
> - 一次促销活动就能让整个系统雪崩（全局过载）
>
> Rate Limiter 是 Gateway 的**第一道防线**，在请求到达业务逻辑之前拦截异常流量，保护三个层面的资源：
>
> | 保护对象 | 风险 | 对应层 |
> | --- | --- | --- |
> | **Gateway 自身 CPU** | 单连接帧洪水 | Layer 1 连接级 |
> | **Runtime Worker 资源** | 用户滥用 Agent | Layer 2 用户级 |
> | **系统整体可用性** | 突发流量雪崩 | Layer 3 全局级 |

### 10.2 为什么需要三层

C 端场景面向公网不可信用户，一个恶意或失控的客户端可能造成三种不同级别的危害。单层限频无法兼顾，因此需要逐层防护：

```text
┌───────────────────────────────────────────────────────────────────────┐
│                                                                       │
│  Layer 1: 连接级 (per connection)                                     │
│  ──────────────────────────────────                                   │
│  防什么: 单个 WS 连接的帧洪水攻击                                    │
│  场景:   恶意脚本每秒发 1000 个 req 帧，耗尽 Gateway 的 CPU           │
│  限制:   每连接每秒最多 10 个 req 帧                                  │
│  实现:   进程内令牌桶（无 IO，纳秒级判断）                            │
│  成本:   零网络开销（纯内存）                                        │
│                                                                       │
│  Layer 2: 用户级 (per user)                                           │
│  ──────────────────────────────────                                   │
│  防什么: 同一用户开多个连接/设备绕过连接级限制                       │
│  场景:   用户开 5 个浏览器标签，每个发 10 req/s = 总计 50 req/s       │
│  限制:   每用户每分钟最多 20 次 agent.submit                          │
│  实现:   Redis 滑动窗口（跨连接、跨 Gateway 实例全局计数）            │
│  成本:   每次写请求 1 次 Redis Eval                                   │
│                                                                       │
│  Layer 3: 全局级 (system-wide)                                        │
│  ──────────────────────────────────                                   │
│  防什么: 大量合法用户同时请求导致 Runtime 过载                       │
│  场景:   1000 个用户同时提交 Agent 任务，Runtime 只能承载 100 并发     │
│  限制:   全系统每秒最多 100 次 agent.submit                           │
│  实现:   Redis 全局计数器                                            │
│  成本:   每次写请求 1 次 Redis INCR                                   │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**三层的执行顺序和短路逻辑**：

```text
客户端 req 帧到达
    │
    ▼
Layer 1: 连接级检查 (本地, ~1ns)
    │ 不通过 → 立即返回 429 RATE_LIMITED
    ▼ 通过
Layer 2: 用户级检查 (Redis, ~0.5ms) — 仅对 agent.submit 等写方法
    │ 不通过 → 返回 429 RATE_LIMITED + retryAfterMs
    ▼ 通过
Layer 3: 全局级检查 (Redis, ~0.3ms) — 仅对 agent.submit
    │ 不通过 → 返回 503 SYSTEM_BUSY + 排队位置
    ▼ 通过
正常执行请求
```

> **关键设计**：Layer 1 在内存中完成（无 IO），可以在不增加 Redis 负载的情况下拦截 90%+ 的恶意流量。
> Layer 2 和 3 只对 `agent.submit` 等重操作生效，`ping`、`session.list` 等轻量操作只过 Layer 1。

### 10.3 客户端收到的限频响应

每层触发时 Gateway 下发的 WS 帧不同，客户端据此区分处理：

**Layer 1 触发**（连接级超限）：

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
    "retryAfterMs": 1000
  }
}
```

**Layer 2 触发**（用户级超限）：

```json
{
  "type": "res",
  "id": "req_01JK...",
  "code": 429,
  "status": "error",
  "error": {
    "reason": "RATE_LIMITED",
    "message": "Agent 提交次数已达上限 (20次/分钟)，请稍后再试",
    "retryable": true,
    "retryAfterMs": 15000,
    "details": {
      "layer": "user",
      "limit": "20/min",
      "remaining": 0,
      "resetAt": 1706000060000
    }
  }
}
```

**Layer 3 触发**（全局过载）：

```json
{
  "type": "res",
  "id": "req_01JK...",
  "code": 503,
  "status": "error",
  "error": {
    "reason": "SYSTEM_BUSY",
    "message": "系统繁忙，已加入排队",
    "retryable": true,
    "retryAfterMs": 5000,
    "details": {
      "layer": "global",
      "queuePosition": 12
    }
  }
}
```

> **客户端处理建议**：
> - `code 429` → 禁用提交按钮，倒计时 `retryAfterMs` 后恢复
> - `code 503` → 显示排队提示和位置，倒计时后自动重试
> - `details.layer` 字段帮助前端展示更精确的提示文案

### 10.4 接口定义

```go
// internal/ratelimit/limiter.go

type Limiter interface {
    // 连接级检查 (本地, 无 IO)
    AllowConn(connID string) bool
    // 用户级检查 (Redis, 仅写方法)
    AllowUser(ctx context.Context, userID, method string) (bool, time.Duration)
    // 全局级检查 (Redis)
    AllowGlobal(ctx context.Context, method string) bool
}

type MultiLimiter struct {
    conn   *ConnLimiter     // 进程内令牌桶
    user   *UserLimiter     // Redis 滑动窗口
    global *GlobalLimiter   // Redis 计数器
}
```

### 10.5 连接级 — 本地令牌桶

```go
// internal/ratelimit/conn_limiter.go

type ConnLimiter struct {
    limiters sync.Map // connID → *rate.Limiter
    rate     rate.Limit
    burst    int
}

func (l *ConnLimiter) AllowConn(connID string) bool {
    rl, _ := l.limiters.LoadOrStore(connID, rate.NewLimiter(l.rate, l.burst))
    return rl.(*rate.Limiter).Allow()
}
```

### 10.6 用户级 — Redis 滑动窗口

```go
// internal/ratelimit/user_limiter.go

// Redis 滑动窗口脚本 (原子操作)
const luaSlidingWindow = `
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])

    redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
    local count = redis.call('ZCARD', key)
    if count < limit then
        redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
        redis.call('EXPIRE', key, math.ceil(window / 1000))
        return 1
    end
    return 0
`

func (l *UserLimiter) AllowUser(ctx context.Context, userID, method string) (bool, time.Duration) {
    key := fmt.Sprintf("rl:user:%s:%s", userID, method)
    result, err := l.redis.Eval(ctx, luaSlidingWindow, []string{key},
        time.Now().UnixMilli(), l.windowMs, l.limit).Int()
    if err != nil || result == 0 {
        retryAfter := l.estimateRetryAfter(ctx, key)
        return false, retryAfter
    }
    return true, 0
}
```

---

## 十一、Session Router

### 11.1 职责

维护 **sessionKey → 用户连接** 的映射关系，支撑两个核心场景：
1. **事件路由**：Broadcaster 收到事件后查找"该 session 的用户连接在哪"
2. **断线恢复**：记录用户的活跃 session 列表，生成 resumeToken

### 11.2 多实例事件路由全流程

> 多 Gateway 实例部署时，每个实例只在内存中持有自己的 WS 连接（Hub），不知道其他实例的连接状态。
> 事件从 Runtime → Event Bus 扇出到所有 Gateway，靠 **Redis 路由表** 决定"该推给谁、该丢弃"。

**完整流程分三个阶段：注册、路由、清理。**

#### 阶段一：连接建立时注册路由

```text
用户 A 从 Web 连接到 Gateway-1，打开 sess_abc 会话
用户 A 从 App 连接到 Gateway-2，也打开 sess_abc 会话

                         Redis 路由表
                         ─────────────
session:route:sess_abc  → SET { "gw1_conn_01", "gw2_conn_02" }
conn:sessions:gw1_conn_01 → SET { "sess_abc" }
conn:sessions:gw2_conn_02 → SET { "sess_abc" }
user:sessions:user_A    → ZSET { sess_abc → 1706000000 }
```

> connID 格式为 `{gatewayId}_{localId}`（如 `gw1_conn_01`），**前缀即可判断归属哪个 Gateway**。

#### 阶段二：事件到达时查表路由

```text
Runtime 执行 sess_abc 的 Agent 任务，产出 delta 事件

    Runtime
       │
       │ publish → Event Bus (Redis Streams)
       ▼
  events:sess_abc ──────────────────────────────────────
       │                    │                    │
       ▼                    ▼                    ▼
   Gateway-1            Gateway-2            Gateway-3
   Consumer             Consumer             Consumer
       │                    │                    │
       ▼                    ▼                    ▼
   LookupConns          LookupConns          LookupConns
   ("sess_abc")         ("sess_abc")         ("sess_abc")
       │                    │                    │
       ▼                    ▼                    ▼
   返回:                 返回:                 返回:
   gw1_conn_01 ✓        gw1_conn_01 ✗        gw1_conn_01 ✗
   gw2_conn_02 ✗        gw2_conn_02 ✓        gw2_conn_02 ✗
       │                    │                    │
       ▼                    ▼                    ▼
   推送给 Web 客户端    推送给 App 客户端      丢弃（无关连接）
```

**判断逻辑**：`LookupConns` 返回的 connID 列表中，只有在本 Gateway Hub 中能 `GetConn()` 到的才推送，其余跳过。

```go
// Broadcaster.processMessage 中的关键逻辑
connIDs := c.sessRouter.LookupConns(event.SessionKey)
for _, connID := range connIDs {
    if conn := c.hub.GetConn(connID); conn != nil {
        // 这个连接在我的 Hub 里 → 推送
        conn.SendEvent(event)
    }
    // GetConn 返回 nil → 不是我的连接 → 跳过
}
```

#### 阶段三：连接断开时清理路由

```text
用户 A 的 Web 浏览器关闭 → Gateway-1 断开 gw1_conn_01

Gateway-1 执行:
  Redis SREM session:route:sess_abc "gw1_conn_01"   ← 移除连接
  Redis DEL  conn:sessions:gw1_conn_01              ← 清理反向索引

路由表变为:
  session:route:sess_abc → SET { "gw2_conn_02" }    ← 只剩 App 连接

后续 sess_abc 的事件只会推送给 Gateway-2 上的 App 客户端。
```

#### 完整生命周期时序

```text
t=0    用户 A Web 连接到 GW-1
       GW-1: Hub.Register(conn) → SessionRouter.Register("sess_abc", "user_A", "gw1_conn_01")
       Redis: session:route:sess_abc → { gw1_conn_01 }

t=1    用户 A App 连接到 GW-2
       GW-2: Hub.Register(conn) → SessionRouter.Register("sess_abc", "user_A", "gw2_conn_02")
       Redis: session:route:sess_abc → { gw1_conn_01, gw2_conn_02 }

t=2    用户 A 提交 Agent 任务 (通过 Web)
       GW-1 → gRPC → Runtime: SubmitTask

t=3-5  Runtime 执行，产出事件 → Event Bus
       GW-1: LookupConns → gw1_conn_01 ✓ → 推送 Web
       GW-2: LookupConns → gw2_conn_02 ✓ → 推送 App
       GW-3: LookupConns → 无匹配 → 丢弃

t=6    用户 A 关闭 Web
       GW-1: Hub.Unregister(conn) → SessionRouter.Unregister("gw1_conn_01")
       Redis: session:route:sess_abc → { gw2_conn_02 }

t=7    后续事件只推给 GW-2 上的 App
```

### 11.3 扇出优化（分阶段）

Phase 1 中所有 Gateway 消费所有事件，查路由表后大部分丢弃。随规模增长需要优化：

| 阶段 | 方案 | 扇出效率 | 实现成本 |
| --- | --- | --- | --- |
| **Phase 1** | 全量消费 + 本地 `LookupConns` 过滤 | 每条事件被 N 个 Gateway 消费，仅 1-2 个有效 | 低 |
| **Phase 2** | Event Bus Pipeline 查路由表，写入目标 Gateway 专属 Stream | 每条事件只被目标 Gateway 消费 | 中 |
| **Phase 3** | NATS subject 粒度订阅 `events.{gatewayId}.*` | 零无效扇出 | 中 |

Phase 2 优化示意：

```text
Phase 1 (当前):
  Runtime → events:sess_abc → 所有 GW 消费 → 查路由 → 大部分丢弃

Phase 2 (优化后):
  Runtime → events:sess_abc → Pipeline 查路由 → 写入 events:gw1, events:gw2
  GW-1 只消费 events:gw1 (只含自己负责的 session 事件)
  GW-2 只消费 events:gw2
  GW-3 不收到任何事件
```

> **何时触发优化**：当 Gateway 实例数 > 5 且观测到 Broadcaster 的"事件丢弃率"（`events_discarded_total / events_consumed_total`）> 80% 时，应启动 Phase 2 优化。

### 11.4 Redis 数据结构

```text
session:route:{sessionKey}     → SET [ connID1, connID2, ... ]
conn:sessions:{connID}         → SET [ sessionKey1, sessionKey2, ... ]
resume:{resumeToken}           → HASH { userId, sessions, createdAt }  TTL 5min
user:sessions:{userID}         → SORTED SET { sessionKey → lastActivityMs }
```

### 11.5 接口实现

```go
// internal/session/router.go

type RedisSessionRouter struct {
    redis     *redis.Client
    gatewayID string
}

func (r *RedisSessionRouter) Register(sessionKey, userID, connID string) error {
    pipe := r.redis.Pipeline()
    pipe.SAdd(ctx, "session:route:"+sessionKey, connID)
    pipe.SAdd(ctx, "conn:sessions:"+connID, sessionKey)
    pipe.ZAdd(ctx, "user:sessions:"+userID, redis.Z{
        Score: float64(time.Now().UnixMilli()), Member: sessionKey,
    })
    _, err := pipe.Exec(ctx)
    return err
}

func (r *RedisSessionRouter) LookupConns(sessionKey string) []string {
    return r.redis.SMembers(ctx, "session:route:"+sessionKey).Val()
}
```

---

## 十二、Worker Registry

### 12.1 职责与定位

Worker Registry 是 Gateway 的 **Runtime Worker 服务发现与健康管理中心**。它维护所有 Agent Runtime Worker 的实时状态，为 Dispatcher 提供"哪些 Worker 可用、负载如何"的决策依据。

```text
Worker Registry 在 Gateway 中的位置:

  ┌───────────────────────────────────────────────────────────────────┐
  │  Gateway                                                          │
  │                                                                   │
  │  Dispatcher (§7)                                                  │
  │    │                                                              │
  │    │  "给我一个健康的 Worker"                                     │
  │    ▼                                                              │
  │  ┌──────────────────────────────────────────────────────────┐    │
  │  │  Worker Registry                                         │    │
  │  │                                                          │    │
  │  │  ┌───────────────────────────────────────────────────┐  │    │
  │  │  │  Worker 表 (内存)                                  │  │    │
  │  │  │                                                    │  │    │
  │  │  │  worker-1: READY,  active=3/10, cpu=45%           │  │    │
  │  │  │  worker-2: READY,  active=8/10, cpu=78%           │  │    │
  │  │  │  worker-3: DRAIN,  active=2/10, cpu=20%  ← 排空中│  │    │
  │  │  │  worker-4: DEAD,   active=0/0,  3次无响应 ← 已摘除│  │    │
  │  │  └───────────────────────────────────────────────────┘  │    │
  │  │       ▲                    ▲                   ▲         │    │
  │  │       │ gRPC GetStatus    │ Dispatcher 反馈   │ Redis    │    │
  │  │       │ (主动轮询)        │ (被动标记)        │ (静态配置│    │
  │  │       │                   │                   │  或注册) │    │
  │  └───────┼───────────────────┼───────────────────┼─────────┘    │
  │          │                   │                   │               │
  └──────────┼───────────────────┼───────────────────┼───────────────┘
             │                   │                   │
             ▼                   │                   │
  ┌──────────────┐               │         ┌─────────────┐
  │  Worker-1    │               │         │   Redis     │
  │  (Python)    │               │         │  worker:*   │
  ├──────────────┤               │         └─────────────┘
  │  Worker-2    │               │
  │  (Python)    │  Dispatcher 调用失败时
  ├──────────────┤  MarkUnhealthy(workerID)
  │  Worker-N    │
  │  (Python)    │
  └──────────────┘
```

**Worker Registry 的 5 项职责**：

| # | 职责 | 说明 |
| --- | --- | --- |
| ① | **Worker 发现** | 从配置或 Redis 获取 Worker 列表，建立 gRPC 连接 |
| ② | **健康检查** | 定期 gRPC 轮询 Worker 状态（GetStatus），检测宕机/过载 |
| ③ | **负载感知** | 收集每个 Worker 的 ActiveTasks、CPU、内存，供 Dispatcher 调度决策 |
| ④ | **状态管理** | 维护 Worker 状态机（READY → DRAINING → DEAD），自动摘除/恢复 |
| ⑤ | **事件通知** | Worker 状态变化时通知 Dispatcher 清除亲和记录（§7.8 OnWorkerDown） |

**与其他模块的关系**：

| 关系 | 目标 | 说明 |
| --- | --- | --- |
| 被依赖 | `Dispatcher` | `ReadyWorkers()` 提供调度候选列表 |
| 被调用 | `Dispatcher` | `MarkUnhealthy(workerID)` gRPC 失败时反馈 |
| 通知 | `Dispatcher.AffinityManager` | Worker 下线时触发 `OnWorkerDown` 清除亲和 |
| 依赖 | `gRPC Pool` | 通过 gRPC 调用 Worker 的 `GetStatus` RPC |
| 依赖 | Redis (可选) | 静态 Worker 列表配置，或动态注册发现 |

### 12.2 核心结构

```go
// internal/registry/registry.go

type Registry struct {
    mu      sync.RWMutex
    workers map[string]*WorkerEntry // workerID → WorkerEntry

    // ── 依赖 ──
    pool       *grpcpool.Pool       // gRPC 连接池
    dispatcher *dispatch.Dispatcher // Worker 下线时通知 Dispatcher
    config     RegistryConfig
    metrics    *RegistryMetrics
}

// WorkerEntry 是 Registry 内部管理的 Worker 状态 (比接口暴露的 WorkerInfo 更丰富)
type WorkerEntry struct {
    // ── 基本信息 (发现时填充, 不变) ──
    ID   string   // Worker 唯一标识
    Addr string   // gRPC 地址 host:port
    Tags []string // 能力标签: ["code-exec", "web-browse", ...]

    // ── 实时状态 (健康检查更新) ──
    State       WorkerState // READY / DRAINING / STARTING / DEAD
    ActiveTasks int         // 当前执行中的任务数
    MaxTasks    int         // 最大并发任务数
    QueuedTasks int         // 本地排队中的任务数

    // ── 资源信息 (健康检查更新) ──
    CPUUsage    float64 // CPU 使用率 (0-100)
    MemoryUsage float64 // 内存使用率 (0-100)
    SandboxIdle int     // 空闲沙箱数
    SandboxInUse int    // 使用中沙箱数

    // ── 健康检查状态 ──
    LastChecked      time.Time // 最后一次成功检查时间
    ConsecutiveFails int       // 连续失败次数
    Version          string    // Worker 代码版本

    // ── gRPC 客户端 ──
    StatusClient workerv1.WorkerServiceClient
    AgentClient  agentv1.AgentServiceClient
}

type WorkerState int

const (
    WorkerStateReady    WorkerState = iota // 正常, 可接收任务
    WorkerStateDraining                     // 排空中, 不接新任务
    WorkerStateStarting                     // 启动中, 尚未就绪
    WorkerStateDead                         // 已宕机, 连续 3 次无响应
)

type RegistryConfig struct {
    // Worker 发现方式
    StaticWorkers []string      // 静态配置: ["worker-1:50051", "worker-2:50051"]
    RedisKey      string        // Redis 动态发现 key (可选)

    // 健康检查
    CheckInterval      time.Duration // 正常轮询间隔 (默认 5s)
    FastCheckInterval  time.Duration // 异常时快速轮询 (默认 1s)
    CheckTimeout       time.Duration // 单次 GetStatus 超时 (默认 3s)
    DeadThreshold      int           // 连续失败几次标记 DEAD (默认 3)
    RecoveryThreshold  int           // DEAD 后连续成功几次恢复 READY (默认 2)

    // 负载阈值
    OverloadCPU     float64 // CPU 超过此值标记为高负载 (默认 85%)
    OverloadMemory  float64 // 内存超过此值标记为高负载 (默认 90%)
}
```

### 12.3 Worker 状态机

```text
                        ┌─────────────────────────────────┐
                        │                                 │
                 发现/注册                          心跳恢复
                        │                          (连续 2 次成功)
                        ▼                                 │
                  ┌────────────┐                          │
   ┌─────────────│  STARTING  │                          │
   │              └─────┬──────┘                          │
   │                    │ 首次 GetStatus 成功              │
   │                    ▼                                 │
   │              ┌────────────┐    Drain RPC         ┌───┴────────┐
   │              │   READY    │ ──────────────────▶  │  DRAINING  │
   │              └─────┬──────┘                      └─────┬──────┘
   │                    │                                   │
   │          连续 3 次 GetStatus 失败                 所有任务完成
   │          或 Dispatcher 标记不健康                  或 Drain 超时
   │                    │                                   │
   │                    ▼                                   ▼
   │              ┌────────────┐                     ┌────────────┐
   │              │    DEAD    │◀────────────────────│  REMOVED   │
   │              └─────┬──────┘                     └────────────┘
   │                    │
   │         快速轮询 (1s) 持续检测
   │         连续 2 次成功 → 恢复
   │                    │
   └────────────────────┘  (回到 STARTING → READY)

  状态转换触发的副作用:

  READY → DEAD:
    ① Dispatcher.AffinityManager.OnWorkerDown(workerID) — 清除亲和记录
    ② 进行中的任务: Event Bus 收不到事件 → 客户端超时 → 自动重试
    ③ metrics: WorkerDownTotal.Inc()

  READY → DRAINING:
    ① ReadyWorkers() 不再返回该 Worker
    ② 已有亲和记录保留 (任务仍在执行中)
    ③ 等待所有任务完成后 → REMOVED

  DEAD → READY:
    ① ReadyWorkers() 重新包含该 Worker
    ② metrics: WorkerRecoveredTotal.Inc()
    ③ slog.Info("worker recovered")
```

### 12.4 健康检查循环

```go
// internal/registry/health.go

func (r *Registry) healthCheckLoop(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }

        r.mu.RLock()
        workers := make([]*WorkerEntry, 0, len(r.workers))
        for _, w := range r.workers {
            workers = append(workers, w)
        }
        r.mu.RUnlock()

        // 并行检查所有 Worker
        var wg sync.WaitGroup
        for _, w := range workers {
            wg.Add(1)
            go func(w *WorkerEntry) {
                defer wg.Done()
                r.checkWorker(ctx, w)
            }(w)
        }
        wg.Wait()

        // 根据整体健康状况决定下次检查间隔
        interval := r.config.CheckInterval // 正常 5s
        if r.hasUnhealthyWorkers() {
            interval = r.config.FastCheckInterval // 快速 1s
        }
        time.Sleep(interval)
    }
}

func (r *Registry) checkWorker(ctx context.Context, w *WorkerEntry) {
    callCtx, cancel := context.WithTimeout(ctx, r.config.CheckTimeout)
    defer cancel()

    start := time.Now()
    resp, err := w.StatusClient.GetStatus(callCtx, &workerv1.GetStatusRequest{})
    duration := time.Since(start)

    r.metrics.HealthCheckDuration.WithLabelValues(w.ID).Observe(duration.Seconds())

    if err != nil {
        r.onCheckFailed(w, err)
        return
    }

    r.onCheckSuccess(w, resp)
}

func (r *Registry) onCheckFailed(w *WorkerEntry, err error) {
    r.mu.Lock()
    defer r.mu.Unlock()

    w.ConsecutiveFails++
    r.metrics.HealthCheckFailTotal.WithLabelValues(w.ID).Inc()

    slog.Warn("worker health check failed",
        "workerId", w.ID, "addr", w.Addr,
        "consecutiveFails", w.ConsecutiveFails, "err", err)

    if w.ConsecutiveFails >= r.config.DeadThreshold && w.State == WorkerStateReady {
        oldState := w.State
            w.State = WorkerStateDead

        slog.Error("worker marked DEAD",
            "workerId", w.ID, "addr", w.Addr,
            "previousState", oldState)

        r.metrics.WorkerDownTotal.Inc()

        // 通知 Dispatcher 清除指向该 Worker 的亲和记录
        go r.dispatcher.Affinity().OnWorkerDown(w.ID)
    }
}

func (r *Registry) onCheckSuccess(w *WorkerEntry, resp *workerv1.GetStatusResponse) {
    r.mu.Lock()
    defer r.mu.Unlock()

    previousState := w.State

    // 更新实时状态
    w.ActiveTasks = int(resp.ActiveTasks)
    w.MaxTasks = int(resp.MaxTasks)
    w.QueuedTasks = int(resp.QueuedTasks)
    w.CPUUsage = float64(resp.CpuUsagePercent)
    w.MemoryUsage = float64(resp.MemoryUsagePercent)
    w.SandboxIdle = int(resp.SandboxPoolIdle)
    w.SandboxInUse = int(resp.SandboxPoolInUse)
    w.Version = resp.Version
    w.LastChecked = time.Now()

    // 状态转换
    newState := mapWorkerState(resp.State)
    if previousState == WorkerStateDead {
        // DEAD → 恢复: 需要连续成功 N 次
        w.ConsecutiveFails-- // 复用字段做反向计数
        if w.ConsecutiveFails <= -r.config.RecoveryThreshold {
            w.State = WorkerStateReady
            w.ConsecutiveFails = 0
            slog.Info("worker recovered from DEAD",
                "workerId", w.ID, "addr", w.Addr)
            r.metrics.WorkerRecoveredTotal.Inc()
        }
        // 恢复中: 仍为 DEAD, 等待下次成功
    } else {
        w.State = newState
        w.ConsecutiveFails = 0
    }

    // 上报 metrics
    r.metrics.WorkerActiveTasks.WithLabelValues(w.ID).Set(float64(w.ActiveTasks))
    r.metrics.WorkerCPU.WithLabelValues(w.ID).Set(w.CPUUsage)
    r.metrics.WorkerMemory.WithLabelValues(w.ID).Set(w.MemoryUsage)
}
```

### 12.5 Worker 发现方式

Registry 支持两种 Worker 发现方式，可根据部署阶段选择：

```text
Phase 1 — 静态配置 (最简单)
───────────────────────────────────────
  环境变量或配置文件中列出所有 Worker 地址:

  WORKER_ADDRS=worker-1:50051,worker-2:50051,worker-3:50051

  Gateway 启动时读取, 为每个地址建立 gRPC 连接。
  Worker 扩缩容需要重启 Gateway 或热更新配置。

Phase 2 — Redis 动态注册 (推荐)
───────────────────────────────────────
  Worker 启动时将自己注册到 Redis, 定期续期:

  Worker 启动 → HSET worker:registry {workerID} {addr,tags,maxTasks,...}
              → EXPIRE worker:registry:{workerID} 30s
  Worker 每 10s → EXPIRE worker:registry:{workerID} 30s  (续期)
  Worker 停止 → HDEL worker:registry {workerID}

  Gateway 每 10s → HGETALL worker:registry
                 → 对比本地列表, 增删 Worker

  优点: Worker 扩缩容自动感知, 无需重启 Gateway

Phase 3+ — K8s Service Discovery (未来)
───────────────────────────────────────
  利用 K8s Endpoints API 或 gRPC 服务发现 (如 dns:///...)
  完全自动化, 与 K8s HPA 联动
```

```go
// internal/registry/discovery.go

// 静态发现
func (r *Registry) discoverStatic(addrs []string) {
    for _, addr := range addrs {
        workerID := addrToWorkerID(addr)
        if _, exists := r.workers[workerID]; exists {
            continue
        }
        r.addWorker(workerID, addr)
    }
}

// Redis 动态发现 (Phase 2)
func (r *Registry) discoverFromRedis(ctx context.Context) {
    entries, err := r.redis.HGetAll(ctx, "worker:registry").Result()
    if err != nil {
        slog.Warn("redis worker discovery failed", "err", err)
        return
    }

    // 新增: Redis 中有但本地没有的 Worker
    for workerID, infoJSON := range entries {
        if _, exists := r.workers[workerID]; !exists {
            var info WorkerRegistration
            json.Unmarshal([]byte(infoJSON), &info)
            r.addWorker(workerID, info.Addr)
            slog.Info("discovered new worker", "workerId", workerID, "addr", info.Addr)
        }
    }

    // 移除: 本地有但 Redis 中已消失的 Worker (TTL 过期)
    for workerID, w := range r.workers {
        if _, exists := entries[workerID]; !exists && w.State != WorkerStateDead {
            slog.Info("worker disappeared from registry", "workerId", workerID)
            w.State = WorkerStateDead
            go r.dispatcher.Affinity().OnWorkerDown(workerID)
        }
    }
}

func (r *Registry) addWorker(workerID, addr string) {
    conn := r.pool.GetOrCreate(addr) // gRPC 连接池
    entry := &WorkerEntry{
        ID:           workerID,
        Addr:         addr,
        State:        WorkerStateStarting,
        StatusClient: workerv1.NewWorkerServiceClient(conn),
        AgentClient:  agentv1.NewAgentServiceClient(conn),
    }
    r.mu.Lock()
    r.workers[workerID] = entry
    r.mu.Unlock()

    r.metrics.WorkersTotalGauge.Inc()
    slog.Info("worker added to registry", "workerId", workerID, "addr", addr)
}
```

### 12.6 ReadyWorkers — 调度候选列表

Dispatcher 调用 `ReadyWorkers()` 获取可用 Worker 列表。此方法需要高频调用（每次 Submit 都会调用），因此必须高效：

```go
// internal/registry/registry.go

// ReadyWorkers 返回所有健康且可接受新任务的 Worker
// 调用频率: 每次 Dispatcher.Submit 调用一次 (可达几百/秒)
func (r *Registry) ReadyWorkers() []*WorkerInfo {
    r.mu.RLock()
    defer r.mu.RUnlock()

    result := make([]*WorkerInfo, 0, len(r.workers))
    for _, w := range r.workers {
        if w.State != WorkerStateReady {
            continue // DRAINING, STARTING, DEAD 都排除
        }
        if w.ActiveTasks >= w.MaxTasks {
            continue // 已满载
        }
        if w.CPUUsage > r.config.OverloadCPU || w.MemoryUsage > r.config.OverloadMemory {
            continue // 资源过载
        }

        result = append(result, w.ToWorkerInfo())
    }
    return result
}

// HasCapacity 判断 Worker 是否有余量接受新任务
// 供 Dispatcher 亲和路由时快速判断
func (r *Registry) HasCapacity(workerID string) bool {
    r.mu.RLock()
    defer r.mu.RUnlock()

    w, ok := r.workers[workerID]
    if !ok {
        return false
    }
    return w.State == WorkerStateReady &&
        w.ActiveTasks < w.MaxTasks &&
        w.CPUUsage <= r.config.OverloadCPU
}

// MarkUnhealthy 由 Dispatcher 在 gRPC 调用失败时调用
// 比健康检查更快地感知 Worker 异常 (即时 vs 5s 轮询)
func (r *Registry) MarkUnhealthy(workerID string) {
    r.mu.Lock()
    defer r.mu.Unlock()

    w, ok := r.workers[workerID]
    if !ok {
        return
    }

    w.ConsecutiveFails++
    if w.ConsecutiveFails >= r.config.DeadThreshold {
        w.State = WorkerStateDead
        r.metrics.WorkerDownTotal.Inc()
        go r.dispatcher.Affinity().OnWorkerDown(workerID)
        slog.Warn("worker marked DEAD by dispatcher feedback",
            "workerId", workerID, "addr", w.Addr)
    }
}
```

### 12.7 Drain — Worker 优雅排空

当需要下线一个 Worker（更新代码、节点维护）时，通过 Drain 让 Worker 完成现有任务后平滑退出：

```go
// internal/registry/drain.go

// DrainWorker 排空指定 Worker
// 1. 标记为 DRAINING (Dispatcher 不再向其调度新任务)
// 2. 发送 gRPC Drain RPC (Worker 停止接受新任务)
// 3. 等待 Worker 上的任务全部完成
// 4. 标记为 REMOVED
func (r *Registry) DrainWorker(ctx context.Context, workerID string, timeoutSeconds int) error {
    r.mu.Lock()
    w, ok := r.workers[workerID]
    if !ok {
        r.mu.Unlock()
        return fmt.Errorf("worker %s not found", workerID)
    }
    w.State = WorkerStateDraining
    r.mu.Unlock()

    slog.Info("draining worker", "workerId", workerID, "timeout", timeoutSeconds)

    // 发送 Drain RPC
    callCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
    defer cancel()

    _, err := w.StatusClient.Drain(callCtx, &workerv1.DrainRequest{
        TimeoutSeconds: int32(timeoutSeconds),
    })
    if err != nil {
        slog.Warn("drain RPC failed, force removing", "workerId", workerID, "err", err)
    }

    // 等待任务完成 (轮询检查)
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-callCtx.Done():
            slog.Warn("drain timeout, force removing", "workerId", workerID)
            goto remove
        case <-ticker.C:
            resp, err := w.StatusClient.GetStatus(ctx, &workerv1.GetStatusRequest{})
            if err != nil || resp.ActiveTasks == 0 {
                goto remove
            }
            slog.Info("drain in progress",
                "workerId", workerID, "activeTasks", resp.ActiveTasks)
        }
    }

remove:
    r.mu.Lock()
    delete(r.workers, workerID)
    r.mu.Unlock()

    r.metrics.WorkersTotalGauge.Dec()
    go r.dispatcher.Affinity().OnWorkerDown(workerID)
    slog.Info("worker drained and removed", "workerId", workerID)
    return nil
}
```

### 12.8 监控指标

```go
type RegistryMetrics struct {
    // Worker 数量
    WorkersTotalGauge    prometheus.Gauge                 // 当前 Worker 总数
    WorkersReadyGauge    prometheus.Gauge                 // 当前 READY 的 Worker 数
    // Worker 状态变化
    WorkerDownTotal      prometheus.Counter               // Worker 标记 DEAD 次数
    WorkerRecoveredTotal prometheus.Counter               // Worker 从 DEAD 恢复次数
    // 健康检查
    HealthCheckDuration  *prometheus.HistogramVec         // 健康检查延迟 labels: workerID
    HealthCheckFailTotal *prometheus.CounterVec           // 健康检查失败次数 labels: workerID
    // Worker 资源 (每次健康检查更新)
    WorkerActiveTasks    *prometheus.GaugeVec             // 活跃任务数 labels: workerID
    WorkerCPU            *prometheus.GaugeVec             // CPU 使用率 labels: workerID
    WorkerMemory         *prometheus.GaugeVec             // 内存使用率 labels: workerID
    WorkerSandboxIdle    *prometheus.GaugeVec             // 空闲沙箱数 labels: workerID
}
```

| 告警规则 | 条件 | 级别 |
| --- | --- | --- |
| 可用 Worker 为 0 | `WorkersReadyGauge == 0` | **Critical** — 无法处理任何 Agent 任务 |
| Worker 宕机 | `WorkerDownTotal` 增加 | Warning |
| 所有 Worker 高负载 | 所有 Worker `ActiveTasks / MaxTasks > 0.8` | Warning — 需要扩容 |
| 健康检查延迟高 | `HealthCheckDuration P99 > 3s` | Warning — 网络问题 |
| Worker 长时间 DEAD | 某 Worker `State == DEAD` 持续 5min | Warning — 可能需要人工介入 |

---

## 十三、HTTP 服务

> **与 sahara-api 的职责边界**：
>
> Gateway 的 HTTP 端点**仅包含**与实时通信直接相关的接口：
> - `/ws` — WebSocket 升级入口
> - `/v1/chat/completions` — OpenAI 兼容 SSE API（需要访问 Dispatcher + Event Bus）
> - `/healthz` `/readyz` `/metrics` — 运维端点
>
> 所有 C 端业务 HTTP 接口（用户注册登录、会话 CRUD、文件上传、配额查询等）由独立的 **sahara-api** 服务承载。
> 两个服务通过 LB 路径路由分流：`/ws` + `/v1/*` → sahara-gw，`/api/*` → sahara-api。
> 两个服务共享 `pkg/auth`（JWT）、`pkg/store`（PG + Redis）、`pkg/model`（领域模型）。
>
> 详见 [API Service 设计](./API-SERVICE-DESIGN.md)。

### 13.1 路由表

```go
// internal/httpapi/server.go

func NewHTTPServer(deps *Dependencies) http.Handler {
    mux := http.NewServeMux()

    // 健康检查 (无认证)
    mux.HandleFunc("GET /healthz", handleHealthz)
    mux.HandleFunc("GET /readyz", handleReadyz(deps))

    // WebSocket 升级
    mux.Handle("GET /ws", deps.Upgrader)

    // OpenAI 兼容 API (JWT 认证)
    mux.Handle("POST /v1/chat/completions",
        authMiddleware(deps.Auth, handleChatCompletions(deps.Dispatcher)))

    // Prometheus 指标
    mux.Handle("GET /metrics", promhttp.Handler())

    return mux
}
```

### 13.2 Gateway 自身健康检测

Gateway 作为有状态的长连接服务，其健康检测比普通 HTTP 微服务复杂得多——不仅要检查进程是否存活，还要检查依赖链（Redis、gRPC Worker）是否可用，以及自身是否仍有能力接受新连接。

#### 两个端点、两种语义

```text
┌─────────────────────────────────────────────────────────────────────┐
│  /healthz — Liveness Probe (存活探针)                               │
│                                                                     │
│  回答的问题: "Gateway 进程是否还活着？"                             │
│  检查内容:   进程存活 (能响应 HTTP 即存活)                          │
│  失败后果:   K8s 杀掉 Pod 并重启                                    │
│  设计原则:   极简, 不检查依赖, 不能误判                             │
│                                                                     │
│  为什么不检查 Redis/Worker?                                         │
│  如果 Redis 挂了, /healthz 返回 503, K8s 会重启 Gateway。          │
│  但 Gateway 重启并不能修复 Redis 故障, 反而会断开所有 WS 连接,     │
│  让情况更糟。所以 Liveness 只检查进程本身。                         │
├─────────────────────────────────────────────────────────────────────┤
│  /readyz — Readiness Probe (就绪探针)                               │
│                                                                     │
│  回答的问题: "Gateway 是否准备好接受新连接？"                       │
│  检查内容:   进程 + 核心依赖 + 是否在排空                          │
│  失败后果:   K8s 从 Service/LB 摘除, 不再分配新连接                │
│              (已有连接不受影响)                                      │
│  设计原则:   全面检查所有依赖, 宁可误判"未就绪"                    │
└─────────────────────────────────────────────────────────────────────┘
```

#### /healthz 实现

```go
// internal/httpapi/health.go

// handleHealthz — Liveness Probe
// 极简检查: 能响应 HTTP 就是存活
func handleHealthz(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{
        "status": "alive",
    })
}
```

#### /readyz 实现

```go
// handleReadyz — Readiness Probe
// 全面检查: 进程 + 所有核心依赖 + 排空状态
func handleReadyz(deps *Dependencies) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        checks := runReadinessChecks(r.Context(), deps)

        w.Header().Set("Content-Type", "application/json")

        if !checks.Ready {
            w.WriteHeader(http.StatusServiceUnavailable) // 503
        } else {
            w.WriteHeader(http.StatusOK)
        }

        json.NewEncoder(w).Encode(checks)
    }
}

type ReadinessResult struct {
    Ready   bool                       `json:"ready"`
    Checks  map[string]*CheckResult    `json:"checks"`
    Message string                     `json:"message,omitempty"`
}

type CheckResult struct {
    OK       bool          `json:"ok"`
    Duration string        `json:"duration"` // 检查耗时
    Detail   string        `json:"detail,omitempty"`
}

func runReadinessChecks(ctx context.Context, deps *Dependencies) *ReadinessResult {
    result := &ReadinessResult{
        Ready:  true,
        Checks: make(map[string]*CheckResult),
    }

    // ── 检查 1: 是否在排空 (优雅关闭中) ──
    if deps.Draining.Load() {
        result.Ready = false
        result.Message = "draining"
        result.Checks["draining"] = &CheckResult{OK: false, Detail: "gateway is shutting down"}
        return result // 排空中直接返回, 不做后续检查
    }
    result.Checks["draining"] = &CheckResult{OK: true}

    // ── 检查 2: Redis 连通性 ──
    start := time.Now()
    err := deps.Redis.Ping(ctx).Err()
    result.Checks["redis"] = &CheckResult{
        OK:       err == nil,
        Duration: time.Since(start).String(),
        Detail:   errStr(err),
    }
    if err != nil {
        result.Ready = false
    }

    // ── 检查 3: 至少有一个健康的 Worker ──
    start = time.Now()
    readyWorkers := deps.Registry.ReadyWorkers()
    result.Checks["workers"] = &CheckResult{
        OK:       len(readyWorkers) > 0,
        Duration: time.Since(start).String(),
        Detail:   fmt.Sprintf("%d ready workers", len(readyWorkers)),
    }
    if len(readyWorkers) == 0 {
        result.Ready = false
    }

    // ── 检查 4: 连接数是否已满 ──
    connCount := deps.Hub.ConnCount()
    maxConns := deps.Config.WS.MaxConnsTotal
    capacityOK := connCount < maxConns
    result.Checks["capacity"] = &CheckResult{
        OK:     capacityOK,
        Detail: fmt.Sprintf("%d/%d connections", connCount, maxConns),
    }
    if !capacityOK {
        result.Ready = false
    }

    // ── 检查 5: Hub 主循环是否在运行 ──
    hubAlive := deps.Hub.IsRunning()
    result.Checks["hub"] = &CheckResult{
        OK:     hubAlive,
        Detail: boolStr(hubAlive, "running", "stopped"),
    }
    if !hubAlive {
        result.Ready = false
    }

    return result
}
```

**示例响应**：

```json
// 正常状态
// GET /readyz → 200 OK
{
  "ready": true,
  "checks": {
    "draining": { "ok": true },
    "redis":    { "ok": true, "duration": "0.3ms" },
    "workers":  { "ok": true, "duration": "0.01ms", "detail": "3 ready workers" },
    "capacity": { "ok": true, "detail": "8234/100000 connections" },
    "hub":      { "ok": true, "detail": "running" }
  }
}

// Redis 不可用
// GET /readyz → 503 Service Unavailable
{
  "ready": false,
  "checks": {
    "draining": { "ok": true },
    "redis":    { "ok": false, "duration": "3.001s", "detail": "dial tcp: connection refused" },
    "workers":  { "ok": true, "duration": "0.01ms", "detail": "3 ready workers" },
    "capacity": { "ok": true, "detail": "8234/100000 connections" },
    "hub":      { "ok": true, "detail": "running" }
  }
}

// 优雅关闭中
// GET /readyz → 503 Service Unavailable
{
  "ready": false,
  "message": "draining",
  "checks": {
    "draining": { "ok": false, "detail": "gateway is shutting down" }
  }
}
```

#### K8s 探针配置

```yaml
# k8s/deployment.yaml — 探针配置
containers:
  - name: sahara-gw
    ports:
      - containerPort: 8080
    livenessProbe:
      httpGet:
        path: /healthz
        port: 8080
      initialDelaySeconds: 5      # 启动后 5s 开始探测
      periodSeconds: 10            # 每 10s 探测一次
      failureThreshold: 3          # 连续 3 次失败才重启 (30s 容忍)
      timeoutSeconds: 2            # 单次超时 2s
      # 重要: Liveness 不能太敏感, 否则偶发 GC 暂停会误杀 Pod

    readinessProbe:
      httpGet:
        path: /readyz
        port: 8080
      initialDelaySeconds: 3      # 启动后 3s 开始探测
      periodSeconds: 5             # 每 5s 探测一次 (比 Liveness 更频繁)
      failureThreshold: 2          # 连续 2 次失败就摘除 (10s 摘除, 快)
      successThreshold: 1          # 1 次成功就恢复
      timeoutSeconds: 3            # 单次超时 3s (包含 Redis Ping)

    startupProbe:
      httpGet:
        path: /readyz
        port: 8080
      initialDelaySeconds: 0
      periodSeconds: 2             # 启动阶段每 2s 探测
      failureThreshold: 30         # 最多等 60s (2s × 30) 启动
      # 启动期间 Liveness 和 Readiness 不生效,
      # 避免 Gateway 加载配置/建立连接池时被误杀
```

#### 三种探针的协作时序

```text
Gateway Pod 生命周期:

  t=0s   Pod 创建, 容器启动
         │
         │  startupProbe 接管 (每 2s 探测 /readyz)
         │  Liveness 和 Readiness 暂停
         │
  t=0-5s Gateway 初始化:
         │  1. 加载配置
         │  2. 建立 Redis 连接池
         │  3. 建立 gRPC 连接池 (连接 Worker)
         │  4. 创建 Hub, Pipeline, Router
         │  5. 启动 Hub.Run() goroutine
         │  6. 启动 healthCheckLoop
         │  7. 启动 HTTP 监听
         │  /readyz → 503 (Worker 首次健康检查未完成)
         │
  t=5-8s  首次 Worker 健康检查完成, ReadyWorkers > 0
         │  /readyz → 200 ✓
         │  startupProbe 通过, 交还给 Liveness + Readiness
         │
  t=8s+  正常运行
         │  Liveness: /healthz 每 10s → 200 (进程存活)
         │  Readiness: /readyz 每 5s → 200 (依赖正常)
         │  LB 开始向此 Pod 分配新的 WS 连接
         │
  ──── 运行中 ────
         │
  t=N    收到 SIGTERM (K8s 滚动更新 / 缩容)
         │  draining = true
         │  /readyz → 503 (排空中)
         │  Readiness 失败 → K8s 从 LB 摘除 → 不再分配新连接
         │  /healthz → 200 (进程仍存活, 不要被 Liveness 杀掉)
         │
  t=N+5  LB 感知到 Pod 摘除, 停止分流
         │  发送 goodbye(silent:true) 给所有客户端
         │  等待任务完成
         │
  t=N+65 terminationGracePeriodSeconds 到达
         │  进程退出
```

#### 健康检测设计原则总结

| 原则 | 说明 | 反例 |
| --- | --- | --- |
| **Liveness 极简** | 只检查进程存活，不检查依赖 | ~~Liveness 检查 Redis~~ → Redis 挂导致所有 Pod 被杀重启，雪崩 |
| **Readiness 全面** | 检查所有核心依赖，宁可摘除 | ~~Readiness 不检查 Worker~~ → 无 Worker 可用仍接收连接，请求全失败 |
| **startupProbe 保护冷启动** | 初始化期间不受 Liveness 误杀 | ~~无 startupProbe~~ → 连接池建立慢时被 Liveness 杀掉重启循环 |
| **排空与 Readiness 联动** | draining → /readyz 503 → LB 摘除 | ~~直接关连接~~ → 新连接仍在进来，关不完 |
| **超时要宽容** | Liveness 30s 才重启，容忍 GC 暂停 | ~~failureThreshold=1~~ → 偶发 STW 暂停导致误杀 |
| **返回结构化 JSON** | 运维可精确定位哪个依赖故障 | ~~只返回 "ok"/"fail"~~ → 排查困难 |

### 13.3 OpenAI 兼容接口

```go
// internal/httpapi/openai_compat.go

func handleChatCompletions(dispatcher TaskDispatcher) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req OpenAIChatRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            writeError(w, 400, "invalid request body")
            return
        }

        // 转换为 Sahara 内部请求
        submitReq := convertToSubmitRequest(req)

        if req.Stream {
            // SSE 流式响应
            handleSSEStream(w, r, dispatcher, submitReq)
        } else {
            // 同步响应 (等待 final)
            handleSyncResponse(w, r, dispatcher, submitReq)
        }
    }
}
```

---

## 十四、优雅关闭与零感知部署

> Gateway 是有状态的长连接服务。每次重启都会断开所有 WS 连接。
> 对于 C 端用户而言，**页面频闪（断连→重连→UI 闪烁）是不可接受的**。
> 本章的目标：让用户**完全感知不到**服务重启——不闪、不跳、不丢事件。

### 14.1 设计目标

| # | 目标 | 可量化指标 |
| --- | --- | --- |
| G1 | 用户无感知 | 重启期间前端 UI 零闪烁、零提示 |
| G2 | 事件不丢失 | 断连期间的 Agent 事件全部通过 Event Bus 回放 |
| G3 | 任务不中断 | 正在执行的 Agent 任务不因 Gateway 重启而中止 |
| G4 | 快速恢复 | 客户端从断连到恢复 < 3 秒 |

### 14.2 关闭流程

```text
SIGTERM / SIGINT 收到
  │
  ▼
┌──────────────────────────────────────────────────────────────────┐
│ Phase 1: 排空准备                                                │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ 1. 标记自身为 draining 状态                         ~0ms    │ │
│ │ 2. 从 LB 健康检查摘除 (/readyz → 503)              ~0ms    │ │
│ │ 3. 等待 LB 感知并停止分流新连接                     ~5s     │ │
│ │    (K8s preStop hook: sleep 5)                              │ │
│ └──────────────────────────────────────────────────────────────┘ │
│                                                                  │
│ Phase 2: 连接排空                                                │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ 4. 停止接受新 WS 连接 (HTTP listener 关闭)          ~0ms    │ │
│ │ 5. 停止 Event Bus 消费 (不再接收新事件)             ~0ms    │ │
│ │ 6. 等待进行中的 Agent 任务完成                      ~0-60s  │ │
│ │    (Gateway 持有 runId→connId 映射，等 final res 返回)      │ │
│ │ 7. 向所有客户端发送 goodbye 事件                    ~100ms  │ │
│ │    { reason: "gateway_restart",                             │ │
│ │      reconnectAfterMs: 1000, silent: true }                 │ │
│ │ 8. 短暂等待客户端处理 goodbye                       ~500ms  │ │
│ │ 9. 关闭所有 WS 连接 (code 4100)                     ~100ms  │ │
│ └──────────────────────────────────────────────────────────────┘ │
│                                                                  │
│ Phase 3: 资源清理                                                │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ 10. 关闭 gRPC 连接池                                ~0ms    │ │
│ │ 11. 关闭 Redis 连接                                 ~0ms    │ │
│ │ 12. 进程退出                                                │ │
│ └──────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**关键改进**（对比原始方案）：

- **步骤 3**：新增 preStop 等待，确保 LB 先停止分流再关闭 listener，避免"连接打到正在关闭的 Pod"
- **步骤 6**：新增任务排空，正在执行的 Agent 任务等 final 返回后再断连，避免用户看到"任务莫名消失"
- **步骤 7**：goodbye 事件新增 `silent: true` 字段，提示客户端不要显示断连 UI

### 14.3 Go 实现

```go
// cmd/sahara-gw/main.go

func main() {
    ctx, stop := signal.NotifyContext(context.Background(),
        os.Interrupt, syscall.SIGTERM)
    defer stop()

    gw := gateway.New(cfg)
    gw.Start()

    <-ctx.Done()
    slog.Info("shutting down, entering drain mode...")

    shutCtx, cancel := context.WithTimeout(context.Background(),
        cfg.Shutdown.GracePeriod) // 默认 120s
    defer cancel()
    gw.Shutdown(shutCtx)
}

func (gw *Gateway) Shutdown(ctx context.Context) {
    // Phase 1: 标记 draining，让 /readyz 返回 503
    gw.draining.Store(true)
    // preStop hook 的 sleep 在 K8s 侧处理，这里无需等待

    // Phase 2: 连接排空
    // 2a. 停止接受新连接
    gw.httpServer.Shutdown(ctx)

    // 2b. 停止消费新事件
    gw.broadcaster.Stop()

    // 2c. 等待进行中的 Agent 任务完成
    //     activeRuns 记录了每个连接上尚未收到 final res 的 runId
    gw.waitActiveRuns(ctx)

    // 2d. 通知所有客户端（静默模式）
    gw.hub.BroadcastAll(EventFrame{
        Event: "goodbye",
        Payload: map[string]any{
            "reason":           "gateway_restart",
            "reconnectAfterMs": 1000,
            "silent":           true,
        },
    })

    // 2e. 给客户端一点时间处理 goodbye
    time.Sleep(500 * time.Millisecond)

    // 2f. 关闭所有连接
    gw.hub.CloseAll(4100, "server restart")

    // Phase 3: 资源清理
    gw.grpcPool.Close()
    gw.redis.Close()

    slog.Info("shutdown complete")
}

// waitActiveRuns 等待所有进行中的任务完成
func (gw *Gateway) waitActiveRuns(ctx context.Context) {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()

    for {
        active := gw.hub.ActiveRunCount()
        if active == 0 {
            slog.Info("all active runs completed")
            return
        }
        select {
        case <-ctx.Done():
            slog.Warn("drain timeout, forcing shutdown",
                "activeRuns", active)
            return
        case <-ticker.C:
            slog.Info("waiting for active runs",
                "remaining", active)
        }
    }
}
```

### 14.4 K8s 滚动更新配置

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sahara-gw
spec:
  replicas: 2                           # 至少 2 个实例
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0                  # 绝不同时杀两个 Pod
      maxSurge: 1                        # 先起新 Pod 再杀旧 Pod
  template:
    spec:
      terminationGracePeriodSeconds: 120 # 给 2 分钟排空
      containers:
      - name: sahara-gw
        readinessProbe:
          httpGet:
            path: /readyz               # draining 时返回 503
            port: 8080
          periodSeconds: 3
          failureThreshold: 1            # 一次失败即摘除
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          periodSeconds: 10
          failureThreshold: 3
        lifecycle:
          preStop:
            exec:
              command: ["sh", "-c", "sleep 5"]  # 等 LB 摘流
```

**滚动更新时间线**：

```text
t=0s    K8s 创建新 Pod (v2)
t=3-6s  新 Pod readinessProbe 通过，LB 开始分流新连接到 v2
t=6s    K8s 向旧 Pod (v1) 发 SIGTERM
t=6s    旧 Pod preStop: sleep 5 (等 LB 感知)
t=11s   旧 Pod 开始 Shutdown:
        - /readyz → 503
        - 停止 accept
        - 等待 active runs 完成
t=11-70s 进行中的 Agent 任务陆续完成
t=70s   所有任务完成 → 发 goodbye (silent:true) → 关闭连接
t=70s+  客户端收到 goodbye → 静默重连到新 Pod (v2)
t=71-73s 客户端恢复，Event Bus 回放错过的事件

用户视角: 完全没有感知到服务重启
```

### 14.5 客户端静默重连协议

goodbye 事件中的 `silent: true` 字段告诉客户端**不要显示任何断连 UI**：

```text
┌─────────────────────────────────────────────────────────────────────┐
│  静默重连 vs 普通重连                                                │
│                                                                     │
│  场景 A: 服务端主动关闭 (goodbye + silent:true)                     │
│  ─────────────────────────────────────────────────                   │
│  • 不显示 "连接已断开" 提示                                         │
│  • 不显示 "正在重连..." loading                                     │
│  • 不显示任何 UI 变化                                               │
│  • 后台静默重连，重连成功后无缝衔接                                 │
│  • 如果 3 秒内重连成功 → 用户完全无感                               │
│  • 如果 3 秒后仍未重连 → 降级为普通重连 (显示提示)                  │
│                                                                     │
│  场景 B: 网络断连 (无 goodbye)                                      │
│  ─────────────────────────────────────────                           │
│  • 等待 2×tickInterval (60s) 确认连接死亡                           │
│  • 显示 "网络连接不稳定" 提示                                       │
│  • 显示重连进度                                                     │
│  • 重连成功后回放事件并显示 "已恢复连接"                            │
│                                                                     │
│  区别关键: goodbye 事件是 "预告"，客户端有准备时间；                │
│           网络断连是 "意外"，需要通知用户。                          │
└─────────────────────────────────────────────────────────────────────┘
```

**完整帧交互示例**——以下是一次静默重连过程中，Gateway 和客户端之间实际传输的每一帧 JSON：

**① 旧 Gateway (v1) 发送 goodbye**：

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

**② WS 连接断开**（code 4100，客户端因 `silent: true` 不显示任何 UI）

**③ 客户端等待 1 秒后重连到新 Gateway (v2)**：

```text
GET /ws?resumeToken=rt_01JKXYZ...&lastSeqs=sess_abc:42
Authorization: Bearer eyJhbGciOi...
X-Client-Id: sahara-web
X-Client-Version: 1.0.0
```

**④ 新 Gateway (v2) 发送 resumed welcome**：

```json
{
  "type": "event",
  "event": "welcome",
  "payload": {
    "protocol": 1,
    "connId": "gw2_conn_03AB...",
    "userId": "user_abc",
    "serverVersion": "0.2.0",
    "resumed": true,
    "resumeToken": "rt_02NEWXYZ...",
    "replayingSessions": ["sess_abc"],
    "missedEvents": 3,
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

**⑤ 新 Gateway 回放断线期间错过的事件**（按 seq 顺序）：

```json
{
  "type": "event",
  "event": "agent.delta",
  "sessionKey": "sess_abc",
  "runId": "run_01JK...",
  "seq": 43,
  "ts": 1706000002000,
  "payload": {
    "text": "实现了快速排序",
    "stream": "assistant"
  }
}
```

```json
{
  "type": "event",
  "event": "agent.tool_start",
  "sessionKey": "sess_abc",
  "runId": "run_01JK...",
  "seq": 44,
  "ts": 1706000003000,
  "payload": {
    "toolCallId": "call_01JK...",
    "toolName": "write",
    "input": { "path": "quicksort.py", "content": "..." }
  }
}
```

```json
{
  "type": "event",
  "event": "agent.run_complete",
  "sessionKey": "sess_abc",
  "runId": "run_01JK...",
  "seq": 45,
  "ts": 1706000005000,
  "payload": {
    "finalText": "我已经帮你实现了快速排序算法...",
    "content": [
      { "type": "text", "text": "我已经帮你实现了快速排序算法..." },
      { "type": "file", "url": "https://files.sahara.com/runs/run_01JK/quicksort.py",
        "filename": "quicksort.py", "mimeType": "text/x-python" }
    ],
    "iterations": 2,
    "toolCalls": 1,
    "durationMs": 8500,
    "tokensUsed": 1800
  }
}
```

**⑥ 回放完毕，发送 replay.complete**：

```json
{
  "type": "event",
  "event": "replay.complete",
  "payload": {
    "sessionsReplayed": ["sess_abc"],
    "eventsReplayed": 3
  }
}
```

**⑦ 切换到实时模式，后续事件正常推送（静默重连完成，用户全程无感）**

---

客户端 SDK 伪代码：

```text
state:
    silentMode = false
    silentTimer = null

onGoodbye(event):
    if event.payload.silent:
        silentMode = true
        // 立即开始重连，不等断连
        scheduleReconnect(delay = event.payload.reconnectAfterMs)
        // 设置静默超时: 超过 3 秒未恢复则降级
        silentTimer = setTimeout(3000, () => {
            silentMode = false
            showReconnectingUI()
        })

onDisconnect(code, reason):
    if silentMode:
        // 服务端主动关闭，不显示任何 UI
        return
    else:
        // 意外断连
        showDisconnectedUI()
        startReconnectWithBackoff()

onReconnected(welcomeEvent):
    clearTimeout(silentTimer)
    if silentMode:
        silentMode = false
        // 完全静默，不做任何 UI 提示
    else:
        showToast("已恢复连接")

    // 通用: 更新 resumeToken，等待事件回放
    resumeToken = welcomeEvent.payload.resumeToken
```

### 14.6 Agent 任务排空策略

Gateway 重启时正在执行的 Agent 任务需要特殊处理，因为任务在 Runtime 侧运行，不会因 Gateway 重启而中止：

```text
┌─────────────────────────────────────────────────────────────────────┐
│  任务排空决策树                                                      │
│                                                                     │
│  Gateway 收到 SIGTERM                                               │
│       │                                                             │
│       ▼                                                             │
│  检查 activeRuns (该 Gateway 上有哪些任务还没收到 final)             │
│       │                                                             │
│       ├── activeRuns == 0                                           │
│       │   → 直接进入 goodbye + 关闭                                 │
│       │                                                             │
│       ├── activeRuns > 0 && 排空时间充裕 (< drainTimeout)           │
│       │   → 继续消费 Event Bus，等待 final res                      │
│       │   → final 返回后逐个发送 goodbye 给对应连接                 │
│       │   → 所有任务完成后统一关闭剩余空闲连接                      │
│       │                                                             │
│       └── activeRuns > 0 && 排空超时                                │
│           → 给剩余连接发 goodbye (silent:true)                      │
│           → 关闭连接                                                │
│           → 客户端重连到新 Pod 后:                                  │
│             Runtime 的事件仍在 Event Bus 中                         │
│             新 Pod 消费并推送，客户端通过 resumeToken 接上           │
│                                                                     │
│  关键: Runtime 不感知 Gateway 重启。任务继续执行，事件继续发射。    │
│       Gateway 重启只影响 "事件推送给谁" 而非 "任务是否继续"。       │
└─────────────────────────────────────────────────────────────────────┘
```

### 14.7 分阶段演进

| 阶段 | 部署策略 | 用户感知 | 实现成本 |
| --- | --- | --- | --- |
| **Phase 1** | K8s 滚动更新 + goodbye + resumeToken + 自动重连 | 1-3 秒重连提示 | 低 |
| **Phase 2** | + 客户端静默重连 + 任务排空 + preStop hook | **几乎无感** | 中 |
| **Phase 3** | + `tableflip` 热重启（空闲连接零断连） | 完全无感 | 高 |

**Phase 3 热重启说明**（按需评估）：

Go 的 `cloudflare/tableflip` 库可以在新旧进程间传递 listener fd，实现：
- 新连接直接由新进程处理
- 旧连接由旧进程继续服务直到空闲
- 空闲连接通过 goodbye + 静默重连迁移到新进程

```text
旧进程 (v1)                    新进程 (v2)
  │                               │
  │  fork + exec (传递 listener fd)
  │──────────────────────────────▶│
  │                               │ 接受新连接
  │  继续服务旧连接               │
  │  旧连接任务完成后:            │
  │  goodbye → 客户端重连到 v2    │
  └───────── exit ────────────────│
```

> Phase 3 的优先级取决于发版频率和在线用户量。如果 Phase 2 的静默重连已经让用户无感，热重启可以不做。

### 14.8 /readyz 端点实现

优雅关闭的核心在于：`draining = true` → `/readyz` 返回 `503` → K8s 从 LB 摘除 Pod。

`/readyz` 端点的完整实现（包含 5 项检查、结构化 JSON 响应、K8s 探针配置）详见 **§13.2 Gateway 自身健康检测**。

关键联动逻辑：

```go
// 优雅关闭触发时:
deps.Draining.Store(true)   // ← 这一行让 /readyz 返回 503
// K8s Readiness Probe 检测到 503 → 从 Service/LB 中摘除 Pod
// 此后不再分配新连接, 已有连接继续服务直到排空
```

---

## 十五、配置管理

### 15.1 配置结构

```go
// internal/config/config.go

type Config struct {
    // 服务
    GatewayID  string `env:"GATEWAY_ID" default:"gw-1"`
    ListenAddr string `env:"LISTEN_ADDR" default:":8080"`

    // WebSocket
    WS struct {
        MaxConnsPerUser int           `env:"WS_MAX_CONNS_PER_USER" default:"5"`
        MaxFrameBytes   int           `env:"WS_MAX_FRAME_BYTES" default:"1048576"`
        WriteBufferSize int           `env:"WS_WRITE_BUFFER_SIZE" default:"256"`
        TickInterval    time.Duration `env:"WS_TICK_INTERVAL" default:"30s"`
        IdleTimeout     time.Duration `env:"WS_IDLE_TIMEOUT" default:"10m"`
    }

    // Runtime Workers
    Runtime struct {
        Addrs          []string      `env:"RUNTIME_ADDRS" default:"localhost:50051"`
        HealthInterval time.Duration `env:"RUNTIME_HEALTH_INTERVAL" default:"5s"`
    }

    // Redis
    Redis struct {
        URL string `env:"REDIS_URL" default:"redis://localhost:6379"`
    }

    // JWT
    JWT struct {
        PublicKeyPath string `env:"JWT_PUBLIC_KEY_PATH" default:"/etc/sahara/jwt-public.pem"`
        Issuer        string `env:"JWT_ISSUER" default:"sahara"`
        Audience      string `env:"JWT_AUDIENCE" default:"sahara-gateway"`
    }

    // Rate Limiting
    RateLimit struct {
        ConnFramesPerSec   int `env:"RL_CONN_FPS" default:"10"`
        UserSubmitsPerMin  int `env:"RL_USER_SUBMIT_PM" default:"20"`
        GlobalSubmitsPerSec int `env:"RL_GLOBAL_SUBMIT_PS" default:"100"`
    }

    // Broadcast
    Broadcast struct {
        AggregateWindowMs int `env:"BROADCAST_AGG_WINDOW_MS" default:"150"`
    }
}
```

### 15.2 加载优先级

```text
1. 环境变量 (最高优先级)
2. .env 文件 (开发环境)
3. 默认值 (兜底)
```

---

## 十六、可观测性

### 16.1 Prometheus 指标

```go
// internal/observe/metrics.go

type Metrics struct {
    // 连接
    ConnectionsGauge   prometheus.Gauge     // 当前连接数
    ConnectionsTotal   prometheus.Counter   // 累计连接数
    DisconnectsTotal   *prometheus.CounterVec // 断连数 (by reason)

    // 帧
    FramesReceivedTotal *prometheus.CounterVec // 收到帧数 (by method)
    FramesSentTotal     *prometheus.CounterVec // 发送帧数 (by event)

    // RPC
    RPCDuration    *prometheus.HistogramVec // RPC 延迟 (by method)
    RPCErrorsTotal *prometheus.CounterVec   // RPC 错误数 (by method, code)

    // 调度
    DispatchDuration    *prometheus.HistogramVec // gRPC 调度延迟 (by worker)
    DispatchRetriesTotal *prometheus.CounterVec  // 调度重试次数
    WorkersGauge        *prometheus.GaugeVec     // Worker 状态 (by state)

    // 事件广播
    EventsConsumedTotal *prometheus.CounterVec // 消费事件数 (by type)
    EventsPushedTotal   *prometheus.CounterVec // 推送事件数 (by type)
    AggregateFlushTotal prometheus.Counter     // 聚合刷新次数

    // 限频
    RateLimitedTotal *prometheus.CounterVec // 被限频次数 (by layer)

    // 慢客户端
    SlowClientsTotal prometheus.Counter // 慢客户端数
}
```

### 16.2 结构化日志

```go
// 使用 slog 标准库，所有日志携带结构化字段

slog.Info("task dispatched",
    "taskId", taskID,
    "runId", runID,
    "workerId", workerID,
    "userId", userID,
    "sessionKey", sessionKey,
    "dispatchMs", elapsed.Milliseconds(),
)

slog.Warn("worker unhealthy",
    "workerId", w.ID,
    "consecutiveFails", w.consecutiveFails,
    "lastError", err.Error(),
)
```

### 16.3 分布式追踪

```go
// OpenTelemetry 集成
// gRPC 出站调用自动携带 trace context
// WS 入站请求生成新 span

import "go.opentelemetry.io/otel"

func (h *AgentSubmitHandler) Handle(ctx *HandlerContext, raw json.RawMessage) (any, error) {
    tracer := otel.Tracer("gateway")
    spanCtx, span := tracer.Start(ctx.TraceCtx, "agent.submit")
    defer span.End()

    span.SetAttributes(
        attribute.String("user_id", ctx.UserID),
        attribute.String("session_key", params.SessionKey),
    )

    resp, err := h.dispatcher.Submit(spanCtx, req) // trace context 传播到 gRPC
    // ...
}
```

---

## 十七、并发模型总览

### 17.1 Goroutine 拓扑

```text
main goroutine
  │
  ├── Hub.Run()                      1 个 goroutine (连接注册/注销事件循环)
  │
  ├── HTTP Listener                  1 个 goroutine (accept loop)
  │   └── 每个 WS 连接:
  │       ├── readPump()             1 个 goroutine
  │       ├── writePump()            1 个 goroutine
  │       └── handler execution      按需 goroutine (go p.execute)
  │
  ├── EventConsumer.Run()            1 个 goroutine (XREADGROUP loop)
  │   └── Aggregator flush           per-session time.AfterFunc goroutine
  │
  ├── Registry.healthCheckLoop()     1 个 goroutine (定时轮询)
  │   └── checkWorker()              per-worker goroutine (并行健康检查)
  │
  └── signal handler                 1 个 goroutine (优雅关闭)
```

**10,000 连接时的 goroutine 数**：

| 来源 | 数量 | 说明 |
| --- | --- | --- |
| 固定 goroutine | ~5 | Hub + Consumer + Registry + HTTP + Signal |
| 每连接 goroutine | 20,000 | 10,000 × 2 (read + write) |
| Handler goroutine | ~50 峰值 | 并发处理中的请求 |
| **总计** | ~20,055 | Go 轻松承载 |

### 17.2 Channel 使用约定

| Channel | 缓冲 | 生产者 | 消费者 | 说明 |
| --- | --- | --- | --- | --- |
| `Hub.register` | 256 | Upgrader | Hub.Run | 新连接注册 |
| `Hub.unregister` | 256 | Conn.Close | Hub.Run | 连接注销 |
| `Conn.writeCh` | 256 | Handler / Broadcaster | writePump | 出站帧队列 |
| `Conn.done` | 0 | Close() | readPump / writePump | 关闭信号 |

### 17.3 锁使用约定

| 锁 | 类型 | 保护的数据 | 持有时间 |
| --- | --- | --- | --- |
| `Hub.mu` | RWMutex | conns / userConns / sessionConns map | 微秒级 (读多写少) |
| `AggBucket.mu` | Mutex | texts / timer | 微秒级 |
| `Registry.mu` | RWMutex | workers slice | 微秒级 |

**原则**：锁只保护内存数据结构，不在锁内做 IO（Redis/gRPC/WS 写入）。

---

## 附录

### 附录 A. 依赖清单

| 依赖 | 版本 | 用途 |
| --- | --- | --- |
| `nhooyr.io/websocket` | v2 | WebSocket |
| `google.golang.org/grpc` | latest | gRPC Client |
| `github.com/redis/go-redis/v9` | latest | Redis Client |
| `github.com/golang-jwt/jwt/v5` | latest | JWT 验证 |
| `github.com/bytedance/sonic` | latest | 高性能 JSON (可选) |
| `github.com/prometheus/client_golang` | latest | Prometheus 指标 |
| `go.opentelemetry.io/otel` | latest | 分布式追踪 |
| `golang.org/x/time/rate` | latest | 本地令牌桶 |

### 附录 B. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| §4 连接管理 | P0-8 WS echo + P1-1 连接管理 | Phase 0-1 |
| §5 帧处理管线 | P1-2 帧解析 + 校验 | Phase 1 |
| §6 RPC 路由器 | P1-3 RPC 路由表 | Phase 1 |
| §7 调度器 | P1-4 Agent 调度器 | Phase 1 |
| §8 事件广播器 | P1-9 事件消费 + P1-10 聚合 | Phase 1 |
| §9 认证模块 | P2-1 JWT 认证 | Phase 2 |
| §10 Rate Limiter | P1-5 (连接级) + P2-3 (三层) | Phase 1-2 |
| §11 Session Router | P2-4 多实例 | Phase 2 |
| §12 Worker Registry | P0-10 健康检查 + P2-5 负载感知 | Phase 0-2 |
| §13 HTTP 服务 | P0-8 HTTP health + P2-8 OpenAI API | Phase 0-2 |
| §14 优雅关闭 | P2-6 Worker 优雅关闭 (Gateway 侧) | Phase 2 |

### 附录 C. Redis 数据结构汇总

Gateway 使用 Redis 作为跨实例共享状态层。以下汇总所有 Key 及其用途：

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Redis 数据结构全景                                                              │
├──────────────────────────────────┬──────────┬────────┬──────────────────────────┤
│  Key 模式                        │  类型    │  TTL   │  用途                    │
├──────────────────────────────────┼──────────┼────────┼──────────────────────────┤
│                                  │          │        │                          │
│  ── Session Router (§11) ─────── │          │        │                          │
│  session:route:{sessionKey}      │  SET     │  —     │  该 session 的用户连接在  │
│                                  │          │        │  哪些 Gateway 上          │
│                                  │          │        │  值: connID 集合          │
│                                  │          │        │  写入: 连接建立时         │
│                                  │          │        │  读取: 每次事件路由       │
│                                  │          │        │                          │
│  conn:sessions:{connID}          │  SET     │  —     │  该连接关注哪些 session   │
│                                  │          │        │  值: sessionKey 集合      │
│                                  │          │        │  用于: 连接断开时反向清理 │
│                                  │          │        │                          │
│  user:sessions:{userID}          │  ZSET    │  —     │  用户的所有 session 列表  │
│                                  │          │        │  score: lastActivityMs    │
│                                  │          │        │  用于: 断线恢复时确定     │
│                                  │          │        │  需要回放哪些 session     │
│                                  │          │        │                          │
│  ── 断线恢复 (§11, §14) ──────── │          │        │                          │
│  resume:{resumeToken}            │  HASH    │  5min  │  断线恢复上下文           │
│                                  │          │        │  fields: userId,          │
│                                  │          │        │  sessions, createdAt      │
│                                  │          │        │  写入: welcome 事件时     │
│                                  │          │        │  读取: 客户端重连时       │
│                                  │          │        │                          │
│  ── Rate Limiter (§10) ───────── │          │        │                          │
│  rl:user:{userID}:{method}       │  ZSET    │  2min  │  用户级滑动窗口           │
│                                  │          │        │  score: 请求时间戳(ms)    │
│                                  │          │        │  member: 请求唯一 ID      │
│                                  │          │        │  用于: Layer 2 用户限频   │
│                                  │          │        │                          │
│  rl:global:{method}              │  STRING  │  自动  │  全局计数器               │
│                                  │ (counter)│        │  INCR + EXPIRE 实现       │
│                                  │          │        │  用于: Layer 3 全局限频   │
│                                  │          │        │                          │
│  ── 幂等去重 (§5) ───────────── │          │        │                          │
│  idemp:{idempotencyKey}          │  STRING  │  10min │  幂等键缓存               │
│                                  │          │        │  值: 首次响应 JSON        │
│                                  │          │        │  用于: 重复请求直接返回   │
│                                  │          │        │  缓存的响应               │
│                                  │          │        │                          │
│  ── Event Bus (§8) ──────────── │          │        │                          │
│  events:{sessionKey}             │  STREAM  │  MAXLEN│  Agent 事件流             │
│                                  │          │  ~5000 │  由 Runtime 写入 (XADD)  │
│                                  │          │        │  由 Gateway 消费          │
│                                  │          │        │  (XREADGROUP)             │
│                                  │          │        │  用于: 事件广播 +         │
│                                  │          │        │  断线回放                 │
│                                  │          │        │                          │
│  ── Auth (§9, 与 sahara-api 共用) │         │        │                          │
│  token:blacklist:{jti}           │  STRING  │  ≤15min│  已登出的 JWT 黑名单      │
│                                  │          │        │  值: "1"                  │
│                                  │          │        │  写入: sahara-api 登出时  │
│                                  │          │        │  读取: Gateway JWT 验证时 │
│                                  │          │        │                          │
│  ── Worker Registry (§12) ────── │          │        │                          │
│  worker:status:{workerID}        │  HASH    │  30s   │  Worker 状态缓存          │
│                                  │          │        │  fields: state,           │
│                                  │          │        │  activeTasks, maxTasks    │
│                                  │          │        │  用于: 多 Gateway 共享    │
│                                  │          │        │  Worker 负载信息          │
│                                  │          │        │                          │
└──────────────────────────────────┴──────────┴────────┴──────────────────────────┘
```

**Key 总数估算**（10,000 在线会话场景）：

| Key 类别 | 数量级 | 内存估算 |
| --- | --- | --- |
| `session:route:*` | ~10,000 | ~5MB（每个 SET 1-5 个 connID） |
| `conn:sessions:*` | ~12,000 | ~3MB |
| `user:sessions:*` | ~8,000 | ~4MB |
| `resume:*` | ~500（活跃重连） | <1MB |
| `rl:user:*` | ~2,000（活跃用户） | ~2MB |
| `rl:global:*` | ~5 | <1KB |
| `idemp:*` | ~5,000 | ~5MB |
| `events:*` (Streams) | ~1,000（活跃任务） | ~50MB |
| `token:blacklist:*` | ~100 | <1MB |
| `worker:status:*` | ~10 | <1KB |
| **合计** | ~38,000 keys | **~70MB** |

> Gateway 的 Redis 内存占用极低，与 10,000 在线会话相比可以忽略不计。
> 最大的 Key 是 `events:*` Stream，通过 MAXLEN ~5000 控制上限。

### 附录 D. Phase 1 最小实现范围

Phase 1 只需实现标 ★ 的部分，其余可用简化版或 TODO 占位：

| 模块 | Phase 1 范围 |
| --- | --- |
| Hub + Conn | ★ 完整实现 |
| FramePipeline | ★ JSON 解析 + 方法路由；限频只做连接级 |
| Router | ★ agent.submit / agent.abort / session.* / ping |
| Dispatcher | ★ 轮询调度 + RESOURCE_EXHAUSTED 重试 |
| Broadcaster | ★ Redis Streams 消费 + 直接推送 (聚合可延后) |
| Auth | 简化：静态 token 验证 (JWT 推迟到 Phase 2) |
| RateLimiter | ★ 连接级令牌桶；用户级/全局级推迟 |
| SessionRouter | 简化：进程内 map (Redis 推迟到 Phase 2 多实例) |
| WorkerRegistry | ★ 静态配置 + 健康检查 |
| HTTP | ★ /healthz + /readyz + /ws；OpenAI SSE 推迟（业务 API 在 sahara-api） |
| 优雅关闭 | ★ SIGTERM 关闭连接 |
