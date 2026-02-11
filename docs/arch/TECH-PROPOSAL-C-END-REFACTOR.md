# 技术方案：面向 C 端高并发场景的架构重构

> 基于现有 OpenClaw 架构（Gateway + Agent Runtime），针对 C 端用户**请求量大、请求方向多**的业务场景，设计分布式解耦方案。
> Gateway 负责请求对接前台 + 调度中心，Agent Runtime 执行请求，两者解耦到不同进程，事件广播机制也解耦到独立进程。
> 可选语言：Python、Golang、Rust（支持混合使用）。

---

## 目录

### Part I — 总体设计

1. [现有架构分析与瓶颈](#一现有架构分析与瓶颈)
2. [新架构总体设计](#二新架构总体设计)

### Part II — 各模块技术选型

3. [Gateway 设计与技术选型](#三gateway-设计与技术选型)
4. [Agent Runtime 设计与技术选型](#四agent-runtime-设计与技术选型)
5. [异步事件传输设计](#五异步事件传输设计)
6. [Sandbox 沙箱技术选型](#六sandbox-沙箱技术选型)
7. [通信协议选型：gRPC vs MQ](#七通信协议选型grpc-vs-mq)

### Part III — 方案选型与成本

8. [三种语言组合方案](#八三种语言组合方案)
9. [容量规划与成本分析](#九容量规划与成本分析) — **热路径瓶颈 + 10000 会话资源估算 + 语言成本对比**
10. [方案对比与推荐](#十方案对比与推荐)
11. [推荐演进路径](#十一推荐演进路径)

### 附录

- [相关架构文档](#附录-a-相关架构文档)
- [参考技术文档](#附录-b-参考技术文档)

---

# Part I — 总体设计

---

## 一、现有架构分析与瓶颈

### 1.1 当前架构要点

当前架构基于 TypeScript (ESM) 单进程模型，核心组件：

- **Gateway (控制平面)**：WebSocket Server、HTTP Server、Channel Manager、Agent Router、Event Broadcaster、Node Registry
- **Agent Runtime (执行引擎)**：LLM 交互循环、工具执行、会话管理、事件产出（含 8 大子系统）
- **事件总线**：进程内 `Set<listener>` 遍历，`emitAgentEvent()` 直接调用内存中的回调函数
- **通信协议**：WebSocket JSON RPC（req/res/event 三种帧类型），协议版本 13

> 详细参考：[GATEWAY-ARCHITECTURE.md](./gateway/GATEWAY-ARCHITECTURE.md)、[AGENT-RUNTIME-v2.md](./agent/AGENT-RUNTIME-v2.md)

### 1.2 C 端场景下的瓶颈

| 维度 | 现状 | C 端场景下的瓶颈 |
| ---- | ---- | ---- |
| **进程模型** | Gateway + Agent Runtime 同进程 | 无法独立扩缩容，单进程资源上限 |
| **事件总线** | 进程内 `Set<listener>` 遍历 | 无法跨进程/跨机器广播 |
| **并发控制** | 双层队列 (session lane + global lane) | 单进程全局队列瓶颈 |
| **通信协议** | WebSocket JSON RPC (req/res/event) | 协议设计合理，可复用 |
| **会话存储** | `sessions.json` 本地文件 | 不支持分布式共享 |
| **渠道管理** | Channel Manager 进程内启动 | 渠道实例绑定单进程 |

### 1.3 核心改造目标

1. **Gateway 与 Agent Runtime 进程解耦** — 独立部署、独立扩缩
2. **事件广播机制解耦** — 跨进程、可扇出、支持多 Gateway 实例
3. **调度中心增强** — 路由多样化、负载均衡、任务排队削峰

---

## 二、新架构总体设计

### 2.1 目标架构

系统有两条独立的通信路径——gRPC（同步调度）和 Redis Streams（异步事件传输）。下图左右分开展示：

```text
                          C 端用户 (Web/App/小程序/API)
                                    │
                           ┌────────┴────────┐
                           │   Load Balancer  │
                           └────────┬────────┘
                              /ws   │   /api/*
                ┌───────────────────┼───────────────────────┐
                ▼                   ▼                       ▼
       ┌─────────────┐    ┌─────────────┐        ┌──────────────────┐
       │  Gateway-1   │    │  Gateway-N   │        │  API Service     │
       │  (WS 实时)   │    │  (WS 实时)   │        │  (RESTful HTTP)  │
       └───┬─────┬───┘    └───┬─────┬───┘        │  用户/会话/文件  │
           │     │            │     │             └────────┬─────────┘
           │     │            │     │                      │ SQL+Redis
           │     │            │     │                      │
     gRPC  │     │ subscribe  │     │ subscribe            ▼
     调度  │     │ 事件       │     │ 事件        ┌──────────────────┐
     (同步)│     │ (异步)     │     │ (异步)      │  State Store     │
           │     │            │     │             │  Redis + PG      │
           │     │     ┌──────┘     │             └──────────────────┘
           │     │     │  gRPC 调度 │                      ▲
           │     │     │  (同步)    │                      │
           │     │     │            │                      │
           ▼     ▼     ▼            ▼                      │
  ═══════════════════════════════════════════════════════════════
  ║                                                             ║
  ║  路径 A: gRPC 同步调度        路径 B: 异步事件传输          ║
  ║  Gateway → gRPC → Runtime    Runtime → XADD → Redis Streams║
  ║  (提交任务/中止/查询)        Redis Streams → XREAD → Gateway║
  ║                              (流式 delta/工具/生命周期事件) ║
  ║                                                             ║
  ═══════════════════════════════════════════════════════════════
           │                                          ▲
           │                                          │
           │          Redis Streams (已有基础设施)      │
           │          事件传输，无独立进程              │
           │                                          │
           ▼                                          │
  ┌──────────────────────────────────────────────────────────────┐
  │                                                              │
  │  ┌─────────┐      ┌─────────┐      ┌─────────┐             │
  │  │ Runtime  │      │ Runtime  │      │ Runtime  │             │
  │  │ Worker-1 │      │ Worker-2 │      │ Worker-N │             │
  │  │ (Agent)  │      │ (Agent)  │      │ (Agent)  │             │
  │  └────┬────┘      └────┬────┘      └────┬────┘             │
  │       │                │                │                    │
  │       │  XADD          │  XADD          │  XADD             │
  │       ▼                ▼                ▼                    │
  │  ┌──────────────────────────────────────────────────────┐   │
  │  │              Redis Streams (事件传输层)               │   │
  │  │              events:{session_key} per-session stream  │   │
  │  │                                                      │   │
  │  │  ★ 不是独立进程 — Runtime 直接写 Redis，             │   │
  │  │    Gateway 直接读 Redis。详见 EVENT-BUS-DESIGN.md    │   │
  │  └──────────────────────────────────────────────────────┘   │
  │                                                              │
  └──────────────────────────────────────────────────────────────┘
```

**两条路径说明**：

| 路径 | 方向 | 协议 | 模式 | 用途 |
| ---- | ---- | ---- | ---- | ---- |
| **路径 A** | Gateway → Runtime | gRPC | 同步请求-响应 | 提交任务、中止任务、人机交互输入、查询状态 |
| **路径 B** | Runtime → Redis Streams → Gateway | Redis XADD/XREAD | 异步事件流 | LLM 流式 delta、工具调用事件、生命周期事件、人机交互通知 |

- **路径 A（gRPC）直达 Runtime**——Redis Streams 不参与调度
- **路径 B（事件流）从 Runtime 写入 Redis Streams，Gateway 读取后推送给客户端**——gRPC 不参与事件广播
- 两条路径完全独立，互不干扰
- **重要**：路径 B 不涉及独立部署的"Event Bus"进程。Redis Streams 是已有 Redis 基础设施的功能，Runtime 和 Gateway 各自通过 Redis 客户端直接读写。详见 [异步事件传输协议](./sahara/EVENT-BUS-DESIGN.md)。

### 2.2 四个独立进程组

| 进程组 | 职责 | 扩缩策略 |
| ---- | ---- | ---- |
| **Gateway** | WS 连接管理、实时事件推送（从 Redis Streams 消费）、gRPC 调度分发、事件聚合(150ms)、Pipeline 处理器 | 按连接数水平扩展 |
| **API Service** | C 端 RESTful HTTP API：用户注册登录、会话 CRUD、文件上传、配额查询 | 按请求量水平扩展（无状态） |
| **Agent Runtime Worker** | LLM 调用、工具执行、沙箱管理、会话持久化、事件发射（XADD 到 Redis Streams） | 按任务量水平扩展 |
| **State Store** | 会话存储、用户数据、配置中心、路由表、**事件传输** (Redis Streams) + 持久化 (PostgreSQL) | 按数据量扩展 |

> **为什么没有独立的 Event Router / Event Bus 进程？** 在详细设计阶段，我们确定异步事件传输通过 Redis Streams 实现——Runtime 直接 `XADD` 写事件，Gateway 直接 `XREAD` 读事件，不需要中间进程。事件聚合(150ms)、Pipeline 处理器(内容安全/PII脱敏)、路由过滤等逻辑都在 **Gateway 侧**完成。这样做的好处是零新增依赖、零新增运维成本，且 Redis Streams 在 50K 在线规模内性能绰绰有余（>100K msg/s）。如果未来达到瓶颈，可迁移到 NATS JetStream，接口已抽象（`EventPublisher` / `EventConsumer`）。

> **Gateway 与 API Service 的分离**：Gateway 专注 WebSocket 长连接和实时事件流，是有状态的（goroutine per conn）。
> API Service 是纯无状态的 HTTP 请求-响应服务，负责 WS 连接建立前后的所有 CRUD 操作。
> 两者通过 LB 路径路由分流（`/ws` + `/v1/*` → Gateway，`/api/*` → API Service），共享 JWT 签名密钥和存储层。
> 详见 [API Service 设计](./sahara/API-SERVICE-DESIGN.md)。

### 2.3 调度流程映射（现有 → 新架构）

```text
现有流程 (单进程)                          新流程 (分布式)
─────────────────                          ──────────────────

① Client → WS → Gateway                   ① Client → WS → Gateway
   帧解析 + RPC 路由                          (不变)

② Gateway → agentCommand()                ② Gateway → gRPC → Runtime Worker
   (同进程函数调用)                            (跨进程 RPC 调用 + 负载均衡)

③ Runtime ← LLM 流式输出                  ③ Runtime ← LLM 流式输出
   (不变)                                     (不变)

④ emitAgentEvent()                        ④ Runtime → Redis Streams (XADD)
   → 进程内 listeners Set                    → 事件持久化在 Redis 中

⑤ Gateway → WS broadcast                  ⑤ Gateway ← Redis Streams (XREAD)
   → 150ms 限频                               → 150ms 聚合 → Pipeline → WS 推送

⑥ Client 收到事件                          ⑥ Client 收到事件
   (不变)                                     (不变)
```

**核心变化**：步骤 ② 从函数调用变为 gRPC；步骤 ④⑤ 从进程内回调变为 Redis Streams 发布/消费。

### 2.4 协议复用

现有 WebSocket 协议（JSON RPC req/res/event 帧）设计合理，C 端场景可直接复用：

- **帧格式**：`req` / `res` / `event` 三种帧类型
- **双响应模式**：先 `accepted`，异步执行后 `final`
- **幂等性**：`idempotencyKey` 防重复
- **序列号**：`seq` 检测丢失

多实例新增考虑：连接 ID 加实例前缀全局唯一；幂等去重缓存迁移到 Redis；心跳 tick 各实例独立。

---

# Part II — 各模块技术选型

---

## 三、Gateway 设计与技术选型

### 3.1 七大职责

| # | 职责 | 说明 |
| ---- | ---- | ---- |
| ❶ | **WS 连接管理** | 接受连接、协议协商、帧解析、心跳维护、慢客户端检测 |
| ❷ | **认证鉴权** | JWT 无状态验证（验证，不签发）、RBAC 权限、Rate Limiting |
| ❸ | **消息路由与调度** | RPC 方法分发、agent → Runtime Worker gRPC 调度、负载均衡 |
| ❹ | **事件广播** | 从 Redis Streams 消费事件 → Pipeline 处理 → 按 session 广播给 WS 客户端 |
| ❺ | **HTTP 服务（精简）** | OpenAI 兼容 SSE API (`/v1/chat/completions`)、WS 升级 (`/ws`)、运维端点 |
| ❻ | **渠道管理** | 启停 Telegram/Discord/Slack 等渠道、账号生命周期、健康监控 |
| ❼ | **状态管理** | 在线状态 (presence)、会话路由表、配置热加载 |

> **职责拆分说明**：原 ❺ HTTP 服务中的 C 端业务接口（用户注册登录、会话 CRUD、文件上传、配额查询等）已拆分为独立的 **API Service** 进程（§2.2）。
> Gateway 仅保留与实时通信直接相关的 HTTP 端点（WS 升级、OpenAI 兼容流式 API、运维探针）。
> API Service 负责 JWT 签发、Refresh Token 管理、用户数据 CRUD 等无需长连接的操作。

### 3.2 为什么选 Go

| 维度 | Go ✅ | Rust | Python ❌ |
| ---- | ---- | ---- | ---- |
| **WS 并发** | goroutine per conn，简单直观 | tokio 异步，性能更极致但代码复杂 | asyncio 可以但 10w 级性能不够 |
| **内存效率** | ~8KB/连接，50w≈200MB | ~2KB/连接，50w≈100MB | ~50KB/连接，10w≈500MB |
| **gRPC** | 官方支持 | tonic (成熟) | grpcio (性能一般) |
| **开发效率** | 快（秒级编译） | 慢（分钟编译） | 最快（无需编译） |
| **招聘** | 容易 | 困难 | 不适合网关 |
| **结论** | **最佳平衡** | 极致性能方案 | 不适合高并发网关 |

Go 的核心优势：goroutine 模型让每个 WS 连接可以有自己的读/写协程，10w 级并发的代码复杂度和 1000 连接几乎一样。

### 3.3 内部架构

```text
┌────────────────────────────────────────────────────────────────────┐
│  Gateway 内部架构 (Go)                                              │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  接入层                                                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐    │
│  │  WS Server   │  │  HTTP Server │  │  Channel Adapters    │    │
│  │  连接池/帧解析│  │  API/Webhook │  │  Telegram/Discord/.. │    │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘    │
│         └─────────────────┼──────────────────────┘                │
│                           ▼                                        │
│  认证层                                                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐    │
│  │  JWT 验证    │  │  RBAC 检查   │  │  Rate Limiter        │    │
│  └──────────────┘  └──────────────┘  └──────────────────────┘    │
│                           ▼                                        │
│  路由与调度层                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐    │
│  │  RPC Router  │  │  Agent       │  │  Load Balancer       │    │
│  │  method→     │  │  Dispatcher  │  │  (选 Runtime Worker) │    │
│  │  handler     │  │  (gRPC 调度) │  │                      │    │
│  └──────────────┘  └──────────────┘  └──────────────────────┘    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐    │
│  │  Dedup       │  │  Session     │  │  Worker Registry     │    │
│  │  (Redis)     │  │  Router      │  │  (服务发现)          │    │
│  └──────────────┘  └──────────────┘  └──────────────────────┘    │
│                           ▼                                        │
│  广播层                                                            │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  Event Consumer (Redis Streams XREAD) → Pipeline 处理   │    │
│  │  → 150ms 限频 / 慢客户端保护 → 按 session WS 推送      │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                    │
│  状态层 (Redis): 连接元数据、session 路由表、presence、去重、限频  │
└────────────────────────────────────────────────────────────────────┘
```

### 3.4 核心设计要点

**goroutine per connection**：每个 WS 连接启动 2 个 goroutine（读+写），写操作通过 channel 串行化，避免并发写锁。

**多实例事件路由**：用户可能连在 Gateway-1，但事件来自 Gateway-2。解法是所有 Gateway 都消费 Redis Streams，收到事件后检查"该 session 有没有连在我这里"，有就推送，无就丢弃。优化：用 sessionKey 做 stream 粒度订阅。

**C 端限频**：三层防护——连接级（单连接每秒 N 个 RPC）、用户级（每分钟 M 个 agent 请求）、全局级（每秒 P 个 agent 请求），用 Redis 滑动窗口计数器实现。

**渠道管理去留**：Phase 1 保留在 Gateway 内（简单）；Phase 2+ 如果渠道数多，可拆为独立 Channel Service（故障隔离）。

### 3.5 Go 技术栈

| 组件 | 推荐 | 理由 |
| ---- | ---- | ---- |
| **WS** | `nhooyr/websocket` | context 集成、并发安全、压缩支持 |
| **HTTP** | `net/http` 标准库 | 零依赖、自定义路由灵活 |
| **gRPC** | `google.golang.org/grpc` | 官方库、与 Python 互通 |
| **JSON** | `sonic` 或 `encoding/json` | 帧解析热路径需高性能 |
| **Redis** | `go-redis/redis` v9 | 连接池、Pipeline、Streams |
| **事件消费** | `go-redis/redis` v9 (Streams) | 从 Redis Streams 消费事件 |
| **JWT** | `golang-jwt/jwt` v5 | C 端无状态认证标准 |
| **日志** | `slog` (Go 1.21+) | 标准库、结构化、零依赖 |
| **指标** | `prometheus/client_golang` | 标准 Prometheus |

---

## 四、Agent Runtime 设计与技术选型

### 4.1 子系统全景

现有 OpenClaw Runtime 包含 8 个子系统（详见 [AGENT-RUNTIME-v2.md](./openclaw/agent/AGENT-RUNTIME-v2.md)）。Sahara 新架构在此基础上扩展为 **20 个模块**，详见 [Runtime 架构设计 (D4)](./sahara/RUNTIME-ARCHITECTURE-DESIGN.md)。以下为核心模块概览：

| # | 模块 | 职责 | D4 章节 |
| ---- | ---- | ---- | ---- |
| ① | **gRPC Server (入口层)** | 接收 Gateway 请求、任务生命周期管理、信号量并发控制 | §3 |
| ② | **Agent Loop (核心循环)** | LLM 交互 → 流式响应 → 工具调用 → 人机交互，每轮迭代的完整状态机 | §4 |
| ③ | **EventEmitter (事件发射)** | 12 种事件类型 → 序列化 → XADD 到 Redis Streams | §5 |
| ④ | **Model Router (模型管理)** | 多 Provider 适配、API Key 轮换、四层 Fallback 防御、Prompt Caching | §6 |
| ⑤ | **Tools (工具系统)** | ToolTier 分级、ToolRegistry + ToolPolicy 9 层策略过滤 + ToolExecutor | §7 |
| ⑥ | **System Prompt Builder** | 动态拼装 PromptSegment 节段（含 Memory Recall） | §8 |
| ⑦ | **Sandbox Manager** | Docker/gVisor 容器池、生命周期管理、资源限制 | §9 |
| ⑧ | **Skills 管理** | SkillTier/SkillMetadata/SkillLoader/SkillFilter，Prompt 驱动的多步引导 | §10 |
| ⑨ | **Context Manager** | 四层防御：Eviction → Compaction → Summarization → Filtering | §11 |
| ⑩ | **Agent Memory** | Working/Short-term/Long-term 三层记忆 + Embedder + MemorySearch | §12 |
| ⑪ | **Hook System** | 12 个生命周期钩子点 + HookRunner + 优先级执行 + 错误隔离 | §13 |
| ⑫ | **Dependencies 注入** | 集中容器管理所有子系统实例的创建和注入 | §14 |
| — | 并发/错误/配置/安全 | 并发模型、错误分类与弹性、配置管理、安全架构总览 | §15-§19 |

### 4.2 新架构下的变化

> 以下对比基于 OpenClaw 原有 8 个子系统。Sahara 新增的模块（Hook 系统、Agent Memory、Skills 等）在 OpenClaw 中不存在，属于全新设计。

| OpenClaw 子系统 | 现有方式 | Sahara 新架构变化 | 幅度 |
| ---- | ---- | ---- | ---- |
| ① 入口与调度 | 进程内双层队列 | gRPC Server + asyncio.Semaphore 并发控制 | 重写 |
| ② 模型与认证 | 本地配置 | Model Router: 多 Provider 适配 + Key 轮换 + 四层 Fallback | 重写 |
| ③ 运行环境 | 本地 Docker + 本地文件 | Sandbox Manager: Docker/gVisor 容器池 + DI 容器注入 | 大改 |
| ④ 系统提示词 | 本地构建 | System Prompt Builder: PromptSegment 动态拼装 + Memory Recall | 中改 |
| ⑤ 工具系统 | 本地创建 + 策略 | Tools: ToolTier 分级 + ToolPolicy 9 层过滤（逻辑保留，架构升级） | 中改 |
| ⑥ AgentSession | SDK 封装 | Agent Loop: 直接用 Python LLM SDK + 自建状态机 + 人机交互支持 | 重写 |
| ⑦ 事件与流式 | 进程内 emit | EventEmitter: 12 种事件 → XADD Redis Streams（含人机交互事件） | 大改 |
| ⑧ 上下文管理 | SDK 内置 | Context Manager: 四层防御 Python 重实现 + tiktoken 精确计数 | 中改 |
| — (新增) | 不存在 | Hook System / Agent Memory / Skills / 安全架构 等全新模块 | 新建 |

### 4.3 为什么选 Python

| 理由 | 详细说明 |
| ---- | ---- |
| **LLM SDK 生态** | OpenAI/Anthropic 官方 SDK 均 Python 首选，流式 + tool_use 一等支持；Go/Rust SDK 功能滞后或无官方版 |
| **AI 工程师** | 90%+ Agent 开源项目是 Python，最容易招有经验的人 |
| **热更新** | Prompt 模板、工具定义、策略规则修改后重启 Worker 即生效，无需编译 |
| **IO 密集** | Runtime 主要工作是等 LLM API 响应（IO），GIL 不是问题；asyncio 完全胜任 |
| **工具兼容** | subprocess/docker-py/playwright/httpx 生态成熟 |

### 4.4 LLM 交互循环核心设计

不用 LangChain 等 Agent 框架，直接用 LLM SDK + 自建循环。原因：需要精确控制每个 delta 事件的粒度、9 层工具策略、四层上下文管理——框架抽象层太厚会阻碍这些定制。

```python
async def run_agent_loop(client, messages, tools, system_prompt,
                         event_emitter, sandbox, run_id, max_iterations=20):
    for iteration in range(max_iterations):
        # 1. 调用 LLM (流式)
        stream = await client.messages.stream(
            model="claude-sonnet-4-20250514", system=system_prompt,
            messages=messages, tools=tools, max_tokens=8192,
        )
        # 2. 处理流式响应，每个 delta 发射到 Redis Streams
        async for event in stream:
            if event.type == "content_block_delta" and event.delta.type == "text_delta":
                await event_emitter.emit({
                    "run_id": run_id, "stream": "assistant",
                    "data": {"text": event.delta.text},
                })
        # 3. 获取完整响应
        response = await stream.get_final_message()
        messages.append({"role": "assistant", "content": response.content})
        # 4. 没有工具调用 → 结束
        if response.stop_reason != "tool_use":
            break
        # 5. 执行工具 → 注入结果 → 继续循环
        tool_results = []
        for tool_call in extract_tool_calls(response):
            result = await execute_tool(tool_call["name"], tool_call["input"], sandbox)
            tool_results.append({"type": "tool_result", "tool_use_id": tool_call["id"],
                                 "content": result})
        messages.append({"role": "user", "content": tool_results})
    return messages
```

### 4.5 会话存储迁移

```text
现有: SessionManager → 本地 .jsonl 文件
新架构: Redis (热数据, 最近 N 条消息) + PostgreSQL (冷数据, 完整历史)

  • 多 Worker 共享同一 session → 需要集中存储
  • Redis 原子锁 → 防止同一 session 并发写入冲突
  • session:{key}:lock → 分布式锁 (等效于现有 session lane)
```

### 4.6 Worker 并发模型

单 Python Worker（4C8G）用 asyncio 可并发 **8-16 个 Agent 任务**（90% 时间在等 LLM API 响应，IO 等待期间 asyncio 切换到其他任务）。单 Worker 吞吐约 0.5 req/s。需要更多并发 → 水平扩展 Worker 数量。

> 详细的并发分析、资源估算和成本对比见 [第九章：容量规划与成本分析](#九容量规划与成本分析)。

### 4.8 Python 技术栈

| 组件 | 推荐 | 理由 |
| ---- | ---- | ---- |
| **gRPC** | `grpcio` (async) | 官方支持，与 Go Gateway 互通 |
| **LLM SDK** | `openai` / `anthropic` 官方 SDK | 流式 + 工具调用一等支持 |
| **Agent 框架** | **不使用**（自建循环） | 需精确控制事件粒度和工具策略 |
| **异步** | `asyncio` + `uvloop` | uvloop 提升 2-4x 事件循环性能 |
| **沙箱** | `docker-py` + 容器池 | 沿用 Docker，池化优化 |
| **会话存储** | `redis.asyncio` + `asyncpg` | Redis 热/PG 冷 |
| **事件发射** | `redis.asyncio` (Streams) | XADD 写入 Redis Streams |
| **Token 计数** | `tiktoken` | 精确 token 计数 |
| **配置** | `pydantic-settings` | 类型安全配置读取 |
| **指标** | `prometheus-client` | Prometheus 标准指标 |

---

## 五、异步事件传输设计

> **设计定稿**：本章内容已在 [异步事件传输协议 (D5)](./sahara/EVENT-BUS-DESIGN.md) 中完整定义。以下为设计要点摘要。

### 5.1 从 "Event Router" 到 "Redis Streams 协议"

在技术方案早期，我们曾设想一个独立部署的"Event Router"进程负责事件路由、聚合和安全检查。**详细设计阶段明确否决了这一方案**：

```text
早期设想 (已否决):  Runtime ──publish──→ Event Router (独立进程) ──subscribe──→ Gateway
最终设计 (采纳):    Runtime ──XADD──→ Redis Streams (已有基础设施) ──XREAD──→ Gateway
```

**否决原因**：
1. Redis Streams 在 50K 在线规模内性能远超需求（>100K msg/s vs 需求 ~4500 msg/s）
2. 事件聚合(150ms)、Pipeline 处理器、路由过滤等逻辑都可以在 Gateway 侧完成（因为 Gateway 拥有连接状态信息）
3. 增加独立进程意味着增加运维复杂度、故障点和延迟，但没有收益
4. 接口已抽象为 `EventPublisher` / `EventConsumer`，未来可平滑迁移到 NATS JetStream

### 5.2 事件完整生命周期

```text
用户: "帮我写排序"  →  Runtime 调用 LLM  →  LLM 流式返回 "好的，我来..."

Step 1: Runtime 发射事件
        Runtime EventEmitter → XADD events:sess_abc123
          AgentEvent { run_id, session_key, type: DELTA, seq: 1, payload: "好" }

Step 2: Gateway 消费事件
        Gateway Consumer → XREAD events:sess_abc123
          检查: sess_abc123 的用户连在我这里? → 是

Step 3: Gateway 做 150ms 窗口聚合 (在 Gateway 内部)
        收集 seq 1-5: "好" "的" "，" "我" "来" → 合并为 "好的，我来"

Step 4: Gateway Pipeline 处理器 (在 Gateway 内部)
        → 内容安全检查 → PII 脱敏 → 审计日志

Step 5: Gateway 推送给客户端
        WS event 帧: { type: "event", event: "agent.delta",
                        payload: { text: "好的，我来", stream: "assistant" } }
```

### 5.3 技术选型与演进路径

| 阶段 | 技术选择 | 适用规模 | 说明 |
| ---- | ---- | ---- | ---- |
| **Phase 1-2** | Redis Streams | ≤50K 在线 | 零新增依赖，已有 Redis 基础设施 |
| **Phase 3** (按需) | NATS JetStream | >50K 在线 | 如 Redis Streams 成为瓶颈，迁移只换实现 |
| **不做** | 自研 Event Bus | — | 事件传输是已解决问题，不是业务护城河 |

### 5.4 Gateway 侧 Pipeline 处理器

聚合和安全检查在 Gateway 消费事件后、推送给客户端前执行：

```text
Gateway 消费 Redis Streams
  │
  ▼
Pipeline Runner (Gateway 内部)
  ├── Processor 1: 内容安全检查 (正则快筛)
  ├── Processor 2: PII 脱敏 (手机号/身份证/邮箱)
  ├── Processor 3: 审计日志 (异步旁路写入)
  └── 150ms Delta 聚合 (减少 WS 帧频)
  │
  ▼
推送给 WS 客户端
```

**分层安全架构**：
- **Layer 1** — Runtime (agent_loop 内)：正则快筛 SQL 注入/XSS/prompt 注入，<0.1ms
- **Layer 2** — Gateway Pipeline：策略规则(敏感词库/PII 脱敏/审计)，<5ms
- **Layer 3** — Gateway (推送前)：用户级策略(年龄/地区内容限制)，<1ms

### 5.5 数据持久化策略

| 数据类型 | 谁写 | 存到哪 | 保留多久 |
| ---- | ---- | ---- | ---- |
| **会话历史** (messages[]) | Agent Runtime | PostgreSQL | 永久 (用户可删) |
| **流式事件** (delta/tool) | Runtime → Redis Streams | Redis Streams (MAXLEN ~5000) | 短期 (小时~天) |
| **运行元数据** (token/model/耗时) | Agent Runtime | PostgreSQL | 永久 |
| **审计日志** | Gateway Pipeline | PG / 对象存储 | 长期 (3-5 年) |

**关键原则**：Runtime 是会话数据的权威来源；Redis Streams 的持久化是为传输可靠性（断线回放），不是业务数据。

---

## 六、Sandbox 沙箱技术选型

### 6.1 现有方案

Docker 容器沙箱（详见 [AGENT-RUNTIME-SANDBOX.md](./agent/AGENT-RUNTIME-SANDBOX.md)）：`exec` 在容器内执行（安全），`read/write` 通过 Volume Mount 在主机操作（性能）。容器长驻，空闲 24h 清理。

### 6.2 C 端新挑战

| 维度 | 现有 | C 端需求 |
| ---- | ---- | ---- |
| 并发量 | 个位数~十几个 | 数千沙箱同时运行 |
| 启动速度 | ~1-3 秒 | 需要 <500ms |
| 单实例内存 | ~30-50MB | 数千实例 = 数十 GB |
| 隔离强度 | 中 (共享内核) | C 端用户不可信，需更强隔离 |
| 生命周期 | 长驻 + 空闲清理 | 用完即销毁 (短生命周期) |

### 6.3 六种方案对比

| 方案 | 启动 | 内存 | 隔离 | 通用性 | 1000 并发 | 开发成本 |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| **① Docker 容器池** | ~200ms(池) | 15-30MB | 中 | ✅ 任意语言 | 15-30GB | 低 (沿用) |
| **② gVisor (runsc)** | ~400ms | 20-40MB | 高 | ✅ 任意语言 | 20-40GB | 低 (换 runtime) |
| **③ Firecracker microVM** | ~125ms | 5-20MB | 最高 (VM级) | ✅ 任意语言 | 5-20GB | 中 (需 KVM) |
| **④ Nsjail** | ~30ms | 1-5MB | 中高 | ✅ 任意语言 | 1-5GB | 中 |
| **⑤ WebAssembly** | ~3ms | 1-3MB | 高 | ❌ 仅 WASM | 1-3GB | 高 |
| **⑥ 云服务 (E2B)** | ~1s | 0 (远端) | 最高 | ✅ 任意语言 | 按量付费 | 低 (SDK) |

### 6.4 容器池设计（核心优化）

将 Docker 启动时间从秒级降到毫秒级：

```text
Worker 启动 → 预创建 N 个 idle 容器 (已启动、已配置安全限制)
请求到来 → 从池中取一个 (~50ms) → 挂载工作目录 → 标记 in-use
请求完成 → 杀死容器内进程 → 清空工作目录 → 放回池中
池水位低 → 后台异步创建新容器补充
```

### 6.5 分阶段推荐

| 阶段 | 方案 | 说明 |
| ---- | ---- | ---- |
| Phase 1 (MVP) | Docker 容器池 | 最低改造成本，精简 Alpine 镜像 <20MB |
| Phase 2 (优化) | + gVisor | Docker runtime 换 runsc，一行配置获得系统调用级隔离 |
| Phase 3 (大规模) | Firecracker microVM | 当并发 >500 且需 VM 级隔离时 |
| 任何阶段 | 云服务 (E2B/Modal) | 不想自建则用云沙箱 API |

---

## 七、通信协议选型：gRPC vs MQ

### 7.1 核心原则

gRPC 和 MQ 解决不同问题——**不是二选一，而是各管各的路径**：

| 判断标准 | → gRPC | → MQ |
| ---- | ---- | ---- |
| 需要即时反馈? | ✅ 是 | ❌ 否 |
| 点对点 or 一对多? | 点对点 | 一对多 (扇出) |
| 发送方需知道接收方? | ✅ 需要 | ❌ 不需要 |
| 需要中间缓冲? | ❌ 不需要 | ✅ 需要 |

### 7.2 调度为什么选 gRPC（不选 MQ）

Gateway 需要在 50ms 内告诉用户"任务被接受"还是"系统繁忙"。gRPC 有请求-响应语义，调用即知结果。MQ 只能告诉你"消息入队了"，有没有 Worker 处理不知道。

6 个具体原因：①即时反馈 ②丰富错误码 ③精确路由到特定 Worker ④内建超时 deadline ⑤中止能力 (AbortTask) ⑥protobuf 类型安全。

### 7.3 事件流为什么选 Redis Streams（不选 gRPC）

事件流是一对多（一个 Runtime 的事件需发给多个 Gateway），Runtime 不关心谁消费，且需要中间缓冲。Redis Streams 天然适合扇出 + 持久化 + 背压控制，且 **零新增依赖**（利用已有 Redis 基础设施）。

### 7.4 混合协作

```text
同步路径 (gRPC):  Client → Gateway → gRPC → Runtime (提交任务/中止/人机交互)
事件路径 (Redis): Runtime → XADD → Redis Streams → XREAD → Gateway → WS → Client (流式事件)
```

### 7.5 过载降级：Redis Streams 做溢出队列

所有 Worker 忙时 gRPC 返回 UNAVAILABLE，Gateway 可降级为放入 Redis Streams 排队，Worker 空闲后自动消费。告诉用户"排队中，预计等待 30 秒"。

### 7.6 事件传输技术对比与演进

| 技术 | 延迟 | 持久化 | 扇出 | 运维 | 适用阶段 |
| ---- | ---- | ---- | ---- | ---- | ---- |
| **Redis Streams** ★ | ~0.5ms | ✅ | ✅ (消费组) | 低 (已有 Redis) | **Phase 1-2 (采用)** |
| **NATS JetStream** | ~0.3ms | ✅ | ✅ | 低 (单二进制) | Phase 3 (备选) |
| **Kafka** | ~5ms | ✅ | ✅ | 高 | 百万级事件/秒 (不推荐) |
| **自研 Rust Bus** | ~0.1ms | ✅ | ✅ | 高 (自维护) | 不做 |

> **最终决策**：Phase 1-2 使用 Redis Streams（零新增依赖）；如 Phase 3 达到瓶颈，迁移到 NATS JetStream。接口已抽象为 `EventPublisher` / `EventConsumer`。

---

# Part III — 语言与方案选型

---

## 八、三种语言组合方案

### 方案 A：Go + Python

> **定位**：工程效率优先、团队易上手

```text
┌──────────────────────────────────┐  ┌───────────────────────────┐
│  Gateway (Go)                     │  │  API Service (Go)         │
│  WS 实时 / gRPC 调度 / 事件广播  │  │  RESTful HTTP / 用户 /    │
└──────────┬───────────────────────┘  │  会话 CRUD / 文件上传     │
           │ gRPC                     └───────────┬───────────────┘
           │                          ┌───────────┘
           │  ┌───────────────────────┼──── State: Redis + PG (共享)
           ▼  ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  Agent Runtime Worker (Python)                                    │
│  gRPC server / LLM SDK / 工具执行 / 事件发射 → Redis XADD       │
└──────────────────────────────────────────────────────────────────┘
事件传输: Redis Streams (Phase 1-2) → 可选迁移 NATS (Phase 3)
```

### 方案 B：Rust + Python

> **定位**：极致性能、最低资源消耗

```text
┌──────────────────────────────────┐
│  Gateway (Rust)                   │
│  tokio+axum+tungstenite/tonic    │
│  事件消费 + 聚合 + Pipeline      │
└──────────┬───────────────────────┘
           │ gRPC (tonic↔grpcio)    ↑ XREAD Redis Streams
           ▼                        │
┌──────────────────────────────────────────────────────────────────┐
│  Agent Runtime Worker (Python)                                    │
│  gRPC server / LLM SDK / 事件发射 → Redis XADD                  │
└──────────────────────────────────────────────────────────────────┘
State: Redis Cluster (含 Streams) + PostgreSQL
```

### 方案 C：Go + Rust + Python

> **定位**：取长补短、工程化最优解

```text
┌─────────────────────────────────┐
│  Gateway (Go)                    │ ← 快速迭代，WS/HTTP
│  gRPC client / 事件消费+聚合    │
│  Pipeline 处理器 (内容安全)      │
└──────────┬────────┬─────────────┘
           │gRPC    │XREAD (Redis Streams)
           ▼        │
┌──────────────────┐ │
│ Runtime (Python)  │ │  如 Redis Streams 瓶颈 → 可选 Rust 高性能事件层
│ gRPC server       │ │  (Phase 3+, 接口已抽象)
│ LLM / 工具 / 沙箱 │ │
│ XADD 事件 ──→ Redis Streams ──→┘
└──────────────────┘
State: Redis (含 Streams) + PostgreSQL
```

---

## 九、容量规划与成本分析

> 选型之后最重要的问题：实际要花多少钱、需要多少机器？本章用 10000 在线会话作为基准场景做系统级估算。

### 9.1 一个请求的时间花在哪里

```text
一个典型 Agent 请求的时间线 (总耗时 5-60 秒):

  ├─ Gateway 路由 + gRPC 调度          ~5ms     (CPU, 极快)
  ├─ Runtime 环境准备                  ~50-200ms (沙箱分配、配置加载)
  ├─ 系统提示词构建                    ~10ms    (CPU, 字符串拼接)
  ├─ ★ 第 1 轮 LLM 调用               ~3-30s   (网络 IO 等待，占 90%+)
  ├─ 工具执行 (如果有)                ~0.1-10s  (docker exec)
  ├─ ★ 第 2 轮 LLM 调用               ~3-30s   (又是 IO 等待)
  ├─ 事件发射到 Redis Streams          ~1ms      (XADD)
  └─ 会话持久化                       ~5ms      (Redis write)

  关键发现: 90%+ 的时间在等待 LLM API 响应 (IO 等待)
           Runtime 自身 CPU 工作 < 100ms
```

### 9.2 连接数 ≠ 并发 Agent 任务

```text
  10000 在线用户在 1 小时内的行为分布:

  ├── 60% 只看不说 (6000 人)         → 0 个 Agent 任务
  ├── 30% 偶尔发 1-2 条 (3000 人)    → ~5000 个任务/小时
  └── 10% 活跃对话 (1000 人)          → ~5000 个任务/小时

  总计: ~10000 个 Agent 任务/小时 → 平均 2.8 req/s → 峰值 ~15 req/s
  每个任务持续 ~15 秒 → 峰值同时在执行: 15 × 15 = ~225 个并发任务
```

### 9.3 热路径瓶颈分析

| 环节 | 延迟 | 吞吐能力 | 是否瓶颈 |
| ---- | ---- | ---- | ---- |
| Gateway (Go) | ~5ms | 10w+ QPS | 不是 |
| gRPC 调度 | ~1ms | 10w+ QPS | 不是 |
| Runtime 准备 | ~50-200ms | 随 Worker 数 | 次瓶颈 (沙箱分配) |
| **LLM API 调用** | **3-30s** | **Provider 限制** | **真正瓶颈** |
| Redis Streams | ~0.5ms | 100w/s | 不是 |
| WS 广播 | ~1ms | 随 Gateway 数 | 不是 |

**结论**：LLM API 是真正瓶颈（延迟最高、吞吐受 provider 限制）。Runtime 自身不是瓶颈——它 90% 时间在等 IO。

### 9.4 10000 会话的资源清单

| 组件 | 实例数 | 规格 | 说明 |
| ---- | ---- | ---- | ---- |
| **Gateway (Go)** | 1-2 | 2C4G | 10000 连接 × 8KB ≈ 80MB，含事件消费 + 150ms 聚合 + Pipeline |
| **Runtime (Python)** | 14-28 | 4C8G | 225 并发 / 8 per Worker ≈ 28 Worker |
| **Redis** | 1 | 4-8G | session 元数据 + 路由表 + 锁 + **Redis Streams 事件传输** (~2GB) |
| **PostgreSQL** | 1 | 按数据量 | 每小时 ~10000 次写入 |
| **总计** | — | — | **Gateway 1 台 + Runtime 7-14 台 + 基础设施 2 台 ≈ 10-17 台** |

Runtime 占总资源的 ~70%——但这不是因为 Python 慢，而是 LLM API 调用本身就要 15 秒/次。

### 9.5 语言对 Runtime 资源的实际影响

| 工作阶段 | Python | Go | 差异 |
| ---- | ---- | ---- | ---- |
| gRPC + 准备 + 提示词 + 策略 | ~47ms | ~8.5ms | 38.5ms |
| **LLM API 调用** | **~15,000ms** | **~15,000ms** | **0ms** |
| 事件发射 + 持久化 | ~7ms | ~2.5ms | 4.5ms |
| **总计** | **~15,054ms** | **~15,011ms** | **~43ms (0.3%)** |

换 Go/Rust **Worker 数量不会减少**（瓶颈在 LLM）。Go 的优势只是单 Worker 内存更低（20MB vs 80MB），同机能多跑 Worker，省约 5 台机器（~$500/月）。但代价是丧失 LLM SDK 生态 + AI 人才招聘优势。

### 9.6 三方案成本对比 (10000 在线会话)

| 资源 | 方案 A (Go+Py) | 方案 B (Rust+Py) | 方案 C (Go+Rust+Py) |
| ---- | ---- | ---- | ---- |
| Gateway | 1× 2C4G (~$30) | 1× 2C2G (~$20) | 1× 2C4G (~$30) |
| Runtime Worker | 7× 4C8G (~$700) | 7× 4C8G (~$700) | 7× 4C8G (~$700) |
| Redis (含 Streams) | 1× 8G (~$80) | 1× 8G (~$80) | 1× 8G (~$80) |
| PostgreSQL | 1× (~$50) | 1× (~$50) | 1× (~$50) |
| **机器总费用/月** | **~$860** | **~$850** | **~$860** |
| **LLM API 费用/月** | **~$3,000-10,000** | **~$3,000-10,000** | **~$3,000-10,000** |

**关键洞察**：机器成本只占总成本的 10-20%，**LLM API 费用才是大头**。三个方案的机器差异不到 $50/月，但 LLM API 费用完全相同。语言选型对总成本影响极小，选择应基于开发效率和团队能力。

### 9.7 不同规模的成本估算

| 在线会话 | 峰值并发 | GW 机器 | API 机器 | RT 机器 | 机器成本/月 | LLM API/月 | 总成本/月 |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| 1,000 | ~23 | 1 台 | 1 台 | 1-2 台 | ~$250 | ~$300-1,000 | ~$550-1,250 |
| 10,000 | ~225 | 1 台 | 2 台 | 7-14 台 | ~$960 | ~$3,000-10,000 | ~$4,000-11,000 |
| 50,000 | ~1,125 | 2 台 | 2-3 台 | 35-70 台 | ~$4,200 | ~$15,000-50,000 | ~$19,200-54,200 |
| 100,000 | ~2,250 | 3 台 | 3-4 台 | 70-140 台 | ~$8,400 | ~$30,000-100,000 | ~$38,400-108,400 |

> API Service 是纯无状态 HTTP 服务，资源占用极低（CPU 密集型 CRUD），每台 2C4G 即可处理 ~5000 RPS。成本增量可忽略。

### 9.8 降低成本的技术方案

| 方案 | 效果 | 实现难度 |
| ---- | ---- | ---- |
| **请求排队 + 等待提示** | Runtime 不需匹配峰值，只需匹配平均速率 | 低 |
| **分级模型** (60% 走快模型 GPT-4o-mini ~2s) | Worker 需求减少 ~50%，LLM 费用减少 ~60% | 低 |
| **本地模型兜底** (vLLM + Qwen/Llama) | 消除 API 配额限制和 API 费用 | 高 (需 GPU) |
| **响应缓存** (语义相似 prompt 命中缓存) | 缓存命中 20% → 资源减少 20% | 中 |
| **K8s 弹性扩缩** (夜间缩容/Spot 实例) | 机器成本降低 40-60% | 中 |

其中**分级模型**是 ROI 最高的方案——同时降低机器成本和 LLM API 费用。

---

## 十、方案对比与推荐

### 10.1 综合对比

| 维度 | 方案 A (Go+Py) | 方案 B (Rust+Py) | 方案 C (Go+Rust+Py) |
| ---- | ---- | ---- | ---- |
| **开发效率** | ★★★★★ | ★★★ | ★★★★ |
| **运行性能** | ★★★★ | ★★★★★ | ★★★★★ |
| **内存效率** | ★★★★ | ★★★★★ | ★★★★★ |
| **团队门槛** | ★★★★★ (低) | ★★★ (高) | ★★★★ (中) |
| **运维复杂度** | ★★★★ (低) | ★★★★ (低) | ★★★ (中) |
| **生态成熟度** | ★★★★★ | ★★★★ | ★★★★ |
| **适合阶段** | MVP → 中期 | 中期 → 大规模 | 中期 → 大规模 |
| **连接上限/实例** | ~50w | ~100w+ | ~50w + 极低延迟事件 |
| **推荐团队** | 3-8 人 | 5-15 人 | 5-12 人 |

### 10.2 决策树

```text
                        团队是否有 Rust 经验?
                         /              \
                       否                是
                       │                  │
                选方案 A (Go+Py)    并发需求多大?
                       │               /        \
                   MVP 起步       < 50w 连接    > 50w 连接
                       │              │              │
                  验证后评估    选方案 A/C       选方案 B/C
                  是否引入 Rust                      │
                                              需要快速迭代?
                                             /          \
                                           是            否
                                           │             │
                                        方案 C        方案 B
```

---

## 十一、推荐演进路径

```text
Phase 1: MVP (1-2 月)  ← 方案 A
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Go Gateway (WS + 基础路由 + gRPC client + Redis Streams 消费)
  • Go API Service (注册/登录/Token 刷新 + 会话 CRUD + 文件上传)
  • Go 共享包 pkg/ (auth/model/store/errcode)
  • Python Runtime 核心:
    - gRPC Server + Agent Loop + EventEmitter
    - Model Router (单 Provider) + System Prompt Builder
    - Tools (exec/read/write) + Docker 沙箱池
    - Context Manager + 基础配置管理
  • Redis Streams 事件传输 (零新增依赖)
  • Redis + PostgreSQL 存储
  → 产出: Gateway + API + Runtime 端到端打通，前端可完整跑通用户流程

Phase 2: 生产化 (2-4 月)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Gateway 多实例 + LB 路径路由 (/ws → GW, /api/* → API)
  • Gateway 150ms 聚合 + Pipeline 处理器 (Phase 2 后半段)
  • API Service: OAuth 三方登录、用量统计、系统公告
  • Runtime 新增模块:
    - Model Router 多 Provider + 四层 Fallback 防御
    - Skills 管理 + Hook 系统 (生命周期钩子)
    - 人机交互 (SendInput gRPC + agent.input WS)
    - Agent Memory (Working + Short-term)
  • JWT 认证 + 三层 Rate Limiting
  • Sticky Affinity (任务级亲和路由，支持人机交互)
  • 渠道管理迁移
  • 监控 + 告警 (Prometheus/Grafana) + OpenTelemetry 分布式追踪
  → 产出: 可承载 10K 在线用户的生产系统

Phase 3: 性能优化 (4-6 月)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • 如 Redis Streams 成为瓶颈 → 迁移到 NATS JetStream (接口已抽象)
  • 沙箱 + gVisor (系统调用级隔离)
  • 内容安全 Pipeline (Gateway 侧)
  • 会话数据冷热分离
  • Agent Memory Long-term (向量检索)
  • 分级模型路由 (简单请求 → 轻量模型)
  → 产出: 50K 在线、P99 事件延迟 < 200ms

Phase 4: 大规模 (6+ 月)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Gateway 核心模块可选 Rust 重写 (如果 Go 成为瓶颈)
  • Firecracker microVM 沙箱
  • Runtime 支持 GPU 调度 (本地模型)
  • 多区域部署 + 事件回放 + 审计
  • 多租户隔离 (数据/配额/模型配置)
  → 产出: 百万级并发系统
```

---

## 附录

### 附录 A. 相关架构文档

**Sahara 新架构设计文档**：

| 文档 | 内容 |
| ---- | ---- |
| [ENGINEERING-PLAN-C-END.md](./sahara/ENGINEERING-PLAN-C-END.md) | 工程计划：分阶段交付、里程碑、团队分工 |
| [GRPC-PROTOCOL-DESIGN.md](./sahara/GRPC-PROTOCOL-DESIGN.md) | D1: gRPC 协议设计 (Gateway ↔ Runtime) |
| [WS-PROTOCOL-DESIGN.md](./sahara/WS-PROTOCOL-DESIGN.md) | D2: WebSocket 协议设计 (Client ↔ Gateway) |
| [GATEWAY-ARCHITECTURE-DESIGN.md](./sahara/GATEWAY-ARCHITECTURE-DESIGN.md) | D3: Gateway 架构设计 |
| [RUNTIME-ARCHITECTURE-DESIGN.md](./sahara/RUNTIME-ARCHITECTURE-DESIGN.md) | D4: Runtime 架构设计 |
| [EVENT-BUS-DESIGN.md](./sahara/EVENT-BUS-DESIGN.md) | D5: 异步事件传输协议（Runtime ↔ Gateway 对接规范） |
| [SANDBOX-DESIGN.md](./sahara/SANDBOX-DESIGN.md) | D6: Sandbox 管理与演进 |
| [API-SERVICE-DESIGN.md](./sahara/API-SERVICE-DESIGN.md) | D7: API Service 设计 (C 端 RESTful HTTP) |
| [OBSERVABILITY-DESIGN.md](./sahara/OBSERVABILITY-DESIGN.md) | D8: 可观测性设计 (Metrics/Logs/Traces) |

**OpenClaw 参考文档（现有架构）**：

| 文档 | 内容 |
| ---- | ---- |
| [GATEWAY-ARCHITECTURE.md](./openclaw/gateway/GATEWAY-ARCHITECTURE.md) | 现有 Gateway 架构总览 |
| [GATEWAY-AGENT-DISPATCH.md](./openclaw/gateway/GATEWAY-AGENT-DISPATCH.md) | 现有 Agent 调度流程 |
| [GATEWAY-PROTOCOL.md](./openclaw/gateway/GATEWAY-PROTOCOL.md) | 现有 WebSocket 协议详解 |
| [GATEWAY-STARTUP.md](./openclaw/gateway/GATEWAY-STARTUP.md) | 现有 Gateway 启动流程 |
| [GATEWAY-CLIENT.md](./openclaw/gateway/GATEWAY-CLIENT.md) | 现有客户端通信 |
| [GATEWAY-AUTH.md](./openclaw/gateway/GATEWAY-AUTH.md) | 现有认证与配对 |
| [AGENT-RUNTIME-v2.md](./openclaw/agent/AGENT-RUNTIME-v2.md) | 现有 Agent Runtime 架构 |
| [AGENT-RUNTIME-SESSION.md](./openclaw/agent/AGENT-RUNTIME-SESSION.md) | 现有会话系统 |
| [AGENT-RUNTIME-SANDBOX.md](./openclaw/agent/AGENT-RUNTIME-SANDBOX.md) | 现有沙箱系统 |
| [AGENT-RUNTIME-TOOLS.md](./openclaw/agent/AGENT-RUNTIME-TOOLS.md) | 现有工具系统 |
| [CHANNELS-ARCHITECTURE.md](./openclaw/channel/CHANNELS-ARCHITECTURE.md) | 现有消息渠道架构 |

### 附录 B. 参考技术文档

| 技术 | 文档 |
| ---- | ---- |
| gRPC | https://grpc.io/docs/ |
| NATS JetStream | https://docs.nats.io/nats-concepts/jetstream |
| Redis Streams | https://redis.io/docs/data-types/streams/ |
| tokio (Rust) | https://tokio.rs/tokio/tutorial |
| axum (Rust) | https://docs.rs/axum/latest/axum/ |
| nhooyr/websocket (Go) | https://github.com/nhooyr/websocket |
| grpcio (Python) | https://grpc.io/docs/languages/python/ |
| docker-py (Python) | https://docker-py.readthedocs.io/ |
| E2B Sandbox | https://e2b.dev/docs |
| Firecracker | https://firecracker-microvm.github.io/ |
