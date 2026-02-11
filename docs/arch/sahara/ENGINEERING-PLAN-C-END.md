# Sahara 工程计划：C 端 AI Agent 平台

> Sahara 是一个面向 C 端高并发场景的 AI Agent 平台，架构参考 OpenClaw (Gateway + Agent Runtime 分层设计)，作为**独立项目**从零构建。
> 技术方案确定 Go Gateway + Python Runtime（方案 A），本文档聚焦：怎么分阶段做、每个阶段交付什么、风险如何缓解。
>
> 技术方案参考：[TECH-PROPOSAL-C-END-REFACTOR.md](./TECH-PROPOSAL-C-END-REFACTOR.md)

---

## 目录

### 总览

1. [系统边界与交付物定义](#一系统边界与交付物定义)
2. [代码仓库与工程结构](#二代码仓库与工程结构)
3. [Proto 契约管理](#三proto-契约管理)

### 分阶段执行

4. [Phase 0 — 基础设施与脚手架 (第 1-2 周)](#四phase-0--基础设施与脚手架-第-1-2-周)
5. [Phase 1 — MVP 端到端打通 (第 3-8 周)](#五phase-1--mvp-端到端打通-第-3-8-周)
6. [Phase 2 — 生产化加固 (第 9-16 周)](#六phase-2--生产化加固-第-9-16-周)
7. [Phase 3 — 性能优化与大规模 (第 17-24 周)](#七phase-3--性能优化与大规模-第-17-24-周)

### 横切关注点

8. [上线与灰度发布策略](#八上线与灰度发布策略)
9. [部署与编排](#九部署与编排)
10. [可观测性](#十可观测性)
11. [测试策略](#十一测试策略)
12. [安全加固](#十二安全加固)
13. [故障转移与容灾](#十三故障转移与容灾)

### 管理

14. [里程碑与验收标准](#十四里程碑与验收标准)
15. [风险登记簿](#十五风险登记簿)
16. [团队分工建议](#十六团队分工建议)

---

# 总览

---

## 一、系统边界与交付物定义

### 1.1 最终交付物

Sahara 最终交付**五个独立可部署服务** + **一套基础设施**：

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Sahara 交付物清单                                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  服务 (可独立部署、独立扩缩)                                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │ ① sahara-gw      │  │ ② sahara-api     │  │ ③ sahara-rt      │   │
│  │    Go Gateway     │  │    Go API Service │  │    Python Runtime │   │
│  │    WS 实时通信    │  │    RESTful HTTP   │  │    Agent 执行引擎 │   │
│  │    Docker 镜像    │  │    Docker 镜像    │  │    Docker 镜像    │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘   │
│  ┌──────────────────┐  ┌──────────────────┐                         │
│  │ ④ sahara-eb      │  │ ⑤ sahara-sb      │                         │
│  │    Event Bus      │  │    Sandbox Pool   │                         │
│  │    (Redis Streams)│  │    (容器池管理)   │                         │
│  └──────────────────┘  └──────────────────┘                         │
│                                                                      │
│  基础设施                                                            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │ Redis (7.x)      │  │ PostgreSQL (16)  │  │ Docker Engine    │   │
│  │ 会话/路由/锁/MQ  │  │ 持久化/审计      │  │ 沙箱容器运行时   │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘   │
│                                                                      │
│  工程产出                                                            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │ Proto 定义       │  │ K8s 部署清单     │  │ CI/CD Pipeline   │   │
│  │ (gRPC 契约)      │  │ (Helm Chart)     │  │ (构建+测试+部署) │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 不在范围内 (Phase 1)

| 排除项 | 原因 |
| ---- | ---- |
| 移动端 App (iOS/Android) | Phase 1 聚焦后端服务；客户端通过 WS/HTTP 对接 |
| 完整渠道集成 (Telegram/Discord/Slack 等) | Phase 2 逐步接入；Phase 1 只提供 WS + HTTP API |
| LLM Provider 全量接入 | Runtime 先支持 OpenAI + Anthropic，其他后续迭代 |
| 自研 Rust Event Bus | Phase 3 根据瓶颈决定是否引入 |
| 管理后台 (Admin Panel) | Phase 2+ 根据运营需求单独规划 |

---

## 二、代码仓库与工程结构

### 2.1 独立仓库 Monorepo 结构

Sahara 作为独立项目，采用 Monorepo 管理（Go Gateway + Python Runtime + Proto 定义 + 部署清单在同一仓库内）：

```text
sahara/
├── README.md
├── .github/
│   └── workflows/                # CI/CD Pipeline
│       ├── ci.yml                # PR 检查 (lint + test)
│       ├── build.yml             # 镜像构建
│       └── deploy.yml            # 部署流程
├── proto/                        # gRPC Proto 定义 (单一来源)
│   ├── buf.yaml                  # Buf 配置
│   ├── buf.gen.yaml              # 代码生成配置
│   └── sahara/
│       ├── gateway/v1/           # Gateway 相关 proto
│       │   └── gateway.proto
│       ├── runtime/v1/           # Runtime 相关 proto
│       │   └── runtime.proto
│       └── event/v1/             # Event 相关 proto
│           └── event.proto
├── pkg/                          # Go 共享包 (gateway & api 公用)
│   ├── auth/                     # JWT 验证、Token 生成 (共享)
│   ├── model/                    # 领域模型定义 (User, Session 等)
│   ├── store/                    # 存储层接口 + 实现 (Redis, PG)
│   ├── middleware/               # HTTP 中间件 (CORS, RequestID, Logging)
│   └── errcode/                  # 统一错误码定义
├── gateway/                      # Go Gateway 服务 (sahara-gw)
│   ├── cmd/
│   │   └── sahara-gw/
│   │       └── main.go           # 入口
│   ├── internal/                 # 内部包
│   │   ├── ws/                   # WebSocket 管理
│   │   ├── dispatch/             # Agent 调度 (gRPC client)
│   │   ├── broadcast/            # 事件广播 (Redis consumer)
│   │   ├── ratelimit/            # 三层限频
│   │   ├── session/              # 会话路由 (WS 连接 ↔ session 映射)
│   │   └── channel/              # 渠道适配层
│   ├── gen/                      # Proto 生成的 Go 代码
│   ├── go.mod
│   ├── go.sum
│   └── Dockerfile
├── api/                          # Go API 服务 (sahara-api)
│   ├── cmd/
│   │   └── sahara-api/
│   │       └── main.go           # 入口
│   ├── internal/                 # 内部包
│   │   ├── handler/              # HTTP Handler (user, session, file, oauth)
│   │   ├── service/              # 业务逻辑层
│   │   └── server.go             # HTTP 路由注册
│   ├── go.mod
│   ├── go.sum
│   └── Dockerfile
├── runtime/                      # Python Runtime 服务 (sahara-rt)
│   ├── sahara_runtime/           # 主包
│   │   ├── __init__.py
│   │   ├── server.py             # gRPC server 入口
│   │   ├── agent_loop.py         # LLM 交互循环
│   │   ├── tools/                # 工具系统
│   │   ├── sandbox/              # 沙箱管理
│   │   ├── session/              # 会话存储
│   │   ├── events/               # 事件发射
│   │   ├── context/              # 上下文管理
│   │   └── prompt/               # 系统提示词
│   ├── gen/                      # Proto 生成的 Python 代码
│   ├── tests/
│   ├── pyproject.toml            # 依赖管理 (uv)
│   ├── uv.lock
│   └── Dockerfile
├── deploy/                       # 部署清单
│   ├── docker-compose.yml        # 本地开发编排
│   ├── docker-compose.test.yml   # 集成测试编排
│   └── k8s/                      # K8s 部署清单
│       ├── namespace.yml
│       ├── gateway/
│       ├── runtime/
│       ├── redis/
│       └── postgres/
├── scripts/                      # 工程脚本
│   ├── proto-gen.sh              # Proto 代码生成 (Go + Python)
│   ├── dev-up.sh                 # 一键启动开发环境
│   ├── dev-down.sh               # 停止开发环境
│   └── integration-test.sh       # 集成测试
├── docs/                         # 项目文档
│   ├── architecture.md           # 架构总览
│   ├── getting-started.md        # 开发环境搭建指南
│   └── api/                      # API 文档
└── .gitignore
```

### 2.2 关键决策说明

| 决策 | 理由 |
| ---- | ---- |
| 独立仓库 Monorepo | Sahara 是独立产品；Proto/Go/Python/部署 在同一仓库内原子提交，避免跨仓库版本不一致 |
| `gateway/` + `runtime/` 顶级目录 | 两个服务地位对等，各自独立构建，目录结构清晰 |
| `proto/` 顶级目录 | Proto 定义是两个服务共享的契约，放在顶级目录表明其核心地位 |
| Python 用 `uv` 管理 | 速度快（Rust 实现）、兼容 pip/pyproject.toml 生态、lockfile 可复现 |
| Go 用标准 `go mod` | 官方工具链，无额外依赖 |

---

## 三、Proto 契约管理

### 3.1 工具链

使用 **Buf** 管理 Proto 定义和代码生成：

```text
Proto 源文件 (proto/)
        │
        ▼
┌─────────────────┐
│  buf lint        │  ← CI 检查: 命名规范、兼容性
│  buf breaking    │  ← CI 检查: 向后兼容性 (对比 main 分支)
│  buf generate    │  ← 生成 Go + Python 代码
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
Go 代码    Python 代码
gateway/   runtime/
gen/       gen/
```

### 3.2 核心 Proto 定义 (初版)

```protobuf
// proto/sahara/runtime/v1/runtime.proto

syntax = "proto3";
package sahara.runtime.v1;

// Gateway → Runtime: 提交 Agent 任务
service AgentRuntimeService {
  // 提交任务，返回 accepted/rejected
  rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse);
  // 中止正在执行的任务
  rpc AbortTask(AbortTaskRequest) returns (AbortTaskResponse);
  // 健康检查 + 负载上报
  rpc ReportStatus(StatusRequest) returns (StatusResponse);
}

message SubmitTaskRequest {
  string task_id = 1;          // 全局唯一任务 ID
  string session_key = 2;      // 会话标识
  string agent_id = 3;         // Agent 配置标识
  string user_message = 4;     // 用户消息
  string idempotency_key = 5;  // 幂等键
  map<string, string> metadata = 6; // 附加信息 (渠道、用户属性等)
}

message SubmitTaskResponse {
  enum Status {
    ACCEPTED = 0;
    REJECTED_BUSY = 1;
    REJECTED_INVALID = 2;
  }
  Status status = 1;
  string task_id = 2;
  string run_id = 3;           // Runtime 分配的执行 ID
}

message AbortTaskRequest {
  string task_id = 1;
  string run_id = 2;
}

message AbortTaskResponse {
  bool success = 1;
}

message StatusRequest {}

message StatusResponse {
  int32 active_tasks = 1;      // 当前执行中的任务数
  int32 max_tasks = 2;         // 最大并发任务数
  float cpu_usage = 3;
  float memory_usage = 4;
}
```

```protobuf
// proto/sahara/event/v1/event.proto

syntax = "proto3";
package sahara.event.v1;

// 事件结构 (用于 Redis Streams 传输，序列化为 protobuf bytes)
message AgentEvent {
  string event_id = 1;
  string run_id = 2;
  string session_key = 3;
  string task_id = 4;
  EventType type = 5;
  bytes payload = 6;           // JSON 编码的事件数据
  int64 timestamp_ms = 7;
  int32 seq = 8;               // 会话内序列号

  enum EventType {
    DELTA = 0;                 // LLM 流式文本片段
    TOOL_START = 1;            // 工具开始执行
    TOOL_RESULT = 2;           // 工具执行结果
    RUN_START = 3;             // Agent 运行开始
    RUN_COMPLETE = 4;          // Agent 运行完成
    RUN_ERROR = 5;             // Agent 运行出错
    RUN_ABORT = 6;             // Agent 运行被中止
    THINKING = 7;              // 模型思考中
    USAGE = 8;                 // Token 用量统计
  }
}
```

### 3.3 版本演进规则

| 规则 | 说明 |
| ---- | ---- |
| 只做加法 | 新字段用新编号；禁止删除或重命名已发布字段 |
| `buf breaking` 门禁 | CI 中对比 main 分支，破坏性变更阻断合并 |
| 大版本用新 package | 如 `runtime.v2`，与 v1 并存，逐步迁移 |
| 每次 Proto 变更必须同步更新 Go + Python 生成代码 | `scripts/proto-gen.sh` 在 CI pre-commit 中自动执行 |

---

# 分阶段执行

---

## 四、Phase 0 -- 基础设施与脚手架 (第 1-2 周)

> 目标：搭建工程骨架，让团队可以并行开发 Gateway 和 Runtime。

### 4.1 任务清单

| # | 任务 | 产出 | 负责 | 预估 |
| ---- | ---- | ---- | ---- | ---- |
| P0-1 | 创建 Sahara 仓库 + 目录结构 | 仓库骨架 | Tech Lead | 0.5d |
| P0-2 | 初始化 Go 项目 (`go mod init github.com/xxx/sahara/gateway`) | gateway 骨架 | Go 端 | 0.5d |
| P0-3 | 初始化 Python 项目 (`uv init` / pyproject.toml) | runtime 骨架 | Python 端 | 0.5d |
| P0-4 | 编写核心 Proto 定义 + Buf 配置 | proto/ 目录 | Tech Lead | 1d |
| P0-5 | 编写 `proto-gen.sh`，生成 Go + Python 代码 | 可执行脚本 | 全栈 | 0.5d |
| P0-6 | 编写 `docker-compose.yml` (Redis + PG + 服务) | 本地开发环境 | DevOps | 1d |
| P0-7 | 配置 CI Pipeline (lint + build + proto check) | GitHub Actions | DevOps | 1d |
| P0-8 | Go Gateway 空壳 (HTTP health + WS echo) | 可启动的 Go 服务 | Go 端 | 1d |
| P0-9 | Python Runtime 空壳 (gRPC health check) | 可启动的 Python 服务 | Python 端 | 1d |
| P0-10 | Gateway 通过 gRPC 调用 Runtime 的 health check | 端到端 gRPC 通信验证 | 全栈 | 0.5d |
| P0-11 | 编写 `getting-started.md` 开发环境搭建文档 | 新人可快速上手 | Tech Lead | 0.5d |

### 4.2 验收标准

- `docker compose up` 一键启动全部服务（Gateway + Runtime + Redis + PG）
- Gateway (Go) 可接受 WS 连接，回显消息
- Runtime (Python) 响应 gRPC health check
- Gateway → Runtime gRPC 调用成功
- CI 绿色：proto lint + Go build + Python lint
- 新成员按 `getting-started.md` 可在 30 分钟内启动开发环境

---

## 五、Phase 1 -- MVP 端到端打通 (第 3-8 周)

> 目标：用户通过 WebSocket 连接 Go Gateway，发送消息，Python Runtime 调用 LLM 返回流式响应。
> 这是一个可演示的最小系统。

### 5.1 里程碑分解

```text
Week 3-4: Gateway 核心 + Runtime gRPC 对接
Week 5-6: Runtime LLM 循环 + 事件流
Week 7-8: 端到端集成 + 沙箱 + 会话持久化
```

### 5.2 Week 3-4: Gateway 核心 + gRPC 调度

| # | 任务 | 详细说明 | 预估 |
| ---- | ---- | ---- | ---- |
| P1-1 | WS 连接管理 | goroutine per conn (读+写)；连接注册/注销；心跳 ping/pong (30s) | 3d |
| P1-2 | WS 帧解析 | JSON RPC 帧格式 (req/res/event)；帧校验；错误帧响应 | 2d |
| P1-3 | RPC 路由表 | method → handler 映射；支持 `agent.submit`、`agent.abort`、`session.list` | 1d |
| P1-4 | Agent 调度器 | gRPC client pool；选择 Runtime Worker (初期：轮询)；提交任务+等待 accepted | 3d |
| P1-5 | 连接级 Rate Limiter | 滑动窗口；单连接每秒 10 个 RPC (可配) | 1d |

**Gateway 代码骨架**：

```go
// gateway/internal/ws/hub.go
type Hub struct {
    conns      map[string]*Conn  // connID → Conn
    mu         sync.RWMutex
    register   chan *Conn
    unregister chan string
}

// gateway/internal/dispatch/dispatcher.go
type Dispatcher struct {
    workers  []WorkerClient        // gRPC client pool
    next     atomic.Uint64         // 轮询索引
}

func (d *Dispatcher) Submit(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
    worker := d.pick()
    return worker.SubmitTask(ctx, req)
}
```

### 5.3 Week 5-6: Runtime LLM 循环 + 事件流

| # | 任务 | 详细说明 | 预估 |
| ---- | ---- | ---- | ---- |
| P1-6 | gRPC Server 实现 | SubmitTask / AbortTask / ReportStatus 三个 RPC | 2d |
| P1-7 | LLM 交互循环 (agent_loop) | 直接用 Anthropic/OpenAI SDK；流式响应处理；工具调用循环 (max 20 轮) | 5d |
| P1-8 | 事件发射到 Redis Streams | 每个 delta/tool/lifecycle 事件 → XADD 到 `events:{session_key}` stream | 2d |
| P1-9 | Gateway 事件消费 | Redis XREADGROUP 消费事件 → 查找 session 对应的 WS 连接 → 推送 | 3d |
| P1-10 | 150ms 限频聚合 | Gateway 侧对 delta 事件做 150ms 窗口合并后再推送给客户端 | 1d |

**Runtime 核心结构**：

```python
# runtime/sahara_runtime/server.py

class RuntimeServicer(AgentRuntimeServiceServicer):
    def __init__(self, config: RuntimeConfig):
        self.semaphore = asyncio.Semaphore(config.max_concurrent_tasks)
        self.active_runs: dict[str, asyncio.Task] = {}
        self.event_emitter = RedisEventEmitter(config.redis_url)

    async def SubmitTask(self, request, context):
        if self.semaphore.locked():
            return SubmitTaskResponse(status=Status.REJECTED_BUSY)

        run_id = generate_run_id()
        task = asyncio.create_task(
            self._execute(request, run_id)
        )
        self.active_runs[run_id] = task
        return SubmitTaskResponse(status=Status.ACCEPTED, run_id=run_id)

    async def _execute(self, request, run_id):
        async with self.semaphore:
            await run_agent_loop(
                session_key=request.session_key,
                user_message=request.user_message,
                run_id=run_id,
                event_emitter=self.event_emitter,
            )
```

### 5.4 Week 7-8: 端到端集成 + 沙箱 + 会话

| # | 任务 | 详细说明 | 预估 |
| ---- | ---- | ---- | ---- |
| P1-11 | 会话存储 (Redis 热 + PG 冷) | Redis 存最近 N 条消息；PG 存完整历史；session 级分布式锁 | 3d |
| P1-12 | Docker 沙箱容器池 | 预创建 N 个 idle 容器；分配/回收/补充；Alpine 精简镜像 (<20MB) | 3d |
| P1-13 | 基础工具实现 | exec (容器内执行)、read/write (文件操作) 三个核心工具 | 2d |
| P1-14 | 端到端集成测试 | WS 客户端 → Gateway → Runtime → LLM mock → 事件回传 → WS 收到回复 | 2d |
| P1-15 | gVisor 沙箱加固 | Docker runtime 换 runsc；seccomp profile；网络隔离 | 2d |

### 5.5 Phase 1 验收标准

```text
端到端流程通过：
  ✅ WS 客户端连接 Go Gateway
  ✅ 发送消息 → Gateway gRPC 调度到 Python Runtime
  ✅ Runtime 调用 LLM (流式) → 事件发射到 Redis Streams
  ✅ Gateway 消费事件 → 150ms 聚合 → WS 推送给客户端
  ✅ 工具调用在 gVisor 沙箱中执行
  ✅ 会话历史持久化到 Redis + PostgreSQL
  ✅ 端到端延迟 (不含 LLM) < 200ms

性能基线：
  ✅ 单 Gateway 承载 1000 WS 连接
  ✅ 单 Runtime Worker 并发 8 个 Agent 任务
  ✅ 集成测试覆盖核心路径
```

---

## 六、Phase 2 -- 生产化加固 (第 9-16 周)

> 目标：多实例部署、认证鉴权、渠道接入、监控告警，可承载 10,000 在线用户。

### 6.1 任务清单

| # | 任务 | 详细说明 | 预估 |
| ---- | ---- | ---- | ---- |
| P2-1 | JWT 无状态认证 | Gateway 验证 JWT；Token 签发/刷新；设备绑定 | 3d |
| P2-2 | RBAC 权限控制 | 角色: admin/user/guest；权限: agent.submit/session.read/config.write | 2d |
| P2-3 | 三层 Rate Limiting | 连接级 + 用户级 + 全局级；Redis 滑动窗口计数器 | 2d |
| P2-4 | Gateway 多实例 | 无状态设计；session 路由表存 Redis；LB (Nginx/Traefik) 前置 | 3d |
| P2-5 | Runtime Worker 池 | 多 Worker 注册/发现；负载感知调度 (基于 ReportStatus) | 3d |
| P2-6 | Worker 优雅关闭 | drain 模式：停止接受新任务，等待执行中任务完成 (最长 60s)，然后退出 | 2d |
| P2-7 | 首批渠道接入 | Telegram + Discord 渠道适配器；消息收发 + 格式转换 | 5d |
| P2-8 | OpenAI 兼容 HTTP API | `/v1/chat/completions` 接口；支持流式 (SSE) 和非流式 | 3d |
| P2-9 | Prometheus 指标埋点 | Gateway: 连接数/RPC 延迟/错误率；Runtime: 任务数/LLM 延迟/Token 用量 | 2d |
| P2-10 | Grafana 仪表盘 | 系统概览、Gateway 面板、Runtime 面板、LLM 成本面板 | 2d |
| P2-11 | 告警规则 | Gateway 5xx > 1%；Runtime 任务排队 > 50；LLM 调用失败率 > 5% | 1d |
| P2-12 | K8s 部署清单 | HPA 配置；PDB 配置；资源限制；ConfigMap/Secret 管理 | 3d |
| P2-13 | 过载降级 | 所有 Worker 忙 → gRPC 返回 UNAVAILABLE → Gateway 返回"排队中"+ 预估等待 | 2d |
| P2-14 | 模型降级策略 | 主模型限流 → 自动切换备用模型 (如 Claude → GPT-4o-mini) | 2d |
| P2-15 | 用户注册与管理 | 基础用户系统：注册/登录/API Key 管理；用量配额 | 3d |

### 6.2 Phase 2 验收标准

```text
  ✅ Gateway 2 实例 + Runtime 4 Worker 稳定运行
  ✅ JWT 认证 + RBAC 鉴权
  ✅ 10,000 并发 WS 连接无异常
  ✅ 渠道 (Telegram + Discord) 消息正常收发
  ✅ OpenAI 兼容 API 可被第三方客户端调用
  ✅ Grafana 仪表盘运行，告警规则生效
  ✅ Worker 滚动更新零停机
  ✅ 过载场景: 用户收到"排队中"提示
  ✅ 压力测试: 50 req/s 持续 10 分钟无错误
```

---

## 七、Phase 3 -- 性能优化与大规模 (第 17-24 周)

> 目标：50,000+ 在线用户；内容安全 Pipeline；可选 Rust Event Bus。

### 7.1 任务清单

| # | 任务 | 详细说明 | 预估 |
| ---- | ---- | ---- | ---- |
| P3-1 | 内容安全 Pipeline | 正则快筛 (Runtime) + 安全 API 检查 (Event Bus) + 用户级策略 (Gateway) | 5d |
| P3-2 | 数据脱敏 | PII 检测 + 脱敏替换，在事件流转发前执行 | 3d |
| P3-3 | 审计日志 | 事件异步写入审计存储 (PG / 对象存储)；保留 3-5 年 | 3d |
| P3-4 | 会话冷热分离 | Redis 热数据 (最近 20 条) + PG 冷数据；冷数据按需加载 | 3d |
| P3-5 | 响应缓存 | 语义相似 prompt 命中缓存；向量化 prompt + 余弦相似度 | 5d |
| P3-6 | 分级模型路由 | 简单请求 → 轻量模型 (GPT-4o-mini)；复杂请求 → 强模型 (Claude Sonnet) | 3d |
| P3-7 | Firecracker 评估 | 如沙箱并发 >500，评估 Firecracker microVM 替换 Docker | 5d |
| P3-8 | Redis Streams 瓶颈评估 | 如事件吞吐 >100K/s，评估 NATS JetStream 或 Rust Event Bus | 3d |
| P3-9 | 多区域部署设计 | 就近接入 (边缘 Gateway)；跨区数据同步策略 | 5d |
| P3-10 | 更多渠道接入 | Slack、微信公众号/小程序、Web Widget 等 | 5d |
| P3-11 | 多租户支持 | 租户隔离 (数据/配额/模型配置)；租户级计费 | 5d |

---

# 横切关注点

---

## 八、上线与灰度发布策略

### 8.1 核心原则

Sahara 是新系统，没有旧系统迁移负担。但上线仍需灰度——**先内部验证，再逐步放量**。

```text
                 ┌─────────────┐
                 │ Load Balancer│
                 └──────┬──────┘
                        │
              ┌─────────┼─────────┐
              │ 灰度策略:          │
              │ 内测用户 → Sahara  │
              │ 灰度用户 → Sahara  │
              │ 其他 → 等待名单    │
              └─────────┼─────────┘
                        │
                        ▼
               ┌──────────────┐
               │  Sahara      │
               │  Go Gateway  │
               │  Py Runtime  │
               └──────────────┘
                        │
                        ▼
                  基础设施
               (Redis / PostgreSQL)
```

### 8.2 上线阶段

| 阶段 | 用户范围 | 验证周期 | 回滚策略 |
| ---- | ---- | ---- | ---- |
| **Stage 0**: 内部测试 | 仅团队成员 | 2 周 | N/A |
| **Stage 1**: 内测邀请 | 100 名内测用户 (邀请码) | 2 周 | 关闭注册入口 |
| **Stage 2**: 小规模公测 | 1,000 用户 (开放注册) | 2 周 | 暂停新注册 |
| **Stage 3**: 扩大公测 | 10,000 用户 | 2 周 | 限流降级 |
| **Stage 4**: 正式上线 | 全量开放 | — | 熔断 + 自动扩缩 |

### 8.3 每阶段上线前的 Checklist

```text
  □ 核心指标正常 (错误率 < 0.1%, P99 延迟达标)
  □ 监控仪表盘无告警
  □ 压力测试通过 (目标并发数的 2 倍)
  □ 安全审计通过 (沙箱逃逸测试、注入测试)
  □ 回滚方案已验证
  □ 容量规划已更新 (Worker 数量、Redis 内存、PG 存储)
```

---

## 九、部署与编排

### 9.1 本地开发环境

一键启动全部服务（用于开发和调试）：

```yaml
# deploy/docker-compose.yml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --appendonly yes

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: sahara
      POSTGRES_USER: sahara
      POSTGRES_PASSWORD: dev_password
    ports: ["5432:5432"]

  gateway:
    build: ../gateway
    ports: ["8080:8080"]      # HTTP + WS
    environment:
      REDIS_URL: redis://redis:6379
      RUNTIME_ADDRS: runtime:50051
    depends_on: [redis]

  runtime:
    build: ../runtime
    ports: ["50051:50051"]     # gRPC
    environment:
      REDIS_URL: redis://redis:6379
      DATABASE_URL: postgresql://sahara:dev_password@postgres:5432/sahara
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
      DOCKER_HOST: unix:///var/run/docker.sock
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on: [redis, postgres]
```

### 9.2 生产环境 (K8s)

```text
Namespace: sahara-prod
├── Deployment: sahara-gw        (replicas: 2, HPA: CPU>60% → max 5)
├── Deployment: sahara-rt        (replicas: 4, HPA: 自定义指标 active_tasks>12 → max 20)
├── StatefulSet: redis           (单节点, 持久卷)
├── StatefulSet: postgres        (单节点, 持久卷, 定时备份)
├── Service: sahara-gw-svc       (ClusterIP, 内部负载均衡)
├── Service: sahara-rt-svc       (ClusterIP, headless, gRPC 负载均衡)
├── Ingress: sahara-ingress      (TLS, WebSocket 支持, 外部流量入口)
├── ConfigMap: sahara-config     (非敏感配置)
├── Secret: sahara-secrets       (API Keys, JWT Secret, DB 密码)
└── PodDisruptionBudget          (gateway: minAvailable 1, runtime: minAvailable 2)
```

### 9.3 Runtime HPA 自定义指标

标准 CPU/Memory 指标不适合 Runtime（90% 时间在等 IO，CPU 很低）。需要基于业务指标扩缩：

```yaml
# 基于 Prometheus 自定义指标
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: sahara-rt-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sahara-rt
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Pods
      pods:
        metric:
          name: sahara_runtime_active_tasks
        target:
          type: AverageValue
          averageValue: "12"    # 每个 Worker 目标 12 个活跃任务 (上限 16)
```

---

## 十、可观测性

### 10.1 三根支柱

```text
┌─────────────────────────────────────────────────────────┐
│                    可观测性架构                            │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  指标 (Metrics)           日志 (Logs)          追踪 (Traces)
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐ │
│  │ Prometheus   │   │ 结构化 JSON  │   │ OpenTelemetry│ │
│  │ + Grafana    │   │ → Loki/ELK   │   │ → Jaeger     │ │
│  └──────────────┘   └──────────────┘   └──────────────┘ │
│                                                          │
│  Gateway (Go):                                           │
│    slog (JSON) + prometheus client + otel-go SDK         │
│                                                          │
│  Runtime (Python):                                       │
│    structlog (JSON) + prometheus client + otel-python SDK │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### 10.2 分布式追踪 (OpenTelemetry)

跨服务请求必须有完整的 Trace，否则排查问题极其困难。

```text
一个请求的 Trace：

TraceID: abc123
├── Span: gateway.ws.receive       (Gateway, 1ms)
│   ├── Span: gateway.dispatch     (Gateway, 3ms)
│   │   └── Span: grpc.SubmitTask  (Gateway → Runtime, 50ms)
│   │       ├── Span: runtime.prepare     (Runtime, 30ms)
│   │       ├── Span: runtime.llm_call    (Runtime, 12000ms)
│   │       │   └── attribute: model=claude-sonnet-4-20250514
│   │       │   └── attribute: tokens_in=1200, tokens_out=850
│   │       ├── Span: runtime.tool_exec   (Runtime, 500ms)
│   │       │   └── attribute: tool=exec, sandbox=container-42
│   │       └── Span: runtime.llm_call    (Runtime, 8000ms)
│   └── Span: gateway.broadcast    (Gateway, 2ms)
│       └── attribute: events_count=45, aggregated_count=12

关键: TraceID 在 gRPC metadata 中传递 (Go → Python)
     事件中也携带 TraceID，实现调度+事件流的关联
```

**实现要点**：
- Go Gateway: 用 `go.opentelemetry.io/otel` + gRPC interceptor 自动注入 TraceID
- Python Runtime: 用 `opentelemetry-python` + grpcio interceptor 自动提取 TraceID
- Redis Streams 事件中携带 `trace_id` 字段，Gateway 消费时关联

### 10.3 核心仪表盘

| 仪表盘 | 关键面板 |
| ---- | ---- |
| **系统概览** | 在线连接数、活跃任务数、事件吞吐、错误率 |
| **Gateway** | WS 连接数/断连率、RPC 延迟 P50/P99、Rate Limit 触发数、每秒帧数 |
| **Runtime** | 活跃任务数/队列深度、LLM 调用延迟/成功率、工具执行延迟、Token 消耗 |
| **LLM 成本** | 按模型/按用户的 Token 用量、API 费用估算、模型降级触发次数 |
| **沙箱** | 容器池水位、分配延迟、容器故障数 |

### 10.4 核心告警规则

| 告警 | 条件 | 严重级别 | 响应 |
| ---- | ---- | ---- | ---- |
| Gateway 高错误率 | 5xx > 1% 持续 5min | Critical | 检查下游 Runtime 健康 |
| Runtime 过载 | 任务排队 > 50 持续 3min | Warning | 扩容 Worker |
| LLM 调用失败率高 | 失败率 > 5% 持续 5min | Critical | 检查 API Key / Provider 状态；触发模型降级 |
| Redis 延迟高 | P99 > 10ms 持续 5min | Warning | 检查 Redis 负载 |
| 沙箱池耗尽 | idle 容器 = 0 持续 1min | Critical | 检查容器泄漏；临时扩大池 |
| WS 连接数异常 | 突增 >200% 或骤降 >50% | Warning | 检查是否 DDoS 或 LB 故障 |

---

## 十一、测试策略

### 11.1 测试金字塔

```text
                    ┌───────────┐
                    │  E2E 测试  │  ← 5% (端到端流程验证)
                    │ (真实 LLM) │
                   ┌┴───────────┴┐
                   │  集成测试     │  ← 25% (跨服务 gRPC + Redis)
                   │ (Docker Compose)│
                  ┌┴──────────────┴┐
                  │  组件测试        │  ← 30% (单服务+外部依赖 mock)
                  │ (Go: testcontainers)│
                  │ (Py: pytest + mock)│
                 ┌┴────────────────┴┐
                 │  单元测试          │  ← 40% (纯逻辑，无 IO)
                 └───────────────────┘
```

### 11.2 各层级测试范围

| 层级 | Gateway (Go) | Runtime (Python) |
| ---- | ---- | ---- |
| **单元测试** | 帧解析、RPC 路由、Rate Limiter 算法、事件聚合逻辑 | Prompt 构建、工具策略过滤、上下文截断/压缩、Token 计数 |
| **组件测试** | WS 连接管理 (真实 WS)、Redis 读写、gRPC client mock | gRPC server (真实 gRPC)、Redis 事件发射、沙箱容器 (testcontainers) |
| **集成测试** | Gateway + Runtime (docker-compose)；提交任务 → mock LLM → 收到事件 | — (在集成测试中与 Gateway 一起测) |
| **E2E 测试** | WS 连接 → 真实 LLM → 收到完整回复；工具执行 → 沙箱 → 结果回传 | — |

### 11.3 CI Pipeline

```text
每次 Push / PR：
  ├── Stage 1 (并行，~2min)
  │   ├── buf lint + buf breaking (Proto 检查)
  │   ├── Go: go vet + staticcheck + golangci-lint
  │   └── Python: ruff check + mypy
  ├── Stage 2 (并行，~5min)
  │   ├── Go: go test ./... (单元+组件，testcontainers)
  │   └── Python: pytest (单元+组件)
  └── Stage 3 (~10min)
      └── 集成测试: docker-compose up → 端到端场景 → docker-compose down

每日 (定时)：
  └── E2E 测试 (真实 LLM API)
```

### 11.4 LLM Mock 策略

集成测试中不调用真实 LLM（昂贵且不稳定），使用 Mock Server：

```python
# runtime/tests/mock_llm_server.py

class MockLLMServer:
    """模拟 LLM API，返回预录制的响应"""

    async def stream_response(self, messages, tools):
        # 如果消息包含工具调用关键词，返回工具调用响应
        if self._should_use_tool(messages):
            yield tool_use_delta(name="exec", input={"command": "ls"})
            yield stop_delta(reason="tool_use")
        else:
            # 返回预录制的文本流
            for chunk in "Hello, I can help you with that.".split():
                yield text_delta(chunk + " ")
                await asyncio.sleep(0.05)  # 模拟流式延迟
            yield stop_delta(reason="end_turn")
```

---

## 十二、安全加固

### 12.1 安全分层

```text
┌────────────────────────────────────────────────────────────┐
│  Layer 1: 网络层                                            │
│  ├── TLS 终止 (Ingress/LB)                                 │
│  ├── DDoS 防护 (云 WAF / Cloudflare)                       │
│  └── 内网通信 (服务间 mTLS 或 VPC 网络隔离)                │
├────────────────────────────────────────────────────────────┤
│  Layer 2: Gateway 认证鉴权                                  │
│  ├── JWT 无状态认证 (RS256, 15min 过期)                    │
│  ├── Refresh Token (Redis 存储, 7d 过期, 单次使用)         │
│  ├── RBAC 权限检查                                         │
│  └── 三层 Rate Limiting (连接级/用户级/全局级)             │
├────────────────────────────────────────────────────────────┤
│  Layer 3: Runtime 内容安全                                  │
│  ├── 输入: 正则快筛 (SQL注入/XSS/prompt注入)               │
│  ├── 输出: LLM 回复内容安全检查 (同步拦截)                 │
│  └── 工具: 命令白名单/黑名单；文件路径沙箱限制             │
├────────────────────────────────────────────────────────────┤
│  Layer 4: 沙箱隔离                                          │
│  ├── gVisor (runsc) 系统调用过滤 — Phase 1 即启用          │
│  ├── 网络: 默认无外网；白名单放行特定域名                  │
│  ├── 资源: CPU 1 core, Memory 256MB, Disk 100MB, PIDs 100 │
│  └── 生命周期: 任务完成即销毁，不复用容器文件系统内容      │
└────────────────────────────────────────────────────────────┘
```

### 12.2 密钥管理

| 密钥类型 | 存储位置 | 轮换策略 |
| ---- | ---- | ---- |
| JWT 签名密钥 | K8s Secret / Vault | 90 天自动轮换 (支持双密钥过渡) |
| LLM API Keys | K8s Secret / Vault | 按 Provider 策略；Key 池轮换 |
| Redis/PG 密码 | K8s Secret | 首次部署生成随机密码 |
| TLS 证书 | cert-manager (Let's Encrypt) | 自动续期 |

---

## 十三、故障转移与容灾

### 13.1 Worker 故障转移

```text
场景: Runtime Worker-2 在执行任务时宕机

Timeline:
  T+0s    Worker-2 崩溃
  T+5s    Gateway 检测到 gRPC 连接断开 (心跳超时)
  T+5s    Gateway 将 Worker-2 从路由表移除
  T+5s    Gateway 检查 Worker-2 上的活跃任务列表

  对于每个未完成的任务:
  T+6s    Gateway 检查 session 锁状态 (Redis)
  T+6s    释放 Worker-2 持有的 session 锁
  T+7s    重新调度任务到 Worker-3 (gRPC SubmitTask)
  T+7s    Worker-3 从 Redis 加载 session 历史，恢复执行
  T+8s    向客户端发送 "重新处理中" 事件

  注意: 沙箱状态 (容器内文件) 丢失
  处理: 工具执行结果已作为 messages[] 的一部分持久化
        Worker-3 从 messages[] 恢复上下文，重新开始本轮 LLM 调用
```

### 13.2 事件有序性保证

```text
保证: 同一 session 的事件严格有序

实现:
  1. Runtime 为每个 session 的事件分配递增 seq (session 粒度)
  2. Redis Streams key = events:{session_key}
     → 同一 session 的事件在同一 stream 中，天然有序
  3. Gateway 消费时用 XREADGROUP，单 consumer 处理单 stream
     → 不会乱序
  4. 如果 Gateway 收到的 seq 不连续 (中间丢了)
     → 等待 500ms；超时后请求 Runtime 重发 (或标记 gap)

跨 session 不需要全局有序——不同用户之间的事件本来就是独立的。
```

### 13.3 LLM Provider 故障

```text
策略: 多 Provider 熔断 + 自动降级

                 ┌──────────────┐
                 │ Runtime      │
                 │ LLM Router   │
                 └──────┬───────┘
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
   ┌──────────┐  ┌──────────┐  ┌──────────┐
   │ Claude   │  │ GPT-4o   │  │ GPT-4o   │
   │ Sonnet   │  │          │  │ mini     │
   │ (主力)   │  │ (备用)   │  │ (兜底)   │
   └──────────┘  └──────────┘  └──────────┘

   熔断规则 (per provider):
     - 连续 3 次失败 → 熔断 30s
     - 熔断期间请求自动路由到下一个 provider
     - 30s 后半开: 放行 10% 请求试探
     - 试探成功 → 恢复; 失败 → 继续熔断 60s
```

### 13.4 Gateway 故障

```text
场景: Gateway-1 宕机

处理:
  1. LB 健康检查失败 → 自动将流量切到 Gateway-2 (~5s)
  2. Gateway-1 上的所有 WS 连接断开
  3. 客户端自动重连 → 被 LB 路由到 Gateway-2
  4. Gateway-2 从 Redis 查询 session 路由表，恢复状态
  5. 进行中的事件流不会丢失:
     - Runtime 发布到 Redis Streams (不依赖 Gateway)
     - Gateway-2 消费 Redis Streams 获取最新事件
     - 客户端重连后继续接收

关键: Gateway 无状态，所有状态在 Redis 中
     客户端重连机制 (指数退避 + jitter) 内建在 WS 帧协议中
```

---

# 管理

---

## 十四、里程碑与验收标准

```text
M0: 脚手架就绪 ─────────────── 第 2 周末
    ✅ 仓库结构 + Proto + CI + docker-compose + gRPC 通信验证
    ✅ 开发环境搭建文档就绪

M1: MVP 端到端 ─────────────── 第 8 周末
    ✅ WS → Gateway → Runtime → LLM → 事件回传 → WS 完整链路
    ✅ 沙箱 (gVisor) + 会话持久化 + 基础工具
    ✅ 1000 连接压测通过
    ✅ 可向利益相关方演示

M2: 生产化 ─────────────────── 第 16 周末
    ✅ JWT + RBAC + Rate Limiting
    ✅ 多实例 Gateway + Runtime Worker 池
    ✅ 首批渠道接入 (Telegram + Discord)
    ✅ 监控 + 告警
    ✅ 10,000 连接压测通过
    ✅ 内测用户可使用

M3: 规模化 ─────────────────── 第 24 周末
    ✅ 内容安全 Pipeline
    ✅ 分级模型路由
    ✅ 多租户支持
    ✅ 50,000 连接压测通过
    ✅ 公测上线
```

---

## 十五、风险登记簿

| # | 风险 | 概率 | 影响 | 缓解措施 |
| ---- | ---- | ---- | ---- | ---- |
| R1 | 团队 Go/Python 经验不足，Phase 0/1 延期 | 中 | 高 | Phase 0 安排 1 周 Go/Python 培训 + pair programming |
| R2 | gRPC 跨语言调试困难 | 中 | 中 | Phase 0 即建立 gRPC 调试工具链 (grpcurl, Postman gRPC, BloomRPC) |
| R3 | Python grpcio 性能不足或 bug | 低 | 中 | 备选: grpclib (纯 Python async)；MVP 后根据实际数据决定 |
| R4 | Redis Streams 在高吞吐下成为瓶颈 | 低 | 高 | Phase 2 压测验证；备选: NATS JetStream (技术方案已评估) |
| R5 | LLM Provider 频繁限流或故障 | 中 | 高 | 多 Provider 熔断降级 (Phase 2)；Key 池 + 配额监控 |
| R6 | 沙箱容器逃逸 (C 端用户恶意输入) | 低 | 极高 | Phase 1 即启用 gVisor；限制网络+资源；定期安全审计 |
| R7 | 分布式系统调试复杂度高 | 高 | 中 | Phase 1 即引入 OpenTelemetry 分布式追踪 |
| R8 | 产品需求变更频繁导致架构返工 | 中 | 高 | Proto 契约先行；模块边界清晰；Phase 1 只做最小内核 |
| R9 | 开源依赖的安全漏洞 | 中 | 中 | Dependabot 自动扫描；Docker 镜像定期 rebuild |
| R10 | 成本超预期 (LLM API 费用) | 中 | 高 | 分级模型路由 (Phase 3)；Token 用量监控 + 用户级配额 (Phase 2) |

---

## 十六、团队分工建议

### 16.1 角色分配 (5-8 人团队)

| 角色 | 人数 | 职责 |
| ---- | ---- | ---- |
| **Tech Lead** | 1 | 架构决策、Proto 设计、跨组协调、Code Review |
| **Go 工程师** | 2 | Gateway 开发 (WS/HTTP/gRPC/认证/路由) |
| **Python 工程师** | 2 | Runtime 开发 (LLM 循环/工具/沙箱/事件) |
| **DevOps / SRE** | 1 | CI/CD、K8s、监控、Redis/PG 运维 |
| **QA / 测试** | 1 (可兼) | 集成测试、压力测试、安全测试 |

### 16.2 并行开发策略

Phase 0 确立 Proto 契约后，Go 和 Python 两组可以完全并行开发：

```text
        Week 1-2         Week 3-4         Week 5-6         Week 7-8
        Phase 0          ─────────── Phase 1 ───────────

Go 组:  [Proto + 骨架]  [WS + 路由]      [调度 + 限频]    [广播 + 集成]
        ──────────────  ──────────────   ──────────────   ──────────────

Py 组:  [Proto + 骨架]  [gRPC server]    [LLM 循环]       [沙箱 + 持久化]
        ──────────────  ──────────────   ──────────────   ──────────────

DevOps: [CI + Compose]  [Redis/PG 配置]  [监控基础]       [集成测试环境]
        ──────────────  ──────────────   ──────────────   ──────────────

        ↑ 契约确定       ↑ 首次联调       ↑ 事件流联调     ↑ 端到端验收
```

**关键同步点**：
- Week 2 末: Proto 定义冻结 (v1)，双方基于生成代码开发
- Week 4 末: 首次 gRPC 联调 (Gateway → Runtime SubmitTask)
- Week 6 末: 事件流联调 (Runtime → Redis → Gateway → WS)
- Week 8 末: 端到端验收

---

## 附录

### 附录 A. 技术选型速查

| 组件 | 技术 | 版本 | 用途 |
| ---- | ---- | ---- | ---- |
| Gateway 语言 | Go | 1.23+ | WS/HTTP/gRPC |
| Runtime 语言 | Python | 3.12+ | LLM/工具/沙箱 |
| gRPC 框架 (Go) | google.golang.org/grpc | latest | RPC 通信 |
| gRPC 框架 (Py) | grpcio (async) | latest | RPC 通信 |
| Proto 工具链 | Buf | latest | lint/breaking/generate |
| WS 库 (Go) | nhooyr/websocket | v2 | WebSocket 管理 |
| HTTP (Go) | net/http 标准库 | — | HTTP 服务 |
| LLM SDK (Py) | anthropic / openai | latest | LLM 交互 |
| 异步 (Py) | asyncio + uvloop | latest | 事件循环 |
| 事件总线 | Redis Streams | 7.x | 跨服务事件传递 |
| 热数据存储 | Redis | 7.x | 会话/路由/锁 |
| 冷数据存储 | PostgreSQL | 16 | 历史/审计/配置 |
| 沙箱 | Docker + gVisor (runsc) | latest | 代码执行隔离 |
| 容器管理 (Py) | docker-py | latest | 沙箱容器操作 |
| 包管理 (Py) | uv | latest | 依赖管理 |
| 指标 | Prometheus | — | 监控指标 |
| 追踪 | OpenTelemetry → Jaeger | — | 分布式追踪 |
| 日志 (Go) | slog | 标准库 | 结构化日志 |
| 日志 (Py) | structlog | latest | 结构化日志 |
| CI | GitHub Actions | — | 构建/测试/部署 |
| 编排 | Kubernetes | 1.28+ | 生产部署 |

### 附录 B. 参考架构文档

> 以下文档来自 OpenClaw 项目，Sahara 在架构设计上参考了其分层思路。

| 文档 | 参考价值 |
| ---- | ---- |
| [TECH-PROPOSAL-C-END-REFACTOR.md](./TECH-PROPOSAL-C-END-REFACTOR.md) | 技术方案 (架构设计 + 选型论证 + 成本分析) |
| [GATEWAY-ARCHITECTURE.md](./gateway/GATEWAY-ARCHITECTURE.md) | Gateway 分层设计参考 |
| [AGENT-RUNTIME-v2.md](./agent/AGENT-RUNTIME-v2.md) | Agent Runtime 八大子系统参考 |
| [GATEWAY-PROTOCOL.md](./gateway/GATEWAY-PROTOCOL.md) | WS 协议设计参考 (JSON RPC 帧格式) |
| [AGENT-RUNTIME-SANDBOX.md](./agent/AGENT-RUNTIME-SANDBOX.md) | 沙箱设计参考 (Docker 容器池思路) |
