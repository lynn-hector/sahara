# AGENT.md — Sahara 项目 Agent 快速上下文

> 本文件为 AI Agent（Cursor / Copilot / 其他 LLM IDE）提供项目全貌，以便快速进入开发状态。

---

## 1. 项目定位

**Sahara** 是一个面向 C 端高并发场景的 AI Agent 平台，采用 **Go Gateway + Python Runtime** 分布式架构。

- **Gateway (Go)**: 管理 WebSocket 连接、gRPC 任务调度、Redis Streams 事件广播、认证、限流
- **Runtime (Python)**: 执行 Agent Loop（LLM 交互 + 工具调用 + 人机交互），管理沙箱、Skills、Hooks
- **API Service (Go)**: 用户/会话/配置管理（Phase 3 规划，尚未实现）

---

## 2. 核心架构

```text
Client ←—WS—→ Gateway (Go) ←—gRPC—→ Runtime Worker (Python)
                  ↑                        │
        Redis Streams (异步事件)            │
                  ↓                        ↓
          Broadcast → Client        LLM API (Anthropic/OpenAI)
                                    ↕ Tools / Sandbox / Skills
```

**两条通信路径：**

| 路径   | 方向                          | 协议          | 用途                               |
| ------ | ----------------------------- | ------------- | ---------------------------------- |
| 同步   | Gateway → Runtime             | gRPC          | SubmitTask, AbortTask, SendInput, GetTaskStatus |
| 异步   | Runtime → Redis Streams → GW  | Protobuf 序列化 | LLM delta 流、工具事件、生命周期事件 |

---

## 3. 目录结构与职责

```text
sahara/
├── proto/                  # gRPC Proto 定义 (Buf 管理)
│   └── sahara/
│       ├── agent/v1/          # AgentService: SubmitTask, AbortTask, SendInput, GetTaskStatus, ListActiveTasks
│       ├── worker/v1/         # WorkerService: GetStatus, Drain, UpdateConfig
│       ├── event/v1/          # AgentEvent: 15+ 事件类型 (delta, tool_start, input_required...)
│       └── common/v1/         # 共享: TaskState 枚举, ErrorDetail
│
├── gen/                    # Proto 生成代码 (Go + Python, .gitignore, 需运行 scripts/proto-gen.sh)
│   ├── go/                    # Go stubs
│   └── python/                # Python stubs
│
├── gateway/                # sahara-gw: Gateway 服务 (Go 1.25)
│   ├── cmd/sahara-gw/main.go    # 入口: HTTP server + WS + gRPC dispatcher + Redis consumer
│   └── internal/
│       ├── auth/                 # JWT 认证 (HS256) + API Key + Open 三模式
│       ├── ws/                   # WebSocket Hub/Router/帧协议/三层限流
│       ├── dispatch/             # gRPC Round-Robin 调度 + Sticky Affinity (SendInput)
│       ├── broadcast/            # Redis Streams 事件消费 + Delta 聚合 → WS 推送
│       ├── compat/               # OpenAI 兼容 HTTP API (/v1/chat/completions, /v1/models)
│       ├── metrics/              # Prometheus 指标定义
│       ├── runtime/              # Worker 健康检查连接池
│       └── config/               # 环境变量配置加载
│
├── runtime/                # sahara-rt: Agent Runtime (Python 3.11+)
│   ├── sahara_runtime/
│   │   ├── server.py              # gRPC 服务入口 + Prometheus /metrics HTTP
│   │   ├── agent_loop.py          # Agent 核心: LLM 流式交互 → 工具调用 → 人机交互循环
│   │   ├── grpc/
│   │   │   ├── agent_servicer.py     # AgentService 实现 (TaskHandle, UserInput, SendInput)
│   │   │   └── worker_servicer.py    # WorkerService 实现 (GetStatus, Drain, UpdateConfig)
│   │   ├── di/container.py        # 依赖注入容器 (所有子系统的生命周期管理)
│   │   ├── model_router/          # 模型路由 + API Key 池 + Session 亲和 + Fallback (重试+轮换)
│   │   ├── tools/                 # 工具系统: ToolRegistry + ToolExecutor + exec/read/write 内置工具
│   │   ├── skills/                # Skills 系统: SkillLoader + SkillFilter + prompt 生成 + sandbox 同步
│   │   ├── hooks/                 # Hook 系统: 8 扩展点 + 4 内置 hook (logging/metrics/safety/context)
│   │   ├── events/                # 事件发射器 (Redis Streams backend)
│   │   ├── context/               # 上下文窗口管理 + Token 计数
│   │   ├── memory/                # 会话存储 (Redis 热 + PG 冷)
│   │   ├── prompt/                # 动态 System Prompt 构建 (identity + safety + tools + skills + runtime)
│   │   ├── sandbox/               # 沙箱管理 (Noop / Docker / E2B 三种 provider)
│   │   ├── config/settings.py     # Pydantic Settings (env prefix: SAHARA_)
│   │   └── observability/         # Prometheus 指标定义
│   ├── skills/                 # 内置 Skills (create-rule, create-skill)
│   └── tests/                  # pytest 单元测试
│
├── api/                    # sahara-api: API 服务 (Go, 仅 go.mod, 尚未实现)
├── pkg/                    # Go 共享库
│   └── errcode/               # 统一错误码 (Code struct + 4 domain: common/gw/rt/api)
├── deploy/                 # Docker Compose (dev + test)
├── scripts/                # dev-up.sh, dev-down.sh, proto-gen.sh
└── docs/arch/sahara/       # 9 份架构设计文档 (D1-D9) + 工程计划
```

---

## 4. 技术栈

| 组件 | 技术 | 版本 |
| --- | --- | --- |
| Gateway | Go, gorilla/websocket, go-redis/v9, gRPC, golang-jwt/jwt/v5, prometheus/client_golang | Go 1.25 |
| Runtime | Python, grpcio async, anthropic/openai SDK, uvloop, pydantic-settings, structlog | Python 3.11+ |
| Proto 工具链 | Buf | >= 1.28 |
| 事件传输 | Redis Streams | Redis 7.x |
| 持久存储 | PostgreSQL (pgvector, pg_trgm) | PG 16.x |
| 沙箱 | Docker / gVisor / E2B | — |
| 可观测性 | Prometheus + Grafana + structlog | — |
| 包管理 | go.work (Go monorepo), uv (Python) | — |

---

## 5. 当前实现进度

### Phase 0 (基础骨架) — 已完成

- Proto 定义 + 代码生成管线
- Go Gateway 骨架 (HTTP/WS/gRPC)
- Python Runtime 骨架 (gRPC server/DI container)
- Gateway ↔ Runtime gRPC 连通

### Phase 1 (最薄端到端切片) — 已完成

- Agent Loop (LLM 流式调用 + 工具调用循环)
- Model Router (Key 池 + Session 亲和)
- WebSocket 帧协议 + agent.submit/abort
- Redis Streams 事件传输 + Broadcast Consumer
- Session Store (Redis 热存储)
- Context Manager + Token Counter
- 内置工具 (exec/read/write) + 沙箱集成
- Hook 系统 (8 扩展点 + 4 内置 hook)
- Prompt Builder

### Phase 2 Wave 1 (核心能力) — 已完成

- P2-1: JWT 无状态认证 (HS256, access/refresh token, 三模式: JWT/APIKey/Open)
- P2-8: OpenAI 兼容 HTTP API (/v1/chat/completions SSE + /v1/models)
- P2-17: Skills 系统 (Loader/Filter/Prompt/Sandbox 同步)

### Phase 2 Wave 2 (生产加固) — 已完成

- P2-15: 人机交互 SendInput (gRPC + WS agent.input + asyncio.Queue 暂停/唤醒)
- P2-6: Worker 优雅关闭 Drain (拒绝新任务 + 等待存量完成)
- P2-13: 过载降级 (WorkersBusyError → 503 retryable)
- P2-9: Prometheus 指标埋点 (Gateway 12 指标 + Runtime 8 指标 + /metrics 端点)
- P2-3: 三层 Rate Limiting (connection 10rps / user 30rps / global 500rps)

### 未实现 / 规划中

- API Service (api/ 目录仅有 go.mod)
- Phase 3: Plugin 系统 (独立设计文档已完成)
- 全局 Worker 注册发现（当前为静态配置）
- PostgreSQL 冷存储落盘
- Grafana Dashboard 配置
- CI/CD Pipeline

---

## 6. 关键设计模式与约定

### 错误码

- 统一 `pkg/errcode` 包，`Code` struct 包含 Value/Domain/HTTP/Retryable
- 按 domain 拆分: `common.go`, `gw.go`, `rt.go`, `api.go`
- Gateway 和 Runtime 各自维护自己 domain 的错误码

### gRPC 服务

- `AgentService`: 面向任务生命周期 (Submit/Abort/SendInput/Status)
- `WorkerService`: 面向运维 (GetStatus/Drain/UpdateConfig)
- 所有 Proto 定义在 `proto/` 目录，通过 `scripts/proto-gen.sh` 生成到 `gen/`

### WebSocket 帧协议

- JSON RPC 风格: `{type, id, method, params}` → `{type, code, status, payload}`
- 三个 method: `agent.submit`, `agent.abort`, `agent.input`
- Event 帧通过 Broadcast Consumer 从 Redis Streams 推送

### Python 依赖注入

- `Container` (di/container.py) 管理所有子系统实例的生命周期
- `startup()` 按序初始化 (Redis → PG → SessionStore → ... → SkillLoader → HookRunner → PromptBuilder)
- `shutdown()` 反序释放

### 配置管理

- Gateway: 环境变量直接读取 (`envOr` 函数)
- Runtime: Pydantic Settings (`env_prefix="SAHARA_"`, 支持 `.env` 文件)

### 认证

- 三种模式自动切换: JWT (设置 JWT_SECRET) > API Key (设置 API_KEY) > Open
- JWT claims 包含 UserID, Roles, Quota
- WS 连接支持 `?token=` query param 或 `Authorization: Bearer` header

---

## 7. 快速启动

```bash
# 1. 基础设施
./scripts/dev-up.sh                          # Redis + PostgreSQL

# 2. Proto 代码生成 (首次或 .proto 变更后)
./scripts/proto-gen.sh

# 3. Runtime
cd runtime && uv sync
SAHARA_ANTHROPIC_API_KEY="sk-..." uv run python -m sahara_runtime.server
# 无 API Key 也可启动 (Mock LLM 模式)

# 4. Gateway
cd gateway && go run ./cmd/sahara-gw

# 5. 验证
curl http://localhost:8080/healthz           # Gateway 健康
curl http://localhost:8080/readyz            # Runtime 连通
curl http://localhost:8080/metrics           # Prometheus 指标 (Gateway)
curl http://localhost:9090/metrics           # Prometheus 指标 (Runtime)
```

---

## 8. 开发注意事项

1. **Go monorepo**: 使用 `go.work` 管理多模块 (`gateway`, `api`, `gen/go`, `pkg`)，编译检查用 `go build ./...`
2. **Proto 生成代码在 .gitignore**: 拉取代码后必须先运行 `scripts/proto-gen.sh`
3. **Python 虚拟环境**: 使用 `uv` 管理，`runtime/.venv/`，运行时需确保 `gen/python` 在 PYTHONPATH
4. **沙箱默认关闭**: `sandbox_enabled=False`, `sandbox_provider=noop`，开发时无需 Docker
5. **E2B 沙箱**: 设置 `SAHARA_SANDBOX_PROVIDER=e2b` + `SAHARA_E2B_API_KEY` 启用云沙箱
6. **日志格式**: Gateway 使用 `slog` JSON，Runtime 使用 `structlog` JSON
7. **测试**: Gateway `go test ./...`，Runtime `uv run pytest`
8. **License**: Apache 2.0

---

## 9. 关键文件索引

| 场景 | 文件 |
| --- | --- |
| Gateway 入口 | `gateway/cmd/sahara-gw/main.go` |
| Runtime 入口 | `runtime/sahara_runtime/server.py` |
| Agent 核心循环 | `runtime/sahara_runtime/agent_loop.py` |
| 依赖注入容器 | `runtime/sahara_runtime/di/container.py` |
| gRPC 任务处理 | `runtime/sahara_runtime/grpc/agent_servicer.py` |
| WS 帧处理 | `gateway/internal/ws/handlers.go` |
| WS 连接管理 | `gateway/internal/ws/hub.go` |
| gRPC 调度 | `gateway/internal/dispatch/dispatcher.go` |
| 事件广播 | `gateway/internal/broadcast/consumer.go` |
| 认证 | `gateway/internal/auth/jwt.go` + `middleware.go` |
| 配置 (GW) | `gateway/internal/config/config.go` |
| 配置 (RT) | `runtime/sahara_runtime/config/settings.py` |
| 错误码 | `pkg/errcode/` |
| 架构设计文档 | `docs/arch/sahara/` (9 份 D1-D9) |
| 工程计划 | `docs/arch/sahara/ENGINEERING-PLAN-C-END.md` |
