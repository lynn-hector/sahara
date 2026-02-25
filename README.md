# Sahara

> C 端高并发 AI Agent 平台 — Go Gateway + Python Runtime 分布式架构。

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

---

## 架构总览

```text
                      C 端用户 (Web / App / 小程序)
                                │
                       ┌────────┴────────┐
                       │  Load Balancer   │
                       └────────┬────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         │                      │                      │
         ▼                      ▼                      ▼
┌──────────────┐   ┌─────────────────┐   ┌──────────────────┐
│  Gateway (Go) │   │  Gateway (Go)    │   │  API Service (Go) │
│  WebSocket    │   │  WebSocket       │   │  RESTful HTTP      │
│  实时通信     │   │  实时通信         │   │  用户/会话/认证    │
└──────┬───────┘   └──────┬──────────┘   └──────┬───────────┘
       │ gRPC              │ subscribe           │
       ▼                   ▼                     │
┌─────────────┐   ┌──────────────┐               │
│  Runtime     │──▶│ Redis Streams │               │
│  Worker (Py) │   │  (事件传输)   │               │
└─────────────┘   └──────────────┘               │
       │                                          │
       └────────────────┬─────────────────────────┘
                  ┌──────┴──────┐
                  │  State Store │
                  │  Redis + PG  │
                  └─────────────┘
```

**两条通信路径：**

| 路径 | 方向 | 协议 | 用途 |
| --- | --- | --- | --- |
| **同步** | Gateway → Runtime | gRPC | 任务提交、中止、状态查询 |
| **异步** | Runtime → Redis Streams → Gateway | Pub/Sub | LLM 流式 delta、工具事件、生命周期 |

## 项目结构

```text
sahara/
├── proto/              # gRPC Proto 定义 (Buf 管理)
│   └── sahara/
│       ├── agent/v1/      # AgentService (任务管理)
│       ├── worker/v1/     # WorkerService (运维管理)
│       ├── event/v1/      # AgentEvent (异步事件)
│       └── common/v1/     # 共享类型 (TaskState, ErrorDetail)
├── gen/                # Proto 生成代码 (Go + Python, 勿手动编辑)
├── gateway/            # sahara-gw: Gateway 服务 (Go)
│   ├── cmd/sahara-gw/     # 入口
│   └── internal/
│       ├── ws/               # WebSocket Hub / Router / 帧协议 / 限频
│       ├── dispatch/         # gRPC 任务调度 (round-robin)
│       ├── broadcast/        # Redis Streams 事件消费 + Delta 聚合
│       ├── runtime/          # Worker 健康检查连接池
│       └── config/           # 环境变量配置加载
├── api/                # sahara-api: API 服务 (Go, Phase 2)
├── runtime/            # sahara-rt: Agent Runtime (Python)
│   ├── sahara_runtime/
│   │   ├── agent_loop.py      # Agent 核心交互循环
│   │   ├── server.py          # gRPC 服务入口
│   │   ├── grpc/              # AgentServicer + WorkerServicer
│   │   ├── di/                # 依赖注入容器
│   │   ├── model_router/      # 模型路由 + Key 池 + Session 亲和
│   │   ├── tools/             # 工具注册 + 执行 + 沙箱集成
│   │   ├── hooks/             # Hook 系统 (8 扩展点 + 4 内置 hook)
│   │   ├── events/            # 事件发射 (Redis Streams backend)
│   │   ├── context/           # 上下文窗口管理 + Token 计数
│   │   ├── memory/            # 会话存储 (SessionStore)
│   │   ├── prompt/            # 动态 Prompt 构建
│   │   ├── sandbox/           # Docker 沙箱管理
│   │   ├── config/            # Pydantic Settings
│   │   ├── observability/     # Prometheus 指标
│   │   └── skills/            # Skills 系统 (Phase 2)
│   └── tests/              # 单元测试 (50+ 用例)
├── pkg/                # Go 共享库
│   └── errcode/           # 统一业务错误码 (按 domain 拆分)
├── deploy/             # Docker Compose (开发 + 测试)
├── scripts/            # 开发脚本 (dev-up/down, proto-gen)
├── docs/               # 文档
│   ├── getting-started.md
│   └── arch/              # 架构设计文档 (D1-D9)
└── .github/workflows/  # CI/CD
```

## 前置依赖

| 工具 | 版本要求 | 安装方式 |
| --- | --- | --- |
| Go | >= 1.25 | [go.dev/dl](https://go.dev/dl/) |
| Python | >= 3.11 | [python.org](https://www.python.org/downloads/) |
| uv | 最新 | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker | >= 24.0 | [docker.com](https://docs.docker.com/get-docker/) |
| Buf | >= 1.28 | `brew install bufbuild/buf/buf` |
| Redis | 7.x | 通过 Docker Compose 提供 |
| PostgreSQL | 16.x | 通过 Docker Compose 提供 |

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/sahara-ai/sahara.git
cd sahara
```

### 2. 启动基础设施

```bash
# 启动 Redis + PostgreSQL (Docker Compose)
./scripts/dev-up.sh

# 验证服务就绪
docker compose -f deploy/docker-compose.yml ps
```

### 3. Proto 代码生成

```bash
# 需要先安装 buf: brew install bufbuild/buf/buf
./scripts/proto-gen.sh
```

### 4. 启动 Runtime (Python)

```bash
cd runtime

# 安装依赖 (使用 uv)
uv sync

# 配置环境变量 (可选, 有默认值)
export SAHARA_ANTHROPIC_API_KEY="your-api-key"   # 不设置则使用 Mock LLM

# 启动 gRPC 服务 (默认 :50051)
uv run python -m sahara_runtime.server
```

### 5. 启动 Gateway (Go)

```bash
cd gateway

# 启动 (默认 :8080)
go run ./cmd/sahara-gw
```

### 6. 验证

```bash
# Gateway 健康检查
curl http://localhost:8080/healthz

# Gateway 就绪检查 (含 Runtime 连通性)
curl http://localhost:8080/readyz

# Runtime Worker 状态
curl http://localhost:8080/healthz/runtime
```

### 7. WebSocket 测试

```bash
# 使用 websocat 或任意 WS 客户端连接
# 如果设置了 API_KEY, 需要带 token 参数
websocat ws://localhost:8080/ws

# 发送任务
{"type":"req","id":"r1","method":"agent.submit","params":{"sessionKey":"test-session","text":"你好"}}
```

## 环境变量

### Gateway

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ADDR` | `:8080` | HTTP/WS 监听地址 |
| `REDIS_URL` | `redis://localhost:6379` | Redis 连接地址 |
| `RUNTIME_ADDRS` | `localhost:50051` | Runtime Worker gRPC 地址 (逗号分隔) |
| `LOG_LEVEL` | `info` | 日志级别 (debug/info/warn/error) |
| `ALLOWED_ORIGINS` | `*` | WebSocket 允许的 Origin (逗号分隔, `*` 表示全部) |
| `API_KEY` | (空) | API Key 认证, 空则不启用 |

### Runtime

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SAHARA_GRPC_PORT` | `50051` | gRPC 监听端口 |
| `SAHARA_REDIS_URL` | `redis://localhost:6379` | Redis 连接地址 |
| `SAHARA_POSTGRES_DSN` | (空) | PostgreSQL 连接串 |
| `SAHARA_ANTHROPIC_API_KEY` | (空) | Anthropic API Key (逗号分隔支持多 Key) |
| `SAHARA_OPENAI_API_KEY` | (空) | OpenAI API Key |
| `SAHARA_DEFAULT_MODEL` | `claude-sonnet-4-20250514` | 默认 LLM 模型 |
| `SAHARA_MAX_CONCURRENT_TASKS` | `10` | 最大并发任务数 |
| `SAHARA_SANDBOX_ENABLED` | `false` | 是否启用 Docker 沙箱 |
| `SAHARA_LOG_LEVEL` | `INFO` | 日志级别 |

## 开发

### 常用命令

```bash
# 启动/停止基础设施
./scripts/dev-up.sh
./scripts/dev-down.sh

# Proto 重新生成 (修改 .proto 文件后)
./scripts/proto-gen.sh

# Gateway 编译检查
cd gateway && go build ./...

# Runtime 单元测试
cd runtime && uv run pytest

# Runtime 代码检查
cd runtime && uv run ruff check .
```

### Docker Compose 全栈启动

```bash
# 启动全部服务 (基础设施 + Gateway + Runtime + API)
docker compose -f deploy/docker-compose.yml --profile full up -d
```

## 技术栈

| 组件 | 技术 |
| --- | --- |
| Gateway | Go 1.25, `gorilla/websocket`, `go-redis/v9`, gRPC |
| API Service | Go 1.25, `net/http` (Phase 2) |
| Runtime | Python 3.11+, `grpcio` async, `anthropic`/`openai` SDK, `uvloop` |
| 事件传输 | Redis Streams |
| 存储 | Redis 7 (热数据) + PostgreSQL 16 (冷数据, pgvector) |
| 沙箱 | Docker + gVisor (runsc) |
| 可观测性 | Prometheus + Grafana + structlog + OpenTelemetry |
| CI/CD | GitHub Actions + Docker |

## 设计文档

| 编号 | 文档 | 内容 |
| --- | --- | --- |
| — | [TECH-PROPOSAL](docs/arch/TECH-PROPOSAL-C-END-REFACTOR.md) | 总体架构方案 |
| — | [ENGINEERING-PLAN](docs/arch/sahara/ENGINEERING-PLAN-C-END.md) | 分阶段工程计划 |
| D1 | [GRPC-PROTOCOL](docs/arch/sahara/GRPC-PROTOCOL-DESIGN.md) | gRPC 协议设计 |
| D2 | [WS-PROTOCOL](docs/arch/sahara/WS-PROTOCOL-DESIGN.md) | WebSocket 协议设计 |
| D3 | [GATEWAY-ARCH](docs/arch/sahara/GATEWAY-ARCHITECTURE-DESIGN.md) | Gateway 架构 |
| D4 | [RUNTIME-ARCH](docs/arch/sahara/RUNTIME-ARCHITECTURE-DESIGN.md) | Runtime 架构 |
| D5 | [EVENT-BUS](docs/arch/sahara/EVENT-BUS-DESIGN.md) | 异步事件传输协议 |
| D6 | [SANDBOX](docs/arch/sahara/SANDBOX-DESIGN.md) | 沙箱管理 |
| D7 | [API-SERVICE](docs/arch/sahara/API-SERVICE-DESIGN.md) | API Service 设计 |
| D8 | [OBSERVABILITY](docs/arch/sahara/OBSERVABILITY-DESIGN.md) | 可观测性设计 |
| D9 | [PLUGIN-SYSTEM](docs/arch/sahara/PLUGIN-SYSTEM-DESIGN.md) | Plugin 系统 (Phase 3) |

## License

Licensed under the [Apache License 2.0](LICENSE).
