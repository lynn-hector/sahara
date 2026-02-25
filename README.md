# Sahara

> C 端高并发 AI Agent 平台 — Go Gateway + Python Runtime 分布式架构。

---

## 架构总览

```text
                      C 端用户 (Web / App / 小程序 / API)
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
├── proto/           # gRPC Proto 定义 (Buf 管理)
│   └── sahara/
│       ├── agent/v1/   # AgentService (任务管理)
│       ├── worker/v1/  # WorkerService (运维管理)
│       ├── event/v1/   # AgentEvent (异步事件)
│       └── common/v1/  # 共享类型
├── gen/             # Proto 生成代码 (Go + Python)
├── gateway/         # sahara-gw: Gateway 服务 (Go)
│   ├── cmd/sahara-gw/  # 入口
│   └── internal/       # WS / Dispatch / Broadcast / Config
├── api/             # sahara-api: API 服务 (Go)
│   ├── cmd/sahara-api/  # 入口
│   └── internal/        # Handler / Service
├── runtime/         # sahara-rt: Agent Runtime (Python)
│   ├── sahara_runtime/  # 主包 (15 个子模块)
│   └── tests/
├── pkg/             # Go 共享包 (errcode, middleware, model, store)
├── deploy/          # Docker Compose + K8s
├── scripts/         # 开发脚本
├── docs/            # 文档
│   ├── getting-started.md
│   └── arch/            # 架构设计文档 (D1-D9)
└── .github/workflows/   # CI/CD
```

## 快速开始

```bash
# 1. 启动基础设施
./scripts/dev-up.sh

# 2. Proto 代码生成
./scripts/proto-gen.sh

# 3. 启动 Gateway
cd gateway && go run ./cmd/sahara-gw

# 4. 启动 Runtime
cd runtime && uv pip install -e ".[dev]" && python -m sahara_runtime.server

# 5. 验证
curl http://localhost:8080/healthz
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
```

详细指南: [docs/getting-started.md](docs/getting-started.md)

## 技术栈

| 组件 | 技术 |
| --- | --- |
| Gateway | Go, `nhooyr/websocket`, `go-redis/redis` v9, gRPC |
| API Service | Go, `net/http`, JWT |
| Runtime | Python 3.12+, `grpcio` async, `anthropic`/`openai` SDK |
| 事件传输 | Redis Streams (Phase 1-2), NATS JetStream (Phase 3 可选) |
| 存储 | Redis 7 (热数据) + PostgreSQL 16 (冷数据 + pgvector) |
| 沙箱 | Docker + gVisor (runsc) |
| 可观测性 | Prometheus + Grafana + structlog + OpenTelemetry |
| CI/CD | GitHub Actions + Docker + Kubernetes |

## 设计文档

| 编号 | 文档 | 内容 |
| --- | --- | --- |
| — | [TECH-PROPOSAL](docs/arch/TECH-PROPOSAL-C-END-REFACTOR.md) | 总体架构方案 |
| — | [ENGINEERING-PLAN](docs/arch/sahara/ENGINEERING-PLAN-C-END.md) | 分阶段工程计划 |
| D1 | [GRPC-PROTOCOL](docs/arch/sahara/GRPC-PROTOCOL-DESIGN.md) | gRPC 协议设计 |
| D2 | [WS-PROTOCOL](docs/arch/sahara/WS-PROTOCOL-DESIGN.md) | WebSocket 协议设计 |
| D3 | [GATEWAY-ARCH](docs/arch/sahara/GATEWAY-ARCHITECTURE-DESIGN.md) | Gateway 架构 |
| D4 | [RUNTIME-ARCH](docs/arch/sahara/RUNTIME-ARCHITECTURE-DESIGN.md) | Runtime 架构 (20 模块) |
| D5 | [EVENT-BUS](docs/arch/sahara/EVENT-BUS-DESIGN.md) | 异步事件传输协议 |
| D6 | [SANDBOX](docs/arch/sahara/SANDBOX-DESIGN.md) | 沙箱管理 |
| D7 | [API-SERVICE](docs/arch/sahara/API-SERVICE-DESIGN.md) | API Service 设计 |
| D8 | [OBSERVABILITY](docs/arch/sahara/OBSERVABILITY-DESIGN.md) | 可观测性设计 |
| D9 | [PLUGIN-SYSTEM](docs/arch/sahara/PLUGIN-SYSTEM-DESIGN.md) | Plugin 系统 (Phase 3) |

## License

Private — All rights reserved.
