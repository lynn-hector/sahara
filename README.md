# Sahara

> 新一代 AI Agent 平台 —— 面向 C 端高并发场景的分布式 Agent 架构。

全新架构，将 Gateway 与 Agent Runtime 分别部署独立进程，通过 gRPC 同步调度 + Event Bus 异步事件实现高并发、可水平扩展的 Agent 服务。

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
  │  Runtime     │──▶│  Event Bus   │               │
  │  Worker (Py) │   │  (Redis/NATS)│               │
  └─────────────┘   └──────────────┘               │
         │                                          │
         └────────────────┬─────────────────────────┘
                   ┌──────┴──────┐
                   │  State Store │
                   │  Redis + PG  │
                   └─────────────┘
```

**两条独立通信路径：**

| 路径 | 方向 | 协议 | 用途 |
| --- | --- | --- | --- |
| **路径 A** | Gateway → Runtime | gRPC | 提交任务、中止任务、等待结果 |
| **路径 B** | Runtime → Event Bus → Gateway | MQ (pub/sub) | LLM 流式 delta、工具调用、生命周期事件 |

## 核心模块

| 模块 | 语言 | 职责 |
| --- | --- | --- |
| **Gateway** (`sahara-gw`) | Go | WS 连接管理、实时事件推送、认证鉴权、消息路由、gRPC 调度分发、事件广播 |
| **API Service** (`sahara-api`) | Go | C 端 RESTful API：用户注册登录、个人信息、会话 CRUD、文件上传、OAuth、配额查询 |
| **Agent Runtime** (`sahara-rt`) | Python | LLM 调用（自建交互循环）、工具执行、沙箱管理、会话持久化、事件发射 |
| **Event Bus** (`sahara-eb`) | Redis Streams / NATS | 事件接收、路由分发、150ms 限频聚合、内容安全 Pipeline、持久化 |
| **State Store** | Redis + PostgreSQL | 会话热/冷存储、用户数据、配置中心、路由表、分布式锁 |

## 技术栈

### Gateway (Go)

| 组件 | 技术选型 |
| --- | --- |
| WebSocket | `nhooyr/websocket` |
| HTTP | `net/http` 标准库 |
| gRPC | `google.golang.org/grpc` |
| Redis | `go-redis/redis` v9 |
| MQ | `nats.go` / Redis Streams |
| JWT | `golang-jwt/jwt` v5 |
| 日志 | `slog` (Go 1.21+) |
| 指标 | `prometheus/client_golang` |

### Agent Runtime (Python)

| 组件 | 技术选型 |
| --- | --- |
| gRPC | `grpcio` (async) |
| LLM SDK | `openai` / `anthropic` 官方 SDK |
| 异步 | `asyncio` + `uvloop` |
| 沙箱 | `docker-py` + 容器池 |
| 会话存储 | `redis.asyncio` + `asyncpg` |
| 事件发射 | `nats-py` / `redis.asyncio` |
| Token 计数 | `tiktoken` |
| 配置 | `pydantic-settings` |
| 指标 | `prometheus-client` |

## 项目结构

```text
sahara/
├── docs/                    # 架构设计与技术文档
│   └── arch/                # 架构方案
├── gateway/                 # sahara-gw: Gateway 服务 (Go) — WS 实时通信
├── api/                     # sahara-api: API 服务 (Go) — RESTful 用户/会话接口
├── runtime/                 # sahara-rt: Agent Runtime Worker (Python)
├── proto/                   # gRPC Proto 定义 (共享)
├── pkg/                     # Go 共享包 (gateway & api 公用: auth, model, store)
├── deploy/                  # 部署配置 (Docker Compose / K8s)
└── scripts/                 # 工具脚本
```

## 快速开始

### 前置要求

- Go 1.21+
- Python 3.11+
- Docker & Docker Compose
- Redis 7+
- PostgreSQL 15+
- NATS Server (可选，也可用 Redis Streams)

### 本地开发

```bash
# 克隆项目
git clone <repo-url> && cd sahara

# 启动基础设施 (Redis + PostgreSQL + NATS)
docker compose -f deploy/docker-compose.dev.yml up -d

# 启动 Gateway (Go)
cd gateway && go run ./cmd/sahara-gw

# 启动 API Service (Go)
cd api && go run ./cmd/sahara-api

# 启动 Runtime Worker (Python)
cd runtime && pip install -r requirements.txt && python -m runtime.main
```

## 设计文档

详细架构设计请参阅：

- [C 端架构重构技术方案](docs/arch/TECH-PROPOSAL-C-END-REFACTOR.md) — 总体架构、模块选型、容量规划与成本分析
- [工程计划](docs/arch/sahara/ENGINEERING-PLAN-C-END.md) — 分阶段交付计划
- [WebSocket 协议设计](docs/arch/sahara/WS-PROTOCOL-DESIGN.md) — Client ↔ Gateway 实时通信协议
- [gRPC 协议设计](docs/arch/sahara/GRPC-PROTOCOL-DESIGN.md) — Gateway ↔ Runtime 内部通信
- [Gateway 架构设计](docs/arch/sahara/GATEWAY-ARCHITECTURE-DESIGN.md) — Go Gateway 实现蓝图
- [API Service 设计](docs/arch/sahara/API-SERVICE-DESIGN.md) — C 端 RESTful API 服务
- [Runtime 架构设计](docs/arch/sahara/RUNTIME-ARCHITECTURE-DESIGN.md) — Python Runtime 实现蓝图
- [Event Bus 设计](docs/arch/sahara/EVENT-BUS-DESIGN.md) — 异步事件传递架构
- [Sandbox 设计](docs/arch/sahara/SANDBOX-DESIGN.md) — 沙箱管理与演进


## License

Private — All rights reserved.
