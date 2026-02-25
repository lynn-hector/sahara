# 开发环境搭建指南

> 本文档帮助新成员在 30 分钟内搭建 Sahara 本地开发环境。

## 前置要求

| 工具 | 最低版本 | 用途 |
|------|---------|------|
| **Go** | 1.23+ | Gateway + API Service 开发 |
| **Python** | 3.11+ | Runtime 开发 |
| **uv** | latest | Python 包管理 (推荐) |
| **Docker** | 24+ | Redis/PG 基础设施 + 沙箱 |
| **Docker Compose** | v2+ | 本地环境编排 |
| **buf** | latest | Proto lint + 代码生成 |
| **grpcurl** | latest | gRPC 调试 (可选) |

### 安装命令参考

```bash
# macOS (Homebrew)
brew install go python@3.12 uv buf grpcurl
brew install --cask docker

# 或使用 Go 官方安装器
# https://go.dev/dl/
```

## 快速开始

### 1. 克隆项目

```bash
git clone <repo-url> && cd sahara
```

### 2. 启动基础设施

```bash
# 启动 Redis + PostgreSQL
./scripts/dev-up.sh

# 验证
docker compose -f deploy/docker-compose.yml ps
# redis     ... Up (healthy)
# postgres  ... Up (healthy)
```

### 3. Proto 代码生成

```bash
./scripts/proto-gen.sh

# 产出:
# gen/go/     → Go protobuf + gRPC 代码
# gen/python/ → Python protobuf + gRPC 代码
```

### 4. 启动 Gateway (Go)

```bash
cd gateway
go run ./cmd/sahara-gw

# 验证
curl http://localhost:8080/healthz
# {"status":"ok","service":"sahara-gw","version":"dev"}
```

### 5. 启动 Runtime (Python)

```bash
cd runtime

# 安装依赖 (首次)
uv pip install -e ".[dev]"

# 复制环境变量
cp .env.example .env
# 编辑 .env 填入 LLM API Key

# 启动
python -m sahara_runtime.server

# 验证 (需要安装 grpcurl)
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
# {"status":"SERVING"}
```

### 6. 验证端到端连通

```bash
# Gateway → Runtime gRPC Health Check
# (Phase 0 验收标准)
curl http://localhost:8080/healthz  # Gateway OK
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check  # Runtime OK
```

## 常用命令

```bash
# 启动/停止基础设施
./scripts/dev-up.sh
./scripts/dev-down.sh

# Proto 代码生成
./scripts/proto-gen.sh

# Go 构建
cd gateway && go build ./...
cd api && go build ./...

# Python 测试
cd runtime && pytest -v

# Python lint
cd runtime && ruff check . && ruff format --check .

# 完整服务编排 (包括应用服务)
docker compose -f deploy/docker-compose.yml --profile full up -d
```

## 项目结构

```text
sahara/
├── proto/           # gRPC Proto 定义 (Buf 管理)
├── gen/             # Proto 生成代码 (Go + Python)
├── gateway/         # Go Gateway 服务
├── api/             # Go API 服务
├── runtime/         # Python Runtime 服务
├── pkg/             # Go 共享包
├── deploy/          # Docker Compose + K8s
├── scripts/         # 开发脚本
└── docs/            # 文档
```

## 环境变量

### Gateway

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ADDR` | `:8080` | HTTP/WS 监听地址 |
| `REDIS_URL` | `redis://localhost:6379` | Redis 连接 |
| `RUNTIME_ADDRS` | `localhost:50051` | Runtime gRPC 地址 |
| `LOG_LEVEL` | `info` | 日志级别 |

### Runtime

所有环境变量使用 `SAHARA_` 前缀，参见 `runtime/.env.example`。

## 常见问题

**Q: Go 编译报错 `log/slog` 找不到？**
A: 需要 Go 1.21+。建议升级到 Go 1.23: `brew upgrade go`

**Q: Proto 生成失败？**
A: 确保安装了 `buf`: `brew install buf`

**Q: Runtime 启动报错找不到 `grpc_health`？**
A: 安装依赖: `uv pip install grpcio-health-checking`
