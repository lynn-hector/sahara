#!/usr/bin/env bash
# =============================================================================
# dev-up.sh — 一键启动本地开发环境
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> 启动基础设施 (Redis + PostgreSQL)..."
docker compose -f "$ROOT_DIR/deploy/docker-compose.yml" up -d redis postgres

echo "==> 等待服务就绪..."
sleep 2

echo "==> 基础设施已启动:"
echo "    Redis:      localhost:6379"
echo "    PostgreSQL:  localhost:5432 (sahara/sahara_dev)"
echo ""
echo "启动 Gateway:  cd gateway && go run ./cmd/sahara-gw"
echo "启动 Runtime:  cd runtime && uv run python -m sahara_runtime.server"
