#!/usr/bin/env bash
# =============================================================================
# dev-down.sh — 停止本地开发环境
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> 停止所有服务..."
docker compose -f "$ROOT_DIR/deploy/docker-compose.yml" down

echo "==> 已停止"
