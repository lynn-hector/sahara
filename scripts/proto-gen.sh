#!/usr/bin/env bash
# =============================================================================
# proto-gen.sh — 从 proto/ 生成 Go + Python 代码
# 依赖: buf (https://buf.build/docs/installation)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$ROOT_DIR/proto"
GEN_DIR="$ROOT_DIR/gen"

echo "==> 清理旧生成代码..."
rm -rf "$GEN_DIR"
mkdir -p "$GEN_DIR/go" "$GEN_DIR/python"

echo "==> 检查 buf 版本..."
buf --version

echo "==> Lint proto 定义..."
(cd "$PROTO_DIR" && buf lint)

echo "==> 生成 Go + Python 代码..."
(cd "$PROTO_DIR" && buf generate)

# 复制生成代码到各服务目录 (符号链接方式，保持 gen/ 为唯一来源)
echo "==> 链接生成代码到 gateway/gen..."
rm -rf "$ROOT_DIR/gateway/gen"
ln -sf "$GEN_DIR/go" "$ROOT_DIR/gateway/gen"

echo "==> 链接生成代码到 api/gen..."
rm -rf "$ROOT_DIR/api/gen"
ln -sf "$GEN_DIR/go" "$ROOT_DIR/api/gen"

echo "==> 链接生成代码到 runtime/gen..."
rm -rf "$ROOT_DIR/runtime/gen"
ln -sf "$GEN_DIR/python" "$ROOT_DIR/runtime/gen"

echo "==> Proto 代码生成完成!"
echo "    Go:     $GEN_DIR/go/"
echo "    Python: $GEN_DIR/python/"
