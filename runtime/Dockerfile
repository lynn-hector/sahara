# ── Build ────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

# 安装 uv (快速包管理器)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# 先复制依赖文件以利用 Docker 缓存
COPY pyproject.toml ./
RUN uv pip install --system --no-cache -r pyproject.toml

# 复制源码
COPY . .

# ── Runtime ──────────────────────────────────────────────────
FROM python:3.12-slim

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 复制已安装的 Python 包
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

EXPOSE 50051

CMD ["python", "-m", "sahara_runtime.server"]
