# ── Build ────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

# 先复制依赖文件以利用 Docker 缓存
COPY go.mod go.sum ./
RUN go mod download

# 复制源码
COPY . .

# 构建二进制 (静态链接, 注入版本)
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w -X main.Version=${VERSION}" \
    -o /sahara-gw ./cmd/sahara-gw

# ── Runtime ──────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /sahara-gw /usr/local/bin/sahara-gw

EXPOSE 8080
ENTRYPOINT ["sahara-gw"]
