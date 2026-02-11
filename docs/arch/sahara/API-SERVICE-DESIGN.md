# API Service 架构设计（sahara-api）

> C 端用户的 RESTful HTTP API 服务。
> 承载所有**无需长连接**的功能性接口：用户注册登录、个人信息、会话 CRUD、文件上传、配额查询、OAuth 等。
> 与 sahara-gw (WebSocket Gateway) 分离部署，共享存储层与认证体系。
>
> 关联文档：
> - [Gateway 架构设计](./GATEWAY-ARCHITECTURE-DESIGN.md) — WebSocket 实时通信（D3）
> - [WebSocket 协议设计](./WS-PROTOCOL-DESIGN.md) — Client ↔ Gateway 帧协议（D2）
> - [工程计划](./ENGINEERING-PLAN-C-END.md) — 分阶段交付计划

---

## 目录

1. [定位与边界](#一定位与边界)
2. [架构总览](#二架构总览)
3. [与 Gateway 的职责划分](#三与-gateway-的职责划分)
4. [技术选型](#四技术选型)
5. [工程结构](#五工程结构)
6. [API 设计规范](#六api-设计规范)
7. [模块设计](#七模块设计)
   - [7.1 认证模块](#71-认证模块)
   - [7.2 用户模块](#72-用户模块)
   - [7.3 会话模块](#73-会话模块)
   - [7.4 文件上传模块](#74-文件上传模块)
   - [7.5 配额与用量模块](#75-配额与用量模块)
   - [7.6 系统模块](#76-系统模块)
8. [API 路由总表](#八api-路由总表)
9. [认证与鉴权](#九认证与鉴权)
10. [数据模型](#十数据模型)
11. [错误处理](#十一错误处理)
12. [中间件栈](#十二中间件栈)
13. [共享包设计 (pkg/)](#十三共享包设计-pkg)
14. [部署架构](#十四部署架构)
15. [可观测性](#十五可观测性)
16. [分阶段交付](#十六分阶段交付)

---

## 一、定位与边界

### 1.1 为什么需要独立 API 服务

C 端产品不仅需要 WebSocket 实时通信，还需要一整套 HTTP API 支撑：

| 场景 | 说明 | 为什么不放在 Gateway |
| --- | --- | --- |
| 用户注册/登录 | 在建立 WS 连接之前就需要获取 JWT | WS 连接尚未建立 |
| OAuth 三方登录 | 需要 HTTP 重定向流程 | WS 不支持重定向 |
| 个人信息管理 | 头像上传、昵称修改、密码重置 | 与实时通信无关 |
| 会话列表/历史 | 页面初次加载获取数据 | HTTP GET 更适合（SEO、缓存、CDN） |
| 文件上传 | 大文件上传、分片上传 | WS 帧上限 1MB，不适合传 binary |
| 配额查询 | 查询用量、订阅套餐 | 低频操作，无需长连接 |
| Token 刷新 | Refresh Token → 新 Access Token | HTTP-only Cookie 更安全 |
| Webhook/回调 | 支付回调、OAuth 回调 | 第三方只支持 HTTP |

### 1.2 设计原则

| # | 原则 | 说明 |
| --- | --- | --- |
| P1 | **Gateway 只做实时** | WS 长连接 + 事件推送，不承载 CRUD |
| P2 | **API 只做请求-响应** | 无状态 HTTP，水平扩展无压力 |
| P3 | **共享存储与认证** | 两个服务共用 Redis + PostgreSQL + JWT 签名密钥 |
| P4 | **共享 Go 包** | `pkg/` 目录放公共代码（auth、model、store），避免重复 |
| P5 | **前端单一域名** | 通过 LB 路径路由：`/ws` → Gateway，`/api/*` → API Service |

---

## 二、架构总览

```text
                        C 端用户 (Web / App / 小程序)
                                  │
                         ┌────────┴────────┐
                         │  Load Balancer   │
                         └────────┬────────┘
                          /ws     │    /api/*
                    ┌─────────────┼─────────────┐
                    ▼                            ▼
          ┌──────────────┐             ┌──────────────────┐
          │  sahara-gw   │             │  sahara-api       │
          │  WebSocket   │             │  RESTful HTTP     │
          │  实时事件推送 │             │  用户/会话/文件   │
          └──────┬───────┘             └──────┬───────────┘
                 │                            │
          gRPC   │                            │ SQL + Redis
                 ▼                            ▼
          ┌─────────────┐            ┌──────────────────┐
          │  sahara-rt   │            │  State Store      │
          │  Runtime     │            │  Redis + PG       │
          └─────────────┘            └──────────────────┘
                 │                            ▲
                 │   publish                  │
                 ▼                            │
          ┌──────────────┐                    │
          │  Event Bus   │────────────────────┘
          │  (Redis/NATS)│   事件持久化
          └──────────────┘
```

**关键点**：
- 前端通过**同一域名**访问两个服务：LB 按路径 `/ws` vs `/api/*` 分流
- 两个 Go 服务共享 JWT 签名密钥，sahara-api 签发 Token，sahara-gw 验证 Token
- 两个服务共享 PostgreSQL 和 Redis，数据模型一致

---

## 三、与 Gateway 的职责划分

```text
┌────────────────────────────────────────────────────────────────────────┐
│                         职责边界对照表                                  │
├────────────────────────────────┬───────────────────────────────────────┤
│  sahara-gw (Gateway)           │  sahara-api (API Service)            │
├────────────────────────────────┼───────────────────────────────────────┤
│  WS 连接管理                   │  用户注册/登录/注销                  │
│  WS 帧解析 + RPC 路由          │  JWT 签发 + Refresh Token 管理       │
│  agent.submit / agent.abort    │  OAuth 三方登录 (微信/GitHub/Google) │
│  事件推送 (delta/tool/complete)│  会话 CRUD (list/get/create/delete)  │
│  断线恢复 + 事件回放           │  会话历史消息查询 (分页)            │
│  JWT 验证 (连接阶段)           │  用户信息管理 (profile/avatar)      │
│  连接级 + 用户级限频           │  文件上传/下载 (头像/附件)          │
│  心跳 (tick)                   │  用量/配额查询                      │
│  auth.refresh (WS 帧内续期)    │  系统公告/维护状态查询              │
│  OpenAI 兼容 SSE API           │  Webhook 回调 (支付/OAuth)          │
├────────────────────────────────┼───────────────────────────────────────┤
│  有状态 (长连接)               │  无状态 (请求-响应)                 │
│  goroutine per conn            │  goroutine per request              │
│  扩展瓶颈: 内存 (连接数)       │  扩展瓶颈: CPU (几乎无)            │
└────────────────────────────────┴───────────────────────────────────────┘
```

> **重叠区域处理**：
>
> 1. **会话操作**：WS 协议中也定义了 `session.list` / `session.get` 等方法（参见 D2 §6），但这些是**轻量快捷方式**——已有 WS 连接的客户端无需额外 HTTP 请求。sahara-api 中的会话 API 是**完整版本**，支持分页、过滤、批量操作等 HTTP 天然优势。
> 2. **Token 刷新**：sahara-api 提供 `/api/auth/refresh` HTTP 接口，sahara-gw 提供 `auth.refresh` WS 方法，两条路径最终都更新同一份 Token。D2 §5.3 的时序图展示了两者的配合。
> 3. **用户信息**：WS 协议中的 `user.profile` / `user.usage` 是 Phase 2 的快捷方式，sahara-api 是完整实现。

---

## 四、技术选型

| 组件 | 选型 | 理由 |
| --- | --- | --- |
| HTTP 框架 | **chi** (`go-chi/chi` v5) | 兼容 `net/http`，零分配路由，中间件链丰富；API 路由较多（~30 条），需要路径参数和路由组 |
| ORM / 查询 | **sqlc** (生成) + `pgx` (驱动) | 类型安全、零反射、编译期校验 SQL；适合 CRUD 密集场景 |
| 验证 | `go-playground/validator` v10 | 结构体 tag 验证，前端错误消息友好 |
| JWT | `golang-jwt/jwt` v5 | 与 Gateway 共用 `pkg/auth` |
| Redis | `go-redis/redis` v9 | 与 Gateway 共用；缓存、Token 黑名单、Rate Limit |
| 文件存储 | S3 兼容 (MinIO / AWS S3) | 头像、附件、Agent 产物统一存储 |
| 密码哈希 | `bcrypt` (标准库 `golang.org/x/crypto`) | 行业标准，自动加盐 |
| 配置 | `koanf` | 支持 YAML + 环境变量 + 热更新 |
| 日志 | `slog` (Go 1.21+) | 与 Gateway 统一 |
| 指标 | `prometheus/client_golang` | 与 Gateway 统一 |
| API 文档 | OpenAPI 3.1 (`swaggo/swag` 生成) | 前端团队自助对接 |

> **为什么 API 用 chi 而 Gateway 用 net/http？**
>
> Gateway 的 HTTP 端点只有 5 个（`/healthz`、`/readyz`、`/ws`、`/v1/chat/completions`、`/metrics`），标准库足够。
> sahara-api 有 ~30 条 RESTful 路由，需要路径参数（`/api/sessions/:id`）、路由组（`/api/v1/`）、中间件链（auth、CORS、logging、recovery）等，chi 是最轻量且完全兼容 `net/http` 的选择。

---

## 五、工程结构

```text
api/
├── cmd/
│   └── sahara-api/
│       └── main.go               # 入口: 加载配置 → 组装依赖 → 启动 HTTP
├── internal/
│   ├── server.go                 # chi 路由注册 + 中间件栈
│   ├── handler/                  # HTTP Handler 层 (接收请求、调用 service、返回响应)
│   │   ├── auth.go               # 注册、登录、刷新、登出
│   │   ├── oauth.go              # OAuth 三方登录
│   │   ├── user.go               # 用户信息、头像
│   │   ├── session.go            # 会话 CRUD + 历史消息
│   │   ├── file.go               # 文件上传/下载
│   │   ├── usage.go              # 配额与用量
│   │   └── system.go             # 公告、维护状态
│   ├── service/                  # 业务逻辑层 (不依赖 HTTP)
│   │   ├── auth_service.go       # 注册/登录逻辑、密码哈希、Token 生成
│   │   ├── oauth_service.go      # OAuth 流程
│   │   ├── user_service.go       # 用户 CRUD
│   │   ├── session_service.go    # 会话 CRUD + 消息查询
│   │   ├── file_service.go       # 文件上传到 S3
│   │   └── usage_service.go      # 用量统计
│   └── dto/                      # 请求/响应 DTO 定义 + 验证 tag
│       ├── auth.go
│       ├── user.go
│       ├── session.go
│       └── common.go             # 分页、排序等通用结构
├── go.mod                        # 依赖管理 (引用 ../pkg)
├── go.sum
├── Dockerfile
└── config.yaml                   # 默认配置
```

**依赖关系**：

```text
api/internal/handler  →  api/internal/service  →  pkg/store (PG + Redis)
                                                    pkg/auth  (JWT)
                                                    pkg/model (领域模型)
```

Handler 层只做 HTTP 协议转换（解析请求体、调用 service、序列化响应），**业务逻辑全部在 service 层**。service 层不依赖 HTTP，可被 Gateway 的 WS handler 复用（如 `session.list` 的 WS 方法可直接调用 `SessionService.List()`）。

---

## 六、API 设计规范

### 6.1 URL 结构

```text
https://api.sahara.com/api/v1/{resource}[/{id}][/{sub-resource}]
```

| 规则 | 示例 |
| --- | --- |
| 资源名用复数 | `/api/v1/sessions`，不是 `/session` |
| 嵌套最多两层 | `/api/v1/sessions/{id}/messages` |
| 版本号放路径 | `/api/v1/`，方便多版本共存 |
| 操作动词用 HTTP method | `POST /sessions` (创建)，`DELETE /sessions/:id` (删除) |

### 6.2 统一响应格式

**成功**：

```json
{
  "code": 200,
  "data": { ... },
  "meta": {
    "requestId": "req_01JKXYZ...",
    "timestamp": 1706000000000
  }
}
```

**分页**：

```json
{
  "code": 200,
  "data": {
    "items": [ ... ],
    "pagination": {
      "total": 120,
      "page": 1,
      "pageSize": 20,
      "hasMore": true
    }
  }
}
```

**失败**：

```json
{
  "code": 400,
  "error": {
    "reason": "VALIDATION_ERROR",
    "message": "邮箱格式不正确",
    "details": {
      "field": "email",
      "rule": "required,email"
    }
  },
  "meta": {
    "requestId": "req_01JKXYZ..."
  }
}
```

> **与 WS 协议一致**：`code`（数字状态码）+ `error.reason`（字符串错误原因码）的设计与 D2 WS 协议帧格式保持一致，前端团队学一次即可。

### 6.3 通用查询参数

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `page` | integer | 页码（从 1 开始） |
| `pageSize` | integer | 每页数量（默认 20，最大 100） |
| `sortBy` | string | 排序字段 |
| `sortOrder` | `asc` / `desc` | 排序方向 |

---

## 七、模块设计

### 7.1 认证模块

#### 注册

```text
POST /api/v1/auth/register
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "StrongP@ss123",
  "nickname": "张三"
}
```

```json
{
  "code": 200,
  "data": {
    "userId": "user_01JKXYZ...",
    "accessToken": "eyJhbGciOi...",
    "refreshToken": "rt_01JKXYZ...",
    "expiresIn": 900
  }
}
```

#### 登录

```text
POST /api/v1/auth/login
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "StrongP@ss123"
}
```

响应结构同注册。

#### Token 刷新

```text
POST /api/v1/auth/refresh
Content-Type: application/json

{
  "refreshToken": "rt_01JKXYZ..."
}
```

```json
{
  "code": 200,
  "data": {
    "accessToken": "eyJhbGciOi...(新)",
    "refreshToken": "rt_02ABCDE...(旋转)",
    "expiresIn": 900
  }
}
```

> **Refresh Token 旋转**：每次刷新都返回新的 refreshToken，旧的立即失效。
> 这是防止 Token 泄露的最佳实践——即使 refreshToken 被截获，攻击者和合法用户只有一方能使用下一轮 Token，另一方会触发异常，系统可据此吊销该 Token 族。

#### 登出

```text
POST /api/v1/auth/logout
Authorization: Bearer <accessToken>
```

- 将当前 Access Token 加入 Redis 黑名单（TTL = Token 剩余有效期）
- 吊销关联的 Refresh Token

### 7.2 用户模块

#### 获取个人信息

```text
GET /api/v1/user/profile
Authorization: Bearer <accessToken>
```

```json
{
  "code": 200,
  "data": {
    "userId": "user_01JKXYZ...",
    "email": "user@example.com",
    "nickname": "张三",
    "avatar": "https://files.sahara.com/avatars/user_01JKXYZ.jpg",
    "roles": ["user"],
    "createdAt": 1706000000000,
    "quota": {
      "plan": "free",
      "maxConcurrentSessions": 5,
      "maxSubmitsPerDay": 100,
      "used": {
        "submitsToday": 23
      }
    }
  }
}
```

#### 更新个人信息

```text
PATCH /api/v1/user/profile
Authorization: Bearer <accessToken>
Content-Type: application/json

{
  "nickname": "新昵称"
}
```

#### 上传头像

```text
POST /api/v1/user/avatar
Authorization: Bearer <accessToken>
Content-Type: multipart/form-data

file: <image binary>
```

```json
{
  "code": 200,
  "data": {
    "avatar": "https://files.sahara.com/avatars/user_01JKXYZ.jpg"
  }
}
```

#### 修改密码

```text
POST /api/v1/user/password
Authorization: Bearer <accessToken>
Content-Type: application/json

{
  "oldPassword": "OldP@ss123",
  "newPassword": "NewP@ss456"
}
```

### 7.3 会话模块

#### 创建会话

```text
POST /api/v1/sessions
Authorization: Bearer <accessToken>
Content-Type: application/json

{
  "title": "新对话",
  "agentId": "main"
}
```

```json
{
  "code": 200,
  "data": {
    "sessionKey": "sess_01JKXYZ...",
    "title": "新对话",
    "agentId": "main",
    "createdAt": 1706000000000
  }
}
```

#### 会话列表

```text
GET /api/v1/sessions?page=1&pageSize=20&sortBy=lastMessageAt&sortOrder=desc
Authorization: Bearer <accessToken>
```

```json
{
  "code": 200,
  "data": {
    "items": [
      {
        "sessionKey": "sess_01JKXYZ...",
        "title": "快速排序算法",
        "agentId": "main",
        "lastMessageAt": 1706000000000,
        "messageCount": 12,
        "isActive": false
      }
    ],
    "pagination": {
      "total": 45,
      "page": 1,
      "pageSize": 20,
      "hasMore": true
    }
  }
}
```

#### 获取会话详情（含历史消息）

```text
GET /api/v1/sessions/:sessionKey?includeHistory=true&historyPage=1&historyPageSize=50
Authorization: Bearer <accessToken>
```

```json
{
  "code": 200,
  "data": {
    "session": {
      "sessionKey": "sess_01JKXYZ...",
      "title": "快速排序算法",
      "agentId": "main",
      "createdAt": 1706000000000,
      "lastMessageAt": 1706000100000
    },
    "history": {
      "items": [
        {
          "id": "msg_01JK...",
          "role": "user",
          "content": "帮我写一个快速排序",
          "ts": 1706000000000,
          "attachments": []
        },
        {
          "id": "msg_02JK...",
          "role": "assistant",
          "content": [
            { "type": "text", "text": "好的，我来帮你实现..." },
            { "type": "file", "url": "https://files.sahara.com/runs/run_01JK/quicksort.py",
              "filename": "quicksort.py", "mimeType": "text/x-python" }
          ],
          "ts": 1706000001000,
          "toolCalls": [
            { "name": "write", "file": "quicksort.py" }
          ],
          "usage": {
            "inputTokens": 1200,
            "outputTokens": 850
          }
        }
      ],
      "pagination": {
        "total": 12,
        "page": 1,
        "pageSize": 50,
        "hasMore": false
      }
    }
  }
}
```

> **多模态历史消息**：assistant 的 `content` 字段可以是字符串（纯文本）或 Content Block 数组（多模态），
> 与 D2 WS 协议中 `agent.run_complete` 的 `content` 格式一致。

#### 重命名会话

```text
PATCH /api/v1/sessions/:sessionKey
Authorization: Bearer <accessToken>
Content-Type: application/json

{
  "title": "新标题"
}
```

#### 删除会话

```text
DELETE /api/v1/sessions/:sessionKey
Authorization: Bearer <accessToken>
```

#### 批量删除

```text
POST /api/v1/sessions/batch-delete
Authorization: Bearer <accessToken>
Content-Type: application/json

{
  "sessionKeys": ["sess_01JKXYZ...", "sess_02ABCDE..."]
}
```

### 7.4 文件上传模块

#### 上传附件（用于 Agent 对话）

```text
POST /api/v1/files/upload
Authorization: Bearer <accessToken>
Content-Type: multipart/form-data

file: <binary>
purpose: "attachment"   // attachment | avatar
```

```json
{
  "code": 200,
  "data": {
    "fileId": "file_01JKXYZ...",
    "url": "https://files.sahara.com/tmp/file_01JKXYZ.csv",
    "filename": "data.csv",
    "mimeType": "text/csv",
    "sizeBytes": 12345,
    "expiresAt": 1706003600000
  }
}
```

> **流程**：前端先通过此接口上传文件获取 URL，然后在 WS 的 `agent.submit` 中将 URL 放入 `params.attachments`。
> 这与 D2 §3.4.1 中"WS 帧永远不传 binary 数据"的原则一致。

#### 上传限制

| 限制 | 值 | 说明 |
| --- | --- | --- |
| 单文件大小 | 50MB | attachment 类型 |
| 头像大小 | 5MB | avatar 类型 |
| 每用户每天上传 | 200 个文件 | 防滥用 |
| 临时文件过期 | 24 小时 | 未被 Agent 引用的临时文件自动清理 |
| 支持格式 | 图片、文档、代码、CSV、JSON | 可配置白名单 |

### 7.5 配额与用量模块

#### 查询用量

```text
GET /api/v1/usage?period=today
Authorization: Bearer <accessToken>
```

```json
{
  "code": 200,
  "data": {
    "period": "today",
    "submits": 23,
    "totalTokens": 45000,
    "inputTokens": 25000,
    "outputTokens": 20000,
    "fileUploads": 5,
    "quota": {
      "plan": "free",
      "maxSubmitsPerDay": 100,
      "maxTokensPerDay": 500000,
      "maxFileUploadsPerDay": 200
    }
  }
}
```

#### 查询历史用量

```text
GET /api/v1/usage/history?startDate=2026-02-01&endDate=2026-02-09&granularity=day
Authorization: Bearer <accessToken>
```

### 7.6 系统模块

#### 健康检查

```text
GET /api/v1/system/health       // 无认证
```

#### 系统公告

```text
GET /api/v1/system/announcements
Authorization: Bearer <accessToken>
```

```json
{
  "code": 200,
  "data": {
    "items": [
      {
        "id": "ann_01JK...",
        "title": "系统升级通知",
        "content": "系统将于 2026-02-10 02:00 进行维护升级...",
        "level": "warning",
        "createdAt": 1706000000000,
        "expiresAt": 1706100000000
      }
    ]
  }
}
```

---

## 八、API 路由总表

```text
┌────────────────────────────────────────────────────────────────────────┐
│  sahara-api 路由总表                                                    │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  认证 (无需 JWT)                                                      │
│  POST   /api/v1/auth/register                Phase 1                  │
│  POST   /api/v1/auth/login                   Phase 1                  │
│  POST   /api/v1/auth/refresh                 Phase 1                  │
│  POST   /api/v1/auth/logout                  Phase 1                  │
│  POST   /api/v1/auth/forgot-password         Phase 2                  │
│  POST   /api/v1/auth/reset-password          Phase 2                  │
│                                                                        │
│  OAuth (无需 JWT)                                                     │
│  GET    /api/v1/oauth/:provider/authorize    Phase 2                  │
│  GET    /api/v1/oauth/:provider/callback     Phase 2                  │
│                                                                        │
│  用户 (需 JWT)                                                        │
│  GET    /api/v1/user/profile                 Phase 1                  │
│  PATCH  /api/v1/user/profile                 Phase 1                  │
│  POST   /api/v1/user/avatar                  Phase 1                  │
│  POST   /api/v1/user/password                Phase 2                  │
│                                                                        │
│  会话 (需 JWT)                                                        │
│  POST   /api/v1/sessions                     Phase 1                  │
│  GET    /api/v1/sessions                     Phase 1                  │
│  GET    /api/v1/sessions/:sessionKey         Phase 1                  │
│  PATCH  /api/v1/sessions/:sessionKey         Phase 1                  │
│  DELETE /api/v1/sessions/:sessionKey         Phase 1                  │
│  POST   /api/v1/sessions/batch-delete        Phase 2                  │
│                                                                        │
│  文件 (需 JWT)                                                        │
│  POST   /api/v1/files/upload                 Phase 1                  │
│  GET    /api/v1/files/:fileId                Phase 1                  │
│                                                                        │
│  用量 (需 JWT)                                                        │
│  GET    /api/v1/usage                        Phase 2                  │
│  GET    /api/v1/usage/history                Phase 2                  │
│                                                                        │
│  系统 (部分无需 JWT)                                                  │
│  GET    /api/v1/system/health                Phase 1 (无 JWT)        │
│  GET    /api/v1/system/announcements         Phase 2                  │
│                                                                        │
│  运维 (内部网络)                                                      │
│  GET    /healthz                             Phase 1                  │
│  GET    /readyz                              Phase 1                  │
│  GET    /metrics                             Phase 1                  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 九、认证与鉴权

### 9.1 Token 体系

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Token 生命周期                                                      │
│                                                                      │
│  用户登录/注册                                                       │
│       │                                                              │
│       ▼                                                              │
│  sahara-api 生成:                                                    │
│  ┌──────────────────┐  ┌──────────────────────────┐                  │
│  │ Access Token     │  │ Refresh Token            │                  │
│  │ JWT (RS256)      │  │ 不透明字符串              │                  │
│  │ TTL: 15 分钟     │  │ TTL: 30 天               │                  │
│  │ 载荷: sub,roles, │  │ 存储: Redis hash          │                  │
│  │   quota          │  │ 关联: userId, deviceId,   │                  │
│  └──────────────────┘  │   tokenFamily             │                  │
│         │              └──────────────────────────┘                  │
│         │                                                            │
│         ▼                                                            │
│  sahara-gw 验证:                                                     │
│  ┌──────────────────┐                                                │
│  │ WS Upgrade 阶段   │                                                │
│  │ 验证 JWT 签名     │ ← 使用与 api 相同的公钥                       │
│  │ 提取 claims       │                                                │
│  └──────────────────┘                                                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 9.2 JWT Claims（与 D2 §5.2 一致）

```json
{
  "sub": "user_01JKXYZ...",
  "iss": "sahara",
  "aud": "sahara-gateway",
  "exp": 1706000900,
  "iat": 1706000000,
  "roles": ["user"],
  "quota": {
    "maxConcurrentSessions": 5,
    "maxSubmitsPerMinute": 20
  }
}
```

> sahara-api 使用 **RS256**（非对称签名）：
> - api 持有私钥（签发）
> - gw 持有公钥（验证）
> - 这样 gw 无法伪造 Token，安全性更高

### 9.3 Token 黑名单

用户登出时，将当前 Access Token 的 `jti` 加入 Redis 黑名单：

```text
Key:   token:blacklist:{jti}
Value: 1
TTL:   Token 剩余有效期 (最大 15 分钟)
```

sahara-gw 在验证 JWT 时需额外检查黑名单：

```go
// pkg/auth/jwt.go (共享)
func (v *JWTValidator) Validate(tokenStr string) (*Claims, error) {
    claims, err := v.parseAndVerify(tokenStr)
    if err != nil {
        return nil, err
    }

    // 检查黑名单
    if v.redis.Exists(ctx, "token:blacklist:"+claims.JTI).Val() > 0 {
        return nil, ErrTokenRevoked
    }

    return claims, nil
}
```

---

## 十、数据模型

### 10.1 PostgreSQL 核心表

```sql
-- 用户表
CREATE TABLE users (
    id          TEXT PRIMARY KEY,              -- ULID: user_01JKXYZ...
    email       TEXT UNIQUE NOT NULL,
    nickname    TEXT NOT NULL DEFAULT '',
    avatar_url  TEXT DEFAULT '',
    password    TEXT NOT NULL,                  -- bcrypt hash
    roles       TEXT[] NOT NULL DEFAULT '{user}',
    plan        TEXT NOT NULL DEFAULT 'free',   -- free / pro / enterprise
    status      TEXT NOT NULL DEFAULT 'active', -- active / suspended / deleted
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- OAuth 绑定表
CREATE TABLE user_oauth (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id),
    provider    TEXT NOT NULL,                  -- github / google / wechat
    provider_id TEXT NOT NULL,                  -- 三方平台用户 ID
    metadata    JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(provider, provider_id)
);

-- 会话表
CREATE TABLE sessions (
    session_key TEXT PRIMARY KEY,               -- ULID: sess_01JKXYZ...
    user_id     TEXT NOT NULL REFERENCES users(id),
    agent_id    TEXT NOT NULL DEFAULT 'main',
    title       TEXT NOT NULL DEFAULT '新对话',
    message_count INTEGER NOT NULL DEFAULT 0,
    is_active   BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_message_at TIMESTAMPTZ
);

CREATE INDEX idx_sessions_user_id ON sessions(user_id, last_message_at DESC);

-- 消息表
CREATE TABLE messages (
    id          TEXT PRIMARY KEY,               -- ULID: msg_01JKXYZ...
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    role        TEXT NOT NULL,                  -- user / assistant / system
    content     JSONB NOT NULL,                -- text 或 ContentBlock[]
    attachments JSONB DEFAULT '[]',
    tool_calls  JSONB DEFAULT '[]',
    usage       JSONB DEFAULT '{}',            -- { inputTokens, outputTokens }
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_messages_session ON messages(session_key, ts ASC);

-- 文件表
CREATE TABLE files (
    id          TEXT PRIMARY KEY,               -- ULID: file_01JKXYZ...
    user_id     TEXT NOT NULL REFERENCES users(id),
    filename    TEXT NOT NULL,
    mime_type   TEXT NOT NULL,
    size_bytes  BIGINT NOT NULL,
    storage_key TEXT NOT NULL,                  -- S3 对象 key
    purpose     TEXT NOT NULL DEFAULT 'attachment', -- attachment / avatar / output
    expires_at  TIMESTAMPTZ,                   -- 临时文件过期时间
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_files_user ON files(user_id, created_at DESC);

-- 用量统计表 (按天聚合)
CREATE TABLE usage_daily (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id),
    date        DATE NOT NULL,
    submits     INTEGER NOT NULL DEFAULT 0,
    input_tokens  BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    file_uploads  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(user_id, date)
);
```

### 10.2 Redis 数据结构

| Key 模式 | 类型 | TTL | 用途 |
| --- | --- | --- | --- |
| `refresh:{tokenId}` | Hash | 30 天 | Refresh Token 存储 |
| `token:blacklist:{jti}` | String | ≤15 分钟 | Access Token 黑名单 |
| `user:profile:{userId}` | Hash | 10 分钟 | 用户信息缓存 |
| `ratelimit:api:{userId}` | String | 滑动窗口 | API 级限频 |
| `session:route:{sessionKey}` | Set | — | 会话→连接路由（与 Gateway 共用） |

---

## 十一、错误处理

### 11.1 错误码表

| reason | code | 说明 |
| --- | --- | --- |
| `VALIDATION_ERROR` | 400 | 请求参数校验失败 |
| `INVALID_CREDENTIALS` | 401 | 邮箱或密码错误 |
| `TOKEN_EXPIRED` | 401 | Access Token 过期 |
| `TOKEN_REVOKED` | 401 | Token 已被吊销 |
| `UNAUTHORIZED` | 401 | 未提供认证信息 |
| `FORBIDDEN` | 403 | 权限不足 |
| `NOT_FOUND` | 404 | 资源不存在 |
| `CONFLICT` | 409 | 资源冲突（如邮箱已注册） |
| `RATE_LIMITED` | 429 | API 请求频率超限 |
| `FILE_TOO_LARGE` | 413 | 上传文件过大 |
| `UNSUPPORTED_FORMAT` | 415 | 不支持的文件格式 |
| `QUOTA_EXCEEDED` | 429 | 用量超过配额 |
| `INTERNAL_ERROR` | 500 | 服务端内部错误 |

> **与 WS 协议统一**：`reason` 字符串 + `code` 数字的模式与 D2 WS 协议的 ErrorShape 完全一致。

---

## 十二、中间件栈

```go
// internal/server.go

func NewRouter(deps *Dependencies) http.Handler {
    r := chi.NewRouter()

    // 全局中间件
    r.Use(middleware.RequestID)           // 注入 X-Request-Id
    r.Use(middleware.RealIP)             // 提取真实 IP
    r.Use(middleware.Logger)             // 请求日志 (slog)
    r.Use(middleware.Recoverer)          // panic 恢复
    r.Use(middleware.Timeout(30 * time.Second))
    r.Use(corsMiddleware())              // CORS

    // 运维端点 (无认证)
    r.Get("/healthz", handleHealthz)
    r.Get("/readyz", handleReadyz(deps))
    r.Handle("/metrics", promhttp.Handler())

    // API v1
    r.Route("/api/v1", func(r chi.Router) {

        // 公开路由 (无认证)
        r.Group(func(r chi.Router) {
            r.Post("/auth/register", deps.AuthHandler.Register)
            r.Post("/auth/login", deps.AuthHandler.Login)
            r.Post("/auth/refresh", deps.AuthHandler.Refresh)
            r.Get("/system/health", deps.SystemHandler.Health)
            r.Get("/oauth/{provider}/authorize", deps.OAuthHandler.Authorize)
            r.Get("/oauth/{provider}/callback", deps.OAuthHandler.Callback)
        })

        // 受保护路由 (需 JWT)
        r.Group(func(r chi.Router) {
            r.Use(jwtAuthMiddleware(deps.Auth))    // JWT 验证
            r.Use(apiRateLimitMiddleware(deps.RL)) // API 限频

            r.Post("/auth/logout", deps.AuthHandler.Logout)

            r.Get("/user/profile", deps.UserHandler.GetProfile)
            r.Patch("/user/profile", deps.UserHandler.UpdateProfile)
            r.Post("/user/avatar", deps.UserHandler.UploadAvatar)
            r.Post("/user/password", deps.UserHandler.ChangePassword)

            r.Post("/sessions", deps.SessionHandler.Create)
            r.Get("/sessions", deps.SessionHandler.List)
            r.Get("/sessions/{sessionKey}", deps.SessionHandler.Get)
            r.Patch("/sessions/{sessionKey}", deps.SessionHandler.Update)
            r.Delete("/sessions/{sessionKey}", deps.SessionHandler.Delete)
            r.Post("/sessions/batch-delete", deps.SessionHandler.BatchDelete)

            r.Post("/files/upload", deps.FileHandler.Upload)
            r.Get("/files/{fileId}", deps.FileHandler.Get)

            r.Get("/usage", deps.UsageHandler.Current)
            r.Get("/usage/history", deps.UsageHandler.History)

            r.Get("/system/announcements", deps.SystemHandler.Announcements)
        })
    })

    return r
}
```

---

## 十三、共享包设计 (pkg/)

sahara-gw 和 sahara-api 共享的 Go 包放在仓库根目录的 `pkg/` 下：

```text
pkg/
├── auth/                         # JWT 相关
│   ├── jwt.go                    # JWT 生成 + 验证 (RS256)
│   ├── claims.go                 # Claims 结构体定义
│   └── blacklist.go              # Token 黑名单检查 (Redis)
├── model/                        # 领域模型
│   ├── user.go                   # User 结构体
│   ├── session.go                # Session 结构体
│   ├── message.go                # Message 结构体 (含 ContentBlock)
│   └── file.go                   # File 结构体
├── store/                        # 存储层
│   ├── postgres.go               # PG 连接池 + 通用操作
│   ├── redis.go                  # Redis 连接池
│   ├── user_store.go             # 用户 CRUD
│   ├── session_store.go          # 会话 CRUD
│   ├── message_store.go          # 消息查询
│   └── file_store.go             # 文件元数据
├── middleware/                   # HTTP 中间件
│   ├── cors.go                   # CORS 配置
│   ├── request_id.go             # Request ID 注入
│   └── logging.go                # 请求日志 (slog)
├── errcode/                      # 统一错误码
│   └── errors.go                 # reason 常量 + AppError 类型
└── s3/                           # S3 对象存储客户端
    └── client.go                 # 上传/下载/签名 URL
```

**共享原则**：

| 包 | 被引用方 | 说明 |
| --- | --- | --- |
| `pkg/auth` | gw + api | gw 用 `Validate()`，api 用 `Generate()` + `Validate()` |
| `pkg/model` | gw + api | 统一领域模型，避免序列化不一致 |
| `pkg/store` | gw + api | gw 用 session/message 读取，api 用全部 CRUD |
| `pkg/errcode` | gw + api | 统一错误码，前端只需学一次 |
| `pkg/middleware` | api (主要) | gw 的 HTTP 端点少，可选用 |
| `pkg/s3` | api (主要) + runtime | 文件上传/Agent 产物存储 |

---

## 十四、部署架构

```text
                         ┌──────────────┐
                         │  CDN / WAF   │
                         └──────┬───────┘
                                │
                         ┌──────┴───────┐
                         │   Nginx /    │
                         │   ALB        │
                         └──────┬───────┘
                           路径路由
                    ┌──────────┼──────────┐
                    │          │          │
              /ws   │    /api/*│    /v1/* │ (OpenAI compat)
                    ▼          ▼          ▼
            ┌──────────┐ ┌──────────┐ ┌──────────┐
            │ sahara-gw│ │sahara-api│ │ sahara-gw│
            │ ×2-5     │ │ ×2-3     │ │ (SSE)    │
            └──────────┘ └──────────┘ └──────────┘
```

**扩缩策略**：

| 服务 | 扩缩指标 | 典型实例数 |
| --- | --- | --- |
| sahara-gw | 连接数（内存） | 2-5（每实例 ~50K 连接） |
| sahara-api | CPU / RPS | 2-3（无状态，按请求量扩缩） |
| sahara-rt | Agent 并发任务数 | 3-10（按 LLM 并发） |

> sahara-api 是纯无状态服务，扩缩最简单——K8s HPA 按 CPU 自动扩缩即可。

### 14.1 LB 路径路由规则

```yaml
# Nginx / ALB 路由规则
rules:
  - path: /ws
    backend: sahara-gw
    protocol: websocket
  - path: /api/
    backend: sahara-api
    protocol: http
  - path: /v1/
    backend: sahara-gw
    protocol: http        # OpenAI 兼容 API + SSE
  - path: /healthz
    backend: sahara-api   # 或 sahara-gw，两者都暴露
    protocol: http
```

---

## 十五、可观测性

### 15.1 核心指标

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `api_http_requests_total` | Counter | HTTP 请求总数 (按 method + path + status) |
| `api_http_request_duration_seconds` | Histogram | 请求延迟分布 |
| `api_auth_login_total` | Counter | 登录次数 (success / failure) |
| `api_auth_register_total` | Counter | 注册次数 |
| `api_file_upload_bytes_total` | Counter | 文件上传总字节 |
| `api_active_users_gauge` | Gauge | 活跃用户数 |
| `api_db_query_duration_seconds` | Histogram | 数据库查询延迟 |

### 15.2 日志规范

```json
{
  "level": "INFO",
  "msg": "http request",
  "method": "POST",
  "path": "/api/v1/auth/login",
  "status": 200,
  "duration_ms": 45,
  "request_id": "req_01JKXYZ...",
  "user_id": "user_01JKXYZ...",
  "ip": "203.0.113.1"
}
```

与 Gateway 统一使用 `slog` 结构化日志，格式一致便于统一日志平台查询。

---

## 十六、分阶段交付

### Phase 1 — 与 Gateway MVP 同步

| 任务 | 优先级 | 说明 |
| --- | --- | --- |
| 项目脚手架 + chi 路由 | P0 | cmd/sahara-api + internal/ 骨架 |
| `pkg/auth` 共享包 | P0 | JWT 生成/验证，与 Gateway 共用 |
| `pkg/store` 共享包 | P0 | PG + Redis 连接池 + 基础 CRUD |
| 注册 / 登录 / Token 刷新 / 登出 | P0 | 认证基础，Gateway 依赖 |
| 用户 Profile CRUD | P1 | 前端首页需要 |
| 会话 CRUD | P1 | 前端会话列表 |
| 文件上传 | P1 | agent.submit 附件前置依赖 |
| 健康检查 + Prometheus | P1 | 运维基础 |

### Phase 2 — 生产化

| 任务 | 说明 |
| --- | --- |
| OAuth 三方登录 | 微信 / GitHub / Google |
| 密码重置 (邮件) | 忘记密码流程 |
| 头像上传 + S3 | CDN 加速 |
| 用量统计 | 按天/按月聚合 |
| 批量会话操作 | 批量删除 |
| 系统公告 | 维护通知 |
| API Rate Limiting | 按用户限频 |

### Phase 3 — 增强

| 任务 | 说明 |
| --- | --- |
| API Key 管理 | 面向开发者的 API Key 签发/吊销 |
| 多团队/组织 | 企业版多租户 |
| 会话导出 | Markdown / PDF 导出 |
| 数据导出 | GDPR 合规的个人数据导出 |

---

## 附录

### 附录 A. 与其他设计文档的接口对照

| 场景 | sahara-api 接口 | sahara-gw 接口 | 关系 |
| --- | --- | --- | --- |
| 用户登录 | `POST /api/v1/auth/login` | — | API 独有 |
| Token 刷新 | `POST /api/v1/auth/refresh` | WS `auth.refresh` 方法 | 互补：HTTP 和 WS 两条路径 |
| 会话列表 | `GET /api/v1/sessions` | WS `session.list` 方法 | 互补：HTTP 带分页+过滤，WS 是快捷方式 |
| 会话详情 | `GET /api/v1/sessions/:id` | WS `session.get` 方法 | 互补：HTTP 带历史消息分页 |
| 创建会话 | `POST /api/v1/sessions` | WS `session.create` 方法 | 互补 |
| 删除会话 | `DELETE /api/v1/sessions/:id` | — | API 独有（WS 未定义） |
| 文件上传 | `POST /api/v1/files/upload` | — | API 独有，WS `agent.submit` 引用 URL |
| 提交 Agent | — | WS `agent.submit` 方法 | Gateway 独有 |
| 事件流 | — | WS event 帧 | Gateway 独有 |

### 附录 B. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| §7.1 认证模块 | 新增: API-1 认证服务 | Phase 1 |
| §7.2 用户模块 | 新增: API-2 用户服务 | Phase 1 |
| §7.3 会话模块 | 新增: API-3 会话服务 | Phase 1 |
| §7.4 文件上传 | 新增: API-4 文件服务 | Phase 1 |
| §13 共享包 | 新增: PKG-1 共享包抽取 | Phase 1 (最先) |
| §7.6 系统模块 | 新增: API-5 系统服务 | Phase 2 |
| §7.5 配额与用量 | 新增: API-6 用量服务 | Phase 2 |
