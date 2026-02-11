# Gateway 认证与配对

> 本文档详解 Gateway 的多层认证体系：WebSocket 连接鉴权、设备配对流程、Token 管理、角色与权限作用域，以及网络绑定的安全策略。

---

## 目录

- [一、认证体系总览](#一认证体系总览)
- [二、WebSocket 连接认证](#二websocket-连接认证)
- [三、设备配对](#三设备配对)
- [四、Token 管理](#四token-管理)
- [五、角色与权限](#五角色与权限)
- [六、HTTP 端点认证](#六http-端点认证)
- [七、渠道级授权](#七渠道级授权)
- [八、网络绑定与安全](#八网络绑定与安全)
- [九、关键源文件索引](#九关键源文件索引)

---

## 一、认证体系总览

### 1.1 多层认证架构

Gateway 的认证分为**三个独立层级**，各自保护不同的入口：

```text
┌────────────────────────────────────────────────────────────────────┐
│                     认证层级                                        │
│                                                                    │
│  层级 1: Gateway 连接认证                                           │
│  ─────────────────────────                                         │
│  保护: WebSocket / HTTP API 入口                                   │
│  方式: Token / Password / 设备 Token / Tailscale                   │
│  效果: 未认证的连接被拒绝                                           │
│                                                                    │
│  层级 2: 角色与权限 (RBAC)                                          │
│  ────────────────────────                                          │
│  保护: Gateway RPC 方法                                             │
│  方式: 角色 (operator/node) + 作用域 (admin/read/write/...)        │
│  效果: 无权限的操作被拒绝                                           │
│                                                                    │
│  层级 3: 渠道级授权                                                 │
│  ──────────────────                                                │
│  保护: 消息渠道的发送者权限                                         │
│  方式: allowFrom 白名单 + DM/群组策略                               │
│  效果: 未授权的消息发送者被忽略或收到配对提示                        │
└────────────────────────────────────────────────────────────────────┘
```

### 1.2 认证方式优先级

Gateway 连接时，按以下顺序尝试认证：

```text
① 设备 Token (device token)     ← 已配对设备的持久化凭证
② Tailscale 身份 (whois)        ← Tailscale 网络的自动认证
③ 共享 Token (token)             ← 配置文件或环境变量中的静态令牌
④ 共享 Password (password)       ← 配置文件中的密码
```

---

## 二、WebSocket 连接认证

### 2.1 连接握手流程

> 源文件: `src/gateway/auth.ts`, `src/gateway/server/ws-connection/message-handler.ts`

```text
客户端                           Gateway
   │                                │
   │  WebSocket 连接建立             │
   │ ═══════════════════════════════│
   │                                │
   │  connect 请求 (必须是第一条消息) │
   │  {                             │
   │    type: "req",                │
   │    method: "connect",          │
   │    params: {                   │
   │      client: { id, mode, ... },│
   │      device?: { publicKey,     │  ← 设备身份 (可选)
   │        signature, signedAt },  │
   │      auth?: { token, password },│  ← 凭证
   │      role?: "operator",        │
   │      scopes?: [...]            │
   │    }                           │
   │  }                             │
   │ ─────────────────────────────→ │
   │                                │
   │                                │  authorizeGatewayConnect()
   │                                │  ├── 验证协议版本
   │                                │  ├── 验证设备签名 (如有)
   │                                │  ├── 验证 Token/Password
   │                                │  ├── 检查配对状态 (如有设备)
   │                                │  └── 分配角色和作用域
   │                                │
   │  ← 成功响应                     │
   │  { type: "res", ok: true,      │
   │    payload: { hello, auth: {   │
   │      deviceToken: "abc..."     │  ← 首次配对时返回设备 Token
   │    }}}                         │
   │ ←───────────────────────────── │
   │                                │
   │  后续 RPC 请求...               │
```

### 2.2 Token 验证

> 源文件: `src/gateway/auth.ts`

```typescript
authorizeGatewayConnect(params: {
  auth: ResolvedGatewayAuth;        // 服务端配置的认证信息
  connectAuth?: ConnectAuth;        // 客户端提供的凭证
  req?: IncomingMessage;            // HTTP 请求 (提取 IP/headers)
  trustedProxies?: string[];        // 可信代理列表
  tailscaleWhois?: TailscaleWhoisLookup;
}): Promise<GatewayAuthResult>
```

**安全措施**:

- 使用 `timingSafeEqual()` 进行**恒定时间**比较，防止计时攻击
- 设备签名使用公钥密码学验证
- 远程连接要求 Nonce 防重放攻击

### 2.3 认证失败

| 失败原因 | 错误码 | 行为 |
| ---- | ---- | ---- |
| 缺少 Token | `token_missing` | 关闭连接 (code 1008) |
| Token 不匹配 | `token_mismatch` | 关闭连接 (code 1008) |
| 密码不匹配 | `password_mismatch` | 关闭连接 (code 1008) |
| 设备未配对 | `NOT_PAIRED` | 创建配对请求，关闭连接 |
| 协议版本不兼容 | `VERSION_MISMATCH` | 关闭连接 |

---

## 三、设备配对

### 3.1 配对流程

> 源文件: `src/infra/device-pairing.ts`

设备配对用于**持久化设备身份**——配对后的设备使用设备 Token 连接，无需每次提供共享 Token。

```text
┌─────────────────────────────────────────────────────────────────┐
│  首次连接 (未配对设备)                                            │
│                                                                 │
│  设备 ──→ 发送公钥 + 签名 ──→ Gateway                           │
│                                     │                           │
│                                     ├── 验证签名                │
│                                     ├── 设备未配对              │
│                                     ├── 创建 PairingRequest     │
│                                     ├── 广播 device.pair.requested │
│                                     └── 关闭连接 (NOT_PAIRED)   │
│                                                                 │
│  管理员 ──→ device.pair.approve ──→ Gateway                     │
│                                     │                           │
│                                     ├── 生成设备 Token           │
│                                     ├── 存储到 paired.json       │
│                                     └── 广播 device.pair.approved│
│                                                                 │
│  设备 ──→ 使用设备 Token 重连 ──→ Gateway ──→ 认证成功           │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 静默配对

来自**本地回环地址**的连接可以自动批准（无需管理员操作）：

```text
isLocalDirectRequest(req)?
    ├── 是 → silent: true → 自动批准 → 立即返回设备 Token
    └── 否 → 需要管理员批准
```

**本地检测条件** (`isLocalDirectRequest`):

- 客户端 IP 是回环地址 (`127.0.0.1` / `::1`)
- Host 头是本地 (`localhost`, `127.0.0.1`, `*.ts.net`)
- 没有来自非可信代理的转发头

### 3.3 配对请求存储

| 存储 | 路径 | TTL |
| ---- | ---- | ---- |
| 待处理请求 | `~/.openclaw/devices/pending.json` | 5 分钟 |
| 已配对设备 | `~/.openclaw/devices/paired.json` | 永久 |

文件权限: `0o600`（仅所有者可读写）。

---

## 四、Token 管理

### 4.1 Token 格式

设备 Token 是去掉短横线的 UUID（32 个十六进制字符）：

```typescript
function newToken() {
  return randomUUID().replaceAll("-", "");
  // 例: "550e8400e29b41d4a716446655440000"
}
```

**不是 JWT**——纯随机字符串，验证通过与服务端存储值比对。

### 4.2 Token 生命周期

```text
生成 (配对批准时)
    │
    ▼
存储: paired.json 中 tokens[role] = { token, role, scopes, createdAtMs }
    │
    │  使用中: 每次连接更新 lastUsedAtMs
    │
    ├── 轮转 (device.token.rotate)
    │   → 生成新 Token，保留 createdAtMs，设置 rotatedAtMs
    │   → 旧 Token 立即失效
    │
    └── 撤销 (device.token.revoke)
        → 设置 revokedAtMs
        → Token 标记为无效
```

**重要**: Token 不会自动过期——必须手动轮转或撤销。

### 4.3 Token 验证

```text
verifyDeviceToken({ deviceId, token, role, scopes })
    │
    ├── 设备是否已配对? → 否: 拒绝
    ├── Token 是否匹配? → 否: 拒绝
    ├── Token 是否已撤销? → 是: 拒绝
    ├── 请求的 scopes ⊆ Token scopes? → 否: 拒绝
    │
    └── 通过 → 更新 lastUsedAtMs → 允许
```

---

## 五、角色与权限

### 5.1 角色

| 角色 | 用途 | 可访问的方法 |
| ---- | ---- | ---- |
| `operator` | 管理员/用户客户端 | 大部分方法（agent、chat、sessions、config...） |
| `node` | 远程设备节点 | 仅 `node.invoke.result`、`node.event`、`skills.bins` |

### 5.2 作用域 (Scopes)

作用域提供**细粒度权限控制**：

| 作用域 | 权限 |
| ---- | ---- |
| `operator.admin` | 配置变更、系统更新、向导操作 |
| `operator.read` | 只读访问（状态、历史、列表） |
| `operator.write` | 写入操作（发送消息、Agent 运行） |
| `operator.approvals` | exec 命令审批管理 |
| `operator.pairing` | 设备/节点配对管理 |

### 5.3 方法级授权

> 源文件: `src/gateway/server-methods.ts`

```text
RPC 请求到达
    │
    ▼
authorizeGatewayMethod(method, client)
    │
    ├── method 是 node-only? (node.invoke.result 等)
    │   → 客户端角色必须是 node
    │
    ├── method 是 admin? (config.set, wizard.* 等)
    │   → 客户端必须有 operator.admin scope
    │
    └── method 是普通 operator?
        → 客户端角色必须是 operator
```

---

## 六、HTTP 端点认证

### 6.1 统一认证

所有 HTTP 端点使用与 WebSocket **相同的认证函数** (`authorizeGatewayConnect`)：

```text
HTTP 请求到达
    │
    ▼
提取 Token:
    ├── Authorization: Bearer <token>     ← 标准方式
    ├── X-OpenClaw-Token: <token>         ← 自定义头
    └── ?token=<token>                    ← 查询参数 (已弃用)
    │
    ▼
authorizeGatewayConnect({ auth, connectAuth: { token } })
    │
    ├── 匹配 → 允许访问
    └── 不匹配 → 401 Unauthorized
```

### 6.2 各 HTTP 端点

| 端点 | 路径 | 认证方式 |
| ---- | ---- | ---- |
| OpenAI 兼容 API | `/v1/chat/completions` | Bearer Token (同 Gateway Token) |
| OpenResponses API | `/v1/responses` | Bearer Token |
| Tools Invoke | `/tools/invoke` | Bearer Token |
| Hooks Webhook | `/hooks/*` | Bearer Token (或独立的 `hooks.token`) |
| Control UI | `/` | 无认证（依赖网络隔离）或 Token |

### 6.3 Hooks 独立 Token

Webhooks 支持单独配置的 Token：

```yaml
hooks:
  token: "webhook-secret-token"  # 独立于 Gateway token
```

---

## 七、渠道级授权

渠道级授权与 Gateway 认证是**独立的两层**——Gateway 认证保护 API 入口，渠道授权保护消息处理。

### 7.1 DM 策略

| 策略 | 行为 |
| ---- | ---- |
| `pairing` | 未知用户收到配对码提示 |
| `allowlist` | 仅 `allowFrom` 中的用户可对话 |
| `open` | 所有用户可对话 |
| `disabled` | 禁用 DM |

### 7.2 Elevated 权限

高风险工具（如提权 exec）需要额外的 elevated allowlist：

```yaml
tools:
  elevated:
    allowFrom:
      telegram: ["admin_user_id"]
      discord: ["123456789"]
```

> 详见: [CHANNELS-ARCHITECTURE.md §六](../channel/CHANNELS-ARCHITECTURE.md)

---

## 八、网络绑定与安全

### 8.1 绑定模式

> 源文件: `src/gateway/net.ts`

| 模式 | 绑定地址 | 安全级别 | 场景 |
| ---- | ---- | ---- | ---- |
| `loopback` (默认) | `127.0.0.1` | 最高 | 单机部署 |
| `tailnet` | Tailscale IP (`100.x.x.x`) | 高 | Tailscale VPN 内 |
| `lan` | `0.0.0.0` | 中 | 局域网部署 |
| `auto` | 优先 loopback | 高 | 自动检测 |
| `custom` | 指定 IP | 取决于网络 | 自定义 |

### 8.2 安全强制规则

```text
非回环绑定 + 无共享密钥?
    → 拒绝启动! "refusing to bind gateway to 0.0.0.0:18789 without auth"
```

**只有回环地址**允许无认证运行。绑定到外部可达地址时，必须配置 Token 或 Password。

### 8.3 IPv6 支持

- 绑定 `127.0.0.1` 时自动同时监听 `::1`
- 双栈监听（IPv4 + IPv6）

### 8.4 可信代理

```yaml
gateway:
  trustedProxies: ["127.0.0.1", "::1", "10.0.0.1"]
```

只有来自可信代理的 `X-Forwarded-For` 和 `X-Real-IP` 头才会被信任。

---

## 九、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/gateway/auth.ts` | `authorizeGatewayConnect`：核心认证函数、Token 验证、本地检测 |
| `src/gateway/server/ws-connection/message-handler.ts` | WebSocket 连接握手、connect 方法处理 |
| `src/infra/device-pairing.ts` | 设备配对：请求/批准/拒绝、Token 生成/轮转/撤销 |
| `src/infra/device-auth-store.ts` | 客户端设备 Token 存储 |
| `src/gateway/server-methods.ts` | `authorizeGatewayMethod`：方法级 RBAC |
| `src/gateway/net.ts` | `resolveGatewayBindHost`：网络绑定模式解析 |
| `src/gateway/server-runtime-config.ts` | 绑定安全强制规则 |
| `src/gateway/openai-http.ts` | OpenAI 兼容 API HTTP 认证 |
| `src/gateway/openresponses-http.ts` | OpenResponses API HTTP 认证 |
| `src/auto-reply/command-auth.ts` | 渠道级发送者授权 |
