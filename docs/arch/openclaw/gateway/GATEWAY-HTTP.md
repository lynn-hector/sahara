# Gateway HTTP 服务

> 本文档详解 Gateway 提供的所有 HTTP 服务：OpenAI 兼容 API、OpenResponses API、Tools Invoke、Webhooks、Control UI、插件路由，以及路由优先级和认证机制。

---

## 目录

- [一、HTTP 服务总览](#一http-服务总览)
- [二、路由优先级](#二路由优先级)
- [三、OpenAI 兼容 API](#三openai-兼容-api)
- [四、OpenResponses API](#四openresponses-api)
- [五、Tools Invoke API](#五tools-invoke-api)
- [六、Webhooks](#六webhooks)
- [七、Control UI](#七control-ui)
- [八、插件 HTTP 路由](#八插件-http-路由)
- [九、关键源文件索引](#九关键源文件索引)

---

## 一、HTTP 服务总览

Gateway 的 HTTP 服务与 WebSocket 共享同一个 HTTP 服务器实例。当请求不是 WebSocket 升级时，进入 HTTP 路由处理。

```text
HTTP 请求到达 Gateway (:18789)
    │
    ├── Upgrade: websocket?
    │   → 是: WebSocket 握手 (不走 HTTP 路由)
    │   → 否: 进入 HTTP 路由 ↓
    │
    ▼
┌────────────────────────────────────────────────────────────────┐
│  HTTP 服务类型                                                  │
│                                                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │ API 服务      │  │ Webhook 服务 │  │ UI 服务              │ │
│  │              │  │              │  │                      │ │
│  │ OpenAI API   │  │ /hooks/*     │  │ Control UI (SPA)     │ │
│  │ OpenResponses│  │ (wake/agent/ │  │ Canvas/A2UI          │ │
│  │ Tools Invoke │  │  mappings)   │  │ 头像端点             │ │
│  └──────────────┘  └──────────────┘  └──────────────────────┘ │
│                                                                │
│  ┌──────────────┐  ┌──────────────┐                           │
│  │ 渠道服务      │  │ 插件路由     │                           │
│  │              │  │              │                           │
│  │ Slack HTTP   │  │ 插件自定义   │                           │
│  │              │  │ 端点         │                           │
│  └──────────────┘  └──────────────┘                           │
└────────────────────────────────────────────────────────────────┘
```

**设计特点**:

- 无路由框架——纯手写顺序匹配，每个 handler 返回 `boolean`（true = 已处理）
- 配置每请求加载（支持热更新 `trustedProxies` 等）
- 所有 API 端点使用与 WebSocket 相同的认证函数

---

## 二、路由优先级

> 源文件: `src/gateway/server-http.ts` — `handleRequest()`

请求按以下**固定顺序**匹配，**第一个匹配的 handler** 处理请求：

```text
HTTP 请求
    │
    ├── ① /hooks/*              → Webhooks 处理器
    ├── ② /tools/invoke         → Tools Invoke API
    ├── ③ (Slack 动态路由)       → Slack HTTP 处理器
    ├── ④ (插件路由)             → 插件 HTTP 路由
    ├── ⑤ /v1/responses         → OpenResponses API (需启用)
    ├── ⑥ /v1/chat/completions  → OpenAI 兼容 API (需启用)
    ├── ⑦ (Canvas/A2UI 路由)    → Canvas Host
    ├── ⑧ /ui/*                 → Control UI (静态 SPA)
    └── ⑨ 无匹配                → 404 Not Found
```

**未匹配请求**: 返回 404。**未捕获异常**: 返回 500 "Internal Server Error"。

---

## 三、OpenAI 兼容 API

> 源文件: `src/gateway/openai-http.ts`

### 3.1 端点

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | `/v1/chat/completions` | Chat Completions (流式/非流式) |

### 3.2 认证

```text
Authorization: Bearer <gateway-token>
```

使用与 WebSocket 相同的 Gateway token/password。

### 3.3 请求格式

```json
{
  "model": "openclaw",
  "stream": true,
  "messages": [
    {"role": "system", "content": "你是一个助手"},
    {"role": "user", "content": "你好"}
  ],
  "user": "session-prefix"
}
```

**model 字段**:

- `"openclaw"` → 使用默认 Agent
- `"openclaw:my-agent"` → 指定 Agent ID
- 也可通过 `X-OpenClaw-Agent-Id` 头指定

**messages 映射到 Agent**:

- `system` 消息 → 提取为 `extraSystemPrompt`
- `user` / `assistant` 消息 → 构建对话历史
- 最后一条 `user` 消息 → Agent prompt

### 3.4 非流式响应

```json
{
  "id": "chatcmpl_<uuid>",
  "object": "chat.completion",
  "created": 1706000000,
  "model": "openclaw",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "你好！有什么可以帮你的？"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
}
```

### 3.5 流式响应 (SSE)

```text
Content-Type: text/event-stream

data: {"id":"chatcmpl_xxx","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}

data: {"id":"chatcmpl_xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"你好"},"index":0}]}

data: {"id":"chatcmpl_xxx","object":"chat.completion.chunk","choices":[{"delta":{"content":"！"},"index":0}]}

data: [DONE]
```

**流式实现**: 监听 `onAgentEvent`，将 `stream: "assistant"` 事件转换为 SSE chunk。

### 3.6 会话管理

- 会话键: `openai:{user}` 或 `openai:{uuid}`
- 每次请求创建独立运行
- 不持久化跨请求的历史（由客户端管理）

---

## 四、OpenResponses API

> 源文件: `src/gateway/openresponses-http.ts`

### 4.1 端点

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | `/v1/responses` | Responses API (流式/非流式) |

### 4.2 请求格式

```json
{
  "model": "openclaw",
  "input": "帮我看看 package.json",
  "instructions": "你是一个代码助手",
  "tools": [{"type": "function", "function": {"name": "my_tool", "parameters": {...}}}],
  "tool_choice": "none" | "required" | {"type": "function", "function": {"name": "..."}},
  "stream": true,
  "max_output_tokens": 4096
}
```

**input 支持多种格式**:

- 字符串: 直接作为 prompt
- 数组: `ItemParam[]`，支持 `message`、`function_call_output` 类型
- 内容部分: `input_text`、`input_image`、`input_file`（含 PDF 渲染）

### 4.3 非流式响应

```json
{
  "id": "resp_<uuid>",
  "object": "response",
  "created_at": 1706000000,
  "status": "completed",
  "model": "openclaw",
  "output": [{
    "type": "message",
    "id": "msg_<uuid>",
    "role": "assistant",
    "content": [{"type": "output_text", "text": "package.json 内容如下..."}],
    "status": "completed"
  }],
  "usage": {"input_tokens": 100, "output_tokens": 200, "total_tokens": 300}
}
```

### 4.4 流式事件

| 事件 | 说明 |
| ---- | ---- |
| `response.created` | 初始响应资源 |
| `response.in_progress` | 状态更新 |
| `response.output_item.added` | 新输出项 |
| `response.content_part.added` | 新内容部分 |
| `response.output_text.delta` | 文本增量 |
| `response.output_text.done` | 文本完成 |
| `response.content_part.done` | 部分完成 |
| `response.output_item.done` | 项完成 |
| `response.completed` | 最终响应（含 usage） |
| `response.failed` | 错误响应 |

### 4.5 与 OpenAI API 的区别

| 维度 | OpenAI Chat API | OpenResponses API |
| ---- | ---- | ---- |
| 端点 | `/v1/chat/completions` | `/v1/responses` |
| 输入格式 | `messages[]` | `input` (字符串/数组) |
| 工具支持 | 不支持 | 支持 `tools` + `tool_choice` |
| 文件上传 | 不支持 | 支持 `input_file` / `input_image` |
| 响应格式 | `choices[].message` | `output[].content[]` |
| SSE 事件 | `chat.completion.chunk` | 多种细粒度事件 |

---

## 五、Tools Invoke API

> 源文件: `src/gateway/tools-invoke-http.ts`

### 5.1 端点

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | `/tools/invoke` | 直接调用工具（不经过 LLM） |

### 5.2 请求格式

```json
{
  "tool": "read",
  "action": "file",
  "args": {"path": "package.json"},
  "sessionKey": "agent:default:main"
}
```

**`action` 合并**: 如果工具 schema 有 `action` 属性，`action` 字段会被合并到 `args` 中。

### 5.3 策略过滤

工具调用前会经过完整的**工具策略链**过滤——profile → provider → agent → group → sandbox。被策略拒绝的工具返回 404。

### 5.4 响应

```json
{"ok": true, "result": <tool_execution_result>}
```

| HTTP 状态 | 含义 |
| ---- | ---- |
| 200 | 成功 |
| 400 | 请求无效 / 工具执行错误 |
| 401 | 未认证 |
| 404 | 工具不可用（被策略过滤） |

---

## 六、Webhooks

> 源文件: `src/gateway/server-http.ts` — `createHooksRequestHandler()`

### 6.1 基础路径

默认: `/hooks`，可通过 `hooks.path` 配置。

### 6.2 认证

独立于 Gateway token——使用 `hooks.token` 配置：

```yaml
hooks:
  enabled: true
  token: "webhook-secret"
```

Token 传递方式: `Authorization: Bearer <token>` (推荐) 或 `X-OpenClaw-Token` 头。

### 6.3 内置端点

#### `/hooks/wake` — 唤醒 Agent

```json
// POST /hooks/wake
{"text": "检查一下新邮件", "mode": "now"}

// 响应
{"ok": true, "mode": "now"}
```

| mode | 行为 |
| ---- | ---- |
| `now` | 立即触发系统事件 |
| `next-heartbeat` | 在下次心跳时处理 |

#### `/hooks/agent` — 触发 Agent 运行

```json
// POST /hooks/agent
{
  "message": "分析今天的日志",
  "name": "Cron Hook",
  "sessionKey": "agent:default:hook:daily",
  "channel": "telegram",
  "to": "12345",
  "model": "anthropic/claude-sonnet-4-20250514",
  "timeoutSeconds": 120
}

// 响应
{"ok": true, "runId": "uuid"}
```

### 6.4 自定义映射

通过 `hooks.mappings` 配置自定义 webhook 路由：

```yaml
hooks:
  mappings:
    - path: "/hooks/github"
      template: "GitHub event: {{payload.action}} on {{payload.repository.name}}"
    - path: "/hooks/alert"
      source: "monitoring"
      template: "Alert: {{payload.message}}"
```

**模板变量**: `{{payload.*}}`、`{{headers.*}}`、`{{query.*}}`

**Transform 模块**: 可通过 JS 模块自定义处理逻辑。

**预设**: `gmail` 预设可直接使用。

### 6.5 限制

- 最大请求体: `hooks.maxBodyBytes`（默认 256KB）
- 仅 POST 方法
- 超限返回 413

---

## 七、Control UI

> 源文件: `src/gateway/control-ui.ts`

### 7.1 路由

| 路径 | 说明 |
| ---- | ---- |
| `/ui/` | SPA 入口 (index.html) |
| `/ui/assets/*` | 静态资源 |
| `/ui/avatar/{agentId}` | Agent 头像 |
| `/ui/avatar/{agentId}?meta=1` | 头像元信息 (JSON) |

基础路径通过 `controlUiBasePath` 配置（默认 `/ui`）。

### 7.2 SPA 服务

- 静态文件从打包目录（`control-ui/`）提供
- 所有未匹配的路径回退到 `index.html`（SPA 路由）
- 配置注入: `window.__OPENCLAW_CONTROL_UI_BASE_PATH__` 等全局变量
- Cache-Control: `no-cache`（确保热更新）
- 路径安全: 防止目录穿越

### 7.3 头像解析

头像来源优先级:

1. 本地文件路径
2. 远程 URL
3. Data URI
4. 默认头像

---

## 八、插件 HTTP 路由

> 源文件: `src/gateway/server/plugins-http.ts`

### 8.1 注册方式

插件通过两种方式注册 HTTP 路由：

```typescript
// 方式 1: 精确路径
api.registerHttpRoute({
  path: "/my-plugin/api",
  handler: async (req, res) => {
    res.statusCode = 200;
    res.end(JSON.stringify({ ok: true }));
  }
});

// 方式 2: 通用处理器
api.registerHttpHandler({
  handler: async (req, res) => {
    if (req.url?.startsWith("/my-plugin/")) {
      // 处理请求
      return true;  // 返回 true 表示已处理
    }
    return false;  // 返回 false 继续下一个处理器
  }
});
```

### 8.2 匹配规则

1. 精确路径路由 (`httpRoutes`) 先匹配
2. 通用处理器 (`httpHandlers`) 按注册顺序依次尝试
3. 第一个返回 `true` 的处理器生效

### 8.3 错误处理

- handler 异常被捕获
- 如果响应头未发送 → 返回 500
- 插件 ID 记录到日志便于调试

---

## 九、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/gateway/server-http.ts` | HTTP 服务器创建、路由分发、Webhook handler |
| `src/gateway/openai-http.ts` | `/v1/chat/completions` OpenAI 兼容 API |
| `src/gateway/openresponses-http.ts` | `/v1/responses` OpenResponses API |
| `src/gateway/tools-invoke-http.ts` | `/tools/invoke` 工具直接调用 |
| `src/gateway/control-ui.ts` | Control UI SPA 服务、头像端点 |
| `src/gateway/control-ui-shared.ts` | UI 根目录解析、配置注入 |
| `src/gateway/server/plugins-http.ts` | 插件 HTTP 路由处理 |
| `src/gateway/server/hooks.ts` | Gateway 内部 hook 调度 |
| `src/gateway/hooks.ts` | Hook 配置解析 |
