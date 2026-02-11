# Gateway Agent 调度流程详解

> 本文档详细介绍 Gateway 如何调度 Agent Runtime：从接收 `agent` RPC 请求，到执行代理任务，再到事件广播和结果返回的完整流程。

---

## 目录

1. [流程概览](#一流程概览)
2. [请求接收与分发](#二请求接收与分发)
3. [agent 方法处理器](#三agent-方法处理器)
4. [agentCommand 执行](#四agentcommand-执行)
5. [Agent Runtime 调用](#五agent-runtime-调用)
6. [事件广播机制](#六事件广播机制)
7. [响应返回流程](#七响应返回流程)
8. [完整数据流图](#八完整数据流图)

---

## 一、流程概览

### 1.1 整体架构

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Gateway 代理请求处理流程                                │
└─────────────────────────────────────────────────────────────────────────────┘

  Client (UI/CLI/API)
         │
         │ WebSocket req: { method: "agent", params: {...} }
         ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                          Gateway Server                                   │
  │                                                                           │
  │  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐   │
  │  │  WebSocket      │───►│ handleGateway    │───►│  agentHandlers    │   │
  │  │  Handler        │    │ Request          │    │  .agent()         │   │
  │  └─────────────────┘    └──────────────────┘    └─────────┬─────────┘   │
  │                                                           │              │
  │                                                           ▼              │
  │                                               ┌───────────────────┐      │
  │                                               │  agentCommand()   │      │
  │                                               │  (src/commands)   │      │
  │                                               └─────────┬─────────┘      │
  │                                                         │                │
  └─────────────────────────────────────────────────────────┼────────────────┘
                                                            │
                                                            ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                          Agent Runtime                                    │
  │                                                                           │
  │  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐   │
  │  │ runEmbedded     │───►│  LLM Provider    │───►│  Tool Execution   │   │
  │  │ PiAgent()       │    │  (Anthropic等)   │    │  (Sandbox)        │   │
  │  └─────────────────┘    └──────────────────┘    └───────────────────┘   │
  │          │                                                               │
  │          │ emitAgentEvent()                                              │
  │          ▼                                                               │
  └──────────┼───────────────────────────────────────────────────────────────┘
             │
             │ Event Stream
             ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                          事件广播系统                                      │
  │                                                                           │
  │  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐   │
  │  │ onAgentEvent    │───►│ createAgentEvent │───►│   broadcast()     │   │
  │  │ (listener)      │    │ Handler          │    │                   │   │
  │  └─────────────────┘    └──────────────────┘    └─────────┬─────────┘   │
  │                                                           │              │
  └───────────────────────────────────────────────────────────┼──────────────┘
                                                              │
                                                              ▼
                                                   ┌───────────────────┐
                                                   │  WebSocket Clients│
                                                   │  (event frames)   │
                                                   └───────────────────┘

```

### 1.2 关键源文件

| 文件 | 描述 |
| ---- | ---- |
| `src/gateway/server-methods.ts` | 请求分发与权限检查 |
| `src/gateway/server-methods/agent.ts` | agent 方法处理器 |
| `src/commands/agent.ts` | agentCommand 主逻辑 |
| `src/agents/pi-embedded.ts` | Agent Runtime 入口 |
| `src/infra/agent-events.ts` | 事件发射系统 |
| `src/gateway/server-chat.ts` | 事件处理与广播 |
| `src/gateway/server-broadcast.ts` | 广播器实现 |

---

## 二、请求接收与分发

### 2.1 handleGatewayRequest

当 WebSocket 服务器收到请求帧后，调用 `handleGatewayRequest` 进行分发：

```typescript
// src/gateway/server-methods.ts

export async function handleGatewayRequest(
  opts: GatewayRequestOptions & { extraHandlers?: GatewayRequestHandlers },
): Promise<void> {
  const { req, respond, client, context } = opts;
  
  // 1. 权限检查
  const authError = authorizeGatewayMethod(req.method, client);
  if (authError) {
    respond(false, undefined, authError);
    return;
  }
  
  // 2. 查找处理器
  const handler = opts.extraHandlers?.[req.method] ?? coreGatewayHandlers[req.method];
  if (!handler) {
    respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, `unknown method: ${req.method}`));
    return;
  }
  
  // 3. 调用处理器
  await handler({
    req,
    params: (req.params ?? {}) as Record<string, unknown>,
    client,
    respond,
    context,
  });
}

```text

### 2.2 权限检查

`agent` 方法需要 `operator.write` scope：

```typescript
const WRITE_METHODS = new Set([
  "send",
  "agent",          // <-- agent 属于写入方法
  "agent.wait",
  "chat.send",
  // ...
]);

function authorizeGatewayMethod(method: string, client: GatewayRequestOptions["client"]) {
  const role = client?.connect?.role ?? "operator";
  const scopes = client?.connect?.scopes ?? [];
  
  // admin scope 可以执行所有方法
  if (scopes.includes("operator.admin")) {
    return null;
  }
  
  // agent 方法需要 write scope
  if (WRITE_METHODS.has(method) && !scopes.includes("operator.write")) {
    return errorShape(ErrorCodes.INVALID_REQUEST, "missing scope: operator.write");
  }
  
  return null;
}

```

---

## 三、agent 方法处理器

### 3.1 处理器入口

```typescript
// src/gateway/server-methods/agent.ts

export const agentHandlers: GatewayRequestHandlers = {
  agent: async ({ params, respond, context }) => {
    // 1. 参数验证
    // 2. 消息预处理
    // 3. 会话解析
    // 4. 投递计划
    // 5. 立即响应 (accepted)
    // 6. 异步执行 agentCommand
    // 7. 最终响应 (completed/error)
  },
  // ...
};

```text

### 3.2 详细处理流程

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        agent 方法处理流程                                    │
└─────────────────────────────────────────────────────────────────────────────┘

                              params (请求参数)
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 1: 参数验证                                                         │
  │                                                                           │
  │  • validateAgentParams(params) - 使用 Ajv 验证参数格式                    │
  │  • 必需参数: message, idempotencyKey                                      │
  │  • 可选参数: agentId, sessionKey, thinking, deliver, channel, ...         │
  │                                                                           │
  │  if (!validateAgentParams(params)) {                                      │
  │    respond(false, undefined, errorShape(INVALID_REQUEST, "..."));         │
  │    return;                                                                │
  │  }                                                                        │
  └──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 2: 幂等性检查                                                       │
  │                                                                           │
  │  const cached = context.dedupe.get(`agent:${idempotencyKey}`);            │
  │  if (cached) {                                                            │
  │    // 返回缓存的响应，避免重复执行                                         │
  │    respond(cached.ok, cached.payload, cached.error, { cached: true });    │
  │    return;                                                                │
  │  }                                                                        │
  └──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 3: 消息预处理                                                       │
  │                                                                           │
  │  • 处理附件 (parseMessageWithAttachments)                                 │
  │  • 注入时间戳 (injectTimestamp)                                           │
  │  • 验证渠道 (isKnownGatewayChannel)                                       │
  │  • 规范化 agentId                                                         │
  └──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 4: 会话解析                                                         │
  │                                                                           │
  │  const { cfg, storePath, entry, canonicalKey } = loadSessionEntry(key);   │
  │                                                                           │
  │  • 解析 sessionKey → 获取/创建 SessionEntry                               │
  │  • 检查 sendPolicy (deny 则拒绝)                                          │
  │  • 注册运行上下文 (registerAgentRunContext)                               │
  │  • 更新会话存储 (updateSessionStore)                                      │
  └──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 5: 投递计划                                                         │
  │                                                                           │
  │  const deliveryPlan = resolveAgentDeliveryPlan({                          │
  │    sessionEntry,                                                          │
  │    requestedChannel,                                                      │
  │    explicitTo,                                                            │
  │    wantsDelivery,                                                         │
  │  });                                                                      │
  │                                                                           │
  │  • 解析目标渠道 (telegram/discord/slack/...)                              │
  │  • 解析目标地址 (to)                                                      │
  │  • 解析线程ID (threadId)                                                  │
  └──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 6: 立即响应 (accepted)                                              │
  │                                                                           │
  │  const accepted = {                                                       │
  │    runId,                                                                 │
  │    status: "accepted",                                                    │
  │    acceptedAt: Date.now(),                                                │
  │  };                                                                       │
  │                                                                           │
  │  // 缓存确认响应                                                          │
  │  context.dedupe.set(`agent:${idem}`, { ok: true, payload: accepted });    │
  │                                                                           │
  │  // 发送第一个响应帧                                                      │
  │  respond(true, accepted, undefined, { runId });                           │
  └──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 7: 异步执行 agentCommand                                            │
  │                                                                           │
  │  void agentCommand({                                                      │
  │    message,                                                               │
  │    images,                                                                │
  │    to: resolvedTo,                                                        │
  │    sessionId,                                                             │
  │    sessionKey,                                                            │
  │    thinking: request.thinking,                                            │
  │    deliver,                                                               │
  │    channel: resolvedChannel,                                              │
  │    runId,                                                                 │
  │    // ...                                                                 │
  │  }, defaultRuntime, context.deps)                                         │
  │    .then((result) => {                                                    │
  │      // Step 8: 成功响应                                                  │
  │      respond(true, { runId, status: "ok", result });                      │
  │    })                                                                     │
  │    .catch((err) => {                                                      │
  │      // Step 8: 错误响应                                                  │
  │      respond(false, { runId, status: "error" }, errorShape(...));         │
  │    });                                                                    │
  └──────────────────────────────────────────────────────────────────────────┘

```text

### 3.3 关键代码片段

```typescript
// src/gateway/server-methods/agent.ts (简化版)

agent: async ({ params, respond, context }) => {
  // 1. 验证参数
  if (!validateAgentParams(params)) {
    respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "invalid params"));
    return;
  }
  
  // 2. 幂等性检查
  const cached = context.dedupe.get(`agent:${idem}`);
  if (cached) {
    respond(cached.ok, cached.payload, cached.error, { cached: true });
    return;
  }
  
  // 3. 预处理消息 (附件、时间戳)
  const parsed = await parseMessageWithAttachments(message, attachments);
  message = injectTimestamp(parsed.message, timestampOpts);
  
  // 4. 解析会话
  const { entry, storePath, canonicalKey } = loadSessionEntry(sessionKey);
  registerAgentRunContext(runId, { sessionKey });
  
  // 5. 立即响应 accepted
  const accepted = { runId, status: "accepted", acceptedAt: Date.now() };
  context.dedupe.set(`agent:${idem}`, { ok: true, payload: accepted });
  respond(true, accepted, undefined, { runId });
  
  // 6. 异步执行
  void agentCommand({
    message,
    sessionKey,
    runId,
    // ...
  })
    .then((result) => {
      const payload = { runId, status: "ok", result };
      context.dedupe.set(`agent:${idem}`, { ok: true, payload });
      respond(true, payload);
    })
    .catch((err) => {
      const error = errorShape(ErrorCodes.UNAVAILABLE, String(err));
      context.dedupe.set(`agent:${idem}`, { ok: false, error });
      respond(false, { runId, status: "error" }, error);
    });
}

```

---

## 四、agentCommand 执行

### 4.1 函数签名

```typescript
// src/commands/agent.ts

export async function agentCommand(
  opts: AgentCommandOpts,
  runtime: RuntimeEnv = defaultRuntime,
  deps: CliDeps = createDefaultDeps(),
): Promise<AgentCommandResult>

```

### 4.2 执行流程

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        agentCommand 执行流程                                 │
└─────────────────────────────────────────────────────────────────────────────┘

                                   opts
                                    │
                                    ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 1: 参数验证与配置加载                                               │
  │                                                                           │
  │  • 验证 message 非空                                                      │
  │  • 加载配置 (loadConfig)                                                  │
  │  • 解析 agentId                                                           │
  │  • 确保工作空间存在 (ensureAgentWorkspace)                                │
  └──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 2: 会话解析                                                         │
  │                                                                           │
  │  const sessionResolution = resolveSession({                               │
  │    cfg, to, sessionId, sessionKey, agentId                                │
  │  });                                                                      │
  │                                                                           │
  │  • 获取或创建 sessionId                                                   │
  │  • 解析 sessionEntry (历史配置)                                           │
  │  • 获取持久化的 thinking/verbose 级别                                     │
  └──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 3: 技能快照                                                         │
  │                                                                           │
  │  const skillsSnapshot = buildWorkspaceSkillSnapshot(workspaceDir, {       │
  │    config: cfg,                                                           │
  │    eligibility: { remote: getRemoteSkillEligibility() },                  │
  │  });                                                                      │
  │                                                                           │
  │  • 扫描工作空间技能                                                       │
  │  • 获取远程技能                                                           │
  │  • 构建快照供 Agent 使用                                                  │
  └──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 4: 模型解析                                                         │
  │                                                                           │
  │  • 从配置获取默认模型 (resolveConfiguredModelRef)                         │
  │  • 检查会话中的模型覆盖 (modelOverride/providerOverride)                  │
  │  • 验证模型是否在允许列表中                                               │
  │  • 解析 thinking 级别                                                     │
  └──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 5: 运行 Agent (带模型降级)                                          │
  │                                                                           │
  │  const fallbackResult = await runWithModelFallback({                      │
  │    cfg, provider, model, agentDir,                                        │
  │    run: (providerOverride, modelOverride) => {                            │
  │      // CLI Provider (如 claude-cli)                                      │
  │      if (isCliProvider(providerOverride)) {                               │
  │        return runCliAgent({ ... });                                       │
  │      }                                                                    │
  │      // 嵌入式 Pi Agent                                                   │
  │      return runEmbeddedPiAgent({ ... });                                  │
  │    },                                                                     │
  │  });                                                                      │
  └──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 6: 更新会话存储                                                     │
  │                                                                           │
  │  await updateSessionStoreAfterAgentRun({                                  │
  │    cfg, sessionId, sessionKey, storePath,                                 │
  │    defaultProvider, defaultModel, result,                                 │
  │  });                                                                      │
  │                                                                           │
  │  • 更新 token 使用量                                                      │
  │  • 记录使用的模型                                                         │
  └──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 7: 结果投递                                                         │
  │                                                                           │
  │  return await deliverAgentCommandResult({                                 │
  │    cfg, deps, runtime, opts,                                              │
  │    sessionEntry, result, payloads,                                        │
  │  });                                                                      │
  │                                                                           │
  │  • 如果 deliver=true，发送到消息渠道                                      │
  │  • 返回执行结果                                                           │
  └──────────────────────────────────────────────────────────────────────────┘

```text

### 4.3 关键代码

```typescript
// src/commands/agent.ts (简化版)

export async function agentCommand(opts, runtime, deps) {
  const cfg = loadConfig();
  
  // 解析会话
  const { sessionId, sessionKey, sessionEntry } = resolveSession({
    cfg, to: opts.to, sessionId: opts.sessionId, sessionKey: opts.sessionKey,
  });
  
  const runId = opts.runId ?? sessionId;
  
  try {
    // 注册运行上下文
    registerAgentRunContext(runId, { sessionKey, verboseLevel });
    
    // 构建技能快照
    const skillsSnapshot = buildWorkspaceSkillSnapshot(workspaceDir, { config: cfg });
    
    // 解析模型
    const { provider, model } = resolveConfiguredModelRef({ cfg });
    
    // 运行 Agent (带降级)
    const { result } = await runWithModelFallback({
      cfg, provider, model,
      run: (p, m) => runEmbeddedPiAgent({
        sessionId, sessionKey, prompt: opts.message,
        provider: p, model: m, thinkLevel, runId,
        // 事件回调
        onAgentEvent: (evt) => {
          if (evt.stream === "lifecycle" && evt.data?.phase === "end") {
            lifecycleEnded = true;
          }
        },
      }),
    });
    
    // 发射生命周期结束事件
    if (!lifecycleEnded) {
      emitAgentEvent({
        runId, stream: "lifecycle",
        data: { phase: "end", startedAt, endedAt: Date.now() },
      });
    }
    
    // 投递结果
    return await deliverAgentCommandResult({ result, opts });
    
  } finally {
    clearAgentRunContext(runId);
  }
}

```

---

## 五、Agent Runtime 调用

### 5.1 runEmbeddedPiAgent

这是 Agent Runtime 的入口函数：

```typescript
// src/agents/pi-embedded-runner/run.ts

export async function runEmbeddedPiAgent(params: RunEmbeddedPiAgentParams): Promise<EmbeddedPiRunResult> {
  // 1. 会话排队 (并发控制)
  // 2. 模型解析与认证
  // 3. 重试循环 (runEmbeddedAttempt)
  // 4. 返回结果
}

```text

### 5.2 执行流程

```

┌─────────────────────────────────────────────────────────────────────────────┐
│                     runEmbeddedPiAgent 执行流程                              │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐
  │ params          │
  │ ─────────────   │
  │ • sessionId     │
  │ • prompt        │
  │ • provider      │
  │ • model         │
  │ • runId         │
  │ • skillsSnap    │
  └────────┬────────┘
           │
           ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 1: 会话排队                                                         │
  │                                                                           │
  │  await sessionQueue.waitForSlot(sessionKey, { runId, timeoutMs });        │
  │                                                                           │
  │  • 防止同一会话并发执行                                                   │
  │  • 实现公平调度                                                           │
  └──────────────────────────────────────────────────────────────────────────┘
           │
           ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Step 2: 重试循环                                                         │
  │                                                                           │
  │  for (let attempt = 0; attempt < maxAttempts; attempt++) {                │
  │    try {                                                                  │
  │      return await runEmbeddedAttempt({                                    │
  │        sessionId, sessionFile, workspaceDir,                              │
  │        prompt, provider, model, thinkLevel,                               │
  │        skillsSnapshot, runId,                                             │
  │      });                                                                  │
  │    } catch (err) {                                                        │
  │      if (!isRetryable(err)) throw err;                                    │
  │    }                                                                      │
  │  }                                                                        │
  └──────────────────────────────────────────────────────────────────────────┘
           │
           ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  runEmbeddedAttempt (单次尝试)                                            │
  │                                                                           │
  │  1. 准备沙箱环境 (buildEmbeddedSandboxInfo)                               │
  │  2. 加载技能 (loadSkillsForSession)                                       │
  │  3. 构建系统提示 (buildSystemPrompt)                                      │
  │  4. 创建工具集 (createCodingTools + createOpenClawTools)                  │
  │  5. 初始化会话管理器 (SessionManager)                                     │
  │  6. 调用 LLM (streamAgentResponse)                                        │
  │  7. 返回结果                                                              │
  └──────────────────────────────────────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────┐
  │ Result          │
  │ ─────────────   │
  │ • text          │
  │ • payloads      │
  │ • meta          │
  └─────────────────┘

```text

---

## 六、事件广播机制

### 6.1 为什么需要事件广播

Agent Runtime 执行 LLM 调用时，响应是**流式的**——每秒可能产出 20+ 个文本片段。这些片段需要实时送达所有连接的客户端（Web UI、macOS App、CLI 等），让用户看到"打字机效果"。

但 Agent Runtime 不知道有哪些客户端连接——这是 Gateway 的职责。所以需要一个**事件广播系统**作为桥梁：

```text
Agent Runtime                    事件总线                      Gateway
(产出事件)                    (进程内 pub/sub)               (广播给客户端)

"好" ──→ emitAgentEvent() ──→ listeners Set ──→ broadcast() ──→ Client 1
"的" ──→ emitAgentEvent() ──→ listeners Set ──→ broadcast() ──→ Client 2
"，" ──→ emitAgentEvent() ──→ listeners Set ──→ broadcast() ──→ Client N
"我" ──→ ...

```

**关键设计**: 这是同进程内的发布/订阅——不走网络，`emitAgentEvent()` 直接遍历内存中的回调函数 Set。

### 6.2 事件系统架构

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                          事件广播系统                                    │
│                                                                         │
│  Agent Runtime                                    Gateway Server        │
│       │                                                │                │
│       │ emitAgentEvent({                               │                │
│       │   runId,                                       │                │
│       │   stream: "assistant",                         │                │
│       │   data: { text: "好的，" }                     │                │
│       │ })                                             │                │
│       │                                                │                │
│       ▼                                                │                │
│  ┌─────────────────┐                                   │                │
│  │ agent-events.ts │                                   │                │
│  │ (事件总线)       │                                   │                │
│  │                 │                                   │                │
│  │ ① 分配 seq 序号 │  ── onAgentEvent(listener) ────► │                │
│  │ ② 附加 ts 时间戳│                                   │                │
│  │ ③ 遍历 listeners│                                   │                │
│  └─────────────────┘                                   │                │
│                                                        ▼                │
│                                               ┌─────────────────┐      │
│                                               │ server-chat.ts  │      │
│                                               │ (事件处理器)      │      │
│                                               │                 │      │
│                                               │ ④ 序号连续性检查│      │
│                                               │ ⑤ 分流:         │      │
│                                               │   chat 流 (限频)│      │
│                                               │   agent 流 (原始)│     │
│                                               └────────┬────────┘      │
│                                                        │               │
│                                                        ▼               │
│                                               ┌─────────────────┐      │
│                                               │ broadcast()     │      │
│                                               │ (WebSocket 广播) │      │
│                                               │                 │      │
│                                               │ ⑥ 构建 JSON 帧  │      │
│                                               │ ⑦ 权限检查      │      │
│                                               │ ⑧ 慢客户端丢弃  │      │
│                                               │ ⑨ 发送给所有客户│      │
│                                               └────────┬────────┘      │
│                                                        │               │
│                ┌───────────────────────────┬────────────┘               │
│                ▼                           ▼                            │
│       ┌─────────────────┐        ┌─────────────────┐                   │
│       │  Web UI          │        │  macOS/iOS App  │                   │
│       │  (chat 流: delta)│        │  (agent 流: 全) │                   │
│       └─────────────────┘        └─────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘

```text

### 6.3 两条事件流

Gateway 将事件分成**两条独立的流**广播给客户端：

| 流名 | WebSocket event | 内容 | 受众 | 限频 |
| ---- | ---- | ---- | ---- | ---- |
| `"chat"` | `{type:"event", event:"chat", ...}` | 用户可见的文本更新 (delta/final) | 聊天界面 | **150ms 节流** |
| `"agent"` | `{type:"event", event:"agent", ...}` | 全部原始事件 (工具/生命周期/文本) | 开发者面板 | 无限频 |

**为什么两条流**: Web UI 聊天窗口只需要文本更新（高频但需限频），而开发者工具面板需要看到所有事件（工具调用、错误等）。分开广播避免聊天界面处理不相关事件。

**chat 流的 150ms 限频**:

```text
LLM delta 事件到达频率: 每秒 20-30 次
    │
    ▼
emitChatDelta() 检查:
    │
    ├── 距上次发送 < 150ms? → 只缓存文本，不发送
    └── 距上次发送 ≥ 150ms? → 发送完整累积文本
                               { state: "delta", message: { text: "好的，我来..." } }

效果: 客户端每秒收到 ~6-7 次更新 (而非 20-30 次)
每次收到的是到目前为止的完整文本 (非增量), 客户端直接替换显示即可

```

### 6.4 emitAgentEvent

```typescript
// src/infra/agent-events.ts

const seqByRun = new Map<string, number>();    // 每个 runId 的序列号
const listeners = new Set<(evt) => void>();    // 监听器集合
const runContextById = new Map<string, AgentRunContext>();  // 运行上下文

export function emitAgentEvent(event: Omit<AgentEventPayload, "seq" | "ts">) {
  // 1. 递增序列号
  const nextSeq = (seqByRun.get(event.runId) ?? 0) + 1;
  seqByRun.set(event.runId, nextSeq);
  
  // 2. 获取上下文
  const context = runContextById.get(event.runId);
  const sessionKey = event.sessionKey ?? context?.sessionKey;
  
  // 3. 构建完整事件
  const enriched: AgentEventPayload = {
    ...event,
    sessionKey,
    seq: nextSeq,
    ts: Date.now(),
  };
  
  // 4. 通知所有监听器
  for (const listener of listeners) {
    try {
      listener(enriched);
    } catch { /* ignore */ }
  }
}

export function onAgentEvent(listener: (evt: AgentEventPayload) => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

```text

### 6.5 createAgentEventHandler

Gateway 服务器注册的事件处理器：

```typescript
// src/gateway/server-chat.ts

export function createAgentEventHandler({
  broadcast,
  nodeSendToSession,
  agentRunSeq,
  chatRunState,
  clearAgentRunContext,
}) {
  return (evt: AgentEventPayload) => {
    const sessionKey = chatLink?.sessionKey ?? resolveSessionKeyForRun(evt.runId);
    const agentPayload = sessionKey ? { ...evt, sessionKey } : evt;
    
    // 1. 检查序列号连续性
    const last = agentRunSeq.get(evt.runId) ?? 0;
    if (evt.seq !== last + 1) {
      broadcast("agent", {
        runId: evt.runId,
        stream: "error",
        data: { reason: "seq gap", expected: last + 1, received: evt.seq },
      });
    }
    agentRunSeq.set(evt.runId, evt.seq);
    
    // 2. 广播给所有客户端
    broadcast("agent", agentPayload);
    
    // 3. 发送给关联的 Node
    nodeSendToSession(sessionKey, "agent", agentPayload);
    
    // 4. 处理 chat 事件 (delta/final)
    if (evt.stream === "assistant" && typeof evt.data?.text === "string") {
      emitChatDelta(sessionKey, clientRunId, evt.seq, evt.data.text);
    }
    
    // 5. 处理生命周期结束
    if (lifecyclePhase === "end" || lifecyclePhase === "error") {
      emitChatFinal(sessionKey, clientRunId, evt.seq, lifecyclePhase);
      clearAgentRunContext(evt.runId);
    }
  };
}

```

### 6.6 broadcast

广播函数将事件发送给所有连接的客户端：

```typescript
// src/gateway/server-broadcast.ts

export function createGatewayBroadcaster(params: { clients: Set<GatewayWsClient> }) {
  let seq = 0;
  
  const broadcast = (event: string, payload: unknown, opts?: { dropIfSlow?: boolean }) => {
    const eventSeq = ++seq;
    
    // 构建 JSON 帧
    const frame = JSON.stringify({
      type: "event",
      event,
      payload,
      seq: eventSeq,
    });
    
    // 发送给所有客户端
    for (const client of params.clients) {
      // 权限检查
      if (!hasEventScope(client, event)) continue;
      
      // 慢客户端处理
      const slow = client.socket.bufferedAmount > MAX_BUFFERED_BYTES;
      if (slow && opts?.dropIfSlow) continue;
      if (slow) {
        client.socket.close(1008, "slow consumer");
        continue;
      }
      
      // 发送
      try {
        client.socket.send(frame);
      } catch { /* ignore */ }
    }
  };
  
  return { broadcast };
}

```text

---

## 七、响应返回流程

### 7.1 双响应模式

Gateway 的 `agent` 方法采用双响应模式：

```

┌─────────────────────────────────────────────────────────────────────────────┐
│                          双响应模式                                          │
└─────────────────────────────────────────────────────────────────────────────┘

  Client                                                        Gateway
     │                                                             │
     │ ═══════════════ 请求 ════════════════════════════════════  │
     │                                                             │
     │  ──── req:agent ─────────────────────────────────────────►  │
     │       { method: "agent", id: "req-1", params: {...} }       │
     │                                                             │
     │ ═══════════════ 第一响应 (立即) ═════════════════════════  │
     │                                                             │
     │  ◄─── res (accepted) ────────────────────────────────────  │
     │       {                                                     │
     │         type: "res", id: "req-1", ok: true,                 │
     │         payload: {                                          │
     │           runId: "run-xxx",                                 │
     │           status: "accepted",                               │
     │           acceptedAt: 1706000000000                         │
     │         }                                                   │
     │       }                                                     │
     │                                                             │
     │ ═══════════════ 流式事件 ════════════════════════════════  │
     │                                                             │
     │  ◄─── event:agent (stream) ──────────────────────────────  │
     │  ◄─── event:agent (stream) ──────────────────────────────  │
     │  ◄─── event:agent (stream) ──────────────────────────────  │
     │       ...                                                   │
     │                                                             │
     │ ═══════════════ 第二响应 (完成后) ═══════════════════════  │
     │                                                             │
     │  ◄─── res (final) ───────────────────────────────────────  │
     │       {                                                     │
     │         type: "res", id: "req-1", ok: true,                 │
     │         payload: {                                          │
     │           runId: "run-xxx",                                 │
     │           status: "ok",                                     │
     │           summary: "completed",                             │
     │           result: { text: "...", payloads: [...] }          │
     │         }                                                   │
     │       }                                                     │
     │                                                             │

```text

### 7.2 响应代码

```typescript
// src/gateway/server-methods/agent.ts

// 立即响应
const accepted = {
  runId,
  status: "accepted" as const,
  acceptedAt: Date.now(),
};
context.dedupe.set(`agent:${idem}`, { ts: Date.now(), ok: true, payload: accepted });
respond(true, accepted, undefined, { runId });

// 异步执行
void agentCommand(...)
  .then((result) => {
    // 成功时的最终响应
    const payload = {
      runId,
      status: "ok" as const,
      summary: "completed",
      result,
    };
    context.dedupe.set(`agent:${idem}`, { ts: Date.now(), ok: true, payload });
    // 发送第二个响应帧 (相同 id)
    respond(true, payload, undefined, { runId });
  })
  .catch((err) => {
    // 失败时的最终响应
    const error = errorShape(ErrorCodes.UNAVAILABLE, String(err));
    const payload = {
      runId,
      status: "error" as const,
      summary: String(err),
    };
    context.dedupe.set(`agent:${idem}`, { ts: Date.now(), ok: false, payload, error });
    respond(false, payload, error, { runId });
  });

```

---

## 八、完整数据流图

### 8.1 请求处理时序图

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                      完整请求处理时序图                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  Client          Gateway           agentHandler      agentCommand       Runtime
     │                │                   │                │                │
     │  req:agent     │                   │                │                │
     │ ─────────────► │                   │                │                │
     │                │ handleGateway     │                │                │
     │                │ Request()         │                │                │
     │                │ ─────────────────►│                │                │
     │                │                   │                │                │
     │                │                   │ validate       │                │
     │                │                   │ params         │                │
     │                │                   │ ◄─────────────►│                │
     │                │                   │                │                │
     │                │                   │ resolve        │                │
     │                │                   │ session        │                │
     │                │                   │ ◄─────────────►│                │
     │                │                   │                │                │
     │  res:accepted  │                   │                │                │
     │ ◄───────────── │ ◄─────────────────│                │                │
     │                │                   │                │                │
     │                │                   │ void           │                │
     │                │                   │ agentCommand() │                │
     │                │                   │ ──────────────►│                │
     │                │                   │                │                │
     │                │                   │                │ runEmbedded    │
     │                │                   │                │ PiAgent()      │
     │                │                   │                │ ──────────────►│
     │                │                   │                │                │
     │                │                   │                │     LLM call   │
     │                │                   │                │ ◄─────────────►│
     │                │                   │                │                │
     │  event:agent   │                   │                │  emitAgent     │
     │ ◄───────────── │ ◄─────────────────│ ◄──────────────│ ◄──────────────│Event
     │  (stream)      │                   │                │                │
     │                │                   │                │     Tool call  │
     │  event:agent   │                   │                │ ◄─────────────►│
     │ ◄───────────── │ ◄─────────────────│ ◄──────────────│ ◄──────────────│
     │  (tool_start)  │                   │                │                │
     │                │                   │                │                │
     │  event:agent   │                   │                │                │
     │ ◄───────────── │ ◄─────────────────│ ◄──────────────│ ◄──────────────│
     │  (tool_result) │                   │                │                │
     │                │                   │                │                │
     │  event:agent   │                   │                │  lifecycle:end │
     │ ◄───────────── │ ◄─────────────────│ ◄──────────────│ ◄──────────────│
     │  (lifecycle)   │                   │                │                │
     │                │                   │                │                │
     │                │                   │ result         │                │
     │                │                   │ ◄──────────────│ ◄──────────────│
     │                │                   │                │                │
     │  res:final     │                   │                │                │
     │ ◄───────────── │ ◄─────────────────│                │                │
     │  (ok/error)    │                   │                │                │
     │                │                   │                │                │

```

### 8.2 数据结构流转

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                         数据结构流转                                         │
└─────────────────────────────────────────────────────────────────────────────┘

  请求参数 (AgentParams)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ {                                                                       │
  │   message: "帮我写一个排序算法",                                        │
  │   idempotencyKey: "idem-abc123",                                        │
  │   sessionKey: "global",                                                 │
  │   thinking: "medium",                                                   │
  │   deliver: false,                                                       │
  │ }                                                                       │
  └─────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
  agentCommand 选项 (AgentCommandOpts)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ {                                                                       │
  │   message: "帮我写一个排序算法",                                        │
  │   sessionKey: "global",                                                 │
  │   thinking: "medium",                                                   │
  │   deliver: false,                                                       │
  │   runId: "idem-abc123",                                                 │
  │   channel: "internal",                                                  │
  │ }                                                                       │
  └─────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
  runEmbeddedPiAgent 参数
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ {                                                                       │
  │   sessionId: "uuid-xxx",                                                │
  │   sessionKey: "global",                                                 │
  │   sessionFile: "~/.openclaw/agents/pi/sessions/xxx.jsonl",              │
  │   workspaceDir: "~/workspace",                                          │
  │   prompt: "帮我写一个排序算法",                                         │
  │   provider: "anthropic",                                                │
  │   model: "claude-sonnet-4-20250514",                                    │
  │   thinkLevel: "medium",                                                 │
  │   skillsSnapshot: {...},                                                │
  │   runId: "idem-abc123",                                                 │
  │ }                                                                       │
  └─────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
  AgentEventPayload (事件)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ {                                                                       │
  │   runId: "idem-abc123",                                                 │
  │   seq: 1,                                                               │
  │   stream: "assistant",                                                  │
  │   ts: 1706000001000,                                                    │
  │   sessionKey: "global",                                                 │
  │   data: {                                                               │
  │     text: "我来帮你实现一个快速排序算法...",                            │
  │   },                                                                    │
  │ }                                                                       │
  └─────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
  EmbeddedPiRunResult (结果)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ {                                                                       │
  │   text: "我来帮你实现一个快速排序算法...\n\n完成了!",                   │
  │   payloads: [...],                                                      │
  │   meta: {                                                               │
  │     provider: "anthropic",                                              │
  │     model: "claude-sonnet-4-20250514",                                  │
  │     inputTokens: 150,                                                   │
  │     outputTokens: 500,                                                  │
  │     aborted: false,                                                     │
  │   },                                                                    │
  │ }                                                                       │
  └─────────────────────────────────────────────────────────────────────────┘

```

---

## 总结

### 关键要点

| 要点 | 描述 |
| ---- | ---- |
| **双响应模式** | 先返回 accepted，异步执行后再返回 final |
| **幂等性** | 通过 idempotencyKey 防止重复执行 |
| **事件流** | emitAgentEvent → onAgentEvent → broadcast (两条流: chat + agent) |
| **会话管理** | SessionEntry 存储会话状态和配置 |
| **模型降级** | runWithModelFallback 支持自动降级 |

### 请求处理要点

```text
1. 接收请求 → handleGatewayRequest
2. 权限检查 → authorizeGatewayMethod
3. 参数验证 → validateAgentParams
4. 幂等检查 → dedupe.get()
5. 立即响应 → respond(accepted)
6. 异步执行 → agentCommand()
7. 事件广播 → emitAgentEvent → broadcast
8. 最终响应 → respond(final)

```

### 相关文档

| 文档 | 描述 |
| ---- | ---- |
| [GATEWAY-STARTUP.md](./GATEWAY-STARTUP.md) | Gateway 启动流程 |
| [GATEWAY-PROTOCOL.md](./GATEWAY-PROTOCOL.md) | 协议格式详解 |
| [GATEWAY-ARCHITECTURE.md](./GATEWAY-ARCHITECTURE.md) | Gateway 架构总览 |
