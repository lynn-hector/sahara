# Agent Runtime 回复投递架构

> 本文档详解 Agent Runtime 产出回复后的"最后一公里"：从 Agent 输出到用户收到消息，经历的串行化、跨渠道路由、格式适配、去重和限频等完整投递管线。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、回复类型与载荷](#二回复类型与载荷)
- [三、调度主流程 (dispatch-from-config)](#三调度主流程-dispatch-from-config)
- [四、ReplyDispatcher 串行化](#四replydispatcher-串行化)
- [五、跨渠道路由](#五跨渠道路由)
- [六、载荷构建](#六载荷构建)
- [七、去重机制](#七去重机制)
- [八、Gateway WebSocket 广播](#八gateway-websocket-广播)
- [九、渠道投递适配](#九渠道投递适配)
- [十、Followup 队列](#十followup-队列)
- [十一、配置参考](#十一配置参考)
- [十二、关键源文件索引](#十二关键源文件索引)

---

## 一、全局视角

### 1.1 两条并行投递路径

Agent Runtime 的回复同时通过两条独立路径到达用户：

```text
Agent Runtime (session.prompt 完成)
    │
    ├─────────────────────────────────────────────────────────┐
    │                                                         │
    ▼                                                         ▼
路径 A: 消息渠道 (回调驱动)                    路径 B: Gateway WebSocket (事件驱动)
┌─────────────────────────────┐              ┌─────────────────────────────┐
│  onBlockReply / onToolResult│              │  emitAgentEvent()           │
│         │                   │              │         │                   │
│         ▼                   │              │         ▼                   │
│  ReplyDispatcher            │              │  全局事件总线               │
│  (串行化 + 人类延迟)        │              │         │                   │
│         │                   │              │         ▼                   │
│         ▼                   │              │  Gateway AgentEventHandler  │
│  routeReply / deliver       │              │  (150ms 限频)              │
│         │                   │              │         │                   │
│         ▼                   │              │         ▼                   │
│  WhatsApp / Telegram /      │              │  WebSocket broadcast       │
│  Discord / Signal / ...     │              │  → Web UI                  │
└─────────────────────────────┘              └─────────────────────────────┘
```

### 1.2 路径差异总结

| 特性 | 路径 A (消息渠道) | 路径 B (Gateway WebSocket) |
| ---- | ---- | ---- |
| 触发方式 | `onBlockReply` / `onToolResult` 回调 | `emitAgentEvent` 全局事件总线 |
| 粒度 | 分块（BlockChunker 控制） | 每个 delta（150ms 限频） |
| 格式 | 纯文本/Markdown + 媒体 | JSON 帧（chat/agent 事件） |
| 排序保证 | ReplyDispatcher 串行队列 | WebSocket 天然有序 |
| 去重 | messaging tool 发送重复检测 | seq 编号 + 客户端去重 |
| 延迟模拟 | 可配置 humanDelay | 无（实时流式） |

---

## 二、回复类型与载荷

### 2.1 ReplyPayload 结构

```typescript
type ReplyPayload = {
  text?: string;              // 文本内容
  mediaUrl?: string;          // 单个媒体 URL
  mediaUrls?: string[];       // 多个媒体 URL
  replyToId?: string;         // 回复目标消息 ID
  replyToTag?: boolean;       // 是否 @ 引用
  replyToCurrent?: boolean;   // 回复当前消息
  audioAsVoice?: boolean;     // 音频作为语音消息发送
  isError?: boolean;          // 是否为错误消息
  channelData?: Record<string, unknown>; // 渠道特定数据
};
```

### 2.2 三种回复类型

```text
Agent 运行过程中:
    │
    ├── tool 回复 (onToolResult)
    │   → 工具执行结果（"读取了 package.json..."）
    │   → 仅在非群聊 + 非原生命令时发送
    │
    ├── block 回复 (onBlockReply)
    │   → 流式文本分块（实时推送给用户）
    │   → 由 BlockChunker 控制粒度
    │
    └── final 回复 (sendFinalReply)
        → Agent 完成后的最终回复
        → 如果已有 block 回复 → 可能被去重跳过
```

**投递顺序保证**: tool → block → final 严格串行，由 ReplyDispatcher 的 Promise 链保证。

### 2.3 特殊回复类型

| 类型 | 说明 |
| ---- | ---- |
| responsePrefix | 回复前缀（如 `"[claude-sonnet] "`），支持模板变量 `{model}` `{provider}` |
| heartbeat | 心跳回复，如果内容仅为 `HEARTBEAT_OK` 则静默丢弃 |
| silent reply | LLM 输出的静默标记，不发送给用户 |

---

## 三、调度主流程 (dispatch-from-config)

> 源文件: `src/auto-reply/reply/dispatch-from-config.ts`

```text
dispatchReplyFromConfig(ctx, cfg, dispatcher)
    │
    ├── ① 入站去重检查 (shouldSkipDuplicateInbound)
    │      → 相同消息在短时间内重复到达? → 跳过
    │
    ├── ② 消息接收 Hook (hookRunner.runMessageReceived)
    │      → 插件可修改/拦截消息
    │
    ├── ③ 跨渠道路由决策
    │      OriginatingChannel !== currentSurface?
    │      → 是: shouldRouteToOriginating = true
    │      → 所有回复通过 routeReply() 而非 dispatcher
    │
    ├── ④ 快速中止检查 (tryFastAbortFromMessage)
    │      → 新消息在排队期间到达? → 中止旧的
    │
    ├── ⑤ 配置回调并执行 Agent
    │      getReplyFromConfig({
    │        onToolResult: (payload) → dispatcher.sendToolResult(payload)
    │        onBlockReply: (payload) → dispatcher.sendBlockReply(payload)
    │      })
    │      → Agent 运行，流式回调被触发
    │
    ├── ⑥ 最终回复处理
    │      ├── 应用 TTS (maybeApplyTtsToPayload)
    │      ├── 路由或投递 (routeReply / dispatcher.sendFinalReply)
    │      └── TTS-only: 如果 block 流成功但无最终文本 → 仅生成语音
    │
    ├── ⑦ 等待投递空闲 (dispatcher.waitForIdle)
    │      → 确保所有排队的回复都已投递
    │
    └── ⑧ 诊断记录 (logMessageProcessed)
```

---

## 四、ReplyDispatcher 串行化

> 源文件: `src/auto-reply/reply/reply-dispatcher.ts`

### 4.1 串行化机制

ReplyDispatcher 使用 **Promise 链** 确保所有回复按到达顺序投递：

```text
sendToolResult(p1) ──→ ┐
sendBlockReply(p2) ──→ ├── Promise chain: p1.then(p2).then(p3).then(p4)
sendBlockReply(p3) ──→ │
sendFinalReply(p4) ──→ ┘

每个投递是链上的一个 .then()
前一个完成后才开始下一个
```

```typescript
// 内部实现 (简化)
let sendChain = Promise.resolve();

function enqueue(payload, kind) {
  pending++;
  sendChain = sendChain.then(async () => {
    if (kind === "block" && sentFirstBlock) {
      await sleep(getHumanDelay());  // 人类延迟
    }
    const normalized = normalizeReplyPayload(payload);
    await deliver(normalized, { kind });
    sentFirstBlock = true;
    pending--;
    if (pending === 0) onIdle?.();
  });
}
```

### 4.2 人类延迟 (humanDelay)

模拟人类打字节奏，避免机器感：

| 配置 | 默认值 | 说明 |
| ---- | ---- | ---- |
| `minMs` | 800 | 最小延迟 |
| `maxMs` | 2500 | 最大延迟 |
| mode `"off"` | 0 | 禁用 |

**规则**: 仅在**第二个及之后**的 block 回复前添加延迟。第一个 block 立即发送。

### 4.3 载荷规范化

`normalizeReplyPayload()` 在投递前对载荷进行清理：

```text
原始 payload
    │
    ├── 添加 responsePrefix（如 "[claude] "）
    ├── 移除 heartbeat token (HEARTBEAT_OK)
    ├── 去除首尾空白
    ├── 检测静默回复 → 跳过
    └── 空内容 + 无媒体 → 跳过
```

### 4.4 空闲检测

```typescript
function waitForIdle(): Promise<void> {
  if (pending === 0) return Promise.resolve();
  return new Promise(resolve => { onIdle = resolve; });
}
```

`dispatchReplyFromConfig` 在最后调用 `waitForIdle()` 确保所有排队回复投递完毕再返回。

---

## 五、跨渠道路由

> 源文件: `src/auto-reply/reply/route-reply.ts`

### 5.1 何时触发跨渠道路由

```text
场景: 用户从 Telegram 发消息，但当前 Agent 会话绑定在 WhatsApp 上

MsgContext:
  Provider: "whatsapp"          ← 当前会话绑定的渠道
  OriginatingChannel: "telegram" ← 消息实际来源
  OriginatingTo: "12345"        ← Telegram 中的 chatId

判断:
  OriginatingChannel ("telegram") !== currentSurface ("whatsapp")
  → shouldRouteToOriginating = true
  → 所有回复通过 routeReply() 路由到 Telegram
```

### 5.2 routeReply 流程

```text
routeReply({
  payload, channel: "telegram", to: "12345",
  threadId, sessionKey, cfg
})
    │
    ├── 解析渠道适配器
    ├── 线程 ID 处理:
    │   ├── Slack: threadId → replyToId (合并)
    │   └── 其他: threadId 单独传递
    │
    ├── 调用 deliverOutboundPayloads()
    │   → 通过目标渠道的 API 发送
    │
    └── 可选 mirror: 同步写入会话转录
```

### 5.3 线程 ID 保留

| 渠道 | 线程处理 |
| ---- | ---- |
| Slack | `threadId` 合并为 `replyToId`（Slack 的线程就是回复） |
| Telegram | `threadId` 作为 `message_thread_id` 传递（话题组） |
| Discord | `threadId` 用于定位线程频道 |
| 其他 | 直接传递 |

---

## 六、载荷构建

### 6.1 从 Agent 输出到 ReplyPayload

> 源文件: `src/agents/pi-embedded-runner/run/payloads.ts` — `buildEmbeddedRunPayloads()`

```text
Agent 运行完成
    │
    ├── assistantTexts[]  ← 收集的助手文本块
    ├── toolMetas[]       ← 工具调用元数据
    ├── lastAssistant     ← 最后的助手消息
    └── lastToolError     ← 最后的工具错误
    │
    ▼ buildEmbeddedRunPayloads()
    │
    ├── ① 处理 assistant 错误消息
    ├── ② 格式化 inline 工具结果（如 verbose 模式）
    ├── ③ 提取 reasoning 文本（如 reasoningLevel="on"）
    ├── ④ 提取回复文本:
    │      ├── parseReplyDirectives() → 提取 mediaUrls, replyToId
    │      ├── 过滤被抑制的错误
    │      └── 构建 ReplyPayload { text, mediaUrls, replyToId, ... }
    ├── ⑤ 处理工具错误（仅无用户回复时展示）
    └── ⑥ 过滤空/静默 payload
    │
    ▼
ReplyPayload[] → 传给上层 dispatchReplyFromConfig
```

### 6.2 responsePrefix 模板

```yaml
# 配置示例
agents:
  defaults:
    responsePrefix: "[{model}] "
```

支持的模板变量:

| 变量 | 说明 | 示例 |
| ---- | ---- | ---- |
| `{model}` | 模型名 | `claude-sonnet-4-20250514` |
| `{provider}` | 提供商 | `anthropic` |
| `{thinkingLevel}` | 思考级别 | `high` |
| `{identity.name}` | Agent 身份名 | `OpenClaw` |

---

## 七、去重机制

### 7.1 消息工具发送去重

> 源文件: `src/agents/pi-embedded-helpers/messaging-dedupe.ts`

当 Agent 通过 `message` 工具（WhatsApp/Telegram 等）直接发送消息后，最终的 final reply 可能是重复的。系统通过文本相似度检测来去重：

```text
Agent 通过 message 工具发送: "已将文件发送到群组"
Agent 最终回复: "好的，我已经将文件发送到群组了"
    │
    ▼ isMessagingToolDuplicate()
    │
    ├── 规范化: 小写 + 去 emoji + 压缩空格
    ├── 比较: 规范化后文本是否包含/被包含
    ├── 最小长度: 10 字符
    │
    └── 判定为重复 → 跳过 final reply
```

### 7.2 目标渠道去重

```text
shouldSuppressMessagingToolReplies()
    │
    ├── 消息工具发送目标与当前渠道/账号/目标相同?
    │   → 是: 抑制所有 final reply（避免重复投递）
    │   → 否: 正常投递
```

### 7.3 入站消息去重

```text
shouldSkipDuplicateInbound(ctx)
    │
    ├── 相同消息 ID 在短时间内重复到达?
    │   → 跳过处理（防止 webhook 重试导致重复回复）
```

---

## 八、Gateway WebSocket 广播

> 源文件: `src/gateway/server-chat.ts`

### 8.1 事件流结构

Gateway 将 Agent 事件分为两个 WebSocket 流：

| 流 | 内容 | 受众 |
| ---- | ---- | ---- |
| `"chat"` | 用户可见的消息更新（delta/final） | Web UI 聊天窗口 |
| `"agent"` | 低级事件（工具调用/生命周期/错误） | Web UI 工具面板/TUI |

### 8.2 chat 流的 150ms 限频

```text
LLM delta 到达 (每秒可能 20+ 次)
    │
    ▼
emitChatDelta()
    │
    ├── 缓存最新文本 (总是更新)
    ├── 距上次发送 < 150ms? → 跳过
    └── 距上次发送 ≥ 150ms?
        → broadcast("chat", { state: "delta", message: { text: 完整文本 } })
        → 记录发送时间
```

**关键设计**: 每次发送的是**到目前为止的完整文本**（非增量），客户端直接替换显示即可。

### 8.3 chat 流的最终消息

```text
Agent 运行结束
    │
    ▼
emitChatFinal()
    │
    ├── 刷新缓冲（发送最后的 delta 文本）
    └── broadcast("chat", { state: "final", message: { text: 完整文本 } })
```

### 8.4 序列号验证

每个事件携带递增的 `seq` 编号。Gateway 检查 `seq` 连续性，跳过乱序事件，确保客户端看到的事件流是有序的。

---

## 九、渠道投递适配

> 源文件: `src/infra/outbound/deliver.ts` — `deliverOutboundPayloads()`

### 9.1 统一投递入口

```text
deliverOutboundPayloads({
  channel: "telegram",
  to: "12345",
  payloads: [{ text: "...", mediaUrls: [...] }],
  threadId, replyToId, ...
})
    │
    ▼
① 加载渠道适配器 (loadChannelOutboundAdapter)
    │
    ▼
② 遍历 payloads:
    │
    ├── 有 channelData + handler.sendPayload?
    │   → 渠道自定义投递
    │
    ├── 有媒体?
    │   → handler.sendMedia(buffer, contentType, caption, ...)
    │   → 第一个媒体携带 caption，后续媒体无 caption
    │
    └── 纯文本?
        ├── 有 chunker? → 分块投递 (Signal 特殊格式化)
        └── 无 chunker → handler.sendText(text, ...)
```

### 9.2 各渠道适配差异

| 渠道 | 文本限制 | 图像投递 | 语音投递 | 线程支持 |
| ---- | ---- | ---- | ---- | ---- |
| WhatsApp | 4096 字符 | `image` + caption | `audio` + ptt | 无 |
| Telegram | 4096 字符 | `sendPhoto` + caption (1024) | `sendVoice` (Opus) | topic/reply |
| Discord | 2000 字符 | 文件附件 | 文件附件 | 频道线程 |
| Signal | 无限制 (分块) | 附件 | 附件 | 无 |
| Slack | 4000 字符 | 文件上传 | 文件上传 | 线程 ts |
| iMessage | 无限制 | `--file` 附件 | `--file` 附件 | 无 |

### 9.3 Signal 特殊处理

Signal 有自己的富文本格式（style ranges），系统会将 Markdown 转换为 Signal 格式：

```text
Markdown: **加粗** _斜体_ `代码`
    │
    ▼ markdownToSignalTextChunks()
    │
Signal: { text: "加粗 斜体 代码", styles: [{start:0,len:4,type:"bold"}, ...] }
```

---

## 十、Followup 队列

> 源文件: `src/auto-reply/reply/followup-runner.ts`

### 10.1 什么是 Followup

当主 Agent 正在运行时，新到达的用户消息可能被放入 followup 队列。主运行结束后，这些排队的消息作为新的 Agent 运行执行。

### 10.2 执行流程

```text
followup 队列中的消息
    │
    ▼
createFollowupRunner() 返回的 runner 函数
    │
    ├── ① 调用 runEmbeddedPiAgent({ prompt: queuedMessage, ... })
    ├── ② 载荷处理:
    │      ├── 去除 heartbeat token
    │      ├── 应用回复线程化 (replyToId)
    │      ├── 消息工具去重
    │      └── 过滤空载荷
    ├── ③ 投递:
    │      ├── 跨渠道? → routeReply()
    │      └── 同渠道? → deliver() 直接发送
    └── ④ 可选: 自动压缩提醒
```

### 10.3 与主调度的关系

- Followup 与主调度共享 `routeReply()` 和载荷处理逻辑
- 独立执行路径（不经过 ReplyDispatcher）
- 使用相同的去重和线程化机制

---

## 十一、配置参考

### 人类延迟

```yaml
agents:
  defaults:
    humanDelay:
      minMs: 800
      maxMs: 2500
      mode: "on"     # "on" | "off"
```

### 回复前缀

```yaml
agents:
  defaults:
    responsePrefix: "[{model}] "
```

### Block 流式回复

```yaml
agents:
  defaults:
    blockStreaming:
      enabled: true
      break: "text_end"    # "text_end" | "message_end"
      chunking:
        minChars: 100
        maxChars: 2000
        breakPreference: "paragraph"  # "paragraph" | "newline" | "sentence"
```

### 消息排队模式

```yaml
channels:
  telegram:
    dms:
      "*":
        queueMode: "steer"  # "steer" | "followup" | "collect" | "interrupt"
```

### 心跳配置

```yaml
agents:
  defaults:
    heartbeat:
      enabled: true
      intervalMinutes: 15
```

---

## 十二、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/auto-reply/reply/dispatch-from-config.ts` | 调度主流程：回调绑定、路由决策、最终回复处理 |
| `src/auto-reply/reply/reply-dispatcher.ts` | ReplyDispatcher：串行化、人类延迟、载荷规范化 |
| `src/auto-reply/reply/route-reply.ts` | 跨渠道路由：OriginatingChannel 检测、线程 ID 保留 |
| `src/auto-reply/reply/normalize-reply.ts` | 载荷规范化：responsePrefix、heartbeat 清理 |
| `src/auto-reply/reply/agent-runner-payloads.ts` | 高层载荷构建：去重过滤、线程化 |
| `src/agents/pi-embedded-runner/run/payloads.ts` | `buildEmbeddedRunPayloads`：从 Agent 输出构建 payload |
| `src/auto-reply/reply/followup-runner.ts` | Followup 队列执行器 |
| `src/auto-reply/reply/inbound-dedupe.ts` | 入站消息去重 |
| `src/agents/pi-embedded-helpers/messaging-dedupe.ts` | 消息工具发送去重 |
| `src/auto-reply/reply/response-prefix-template.ts` | responsePrefix 模板变量解析 |
| `src/infra/outbound/deliver.ts` | 统一渠道投递入口 |
| `src/infra/agent-events.ts` | 全局事件总线（`emitAgentEvent` / `onAgentEvent`） |
| `src/gateway/server-chat.ts` | Gateway WebSocket 广播：chat/agent 流、150ms 限频 |
