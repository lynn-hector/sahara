# Event Bus 架构与对接协议

> Runtime → Event Bus → Gateway 的异步事件传输通道。
> 定义 Redis Streams 数据模型、生产/消费协议、事件聚合、断线回放、Pipeline 处理器接口与演进路径。
>
> 关联文档：
> - [gRPC 协议设计](./GRPC-PROTOCOL-DESIGN.md) — 同步调度路径（路径 A）
> - [Gateway 架构设计 §8](./GATEWAY-ARCHITECTURE-DESIGN.md) — Gateway Broadcaster 模块
> - [Runtime 架构设计 §5](./RUNTIME-ARCHITECTURE-DESIGN.md) — Runtime EventEmitter 模块
> - [WebSocket 协议设计 §9](./WS-PROTOCOL-DESIGN.md) — 断线恢复与事件回放
> - [技术方案 §五](./TECH-PROPOSAL-C-END-REFACTOR.md) — Event Router 选型论证

---

## 目录

1. [概述](#一概述)
2. [架构定位](#二架构定位)
3. [Redis Streams 数据模型](#三redis-streams-数据模型)
4. [事件生产协议 (Runtime → Bus)](#四事件生产协议-runtime--bus)
5. [事件消费协议 (Bus → Gateway)](#五事件消费协议-bus--gateway)
6. [事件聚合 (Delta Aggregation)](#六事件聚合-delta-aggregation)
7. [断线恢复与事件回放](#七断线恢复与事件回放)
8. [Pipeline 处理器](#八pipeline-处理器)
9. [数据持久化与生命周期](#九数据持久化与生命周期)
10. [多 Gateway 实例路由](#十多-gateway-实例路由)
11. [背压与过载保护](#十一背压与过载保护)
12. [可观测性](#十二可观测性)
13. [容量规划](#十三容量规划)
14. [演进路径](#十四演进路径)

---

## 一、概述

### 1.1 一句话定义

Event Bus 是 Runtime 和 Gateway 之间的**异步事件传输通道**。Runtime 执行 Agent 任务时产生的每个 LLM delta、工具执行、生命周期事件，都通过 Event Bus 传递给 Gateway，再由 Gateway 推送给终端用户。

### 1.2 为什么不用 gRPC Streaming

| 对比 | gRPC Streaming | Event Bus (MQ) |
| --- | --- | --- |
| 通信模式 | 点对点 (1 Runtime → 1 Gateway) | 发布/订阅 (1 Runtime → N Gateway) |
| 生产者是否知道消费者 | 是 | **否** (解耦) |
| 中间缓冲 | 无 | **有** (流量削峰) |
| 多 Gateway 扇出 | 需要 Runtime 维护多连接 | **自然支持** |
| 事件持久化 | 无 | **有** (断线回放) |
| 背压控制 | TCP 层 | **Consumer Group** |

**结论**：gRPC 管同步调度 (路径 A)，MQ 管异步事件 (路径 B)，各管各的，互不干扰。

---

## 二、架构定位

### 2.1 两条路径全景

```text
┌──────────┐                                            ┌──────────┐
│  Gateway │  ── 路径 A: gRPC (同步) ──────────────────▶│  Runtime │
│          │                                            │          │
│          │  ◀─ 路径 B: Event Bus (异步, 本文档) ──────│          │
└──────────┘                                            └──────────┘

路径 A: Gateway → gRPC SubmitTask → Runtime (提交/中止/查询)
路径 B: Runtime → Redis Streams XADD → Gateway XREADGROUP → WS Push (事件流)
```

### 2.2 Event Bus 不做什么

| 不做 | 谁做 |
| --- | --- |
| 任务调度 | gRPC (路径 A) |
| 会话持久化 | Runtime → Redis/PG |
| 客户端协议 | Gateway → WS |
| 认证鉴权 | Gateway (JWT) |

Event Bus **只做一件事**：把 Runtime 产出的事件可靠地送达 Gateway。

---

## 三、Redis Streams 数据模型

### 3.1 Phase 1 选择 Redis Streams

| 维度 | Redis Streams | NATS JetStream |
| --- | --- | --- |
| 依赖 | 已有 Redis（会话/路由/锁都在用） | 需新增 NATS 服务 |
| 运维成本 | 零新增 | 需单独部署/监控 |
| 持久化 | 支持 (AOF/RDB) | 支持 (文件) |
| Consumer Group | 原生支持 | 原生支持 |
| 性能 | 10w msg/s (单线程) | 100w msg/s |
| 适合阶段 | Phase 1-2 (≤50K 在线) | Phase 3+ (>50K) |

**Phase 1 用 Redis Streams，Phase 3 按需迁移到 NATS。** 接口抽象为 `EventPublisher` / `EventConsumer`，迁移时只换实现。

### 3.2 Stream Key 设计

```text
events:{session_key}

示例:
  events:sess_abc123
  events:sess_def456
```

**每个 session 一个 stream**。原因：
1. 同一 session 的事件天然有序（seq 递增）
2. Gateway 只需订阅有活跃用户的 session stream
3. 不同 session 互不干扰，某个 session 事件量大不影响其他

### 3.3 消息格式

Redis Streams 的每条消息是一个 field-value 对集合。Sahara 使用单字段 `data`，值为 Protobuf 序列化的 `AgentEvent`。

```text
Stream: events:sess_abc123
  ├── ID: 1706000001000-0
  │   └── data: <protobuf bytes of AgentEvent>
  ├── ID: 1706000001050-0
  │   └── data: <protobuf bytes of AgentEvent>
  └── ...
```

AgentEvent Protobuf 定义（详见 [gRPC 协议设计 §7.2](./GRPC-PROTOCOL-DESIGN.md)）：

```text
AgentEvent {
  event_id:     "evt_01JK..."        ULID
  run_id:       "run_01JK..."        执行 ID
  session_key:  "sess_abc123"        会话标识
  task_id:      "task_01JK..."       任务 ID
  type:         EVENT_TYPE_DELTA     事件类型 (9 种)
  timestamp_ms: 1706000001000        时间戳
  seq:          42                   session 内递增序列号
  trace_id:     "abc123..."          OpenTelemetry trace

  payload: oneof {                   事件具体数据
    delta { text, stream }
    tool_start { tool_call_id, tool_name, input_json }
    tool_result { tool_call_id, tool_name, success, output, duration_ms }
    run_start { agent_id, model }
    run_complete { final_text, iterations, duration_ms }
    run_error { error_code, error_message, retryable }
    run_abort { reason, aborted_by }
    thinking { text }
    usage { model, input_tokens, output_tokens, iteration }
  }
}
```

### 3.4 Consumer Group

```text
Stream:        events:sess_abc123
Consumer Group: sahara-gw                  ← 所有 Gateway 实例共用一个组
Consumers:      gw-1, gw-2, gw-3          ← 每个 Gateway 实例一个 consumer

  注意: 同一条消息只会被组内一个 consumer 读取。
  但我们需要所有 Gateway 都能看到同一 session 的事件
  (因为用户可能连在任意 Gateway 上)。
  
  解决方案: 见 §10 多 Gateway 实例路由。
```

> **重要设计决策**：不使用 Consumer Group 做负载分发。改用每个 Gateway 独立 `XREAD`（不是 XREADGROUP），这样每个 Gateway 都能收到所有事件，然后自行过滤"该 session 的用户是否连接在我这里"。详见 §10。

---

## 四、事件生产协议 (Runtime → Bus)

### 4.1 发布流程

```text
Agent Loop 内部
  │
  ├── LLM 流式 delta ──▶ emitter.emit_delta("好的")
  │                       │
  │                       ├── 1. 构建 AgentEvent protobuf
  │                       ├── 2. 序列化为 bytes
  │                       ├── 3. XADD events:{session_key} * data <bytes>
  │                       │        └── MAXLEN ~5000 (自动裁剪)
  │                       └── 4. 返回 (不等待消费确认)
  │
  ├── 工具开始 ──▶ emitter.emit_tool_start(...)
  ├── 工具结果 ──▶ emitter.emit_tool_result(...)
  └── 执行完成 ──▶ emitter.emit_run_complete(...)
```

### 4.2 XADD 调用规范

```python
# Runtime 发布事件

stream_key = f"events:{session_key}"
event_bytes = agent_event.SerializeToString()

await redis.xadd(
    name=stream_key,
    fields={"data": event_bytes},
    maxlen=5000,           # 保留最近 5000 条
    approximate=True,      # 近似裁剪 (性能更好)
)
```

| 参数 | 值 | 说明 |
| --- | --- | --- |
| Stream Key | `events:{session_key}` | 每 session 一个 stream |
| ID | `*` (自动) | Redis 自动生成时间戳 ID |
| Field | `data` | 单字段，值为 protobuf bytes |
| MAXLEN | `~5000` | 近似裁剪，保留最近 5000 条事件 |

### 4.3 发布保证

| 保证 | 级别 | 说明 |
| --- | --- | --- |
| 顺序性 | 同一 session 内有序 | 单 Runtime 顺序 XADD；Redis 单线程保证写入顺序 |
| 持久性 | Redis AOF (可配) | 默认 `appendfsync everysec`，最多丢 1 秒事件 |
| 至少一次 | 是 | Runtime 侧不做去重；Gateway 侧用 seq 去重 |
| 延迟 | <0.5ms (内网) | Redis 内网延迟极低 |

### 4.4 seq 生成规则

```python
class RunEmitter:
    def __init__(self, ...):
        self._seq = 0                 # 每个 run 从 0 开始

    async def _emit(self, event_type, ...):
        self._seq += 1                # 单调递增
        event.seq = self._seq
        ...
```

- **范围**：per-session per-run（每次执行从 1 开始）
- **生成者**：Runtime（单 asyncio task，无并发竞争）
- **用途**：Gateway 和客户端检测事件丢失/乱序

---

## 五、事件消费协议 (Bus → Gateway)

### 5.1 消费方式选择

```text
方案 A: XREADGROUP (Consumer Group)
  └── 消息分发给组内某一个 consumer
  └── ❌ 不适用: 用户可能连在任意 Gateway，需要所有 Gateway 都看到事件

方案 B: 每个 Gateway 独立 XREAD + 本地过滤     ← 选这个
  └── 所有 Gateway 都读取所有事件
  └── 本地检查 "该 session 的用户是否连接在我这里"
  └── 有 → 推送；无 → 丢弃

方案 C: 按 session→gateway 路由表做 topic 级订阅   ← Phase 2 优化
  └── Gateway 只订阅有活跃用户的 session stream
  └── 减少无效读取
```

### 5.2 Phase 1: 独立 XREAD + 本地过滤

```go
// Gateway: internal/broadcast/consumer.go

func (c *Consumer) Run(ctx context.Context) {
    // 维护一份"当前需要监听的 session stream 列表"
    // 来源: Hub 中连接的用户 → 该用户的活跃 session
    activeStreams := c.sessionRouter.ActiveStreams()

    for {
        if len(activeStreams) == 0 {
            time.Sleep(500 * time.Millisecond)
            activeStreams = c.sessionRouter.ActiveStreams()
            continue
        }

        // XREAD: 从多个 stream 读取新消息
        // 每个 stream 记录上次读取的 ID (lastID)
        args := make([]string, 0, len(activeStreams)*2)
        for _, s := range activeStreams {
            args = append(args, s.Key)       // stream key
        }
        for _, s := range activeStreams {
            args = append(args, s.LastID)     // ">" 或具体 ID
        }

        results, err := c.redis.XRead(ctx, &redis.XReadArgs{
            Streams: args,
            Count:   100,
            Block:   1 * time.Second,
        }).Result()

        if err != nil {
            if errors.Is(err, redis.Nil) {
                continue
            }
            // 错误处理...
            continue
        }

        for _, stream := range results {
            for _, msg := range stream.Messages {
                c.handleEvent(stream.Stream, msg)
            }
        }

        // 定期刷新活跃 stream 列表
        activeStreams = c.sessionRouter.ActiveStreams()
    }
}
```

### 5.3 Phase 2 优化: 动态订阅管理

```text
用户连接到 Gateway-2
  │
  ▼
Hub.register(conn)
  │
  ▼
SessionRouter.Register(session_key, conn_id)
  │
  ▼
Consumer.Subscribe(session_key)          ← 开始监听该 session stream
  │
  ▼
... 事件流 ...
  │
用户断开连接
  │
  ▼
Consumer.Unsubscribe(session_key)        ← 停止监听 (如果本 Gateway 上无该 session 用户)
```

这样 Gateway 只 XREAD 有活跃连接的 session，不浪费 IO 读取无用事件。

### 5.4 消费后处理

```text
XREAD 返回消息
  │
  ▼
反序列化 protobuf → AgentEvent
  │
  ▼
查找目标连接: sessionRouter.LookupConns(event.session_key)
  │
  ├── 无连接 → 丢弃 (正常: 用户不在本 Gateway)
  │
  ├── 有连接 → 分发:
  │   │
  │   ├── 事件类型 == DELTA?
  │   │   ├── 是 → 送入 Aggregator (150ms 窗口)
  │   │   └── 否 → 直接推送给所有目标连接
  │   │
  │   ├── 事件类型 == RUN_COMPLETE / RUN_ERROR / RUN_ABORT?
  │   │   └── 触发 final 响应 (双响应模式的第二个 res 帧)
  │   │
  │   └── 推送: conn.SendEvent(eventName, payload)
  │
  └── 更新 lastID (记住读取进度)
```

---

## 六、事件聚合 (Delta Aggregation)

### 6.1 为什么要聚合

LLM 流式输出每秒产生 20-30 个 text_delta。如果每个 delta 都作为一个 WS 帧推送给客户端：
- 客户端每秒收到 20-30 帧，UI 重渲染压力大
- WS 帧开销（JSON 封装 + WS 头）占比过高
- 慢客户端更容易缓冲溢出

**聚合策略**：在 Gateway 侧，对同一 session 的 `DELTA` 事件做 150ms 窗口合并。

### 6.2 聚合逻辑

```text
Timeline (同一 session 的 delta 事件):

  T+0ms    delta: "好"        → 开启 150ms 窗口
  T+30ms   delta: "的"        → 追加到窗口
  T+60ms   delta: "，"        → 追加到窗口
  T+90ms   delta: "我"        → 追加到窗口
  T+120ms  delta: "来"        → 追加到窗口
  T+150ms  ★ 窗口到期 → flush → 推送一个合并帧: "好的，我来"

  T+160ms  delta: "帮"        → 新窗口
  T+190ms  delta: "你"        → 追加
  T+220ms  tool_start: exec   → 非 delta，立即 flush + 推送 tool_start
                                 同时 flush 窗口 → 推送 "帮你"
```

### 6.3 聚合规则

| 规则 | 说明 |
| --- | --- |
| 窗口时长 | 150ms（可配） |
| 触发 flush | 窗口到期 **或** 收到非 delta 事件 **或** 缓冲文本 >4KB |
| 聚合范围 | 同一 session + 同一 run + 同一 stream (assistant/thinking) |
| seq | 使用窗口内最后一个 delta 的 seq |
| 非 delta 事件 | 不参与聚合，立即推送 |
| 慢客户端 | 窗口增大到 500ms → 只保留 lifecycle 事件 → 断开 |

### 6.4 实现位置

聚合在 **Gateway 侧** 做（不在 Event Bus 中间层）。原因：
1. 每个 Gateway 实例的连接写缓冲压力不同，聚合粒度需要自适应
2. 慢客户端检测是 per-connection 的，只有 Gateway 有信息
3. Event Bus (Redis Streams) 保留完整粒度事件，便于回放和审计

---

## 七、断线恢复与事件回放

### 7.1 机制概述

客户端断线后重连到任意 Gateway 实例，携带 `resumeToken` + `lastSeqs`。新 Gateway 从 Redis Streams 中读取断线期间的事件，按 seq 顺序回放。

```text
Client 断线前:
  ├── 最后收到 sess_abc123 的 seq=42
  └── resumeToken = "rt_01JK..."

Client 重连时 (可能连到不同 Gateway):
  GET /ws?resumeToken=rt_01JK...&lastSeqs=sess_abc123:42

新 Gateway:
  1. 验证 resumeToken (Redis 查询, TTL 5min)
  2. 查询 events:sess_abc123 中 seq > 42 的事件
  3. 按 seq 顺序推送给 Client
  4. 推送 replay.complete 事件
  5. 切换到实时消费模式
```

### 7.2 回放实现

```go
// Gateway: internal/session/resume.go

func (r *Replayer) Replay(conn *ws.Conn, sessions []ResumeSession) error {
    for _, sess := range sessions {
        // 从 Redis Stream 读取 lastSeq 之后的事件
        streamKey := "events:" + sess.SessionKey
        messages, err := r.redis.XRangeN(ctx, streamKey, sess.LastStreamID, "+", 1000).Result()
        if err != nil {
            continue
        }

        replayed := 0
        for _, msg := range messages {
            event, err := deserializeEvent(msg)
            if err != nil {
                continue
            }

            // 跳过已接收的 seq
            if event.Seq <= sess.LastSeq {
                continue
            }

            // 推送给客户端 (不做聚合，回放要完整)
            conn.SendEvent(mapEventName(event.Type), eventToPayload(event))
            replayed++
        }

        slog.Info("replay complete",
            "sessionKey", sess.SessionKey,
            "fromSeq", sess.LastSeq,
            "replayed", replayed,
        )
    }

    // 发送 replay.complete
    conn.SendEvent("replay.complete", map[string]any{
        "sessionsReplayed": len(sessions),
    })

    return nil
}
```

### 7.3 回放限制

| 限制 | 值 | 说明 |
| --- | --- | --- |
| resumeToken TTL | 5 分钟 | 超过后视为新连接（不回放） |
| 最大回放事件数 | 1000 per session | 超过则截断，通知客户端刷新 |
| Stream 保留时长 | 取决于 MAXLEN | ~5000 条 ≈ 几小时到一天 |
| 回放超时 | 10 秒 | 超过后放弃回放，切到实时 |

### 7.4 resumeToken 存储

```text
Redis Key: resume:{token}
Type:      HASH
Fields:
  user_id:          "user_abc"
  sessions:         "sess_abc123:42,sess_def456:18"   ← session:lastSeq 对
  gateway_id:       "gw-1"                            ← 原来的 Gateway
  created_at:       "1706000000"
TTL:               300 (5 分钟)
```

---

## 八、Pipeline 处理器

### 8.1 概念

C 端场景需要在事件传输过程中做自定义处理（内容安全、脱敏、审计）。采用 **Pipeline 模式**：事件依次经过一组处理器，每个处理器可修改/拦截/旁路。

```text
Runtime XADD → Redis Streams → Gateway 消费
                                   │
                                   ▼
                         ┌──────────────────┐
                         │  Pipeline Runner │
                         │  (in Gateway)    │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ Content  │ │ PII      │ │ Audit    │
              │ Safety   │ │ Masking  │ │ Logger   │
              └────┬─────┘ └────┬─────┘ └────┬─────┘
                   │             │             │
                   └─────────────┼─────────────┘
                                 ▼
                         事件推送给客户端
```

### 8.2 处理器接口

```go
// internal/broadcast/pipeline.go

type EventProcessor interface {
    // Process 处理一个事件。
    // 返回修改后的事件 + 是否继续传递。
    // event=nil 表示拦截（不传递给后续处理器和客户端）。
    Process(ctx context.Context, event *AgentEvent) (result *AgentEvent, proceed bool)

    // Name 处理器名称（用于日志和指标）
    Name() string
}
```

### 8.3 内置处理器

**Phase 2 — 内容安全**

```go
type ContentSafetyProcessor struct {
    patterns []*regexp.Regexp  // 正则快筛
}

func (p *ContentSafetyProcessor) Process(ctx context.Context, event *AgentEvent) (*AgentEvent, bool) {
    if event.Type != EVENT_TYPE_DELTA {
        return event, true  // 非文本事件，跳过
    }

    text := event.GetDelta().Text
    for _, pattern := range p.patterns {
        if pattern.MatchString(text) {
            // 命中敏感内容 → 替换为 [内容已过滤]
            event.GetDelta().Text = "[内容已过滤]"
            return event, true
        }
    }

    return event, true
}
```

**Phase 2 — PII 脱敏**

```go
type PIIMaskingProcessor struct { /* ... */ }

func (p *PIIMaskingProcessor) Process(ctx context.Context, event *AgentEvent) (*AgentEvent, bool) {
    if event.Type != EVENT_TYPE_DELTA && event.Type != EVENT_TYPE_TOOL_RESULT {
        return event, true
    }
    // 正则替换手机号/身份证/邮箱等
    text := maskPII(extractText(event))
    setEventText(event, text)
    return event, true
}
```

**Phase 2 — 审计日志**

```go
type AuditLogProcessor struct {
    writer AuditWriter  // 异步写入 PG / 对象存储
}

func (p *AuditLogProcessor) Process(ctx context.Context, event *AgentEvent) (*AgentEvent, bool) {
    // 异步旁路写入，不阻塞事件传递
    go p.writer.Write(ctx, event)
    return event, true  // 不修改，继续
}
```

### 8.4 Pipeline 执行

```go
type Pipeline struct {
    processors []EventProcessor
}

func (p *Pipeline) Run(ctx context.Context, event *AgentEvent) *AgentEvent {
    current := event
    for _, proc := range p.processors {
        result, proceed := proc.Process(ctx, current)
        if !proceed || result == nil {
            return nil  // 事件被拦截
        }
        current = result
    }
    return current
}
```

### 8.5 分层安全架构

| 层 | 位置 | 延迟要求 | 策略 |
| --- | --- | --- | --- |
| **Layer 1** | Runtime (agent_loop 内) | <0.1ms | 正则快筛：SQL 注入 / XSS / 常见 prompt 注入 |
| **Layer 2** | Gateway Pipeline | <5ms | 策略规则：敏感词库 / PII 脱敏 / 审计 |
| **Layer 3** | Gateway (推送前) | <1ms | 用户级策略：年龄/地区内容限制 |

---

## 九、数据持久化与生命周期

### 9.1 数据归属

| 数据类型 | 写入者 | 存储位置 | 保留策略 | 权威来源 |
| --- | --- | --- | --- | --- |
| **会话历史** (messages[]) | Runtime | Redis + PG | 永久 | Runtime |
| **流式事件** (delta/tool) | Runtime → Bus | Redis Streams | MAXLEN ~5000 | Event Bus |
| **运行元数据** (token/耗时) | Runtime | PG | 永久 | Runtime |
| **审计日志** | Gateway Pipeline | PG / 对象存储 | 3-5 年 | Pipeline |
| **Resume Token** | Gateway | Redis | TTL 5min | Gateway |

### 9.2 Stream 生命周期

```text
events:{session_key}

创建: Runtime 首次 XADD 时自动创建
增长: 每个事件一条消息 (XADD)
裁剪: MAXLEN ~5000 (近似, 自动裁剪旧消息)
清理: Session 删除时, TTL 或定时任务清理 (Phase 2)
```

### 9.3 Redis 内存估算

```text
单条事件大小:
  ├── Redis Stream 开销: ~100 bytes
  └── Protobuf AgentEvent: ~200 bytes (delta) / ~500 bytes (tool_result)
  
单个 session stream (5000 条 MAXLEN):
  ├── delta 为主: 5000 × 300 bytes ≈ 1.5 MB
  └── 混合类型:   5000 × 400 bytes ≈ 2 MB

1000 活跃 session:
  └── 1000 × 2 MB = 2 GB

10000 活跃 session:
  └── 10000 × 2 MB = 20 GB  ← 需要 Redis 足够内存
  
优化: 降低 MAXLEN 到 1000 → 10000 session ≈ 4 GB
```

---

## 十、多 Gateway 实例路由

### 10.1 问题

用户可能连接在 Gateway-1，但事件由 Runtime 发布到 Redis Streams。所有 Gateway 都需要能看到事件。

### 10.2 Phase 1: 广播 + 本地过滤

```text
Runtime XADD → events:sess_abc123

Gateway-1 XREAD → 收到事件 → 检查 sess_abc123 的用户在我这? → 否 → 丢弃
Gateway-2 XREAD → 收到事件 → 检查 sess_abc123 的用户在我这? → 是 → 推送
Gateway-3 XREAD → 收到事件 → 检查 sess_abc123 的用户在我这? → 否 → 丢弃
```

**代价**：3 个 Gateway 都读了这条事件，只有 1 个有用。
**可接受**：10K 在线时，每秒事件 ~4500 条，每个 Gateway 读 4500 条 → Redis 15K reads/s，远低于上限。

### 10.3 Phase 2: 路由表优化

Gateway 只 XREAD 自己有活跃连接的 session stream。

```text
Redis 路由表:
  session:gateway:sess_abc123 → SET { gw-2 }
  session:gateway:sess_def456 → SET { gw-1, gw-3 }  (用户多设备)

Gateway-2 启动 Consumer 时:
  1. 查询 "我有哪些活跃 session?" → [sess_abc123, sess_xyz789]
  2. 只 XREAD events:sess_abc123, events:sess_xyz789
  3. 用户新连接 → 动态增加 XREAD stream
  4. 用户断连 → 动态移除 XREAD stream
```

**优势**：Gateway 只读自己需要的事件，零无效读取。
**代价**：需要维护路由表的实时性（连接/断连时更新 Redis）。

### 10.4 多设备事件扇出

同一用户从 Web + App 同时在线，可能连接在不同 Gateway：

```text
User-A:
  ├── Web  → Gateway-1
  └── App  → Gateway-3

session:gateway:sess_abc123 → SET { gw-1, gw-3 }

事件到达时:
  Gateway-1 读取 → 推送给 Web 连接
  Gateway-3 读取 → 推送给 App 连接
```

---

## 十一、背压与过载保护

### 11.1 生产者侧 (Runtime)

Runtime 的 XADD 是 fire-and-forget。如果 Redis 不可用：
- 重试 3 次（指数退避）
- 仍失败 → 记录日志，事件丢失（LLM 继续执行，不中断）
- 会话历史由 Runtime 独立持久化到 PG，不依赖 Event Bus

### 11.2 消费者侧 (Gateway)

```text
正常: XREAD → 处理 → XREAD → 处理 ...

积压检测:
  ├── 消费延迟 > 1s → 告警 (lag increasing)
  ├── 消费延迟 > 5s → 跳过 delta 事件，只保留 lifecycle
  └── 消费延迟 > 30s → 从最新位置消费 (放弃回放中间事件)

客户端背压:
  ├── writeCh 满 → 慢客户端标记
  ├── 慢客户端 → 聚合窗口 150ms → 500ms
  ├── 仍然满 → 丢弃 delta, 保留 lifecycle
  └── 持续满 → 断开连接
```

### 11.3 Redis 过载保护

```text
Redis 内存超过阈值:
  ├── MAXLEN 自动裁剪旧事件 (已有)
  ├── 配合 maxmemory-policy: allkeys-lru (最后手段)
  └── 监控: redis_used_memory / redis_memory_max 告警
```

---

## 十二、可观测性

### 12.1 指标

**Runtime 侧 (生产者)：**

```python
events_published_total     Counter   { session_key, event_type }
event_publish_duration_ms  Histogram { }
event_publish_errors       Counter   { error_type }
```

**Gateway 侧 (消费者)：**

```go
events_consumed_total      Counter   { event_type }
events_pushed_total        Counter   { event_type }
events_discarded_total     Counter   { reason }       // no_conn, slow_client
consumer_lag_seconds       Gauge     { stream }
aggregate_flush_total      Counter   { }
aggregate_window_ms        Histogram { }
pipeline_duration_ms       Histogram { processor }
pipeline_blocked_total     Counter   { processor }
replay_events_total        Counter   { session_key }
```

### 12.2 关键告警

| 告警 | 条件 | 严重级别 | 响应 |
| --- | --- | --- | --- |
| 消费延迟高 | `consumer_lag_seconds > 2s` 持续 1min | Warning | 检查 Gateway 负载 |
| 事件发布失败 | `event_publish_errors > 10/min` | Critical | 检查 Redis 连通性 |
| Stream 内存高 | Redis used_memory > 80% | Warning | 降低 MAXLEN 或扩容 |
| 事件丢弃多 | `events_discarded_total > 100/min` | Warning | 检查慢客户端或路由错误 |

### 12.3 调试命令

```bash
# 查看某个 session 的事件 stream
redis-cli XINFO STREAM events:sess_abc123

# 查看最近 5 条事件
redis-cli XREVRANGE events:sess_abc123 + - COUNT 5

# 查看 stream 长度
redis-cli XLEN events:sess_abc123

# 查看消费者组信息
redis-cli XINFO GROUPS events:sess_abc123

# 查看所有事件 stream
redis-cli KEYS events:*
```

---

## 十三、容量规划

### 13.1 事件吞吐估算 (10,000 在线会话)

```text
活跃 Agent 任务: ~225 个并发 (见技术方案 §9.2)

每个任务的事件量:
  ├── LLM delta: ~100 个/轮 × 2 轮 = ~200 delta
  ├── tool: ~2 × (start + result) = ~4
  ├── lifecycle: run_start + run_complete + usage = ~3
  └── 合计: ~207 个事件/任务

每秒新增任务: ~2.8 (均值), ~15 (峰值)

峰值事件吞吐:
  15 任务/s × 207 事件/任务 ÷ 15s 任务时长 ≈ ~200 事件/s (持续)
  峰值突发: ~1000 事件/s

Redis Streams 能力: >100,000 msg/s
结论: Redis Streams 绰绰有余
```

### 13.2 Redis 内存估算

```text
活跃 session stream 数: ~1000 (10% 用户活跃)
每 stream MAXLEN: 5000
每条消息: ~400 bytes

内存: 1000 × 5000 × 400 = 2 GB
加上 Redis 开销 (2x): ~4 GB

结论: 4GB Redis 内存足够 10,000 在线
```

### 13.3 瓶颈阈值

| 指标 | 预计瓶颈 | 解决方案 |
| --- | --- | --- |
| Redis 写入 QPS | ~100K (单线程) | Redis Cluster 分片 |
| Redis 内存 | 配置的 maxmemory | 降低 MAXLEN / 缩短 TTL |
| Gateway 消费延迟 | 与 stream 数量成正比 | 路由表优化 (§10.3) |
| 全 Gateway 广播读取 | Gateway 数 × 事件量 | 路由表优化 / NATS 迁移 |

---

## 十四、演进路径

### 14.1 三阶段

```text
Phase 1 (MVP): Redis Streams
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • 零新增依赖 (已有 Redis)
  • 每个 Gateway 独立 XREAD + 本地过滤
  • 无 Pipeline 处理器
  • 简单回放 (XRANGE)
  • 适用: ≤10K 在线

Phase 2 (生产化): Redis Streams + 路由优化
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • 路由表优化 (Gateway 只订阅活跃 session)
  • Pipeline 处理器 (内容安全, PII, 审计)
  • 消费延迟监控 + 自动降级
  • 适用: ≤50K 在线

Phase 3 (大规模): NATS JetStream / 自研
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • 如果 Redis Streams 成为瓶颈 (>50K)
  • 迁移到 NATS JetStream:
    ├── 100 万 msg/s 吞吐
    ├── 原生 subject 级路由 (替代路由表)
    ├── 持久化 + 回放原生支持
    └── 多集群/多区域
  • 或自研 Rust Event Bus (极致延迟)
```

### 14.2 迁移接口抽象

为了 Phase 3 迁移平滑，生产者和消费者通过接口隔离实现：

```python
# Runtime 侧
class EventPublisher(ABC):
    @abstractmethod
    async def publish(self, session_key: str, event: AgentEvent) -> None: ...

class RedisStreamPublisher(EventPublisher): ...   # Phase 1-2
class NATSPublisher(EventPublisher): ...           # Phase 3
```

```go
// Gateway 侧
type EventConsumer interface {
    Run(ctx context.Context) error
    Subscribe(sessionKey string)
    Unsubscribe(sessionKey string)
}

type RedisStreamConsumer struct { ... }  // Phase 1-2
type NATSConsumer struct { ... }         // Phase 3
```

---

## 附录

### 附录 A. 事件类型速查

| 事件类型 | 频率 | 大小 | 聚合 | 说明 |
| --- | --- | --- | --- | --- |
| `DELTA` | 极高 (20-30/s) | ~50B | 是 (150ms) | LLM 流式文本 |
| `THINKING` | 高 (10-20/s) | ~50B | 是 (150ms) | 模型思考 |
| `TOOL_START` | 低 (0-2/任务) | ~200B | 否 | 工具开始 |
| `TOOL_RESULT` | 低 (0-2/任务) | ~500B | 否 | 工具结果 |
| `RUN_START` | 每任务 1 次 | ~100B | 否 | 执行开始 |
| `RUN_COMPLETE` | 每任务 1 次 | ~200B | 否 | 执行完成 ★ 触发 final |
| `RUN_ERROR` | 偶发 | ~200B | 否 | 执行出错 ★ 触发 final |
| `RUN_ABORT` | 偶发 | ~100B | 否 | 执行中止 ★ 触发 final |
| `USAGE` | 每轮 1 次 | ~100B | 否 | Token 用量 |

### 附录 B. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| §4 事件生产 | P1-8 事件发射到 Redis Streams | Phase 1 |
| §5 事件消费 | P1-9 Gateway 事件消费 | Phase 1 |
| §6 事件聚合 | P1-10 150ms 限频聚合 | Phase 1 |
| §7 断线回放 | P2-4 Gateway 多实例 | Phase 2 |
| §8 Pipeline | P3-1 内容安全 + P3-2 脱敏 + P3-3 审计 | Phase 3 |
| §10 路由优化 | P2-4 Gateway 多实例 | Phase 2 |
