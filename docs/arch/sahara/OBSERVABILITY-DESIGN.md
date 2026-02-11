# 可观测性架构设计

> 本文档定义 Sahara 平台的全局可观测性架构，覆盖 Metrics（指标）、Logs（日志）、Traces（追踪）三大支柱。
> 可观测性是横切关注点，需要从**整体项目架构视角**统一规划，而非仅限于单个服务。
>
> 当前版本以 Runtime 侧指标为起点（从 [Runtime 架构设计](./RUNTIME-ARCHITECTURE-DESIGN.md) §17 迁移），
> 后续将扩展覆盖 Gateway、API Service、Event Bus 等全链路组件。
>
> 关联文档：
> - [Runtime 架构设计](./RUNTIME-ARCHITECTURE-DESIGN.md) — Agent Runtime 内部模块划分
> - [Gateway 架构设计](./GATEWAY-ARCHITECTURE-DESIGN.md) — Gateway 网关层
> - [API Service 设计](./API-SERVICE-DESIGN.md) — API 服务层
> - [异步事件传输协议](./EVENT-BUS-DESIGN.md) — Runtime ↔ Gateway 事件传输规范

---

## 目录

1. [设计目标与原则](#一设计目标与原则)
2. [可观测性分层架构](#二可观测性分层架构)
3. [Metrics 指标体系设计](#三metrics-指标体系设计)
   - [3.1 指标命名规范](#31-指标命名规范)
   - [3.2 Layer 1: 任务级指标 (Agent Task)](#32-layer-1-任务级指标-agent-task)
   - [3.3 Layer 2: LLM 调用指标](#33-layer-2-llm-调用指标)
   - [3.4 Layer 3: 工具执行指标](#34-layer-3-工具执行指标)
   - [3.5 Layer 4: 沙箱指标](#35-layer-4-沙箱指标)
   - [3.6 Layer 5: 上下文管理指标](#36-layer-5-上下文管理指标)
   - [3.7 Layer 6: 记忆系统指标](#37-layer-6-记忆系统指标)
   - [3.8 Layer 7: 事件系统指标](#38-layer-7-事件系统指标)
   - [3.9 Layer 8: 基础设施指标](#39-layer-8-基础设施指标)
   - [3.10 指标体系全景图](#310-指标体系全景图)
4. [结构化日志](#四结构化日志)
   - [4.1 日志基础设施](#41-日志基础设施)
   - [4.2 上下文注入](#42-上下文注入)
   - [4.3 关键日志点](#43-关键日志点)
   - [4.4 日志输出示例](#44-日志输出示例)
5. [分布式追踪 (Phase 2)](#五分布式追踪-phase-2)
6. [Grafana Dashboard 设计](#六grafana-dashboard-设计)
7. [指标采集与暴露](#七指标采集与暴露)
8. [TODO: 全局可观测性扩展](#八todo-全局可观测性扩展)

---

## 一、设计目标与原则

| 目标 | 说明 |
| --- | --- |
| **三支柱覆盖** | Metrics 回答"多少/多快"，Logs 回答"发生了什么"，Traces 回答"经过了哪些环节" |
| **关联可追溯** | 所有维度通过 `run_id` / `session_key` / `worker_id` / `trace_id` 关联 |
| **低基数标签** | 禁止 `run_id`、`session_key` 等高基数值作为 Prometheus 标签 |
| **Phase 分期** | Phase 1: Metrics + Logs；Phase 2: Traces (OpenTelemetry) |
| **全局视角** | 后续需覆盖 Gateway (Go) → Runtime (Python) → LLM API → Event Bus 全链路 |

---

## 二、可观测性分层架构

```text
  ┌─────────────────────────────────────────────────────────────────────┐
  │                     可观测性三大支柱                                │
  │                                                                     │
  │  ┌── Metrics (指标) ──┐  ┌── Logs (日志) ───┐  ┌── Traces (追踪) ─┐│
  │  │                     │  │                   │  │                   ││
  │  │  Prometheus         │  │  structlog (JSON) │  │  OpenTelemetry   ││
  │  │  ↓                  │  │  ↓                │  │  ↓               ││
  │  │  Grafana Dashboard  │  │  Loki / ELK       │  │  Jaeger / Tempo  ││
  │  │                     │  │                   │  │                   ││
  │  │  回答: 多少? 多快?  │  │  回答: 什么?      │  │  回答: 链路?     ││
  │  │  告警: 超阈值通知   │  │  排查: 精确定位   │  │  分析: 瓶颈定位  ││
  │  │                     │  │                   │  │                   ││
  │  │  ★ Phase 1          │  │  ★ Phase 1        │  │  Phase 2         ││
  │  └─────────────────────┘  └───────────────────┘  └─────────────────┘│
  │                                                                     │
  │  贯穿所有维度的关联键:                                              │
  │    run_id       — 单次 Agent 任务的唯一标识                         │
  │    session_key  — 会话标识, 串联同一对话的多次任务                   │
  │    worker_id    — Worker 实例标识                                    │
  │    trace_id     — 分布式追踪 ID (Phase 2, 从 Gateway → Runtime)     │
  │                                                                     │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## 三、Metrics 指标体系设计

指标按照 **RED + USE** 方法论分层：

- **RED** (面向服务): Rate（请求率）、Errors（错误率）、Duration（延迟）
- **USE** (面向资源): Utilization（利用率）、Saturation（饱和度）、Errors（错误数）

### 3.1 指标命名规范

```text
命名模式: sahara_rt_{子系统}_{指标名}_{单位}

前缀:     sahara_rt_           ← 全局前缀, 与其他服务区分
子系统:   task / llm / tool / sandbox / session / context / memory / event / grpc
单位后缀: _total (Counter) / _seconds (Histogram/Summary) / 无后缀 (Gauge)
标签:     低基数标签 (provider/model/tool/status), 禁止高基数标签 (run_id/session_key)

示例:
  sahara_rt_llm_call_duration_seconds{provider="anthropic", model="claude-sonnet-4-20250514"}
  sahara_rt_task_completed_total{status="success"}
  sahara_rt_sandbox_pool_in_use
```

> **注意**：当可观测性扩展到 Gateway 和 API Service 时，前缀将相应调整：
> - Runtime: `sahara_rt_*`
> - Gateway: `sahara_gw_*`
> - API Service: `sahara_api_*`

### 3.2 Layer 1: 任务级指标 (Agent Task)

> 最顶层指标，直接反映用户体验和系统吞吐。

```python
# sahara_runtime/observability/metrics/task.py

from prometheus_client import Counter, Histogram, Gauge

# ── RED: Rate / Error / Duration ──

task_active = Gauge(
    "sahara_rt_task_active",
    "当前正在执行的 Agent 任务数",
)

task_completed_total = Counter(
    "sahara_rt_task_completed_total",
    "已完成的任务数",
    ["status"],  # "success" / "error" / "timeout" / "cancelled"
)

task_duration_seconds = Histogram(
    "sahara_rt_task_duration_seconds",
    "单个 Agent 任务从接收到结束的总耗时",
    ["status"],
    buckets=[1, 5, 10, 30, 60, 120, 300],  # Agent 任务通常 10s-5min
)

task_iterations_total = Histogram(
    "sahara_rt_task_iterations_total",
    "单个任务的 Agent Loop 迭代次数",
    buckets=[1, 2, 3, 5, 8, 10, 15, 20],
)

# ── USE: Utilization / Saturation ──

task_concurrency_utilization = Gauge(
    "sahara_rt_task_concurrency_utilization",
    "并发利用率 (active_tasks / max_concurrent_tasks)",
    # 0.0 ~ 1.0, 用于 Grafana 仪表盘和自动扩缩容
)

task_rejected_total = Counter(
    "sahara_rt_task_rejected_total",
    "因容量满被拒绝的任务数 (触发 backpressure)",
    ["reason"],  # "capacity_full" / "draining"
)

task_queue_wait_seconds = Histogram(
    "sahara_rt_task_queue_wait_seconds",
    "任务从 gRPC 接收到开始执行的排队耗时",
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5],
)
```

**关键告警规则**:

| 指标 | 告警条件 | 级别 | 含义 |
| --- | --- | --- | --- |
| `task_active` | > `max_concurrent_tasks * 0.9` 持续 2min | Warning | 即将饱和, 需扩容 |
| `task_completed_total{status="error"}` | 5min 错误率 > 10% | Critical | 大量任务失败 |
| `task_duration_seconds` | p99 > 120s 持续 5min | Warning | 任务异常慢 |
| `task_rejected_total` | 5min 增量 > 0 | Warning | 开始拒绝请求 |

### 3.3 Layer 2: LLM 调用指标

> 核心成本中心和延迟来源。需要同时监控性能、成本、弹性三个方面。

```python
# sahara_runtime/observability/metrics/llm.py

# ── 性能 ──

llm_call_duration_seconds = Histogram(
    "sahara_rt_llm_call_duration_seconds",
    "单次 LLM API 调用耗时 (含流式接收全部 token)",
    ["provider", "model"],
    buckets=[0.5, 1, 2, 5, 10, 20, 30, 60, 120],
)

llm_ttft_seconds = Histogram(
    "sahara_rt_llm_ttft_seconds",
    "Time To First Token — 首 token 延迟 (用户体感延迟)",
    ["provider", "model"],
    buckets=[0.1, 0.2, 0.5, 1, 2, 5, 10],
)

llm_streaming_throughput = Histogram(
    "sahara_rt_llm_streaming_tokens_per_second",
    "流式输出吞吐 (tokens/s)",
    ["provider", "model"],
    buckets=[5, 10, 20, 40, 60, 80, 100],
)

# ── 成本 ──

llm_tokens_total = Counter(
    "sahara_rt_llm_tokens_total",
    "累计 token 消耗",
    ["provider", "model", "direction"],  # direction: "input" / "output"
)

llm_cache_hit_tokens_total = Counter(
    "sahara_rt_llm_cache_hit_tokens_total",
    "命中 LLM Provider 缓存的 input token 数 (Anthropic cache_read)",
    ["provider", "model"],
    # 缓存节省 = cache_hit_tokens / total_input_tokens
)

llm_cost_dollars = Counter(
    "sahara_rt_llm_cost_dollars_total",
    "累计 LLM 成本估算 (美元)",
    ["provider", "model"],
    # 基于各 Provider 的 token 价格计算
)

# ── 弹性 ──

llm_errors_total = Counter(
    "sahara_rt_llm_errors_total",
    "LLM 调用错误",
    ["provider", "error_type"],
    # error_type: "rate_limit" / "auth" / "timeout" / "connection" /
    #             "stream_interrupted" / "invalid_request" / "overloaded"
)

llm_retry_total = Counter(
    "sahara_rt_llm_retry_total",
    "LLM 重试次数",
    ["provider", "model", "attempt"],  # attempt: "1" / "2" / "3"
)

llm_retry_exhausted_total = Counter(
    "sahara_rt_llm_retry_exhausted_total",
    "LLM 重试耗尽 (全部尝试失败)",
    ["provider", "model"],
)

key_state = Gauge(
    "sahara_rt_key_state",
    "API Key 状态 (0=healthy, 1=degraded, 2=circuit_open, 3=disabled)",
    ["key_id", "provider"],
)

key_circuit_open_total = Counter(
    "sahara_rt_key_circuit_open_total",
    "Key 熔断器打开次数",
    ["key_id", "provider"],
)

model_fallback_total = Counter(
    "sahara_rt_model_fallback_total",
    "模型降级触发次数",
    ["from_model", "to_model"],
)
```

**关键告警规则**:

| 指标 | 告警条件 | 级别 | 含义 |
| --- | --- | --- | --- |
| `llm_errors_total{error_type="rate_limit"}` | 5min 增速 > 10/min | Warning | 频繁限流, 检查 Key 配额 |
| `llm_retry_exhausted_total` | 5min 增量 > 0 | Critical | LLM 完全不可用 |
| `key_state` | 任何 Key 进入 3 (disabled) | Critical | Key 失效, 需更换 |
| `llm_ttft_seconds` | p99 > 10s 持续 5min | Warning | LLM 响应异常慢 |
| `llm_cost_dollars_total` | 1h 增量超预算 | Warning | 成本异常 |
| `llm_cache_hit_tokens / input_tokens` | < 30% 持续 30min | Info | 缓存效率低, 检查亲和策略 |

### 3.4 Layer 3: 工具执行指标

```python
# sahara_runtime/observability/metrics/tool.py

tool_call_total = Counter(
    "sahara_rt_tool_call_total",
    "工具调用次数",
    ["tool", "tier"],  # tier: "core" / "enhanced" / "extended" / "plugin"
)

tool_duration_seconds = Histogram(
    "sahara_rt_tool_duration_seconds",
    "工具执行耗时",
    ["tool"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30],
)

tool_errors_total = Counter(
    "sahara_rt_tool_errors_total",
    "工具执行失败次数",
    ["tool", "error_type"],  # "timeout" / "execution_error" / "access_denied" / "unexpected"
)

tool_result_size_bytes = Histogram(
    "sahara_rt_tool_result_size_bytes",
    "工具返回结果大小 (截断前)",
    ["tool"],
    buckets=[100, 1000, 5000, 10000, 50000, 100000],
)

tool_result_truncated_total = Counter(
    "sahara_rt_tool_result_truncated_total",
    "工具结果被截断次数",
    ["tool"],
)

tool_confirmation_total = Counter(
    "sahara_rt_tool_confirmation_total",
    "需要用户确认的高危工具调用次数",
    ["tool", "result"],  # result: "approved" / "denied" / "timeout"
)
```

### 3.5 Layer 4: 沙箱指标

```python
# sahara_runtime/observability/metrics/sandbox.py

# ── 资源池 (USE) ──

sandbox_pool_total = Gauge(
    "sahara_rt_sandbox_pool_total",
    "沙箱池总容量",
)

sandbox_pool_idle = Gauge(
    "sahara_rt_sandbox_pool_idle",
    "空闲沙箱数",
)

sandbox_pool_in_use = Gauge(
    "sahara_rt_sandbox_pool_in_use",
    "使用中沙箱数",
)

sandbox_pool_creating = Gauge(
    "sahara_rt_sandbox_pool_creating",
    "正在创建中的沙箱数",
)

# ── 生命周期 ──

sandbox_acquire_duration_seconds = Histogram(
    "sahara_rt_sandbox_acquire_duration_seconds",
    "获取沙箱耗时 (含创建/复用)",
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10],
)

sandbox_lifetime_seconds = Histogram(
    "sahara_rt_sandbox_lifetime_seconds",
    "单个沙箱从分配到释放的生命周期",
    buckets=[10, 30, 60, 120, 300, 600],
)

# ── 错误 ──

sandbox_errors_total = Counter(
    "sahara_rt_sandbox_errors_total",
    "沙箱错误",
    ["error_type"],  # "create_failed" / "oom" / "timeout" / "daemon_unreachable"
)

sandbox_health_check_total = Counter(
    "sahara_rt_sandbox_health_check_total",
    "健康检查结果",
    ["result"],  # "healthy" / "unhealthy" / "timeout"
)
```

### 3.6 Layer 5: 上下文管理指标

> 详细指标定义见 [Context Management Design §13](./CONTEXT-MANAGEMENT-DESIGN.md#131-核心指标)

```python
# sahara_runtime/observability/metrics/context.py

context_fit_duration_seconds = Histogram(
    "sahara_rt_context_fit_duration_seconds",
    "ContextManager.fit() 单次编排耗时",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
)

context_effective_length = Histogram(
    "sahara_rt_context_effective_length_tokens",
    "fit() 后的有效上下文长度 (tokens)",
    buckets=[1000, 5000, 10000, 20000, 50000, 100000, 200000],
)

context_compression_ratio = Histogram(
    "sahara_rt_context_compression_ratio",
    "上下文压缩率 (fit 后 / fit 前)",
    buckets=[0.1, 0.2, 0.3, 0.5, 0.7, 0.8, 0.9, 1.0],
)

context_strategy_triggered_total = Counter(
    "sahara_rt_context_strategy_triggered_total",
    "各策略触发次数",
    ["strategy"],  # "filtering" / "compaction_soft" / "compaction_hard" /
                    # "eviction" / "summarization" / "emergency"
)

context_tokens_saved_total = Counter(
    "sahara_rt_context_tokens_saved_total",
    "各策略节省的 token 数",
    ["strategy"],
)

context_overflow_total = Counter(
    "sahara_rt_context_overflow_total",
    "上下文溢出次数 (触发 Emergency 或 OverflowHandler)",
)
```

### 3.7 Layer 6: 记忆系统指标

> 详细指标定义见 [Agent Memory Design §7](./AGENT-MEMORY-DESIGN.md#71-核心指标)

```python
# sahara_runtime/observability/metrics/memory.py

# ── Session Store (短期记忆) ──

session_load_duration_seconds = Histogram(
    "sahara_rt_session_load_duration_seconds",
    "Session 加载耗时",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
)

session_save_duration_seconds = Histogram(
    "sahara_rt_session_save_duration_seconds",
    "Session 保存耗时",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
)

session_cache_hit_total = Counter(
    "sahara_rt_session_cache_hit_total",
    "Redis 缓存命中/未命中",
    ["result"],  # "hit" / "miss"
)

session_active = Gauge(
    "sahara_rt_session_active",
    "当前活跃会话数",
)

# ── Knowledge Store (长期记忆) [Phase 2] ──

memory_search_duration_seconds = Histogram(
    "sahara_rt_memory_search_duration_seconds",
    "记忆搜索耗时",
    buckets=[0.01, 0.05, 0.1, 0.5, 1],
)

memory_search_results_count = Histogram(
    "sahara_rt_memory_search_results_count",
    "单次搜索返回结果数",
    buckets=[0, 1, 2, 3, 5, 10],
)

memory_index_duration_seconds = Histogram(
    "sahara_rt_memory_index_duration_seconds",
    "记忆索引 (chunking + embedding + store) 耗时",
    buckets=[0.1, 0.5, 1, 2, 5],
)

memory_entries_total = Gauge(
    "sahara_rt_memory_entries_total",
    "记忆条目总数",
    ["source", "category"],
)

embedding_api_duration_seconds = Histogram(
    "sahara_rt_embedding_api_duration_seconds",
    "Embedding API 调用耗时",
    ["provider"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1],
)
```

### 3.8 Layer 7: 事件系统指标

```python
# sahara_runtime/observability/metrics/event.py

event_emitted_total = Counter(
    "sahara_rt_event_emitted_total",
    "发射的事件总数",
    ["type"],  # EventType 枚举值
)

event_publish_duration_seconds = Histogram(
    "sahara_rt_event_publish_duration_seconds",
    "事件发布到 MQ 的耗时",
    ["backend"],  # "redis_streams" / "kafka" / "nats"
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
)

event_publish_failed_total = Counter(
    "sahara_rt_event_publish_failed_total",
    "事件发布失败次数",
    ["type", "reason"],  # reason: "timeout" / "connection" / "unknown"
)

event_terminal_retry_total = Counter(
    "sahara_rt_event_terminal_retry_total",
    "终态事件重试次数 (RUN_COMPLETE/RUN_ERROR/RUN_ABORT)",
    ["type"],
)
```

### 3.9 Layer 8: 基础设施指标

```python
# sahara_runtime/observability/metrics/infra.py

# ── gRPC Server ──

grpc_request_total = Counter(
    "sahara_rt_grpc_request_total",
    "gRPC 请求总数",
    ["method", "status"],  # status: gRPC status code
)

grpc_request_duration_seconds = Histogram(
    "sahara_rt_grpc_request_duration_seconds",
    "gRPC 请求处理耗时",
    ["method"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1],
)

grpc_active_streams = Gauge(
    "sahara_rt_grpc_active_streams",
    "当前活跃的 gRPC 流连接数",
)

# ── Redis ──

redis_command_duration_seconds = Histogram(
    "sahara_rt_redis_command_duration_seconds",
    "Redis 命令耗时",
    ["command"],  # "get" / "set" / "xadd" / "setnx" ...
    buckets=[0.0005, 0.001, 0.005, 0.01, 0.05, 0.1],
)

redis_connection_pool_active = Gauge(
    "sahara_rt_redis_connection_pool_active",
    "Redis 连接池活跃连接数",
)

# ── Python 进程 ──

python_asyncio_tasks_active = Gauge(
    "sahara_rt_asyncio_tasks_active",
    "asyncio 事件循环中的活跃 Task 数",
)

python_event_loop_lag_seconds = Gauge(
    "sahara_rt_event_loop_lag_seconds",
    "事件循环延迟 (期望 < 10ms, > 50ms 说明有阻塞)",
)
```

### 3.10 指标体系全景图

```text
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        Metrics 指标体系 (RED + USE)                         │
  │                                                                             │
  │  ┌─ Layer 1: 任务级 (用户视角) ──────────────────────────────────────────┐  │
  │  │  task_active | task_completed_total | task_duration_seconds             │  │
  │  │  task_iterations_total | task_rejected_total | task_queue_wait          │  │
  │  └────────────────────────────────────────────────────────────────────────┘  │
  │       ↓ 分解                                                                │
  │  ┌─ Layer 2: LLM 调用 (成本 & 延迟核心) ─────────────────────────────────┐  │
  │  │  性能: call_duration | ttft | streaming_throughput                      │  │
  │  │  成本: tokens_total | cache_hit_tokens | cost_dollars                   │  │
  │  │  弹性: errors | retry | retry_exhausted | key_state | model_fallback   │  │
  │  └────────────────────────────────────────────────────────────────────────┘  │
  │       ↓                                                                     │
  │  ┌─ Layer 3: 工具执行 ──────────────────┐ ┌─ Layer 4: 沙箱 ─────────────┐  │
  │  │  tool_call | duration | errors        │ │  pool (total/idle/in_use)    │  │
  │  │  result_size | truncated              │ │  acquire_duration | lifetime │  │
  │  │  confirmation (approved/denied)       │ │  errors | health_check      │  │
  │  └──────────────────────────────────────┘ └─────────────────────────────┘  │
  │       ↓                                                                     │
  │  ┌─ Layer 5: 上下文管理 ────────────────┐ ┌─ Layer 6: 记忆系统 ─────────┐  │
  │  │  fit_duration | effective_length      │ │  session: load/save/cache    │  │
  │  │  compression_ratio | strategy         │ │  memory: search/index        │  │
  │  │  tokens_saved | overflow              │ │  embedding: api_duration     │  │
  │  └──────────────────────────────────────┘ └─────────────────────────────┘  │
  │       ↓                                                                     │
  │  ┌─ Layer 7: 事件系统 ──────────────────┐ ┌─ Layer 8: 基础设施 ─────────┐  │
  │  │  emitted | publish_duration           │ │  gRPC: request/duration      │  │
  │  │  publish_failed | terminal_retry      │ │  Redis: cmd_duration/pool    │  │
  │  └──────────────────────────────────────┘ │  asyncio: tasks/loop_lag     │  │
  │                                            └─────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 四、结构化日志

### 4.1 日志基础设施

```python
# sahara_runtime/observability/logging.py

import structlog

def configure_logging(log_level: str = "INFO"):
    """配置结构化日志。所有日志输出 JSON 格式, 方便 ELK/Loki 采集。"""
    structlog.configure(
        processors=[
            # 1. 合并 contextvars (run_id, session_key 等上下文信息)
            structlog.contextvars.merge_contextvars,
            # 2. 添加日志级别
            structlog.processors.add_log_level,
            # 3. ISO 格式时间戳
            structlog.processors.TimeStamper(fmt="iso"),
            # 4. 异常堆栈格式化
            structlog.processors.format_exc_info,
            # 5. JSON 输出
            structlog.processors.JSONRenderer(),
        ],
    )
```

### 4.2 上下文注入

每个 Agent 任务开始时，将关键标识注入 `contextvars`，后续所有日志自动携带：

```python
# Agent Loop 入口处:

async def run_agent_loop(session_key, agent_id, task, deps, emitter):
    # 绑定结构化日志上下文 — 后续所有 logger 调用自动包含这些字段
    structlog.contextvars.bind_contextvars(
        run_id=emitter.run_id,
        session_key=session_key,
        agent_id=agent_id,
        worker_id=deps.config.worker_id,
    )
    # ...
```

### 4.3 关键日志点

```text
任务生命周期中的关键日志:

  ┌── 阶段 ──────────── 日志事件 ──────────────── 级别 ── 附加字段 ─────────────┐
  │                                                                              │
  │  任务开始   task_started                    INFO    model, agent_id          │
  │  Session    session_loaded                  INFO    message_count, source    │
  │  Prompt     prompt_built                    DEBUG   segments, total_tokens   │
  │  Context    context_fit_complete            INFO    before/after tokens      │
  │                                                                              │
  │  LLM 调用   llm_call_start                  DEBUG   model, input_tokens      │
  │             llm_first_token                  DEBUG   ttft_ms                  │
  │             llm_call_complete                INFO    tokens (in/out), cost    │
  │             llm_rate_limited                 WARN    attempt, retry_in        │
  │             llm_retries_exhausted            ERROR   max_retries, last_error  │
  │             llm_auth_failed                  ERROR   key_prefix               │
  │                                                                              │
  │  工具调用   tool_start                       INFO    tool_name, tier          │
  │             tool_complete                    INFO    tool_name, duration_ms   │
  │             tool_error                       WARN    tool_name, error_type    │
  │             tool_result_truncated            INFO    original/truncated_size  │
  │             tool_confirmation_required       INFO    tool_name                │
  │                                                                              │
  │  沙箱       sandbox_acquired                 DEBUG   sandbox_id, duration_ms  │
  │             sandbox_released                 DEBUG   sandbox_id, lifetime_s   │
  │             sandbox_error                    ERROR   error_type, sandbox_id   │
  │                                                                              │
  │  事件       event_published                  DEBUG   event_type               │
  │             event_publish_failed             WARN    event_type, reason       │
  │             terminal_event_retry             WARN    event_type, attempt      │
  │                                                                              │
  │  任务结束   task_completed                   INFO    status, iterations       │
  │             task_timeout                     WARN    elapsed_seconds          │
  │             task_cancelled                   INFO    reason                   │
  │             task_unexpected_error            ERROR   exception (含堆栈)       │
  │                                                                              │
  └──────────────────────────────────────────────────────────────────────────────┘
```

### 4.4 日志输出示例

```json
{
  "timestamp": "2026-02-09T10:23:45.678Z",
  "level": "info",
  "event": "llm_call_complete",
  "run_id": "run_abc123",
  "session_key": "sess_xyz789",
  "agent_id": "agent_001",
  "worker_id": "rt-3",
  "model": "claude-sonnet-4-20250514",
  "provider": "anthropic",
  "iteration": 3,
  "input_tokens": 15234,
  "output_tokens": 892,
  "cache_read_tokens": 12000,
  "duration_ms": 3420,
  "ttft_ms": 210,
  "cost_usd": 0.0087
}
```

---

## 五、分布式追踪 [Phase 2]

> Phase 1 先用 `run_id` + 结构化日志实现基本的请求关联。
> Phase 2 引入 OpenTelemetry，实现从 Gateway → Runtime → LLM API 的全链路追踪。

```text
追踪 Span 层次:

  [Gateway]  handle_ws_message
      └── [Runtime]  run_agent_loop          ← root span
              ├── load_session
              ├── build_prompt
              ├── context_fit
              ├── llm_call (iteration=1)     ← 最关键, 占用 80%+ 时间
              │       ├── http_request (anthropic API)
              │       └── stream_receive
              ├── tool_execute (exec)
              │       └── sandbox_exec
              ├── llm_call (iteration=2)
              │       └── ...
              ├── save_session
              └── emit_run_complete
```

```python
# Phase 2: OpenTelemetry 集成示意

from opentelemetry import trace

tracer = trace.get_tracer("sahara-runtime")

async def run_agent_loop(...):
    with tracer.start_as_current_span("run_agent_loop",
            attributes={"run_id": run_id, "session_key": session_key}) as span:
        # ...
        with tracer.start_as_current_span("llm_call",
                attributes={"iteration": i, "model": model}) as llm_span:
            response = await call_llm_with_retry(...)
            llm_span.set_attribute("tokens.input", usage.input)
            llm_span.set_attribute("tokens.output", usage.output)
```

---

## 六、Grafana Dashboard 设计

| Dashboard | 面向 | 核心 Panel |
| --- | --- | --- |
| **Runtime Overview** | SRE / 运维 | 活跃任务数, 成功率, p50/p95/p99 延迟, 错误率趋势, Worker 容量利用率 |
| **LLM Cost & Performance** | Tech Lead / CTO | 按 model 的 token 消耗, 成本趋势, 缓存命中率, TTFT 分布, 错误类型分布 |
| **LLM Resilience** | SRE | Key 状态矩阵, 重试率, 熔断事件, 模型降级事件, 重试耗尽率 |
| **Tool & Sandbox** | 开发 | 工具调用 Top-N, 工具耗时分布, 沙箱池水位, 沙箱获取延迟 |
| **Context & Memory** | 开发 | 上下文压缩率, 各策略触发比例, token 节省量, Session 缓存命中率 |
| **Infrastructure** | SRE | gRPC QPS, Redis 延迟, Event Loop Lag, asyncio 活跃 Task 数 |

---

## 七、指标采集与暴露

```python
# sahara_runtime/observability/server.py

from prometheus_client import start_http_server, CollectorRegistry, REGISTRY
from prometheus_client import multiprocess  # 预留多进程支持

def start_metrics_server(port: int = 9090):
    """启动独立的 Prometheus metrics HTTP server。

    与 gRPC server 分离, 避免:
      1. gRPC 和 metrics 端口冲突
      2. metrics 采集影响 gRPC 性能
      3. K8s ServiceMonitor 可独立配置
    """
    start_http_server(port, registry=REGISTRY)
    logger.info("metrics_server_started", port=port)
```

```yaml
# K8s ServiceMonitor 配置示例
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: sahara-runtime
spec:
  selector:
    matchLabels:
      app: sahara-runtime
  endpoints:
    - port: metrics        # 9090
      interval: 15s
      path: /metrics
```

---

## 八、TODO: 全局可观测性扩展

> 以下内容将在后续设计迭代中补充，使可观测性覆盖整个 Sahara 平台而不仅限于 Runtime。

| 待补充内容 | 说明 | 优先级 |
| --- | --- | --- |
| **Gateway 指标体系** | Go 侧 Prometheus 指标：WebSocket 连接数、消息分发延迟、路由耗时、JWT 校验 | P1 |
| **API Service 指标体系** | Go 侧 HTTP/REST 指标：接口 QPS、延迟、错误率、Agent CRUD 操作 | P1 |
| **Event Bus 指标** | Redis Streams / Kafka 消费延迟、Consumer Group lag、事件积压 | P1 |
| **全链路 Trace 拓扑** | Gateway → Runtime → LLM API → Event Bus → Gateway 的完整 Span 设计 | P2 |
| **统一告警规则** | 跨服务告警策略：级联故障检测、SLO/SLI 定义 | P2 |
| **日志聚合策略** | 多语言 (Go + Python) 日志格式统一、采集管道、Loki/ELK 配置 | P1 |
| **成本仪表盘** | LLM token 成本、基础设施成本、按 Agent/用户的成本分摊 | P2 |
| **SLO 定义** | 核心 SLI (TTFT p99 < 2s, 任务成功率 > 99%) 与 Error Budget | P2 |
