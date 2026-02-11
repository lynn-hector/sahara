# gRPC 服务契约与协议设计

> Gateway (Go) 与 Agent Runtime Worker (Python) 之间的同步通信协议。
> 本文档定义完整的服务接口、消息格式、错误处理、连接管理和演进规则，是两端并行开发的契约基础。
>
> 关联文档：
> - [技术方案](./TECH-PROPOSAL-C-END-REFACTOR.md) — 第七章 gRPC vs MQ 选型论证
> - [工程计划](./ENGINEERING-PLAN-C-END.md) — 第三章 Proto 初版定义
> - [WebSocket 协议设计](./WS-PROTOCOL-DESIGN.md) — 客户端通信协议（D2）
> - [异步事件传输协议](./EVENT-BUS-DESIGN.md) — Runtime ↔ Gateway 事件传输规范（D5）
> - [Runtime 架构设计](./RUNTIME-ARCHITECTURE-DESIGN.md) — Runtime Worker 内部架构（D4）
> - [Gateway 架构设计](./GATEWAY-ARCHITECTURE-DESIGN.md) — Gateway 内部架构（D7）

---

## 目录

1. [概述与范围](#一概述与范围)
2. [设计原则](#二设计原则)
3. [Proto 包结构](#三proto-包结构)
4. [服务定义总览](#四服务定义总览)
5. [AgentService — 核心任务服务](#五agentservice--核心任务服务)
6. [WorkerService — Worker 生命周期](#六workerservice--worker-生命周期)
7. [公共类型与事件定义](#七公共类型与事件定义)
8. [错误处理规范](#八错误处理规范)
9. [连接管理](#九连接管理)
10. [负载均衡与调度策略](#十负载均衡与调度策略)
11. [超时与重试策略](#十一超时与重试策略)
12. [Metadata 与上下文传播](#十二metadata-与上下文传播)
13. [安全](#十三安全)
14. [版本演进规则](#十四版本演进规则)
15. [性能调优](#十五性能调优)
16. [调试工具链](#十六调试工具链)

---

## 一、概述与范围

### 1.1 gRPC 在架构中的位置

```text
C 端用户
  │
  ▼ (WebSocket / HTTP)
┌──────────────┐
│   Gateway    │──────── gRPC (本文档) ────────▶┌──────────────┐
│   (Go)       │                                │  Runtime     │
│              │◀──── Event Bus (MQ, 另一文档) ──│  Worker (Py) │
└──────────────┘                                └──────────────┘
```

**gRPC 只负责同步调度路径（路径 A）**，即 Gateway → Runtime Worker 的请求-响应通信。异步事件路径（路径 B）走 Event Bus（Redis Streams / NATS），不在本文档范围内。

### 1.2 本文档覆盖的通信

| 方向 | 调用方 | 被调用方 | 用途 |
| --- | --- | --- | --- |
| Gateway → Runtime | Go gRPC Client | Python gRPC Server | 提交任务、中止任务、查询状态、人机交互输入 |
| Gateway → Runtime | Go gRPC Client | Python gRPC Server | Worker 健康检查、负载查询、排空 |

### 1.3 不在范围内

| 通信 | 由哪个文档覆盖 |
| --- | --- |
| Client ↔ Gateway (WebSocket) | WS-PROTOCOL-DESIGN.md |
| Client → Gateway (HTTP API) | HTTP-API-DESIGN.md |
| Runtime → Redis Streams → Gateway (事件流) | EVENT-BUS-DESIGN.md |
| 服务 → Redis / PostgreSQL | 各模块架构设计文档 |

---

## 二、设计原则

| # | 原则 | 说明 |
| --- | --- | --- |
| P1 | **gRPC 状态码表达传输语义，业务状态在响应体中** | UNAVAILABLE = Worker 不可用（重试另一台）；INVALID_ARGUMENT = 请求有误（不重试）；OK + 响应体 = 业务结果 |
| P2 | **单向调用，不使用双向流** | Gateway 调 Runtime，不反向调。事件走 Event Bus，不走 gRPC streaming |
| P3 | **幂等优先** | 所有写操作携带幂等键，重复调用返回相同结果 |
| P4 | **Deadline 强制传播** | 每个 RPC 必须设置 Deadline（超时），Runtime 必须检查并遵守 |
| P5 | **Proto 是唯一事实来源** | 接口变更必须先改 proto，然后生成代码，禁止手写 gRPC stub |
| P6 | **向后兼容演进** | 只加字段/方法，不删不改；破坏性变更走新版本 package |

---

## 三、Proto 包结构

```text
proto/
├── buf.yaml                       # Buf workspace 配置
├── buf.gen.yaml                   # 代码生成配置 (Go + Python)
├── buf.lock
└── sahara/
    ├── common/v1/
    │   └── common.proto           # 公共类型：TaskState, ErrorDetail 等
    ├── agent/v1/
    │   └── agent.proto            # AgentService: 核心任务管理
    ├── worker/v1/
    │   └── worker.proto           # WorkerService: Worker 生命周期管理
    └── event/v1/
        └── event.proto            # AgentEvent: 事件消息定义 (供 Event Bus 序列化)
```

**拆分理由**：

| 包 | 为什么独立 |
| --- | --- |
| `common/v1` | 公共枚举和消息，被其他包 import，避免循环依赖 |
| `agent/v1` | 核心业务 RPC，变更频率最高，独立版本管理 |
| `worker/v1` | 运维侧 RPC（健康、排空），变更频率低，不影响业务 RPC |
| `event/v1` | 事件格式定义，被 Runtime（生产者）和 Gateway（消费者）共用，但不涉及 gRPC 服务 |

---

## 四、服务定义总览

```text
┌────────────────────────────────────────────────────────────────────┐
│  Runtime Worker (Python) 暴露的 gRPC 服务                          │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  AgentService (sahara.agent.v1)           核心业务                 │
│  ├── SubmitTask       提交 Agent 任务          Phase 1             │
│  ├── AbortTask        中止执行中的任务          Phase 1             │
│  ├── SendInput        人机交互输入投递          Phase 2 (人机交互) │
│  ├── GetTaskStatus    查询单个任务状态          Phase 1             │
│  └── ListActiveTasks  列出所有活跃任务          Phase 2 (故障恢复) │
│                                                                    │
│  WorkerService (sahara.worker.v1)         运维管理                 │
│  ├── GetStatus        查询 Worker 负载/健康     Phase 1             │
│  ├── Drain            排空 Worker (优雅关闭)    Phase 2             │
│  └── UpdateConfig     热更新配置               Phase 3             │
│                                                                    │
│  grpc.health.v1.Health                    标准健康检查              │
│  ├── Check            单次健康检查              Phase 1             │
│  └── Watch            持续监听健康状态          Phase 1             │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## 五、AgentService — 核心任务服务

### 5.1 完整 Proto 定义

```protobuf
// proto/sahara/agent/v1/agent.proto

syntax = "proto3";
package sahara.agent.v1;

option go_package = "github.com/example/sahara/gen/sahara/agent/v1;agentv1";

import "sahara/common/v1/common.proto";

// ==========================================================
// AgentService: Gateway → Runtime Worker 的核心任务管理
// ==========================================================
service AgentService {
  // 提交 Agent 任务。
  // 成功返回 run_id；Worker 过载返回 gRPC RESOURCE_EXHAUSTED。
  rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse);

  // 中止正在执行的任务。幂等操作：任务已结束时返回 OK。
  rpc AbortTask(AbortTaskRequest) returns (AbortTaskResponse);

  // 人机交互：将用户输入/确认投递给挂起中的 Agent 任务。
  // 必须路由到持有该任务的 Worker（Sticky Affinity）。
  rpc SendInput(SendInputRequest) returns (SendInputResponse);

  // 查询指定任务的当前状态。
  rpc GetTaskStatus(GetTaskStatusRequest) returns (GetTaskStatusResponse);

  // 列出 Worker 上所有活跃任务（用于 Gateway 重启后的状态恢复）。
  rpc ListActiveTasks(ListActiveTasksRequest) returns (ListActiveTasksResponse);
}
```

### 5.2 SubmitTask — 提交任务

**调用时机**：Gateway 收到用户消息后，选择一个 Runtime Worker 提交任务。

```protobuf
message SubmitTaskRequest {
  // 全局唯一任务 ID (由 Gateway 生成, ULID 格式)
  string task_id = 1;

  // 会话标识 (格式: {agent_id}:{user_id}:{channel}:{peer_id})
  string session_key = 2;

  // Agent 配置标识
  string agent_id = 3;

  // 用户消息内容
  UserMessage user_message = 4;

  // 幂等键 (客户端传入，防止网络重试导致重复执行)
  string idempotency_key = 5;

  // 任务选项
  TaskOptions options = 6;

  // 附加元数据 (渠道信息、用户属性、A/B 实验标记等)
  map<string, string> metadata = 7;
}

message UserMessage {
  // 文本内容
  string text = 1;

  // 附件列表 (图片、文件等)
  repeated Attachment attachments = 2;

  // 引用的消息 ID (回复场景)
  string reply_to_message_id = 3;
}

message Attachment {
  string filename = 1;
  string mime_type = 2;
  // 内容来源：URL 或 inline bytes (小文件 <1MB)
  oneof content {
    string url = 3;
    bytes data = 4;
  }
  int64 size_bytes = 5;
}

message TaskOptions {
  // 覆盖默认模型 (为空则用 Agent 配置的默认模型)
  string model_override = 1;

  // 最大工具调用轮数 (0 = 使用默认值 20)
  int32 max_iterations = 2;

  // 是否启用沙箱 (为空则用 Agent 配置)
  optional bool sandbox_enabled = 3;

  // 流式事件的目标 session (用于多设备同步)
  repeated string broadcast_session_keys = 4;

  // 思考级别覆盖 ("none" / "brief" / "full", 为空则用 Agent 配置的默认值)
  string thinking_level = 5;
}

message SubmitTaskResponse {
  // Runtime 分配的执行 ID (每次重试生成新的)
  string run_id = 1;

  // Worker 标识 (用于后续 AbortTask 精确路由)
  string worker_id = 2;

  // 任务创建时间
  int64 accepted_at_ms = 3;
}
```

**关键行为**：

| 场景 | gRPC 状态码 | 说明 |
| --- | --- | --- |
| 成功接受 | `OK` | 返回 `run_id`，任务开始异步执行 |
| Worker 满载 | `RESOURCE_EXHAUSTED` | Gateway 应尝试下一个 Worker |
| Worker 正在排空 | `UNAVAILABLE` | Gateway 应将此 Worker 从候选列表移除 |
| 请求参数无效 | `INVALID_ARGUMENT` | 附带 `ErrorDetail` 说明具体字段问题 |
| 幂等键重复 (相同任务) | `OK` | 返回之前生成的 `run_id`（不重复执行） |
| session 被锁 (并发冲突) | `ABORTED` | 同一 session 已有任务在执行，Gateway 可排队或拒绝 |
| 超时 | `DEADLINE_EXCEEDED` | Gateway 设置的 deadline 到期 |

**时序图**：

```text
Gateway                          Runtime Worker
  │                                    │
  │  SubmitTask(task_id, session_key)  │
  │ ──────────────────────────────────▶│
  │                                    │ 1. 检查幂等键 (Redis)
  │                                    │ 2. 获取 session 锁 (Redis)
  │                                    │ 3. 检查并发容量
  │                                    │ 4. 创建 asyncio.Task
  │  SubmitTaskResponse(run_id)        │
  │ ◀──────────────────────────────────│
  │                                    │
  │  (任务异步执行中...)               │
  │                                    │ 5. 发射 RUN_START 到 Event Bus
  │                                    │ 6. 调用 LLM (流式)
  │                                    │ 7. 发射 DELTA 事件到 Event Bus
  │                                    │ 8. 执行工具
  │                                    │ 9. 发射 RUN_COMPLETE 到 Event Bus
  │                                    │ 10. 释放 session 锁
```

### 5.3 AbortTask — 中止任务

```protobuf
message AbortTaskRequest {
  string task_id = 1;
  string run_id = 2;
  // 中止原因 (用于审计日志)
  string reason = 3;
}

message AbortTaskResponse {
  // 任务在中止前的状态
  sahara.common.v1.TaskState previous_state = 1;
}
```

**关键行为**：

| 场景 | gRPC 状态码 | 说明 |
| --- | --- | --- |
| 任务正在执行，成功中止 | `OK` | previous_state = RUNNING |
| 任务已自然完成 | `OK` | previous_state = COMPLETED（幂等） |
| 任务已被中止 | `OK` | previous_state = ABORTED（幂等） |
| 任务不存在 | `NOT_FOUND` | task_id 或 run_id 不匹配 |

**中止机制**：
1. Runtime 收到 AbortTask 后，调用 `asyncio.Task.cancel()` 触发 `CancelledError`
2. agent_loop 在下一个 `await` 点捕获取消，执行清理（释放沙箱、关闭 LLM 流）
3. 发射 `RUN_ABORT` 事件到 Event Bus，携带 `reason`
4. 释放 session 锁

### 5.4 SendInput — 人机交互输入

**调用时机**：Agent 在执行过程中需要用户输入（如文本回答）或操作确认（如高危工具确认）时，Runtime 通过 Event Bus 发射 `INPUT_REQUIRED` / `TOOL_CONFIRM_REQUIRED` 事件。Gateway 收到该事件后推送到客户端，客户端响应后 Gateway 调用 `SendInput` 将用户输入投递回持有该任务的 Worker。

```protobuf
message SendInputRequest {
  // 关联的任务 ID
  string task_id = 1;

  // 关联的执行 ID
  string run_id = 2;

  // 用户动作: "approve" (确认工具执行) / "reject" (拒绝) / "input" (文本输入)
  string action = 3;

  // 用户输入文本 (action="input" 时必填; action="reject" 时可附带拒绝原因)
  string input = 4;
}

message SendInputResponse {
  // 是否成功投递到 Agent 任务
  bool delivered = 1;
}
```

**关键行为**：

| 场景 | gRPC 状态码 | 说明 |
| --- | --- | --- |
| 成功投递 | `OK` | delivered = true，Agent 恢复执行 |
| 任务不存在或已结束 | `NOT_FOUND` | task_id / run_id 不匹配或任务已完成 |
| 任务未在等待输入 | `FAILED_PRECONDITION` | 任务正在正常执行，不接受输入 |
| 输入通道已满 | `RESOURCE_EXHAUSTED` | 极端情况，多个客户端同时发送 |

**路由要求**：

> **Sticky Affinity**：`SendInput` **必须**路由到持有该任务的 Worker。
> Gateway 在收到 `INPUT_REQUIRED` / `TOOL_CONFIRM_REQUIRED` 事件时，记录 `(task_id, run_id) → worker_id` 映射，
> 后续 `SendInput` 使用此映射直接路由，不走负载均衡（详见 §10.5）。

**人机交互完整时序图**：

```text
Client              Gateway                  Runtime Worker              Event Bus
  │                    │                          │                          │
  │                    │  (任务执行中, Agent 遇到高危工具)                    │
  │                    │                          │                          │
  │                    │                          │ emit TOOL_CONFIRM_REQUIRED│
  │                    │                          │ ────────────────────────▶│
  │                    │     consume event         │                          │
  │                    │ ◀─────────────────────────────────────────────────── │
  │                    │                          │                          │
  │                    │ MarkSticky(task→worker)   │                          │
  │                    │ (记录亲和映射)             │                          │
  │                    │                          │                          │
  │  WS: tool_confirm │                          │                          │
  │ ◀─────────────────│                          │                          │
  │  (展示确认弹窗)    │                          │                          │
  │                    │                          │                          │
  │  WS: agent.input   │                          │                          │
  │  action="approve"  │                          │                          │
  │ ──────────────────▶│                          │                          │
  │                    │                          │                          │
  │                    │  gRPC SendInput (Sticky) │                          │
  │                    │ ────────────────────────▶│                          │
  │                    │                          │ input_channel.put()       │
  │                    │                          │ (Agent 恢复执行)          │
  │                    │  SendInputResponse       │                          │
  │                    │ ◀────────────────────────│                          │
  │                    │                          │                          │
  │                    │                          │ (继续工具执行 → emit 后续事件)
```

**超时机制**：
- Agent 等待输入的默认超时为 **120 秒**（由 Runtime 内部控制，不通过 gRPC 传递）
- 超时后 Agent 自动拒绝（工具确认场景）或跳过（文本输入场景）
- 超时不影响 gRPC 层面，属于 Runtime 内部 Agent Loop 逻辑

### 5.5 GetTaskStatus — 查询任务状态

```protobuf
message GetTaskStatusRequest {
  string task_id = 1;
  // 可选：不传则按 task_id 查找
  string run_id = 2;
}

message GetTaskStatusResponse {
  string task_id = 1;
  string run_id = 2;
  string session_key = 3;
  sahara.common.v1.TaskState state = 4;

  // 执行统计
  int64 started_at_ms = 5;
  int64 finished_at_ms = 6;       // 0 表示仍在执行
  int32 current_iteration = 7;    // 当前 LLM 调用轮次
  int32 total_tokens_used = 8;    // 已消耗 token 数

  // 错误信息 (仅 state = FAILED 时有值)
  string error_message = 9;

  // 是否正在等待用户输入 (state = WAITING_FOR_INPUT 时为 true)
  bool waiting_for_input = 10;

  // 当前实际使用的模型 (可能因 Fallback 降级而与 TaskOptions.model_override 不同)
  string current_model = 11;
}
```

### 5.6 ListActiveTasks — 列出活跃任务

```protobuf
message ListActiveTasksRequest {
  // 可选：按 session_key 过滤
  string session_key_filter = 1;
}

message ListActiveTasksResponse {
  repeated ActiveTask tasks = 1;
}

message ActiveTask {
  string task_id = 1;
  string run_id = 2;
  string session_key = 3;
  sahara.common.v1.TaskState state = 4;
  int64 started_at_ms = 5;
  int32 current_iteration = 6;
}
```

**使用场景**：
- Gateway 重启后，需要知道哪些任务仍在哪些 Worker 上执行，以恢复事件订阅
- 运维排查：查看某个 Worker 当前在跑什么

---

## 六、WorkerService — Worker 生命周期

### 6.1 完整 Proto 定义

```protobuf
// proto/sahara/worker/v1/worker.proto

syntax = "proto3";
package sahara.worker.v1;

option go_package = "github.com/example/sahara/gen/sahara/worker/v1;workerv1";

// ==========================================================
// WorkerService: Gateway → Runtime Worker 的运维管理
// ==========================================================
service WorkerService {
  // 查询 Worker 负载与健康状态（Gateway 定期轮询，用于调度决策）
  rpc GetStatus(GetStatusRequest) returns (GetStatusResponse);

  // 排空 Worker：停止接受新任务，等待现有任务完成
  rpc Drain(DrainRequest) returns (DrainResponse);

  // 热更新 Worker 配置（模型配置、并发上限等）
  rpc UpdateConfig(UpdateConfigRequest) returns (UpdateConfigResponse);
}
```

### 6.2 GetStatus — Worker 状态查询

```protobuf
message GetStatusRequest {}

message GetStatusResponse {
  // Worker 标识
  string worker_id = 1;

  // 容量信息
  int32 active_tasks = 2;          // 当前执行中的任务数
  int32 max_tasks = 3;             // 最大并发任务数
  int32 queued_tasks = 4;          // 本地排队中的任务数

  // 资源信息
  float cpu_usage_percent = 5;     // CPU 使用率 (0-100)
  float memory_usage_percent = 6;  // 内存使用率 (0-100)
  int64 memory_used_bytes = 7;     // 已用内存 (bytes)

  // 沙箱池状态
  int32 sandbox_pool_idle = 8;     // 空闲沙箱数
  int32 sandbox_pool_in_use = 9;   // 使用中沙箱数
  int32 sandbox_pool_total = 10;   // 总沙箱数

  // Worker 状态
  WorkerState state = 11;
  int64 uptime_seconds = 12;       // 运行时间
  string version = 13;             // 代码版本
}

enum WorkerState {
  WORKER_STATE_UNSPECIFIED = 0;
  WORKER_STATE_READY = 1;          // 正常接收任务
  WORKER_STATE_DRAINING = 2;       // 排空中，不接受新任务
  WORKER_STATE_STARTING = 3;       // 启动中，尚未就绪
}
```

**Gateway 轮询策略**：
- 正常状态：每 **5 秒** 轮询一次
- Worker 异常时：降为每 **1 秒** 轮询
- 连续 3 次无响应：标记 Worker 为 DEAD，从路由表移除

### 6.3 Drain — 排空 Worker

```protobuf
message DrainRequest {
  // 排空超时 (秒)。超时后强制中止剩余任务。0 = 使用默认值 (60s)
  int32 timeout_seconds = 1;
}

message DrainResponse {
  // 排空开始时的活跃任务数
  int32 remaining_tasks = 1;
  // 预计完成时间 (ms timestamp)
  int64 estimated_complete_at_ms = 2;
}
```

**使用场景**：
- K8s 滚动更新时，preStop hook 调用 `Drain`
- 运维手动下线 Worker

**排空流程**：
```text
Gateway                          Runtime Worker
  │                                    │
  │  Drain(timeout=60s)                │
  │ ──────────────────────────────────▶│
  │                                    │ 1. 设置 state = DRAINING
  │  DrainResponse(remaining=5)        │
  │ ◀──────────────────────────────────│
  │                                    │
  │  后续 SubmitTask 调用              │
  │ ──────────────────────────────────▶│
  │  gRPC UNAVAILABLE                  │ 2. 拒绝新任务
  │ ◀──────────────────────────────────│
  │                                    │
  │  GetStatus (轮询)                  │
  │ ──────────────────────────────────▶│
  │  active_tasks: 3 → 1 → 0          │ 3. 任务逐个完成
  │ ◀──────────────────────────────────│
  │                                    │
  │                                    │ 4. 所有任务完成 → 进程退出
```

### 6.4 UpdateConfig — 热更新配置

```protobuf
message UpdateConfigRequest {
  // 新的最大并发任务数 (0 = 不变)
  int32 max_tasks = 1;

  // 新的模型配置 (JSON 格式，为空 = 不变)
  string model_config_json = 2;

  // 新的日志级别 (为空 = 不变)
  string log_level = 3;
}

message UpdateConfigResponse {
  // 更新后的完整配置快照
  string current_config_json = 1;
}
```

---

## 七、公共类型与事件定义

### 7.1 公共类型

```protobuf
// proto/sahara/common/v1/common.proto

syntax = "proto3";
package sahara.common.v1;

option go_package = "github.com/example/sahara/gen/sahara/common/v1;commonv1";

// 任务状态 (全生命周期)
enum TaskState {
  TASK_STATE_UNSPECIFIED = 0;
  TASK_STATE_QUEUED = 1;                 // 已接受，排队中
  TASK_STATE_RUNNING = 2;                // 正在执行
  TASK_STATE_COMPLETED = 3;              // 正常完成
  TASK_STATE_FAILED = 4;                 // 执行出错
  TASK_STATE_ABORTED = 5;                // 被主动中止
  TASK_STATE_TIMEOUT = 6;                // 超时
  TASK_STATE_WAITING_FOR_INPUT = 7;      // 挂起，等待用户输入或确认
}

// 错误详情 (附加在 gRPC Status.details 中)
message ErrorDetail {
  // 业务错误码 (Sahara 定义的错误码，非 gRPC 状态码)
  string error_code = 1;

  // 人类可读的错误描述
  string message = 2;

  // 相关字段名 (用于参数校验错误)
  string field = 3;

  // 附加信息
  map<string, string> metadata = 4;
}
```

### 7.2 事件定义

```protobuf
// proto/sahara/event/v1/event.proto

syntax = "proto3";
package sahara.event.v1;

option go_package = "github.com/example/sahara/gen/sahara/event/v1;eventv1";

import "sahara/common/v1/common.proto";

// ==========================================================
// AgentEvent: Runtime → Event Bus → Gateway 的异步事件
// 序列化为 protobuf bytes 后通过 Redis Streams 传输
// ==========================================================
message AgentEvent {
  // 事件唯一 ID (ULID)
  string event_id = 1;

  // 关联的执行 ID
  string run_id = 2;

  // 关联的会话标识
  string session_key = 3;

  // 关联的任务 ID
  string task_id = 4;

  // 事件类型
  EventType type = 5;

  // 事件产生时间
  int64 timestamp_ms = 6;

  // 会话内递增序列号 (用于检测丢失和排序)
  int32 seq = 7;

  // 追踪 ID (OpenTelemetry trace_id)
  string trace_id = 8;

  // 事件数据 (根据 type 不同，内容不同)
  oneof payload {
    DeltaPayload delta = 10;
    ToolStartPayload tool_start = 11;
    ToolResultPayload tool_result = 12;
    RunStartPayload run_start = 13;
    RunCompletePayload run_complete = 14;
    RunErrorPayload run_error = 15;
    RunAbortPayload run_abort = 16;
    ThinkingPayload thinking = 17;
    UsagePayload usage = 18;
    InputRequiredPayload input_required = 19;
    ToolConfirmRequiredPayload tool_confirm_required = 20;
    ModelFallbackPayload model_fallback = 21;
  }
}

enum EventType {
  EVENT_TYPE_UNSPECIFIED = 0;
  EVENT_TYPE_DELTA = 1;                    // LLM 流式文本片段
  EVENT_TYPE_TOOL_START = 2;               // 工具开始执行
  EVENT_TYPE_TOOL_RESULT = 3;              // 工具执行结果
  EVENT_TYPE_RUN_START = 4;                // Agent 运行开始
  EVENT_TYPE_RUN_COMPLETE = 5;             // Agent 运行完成
  EVENT_TYPE_RUN_ERROR = 6;                // Agent 运行出错
  EVENT_TYPE_RUN_ABORT = 7;                // Agent 运行被中止
  EVENT_TYPE_THINKING = 8;                 // 模型思考中
  EVENT_TYPE_USAGE = 9;                    // Token 用量统计
  EVENT_TYPE_INPUT_REQUIRED = 10;          // Agent 需要用户文本输入
  EVENT_TYPE_TOOL_CONFIRM_REQUIRED = 11;   // 高危工具需要用户确认
  EVENT_TYPE_MODEL_FALLBACK = 12;          // 模型降级通知 (Phase 2)
}

// --- 各事件的 Payload 定义 ---

message DeltaPayload {
  // 流式文本片段
  string text = 1;
  // 文本流标识 (区分 assistant 回复 vs thinking)
  string stream = 2;
}

message ToolStartPayload {
  string tool_call_id = 1;
  string tool_name = 2;
  // 工具输入参数 (JSON)
  string input_json = 3;
}

message ToolResultPayload {
  string tool_call_id = 1;
  string tool_name = 2;
  // 是否执行成功
  bool success = 3;
  // 结果内容 (截断到合理长度)
  string output = 4;
  // 执行耗时 (ms)
  int64 duration_ms = 5;
}

message RunStartPayload {
  string agent_id = 1;
  string model = 2;
  int64 started_at_ms = 3;
}

message RunCompletePayload {
  // 最终回复文本
  string final_text = 1;
  // 总执行轮数
  int32 iterations = 2;
  int64 duration_ms = 3;
}

message RunErrorPayload {
  string error_code = 1;
  string error_message = 2;
  // 是否可重试
  bool retryable = 3;
}

message RunAbortPayload {
  string reason = 1;
  string aborted_by = 2;          // "user" / "system" / "timeout"
}

message ThinkingPayload {
  string text = 1;
}

message UsagePayload {
  string model = 1;
  int32 input_tokens = 2;
  int32 output_tokens = 3;
  int32 cache_read_tokens = 4;
  int32 cache_write_tokens = 5;
  // 本轮迭代编号
  int32 iteration = 6;
}

// --- 人机交互事件 Payload ---

message InputRequiredPayload {
  // 输入类型: "text_input"
  string input_type = 1;
  // 提示文本 (告诉用户需要输入什么)
  string prompt = 2;
  // 超时时间 (秒, 默认 120)
  int32 timeout_seconds = 3;
}

message ToolConfirmRequiredPayload {
  // 输入类型: "tool_confirm"
  string input_type = 1;
  // 需要确认的工具调用 ID
  string tool_call_id = 2;
  // 工具名称
  string tool_name = 3;
  // 工具输入参数 (JSON)
  string input_json = 4;
  // 风险描述 (人类可读, 如 "将执行 rm -rf /workspace/build")
  string risk_description = 5;
  // 超时时间 (秒, 默认 120)
  int32 timeout_seconds = 6;
}

// --- 模型降级事件 Payload (Phase 2) ---

message ModelFallbackPayload {
  // 原始模型
  string from_model = 1;
  // 降级目标模型
  string to_model = 2;
  // 降级原因
  string reason = 3;
  // 降级层级 (1=重试, 2=Key轮换, 3=上下文压缩, 4=模型降级链)
  int32 fallback_layer = 4;
}
```

---

## 八、错误处理规范

### 8.1 gRPC 状态码使用约定

**核心原则**：gRPC 状态码表达"传输/协议层发生了什么"，`ErrorDetail` 表达"业务层发生了什么"。

| gRPC 状态码 | 何时使用 | Gateway 行为 |
| --- | --- | --- |
| `OK` | 请求成功处理（含幂等命中） | 正常处理响应 |
| `INVALID_ARGUMENT` | 请求参数校验失败 | 返回错误给客户端，不重试 |
| `NOT_FOUND` | task_id / run_id 不存在 | 返回错误给客户端，不重试 |
| `ABORTED` | session 锁冲突（同一 session 并发请求） | 排队等待或返回"处理中" |
| `RESOURCE_EXHAUSTED` | Worker 并发任务数已满 | **尝试下一个 Worker** |
| `UNAVAILABLE` | Worker 不可用（排空中 / 启动中 / 故障） | **将此 Worker 标记为不可用，尝试下一个** |
| `DEADLINE_EXCEEDED` | 请求超时 | 返回超时错误给客户端 |
| `INTERNAL` | Runtime 内部异常 | 记录错误日志，返回内部错误给客户端 |
| `CANCELLED` | Gateway 主动取消（如用户断开） | 清理状态 |

### 8.2 错误详情传递

Runtime 在返回非 OK 状态时，应在 gRPC Status 的 `details` 字段中附加 `ErrorDetail`：

```python
# Python Runtime 示例
from grpc import StatusCode
from google.protobuf.any_pb2 import Any
from sahara.common.v1 import common_pb2

def _reject_invalid(context, field, message):
    detail = common_pb2.ErrorDetail(
        error_code="VALIDATION_ERROR",
        message=message,
        field=field,
    )
    rich_status = rpc_status.to_status(
        code=code_pb2.INVALID_ARGUMENT,
        message=message,
        details=[detail],
    )
    context.abort_with_status(rich_status)
```

```go
// Go Gateway 解析示例
resp, err := client.SubmitTask(ctx, req)
if err != nil {
    st := status.Convert(err)
    for _, detail := range st.Details() {
        if ed, ok := detail.(*commonv1.ErrorDetail); ok {
            log.Warn("business error",
                "code", ed.ErrorCode,
                "field", ed.Field,
                "message", ed.Message,
            )
        }
    }
}
```

### 8.3 业务错误码表

| 错误码 | gRPC 状态码 | 说明 |
| --- | --- | --- |
| `VALIDATION_ERROR` | INVALID_ARGUMENT | 请求参数校验失败 |
| `SESSION_LOCKED` | ABORTED | session 正在被其他任务占用 |
| `SESSION_NOT_FOUND` | NOT_FOUND | 找不到指定 session |
| `TASK_NOT_FOUND` | NOT_FOUND | 找不到指定 task |
| `WORKER_FULL` | RESOURCE_EXHAUSTED | Worker 并发已满 |
| `WORKER_DRAINING` | UNAVAILABLE | Worker 正在排空 |
| `WORKER_NOT_READY` | UNAVAILABLE | Worker 尚未完成初始化 |
| `MODEL_UNAVAILABLE` | UNAVAILABLE | 指定的 LLM 模型不可用 |
| `IDEMPOTENCY_CONFLICT` | ABORTED | 相同幂等键但参数不同 |
| `SANDBOX_UNAVAILABLE` | INTERNAL | 沙箱池耗尽 |
| `TASK_NOT_WAITING` | FAILED_PRECONDITION | 任务未在等待输入状态 (SendInput) |
| `INPUT_CHANNEL_FULL` | RESOURCE_EXHAUSTED | 输入通道已满 (SendInput) |

---

## 九、连接管理

### 9.1 连接架构

```text
┌──────────────────────────────────────────────────────────────────┐
│  Gateway (Go)                                                    │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Worker Registry (服务发现层)                              │   │
│  │                                                            │   │
│  │  worker-1: 10.0.1.1:50051  state=READY   load=5/16       │   │
│  │  worker-2: 10.0.1.2:50051  state=READY   load=12/16      │   │
│  │  worker-3: 10.0.1.3:50051  state=DRAINING load=3/16      │   │
│  └──────────────────────────────────────────────────────────┘   │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  gRPC Client Pool                                          │   │
│  │                                                            │   │
│  │  每个 Worker 一个 grpc.ClientConn (HTTP/2 多路复用)        │   │
│  │  单连接即可支撑数千并发 RPC                                │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 9.2 连接参数

| 参数 | 值 | 说明 |
| --- | --- | --- |
| Transport | HTTP/2 (明文) | 内网通信；生产环境可启用 mTLS |
| Keepalive Time | 30s | 每 30s 发送 keepalive ping |
| Keepalive Timeout | 10s | ping 无响应 10s 后标记连接异常 |
| Max Connection Idle | 5min | 空闲连接 5min 后关闭 |
| Max Connection Age | 30min | 连接最长存活 30min，之后优雅重建 |
| Initial Window Size | 1MB | HTTP/2 流量控制窗口 |
| Max Recv Message Size | 4MB | 单条消息最大体积 (附件场景) |
| Max Send Message Size | 4MB | 同上 |

```go
// Go Gateway 连接配置示例
conn, err := grpc.NewClient(
    workerAddr,
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithKeepaliveParams(keepalive.ClientParameters{
        Time:                30 * time.Second,
        Timeout:             10 * time.Second,
        PermitWithoutStream: true,
    }),
    grpc.WithDefaultCallOptions(
        grpc.MaxCallRecvMsgSize(4 * 1024 * 1024),
        grpc.MaxCallSendMsgSize(4 * 1024 * 1024),
    }),
    grpc.WithDefaultServiceConfig(`{
        "methodConfig": [{
            "name": [{}],
            "waitForReady": true
        }]
    }`),
)
```

### 9.3 服务发现

**Phase 1（静态配置）**：
```yaml
# Gateway 配置
runtime:
  workers:
    - addr: "runtime-0:50051"
    - addr: "runtime-1:50051"
    - addr: "runtime-2:50051"
```

**Phase 2（K8s Headless Service）**：
```yaml
# K8s Service
apiVersion: v1
kind: Service
metadata:
  name: sahara-rt-headless
spec:
  clusterIP: None              # Headless → DNS 返回所有 Pod IP
  selector:
    app: sahara-rt
  ports:
    - port: 50051
```

Gateway 使用 gRPC 的 dns resolver 自动发现所有 Worker Pod：
```go
conn, _ := grpc.NewClient(
    "dns:///sahara-rt-headless.sahara.svc.cluster.local:50051",
    grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
)
```

**Phase 3（服务注册中心）**：Worker 启动时向 Redis 注册，定期续约。Gateway 监听注册表变化。

---

## 十、负载均衡与调度策略

### 10.1 三种策略

```text
策略 1: 轮询 (Round Robin)         ← Phase 1 默认
策略 2: 最少活跃任务 (Least Active) ← Phase 2 推荐
策略 3: 加权负载 (Weighted)        ← Phase 3 大规模
```

### 10.2 Phase 1 — 轮询 + 降级重试

```text
Gateway 选择 Worker 的流程:

  ┌─────────────────┐
  │ 取下一个 Worker  │
  │ (Round Robin)   │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐    RESOURCE_EXHAUSTED
  │ SubmitTask()    │──────────────────────┐
  └────────┬────────┘                      │
           │ OK                            ▼
           ▼                      ┌─────────────────┐
      ✅ 成功                    │  重试次数 < N?   │
                                  └────────┬────────┘
                                       是 │      否
                                          ▼       ▼
                                   取下一个    返回 "排队中"
                                   Worker      给客户端
```

```go
// Go Gateway 调度器伪代码
func (d *Dispatcher) Submit(ctx context.Context, req *agentv1.SubmitTaskRequest) (*agentv1.SubmitTaskResponse, error) {
    workers := d.registry.ReadyWorkers()
    if len(workers) == 0 {
        return nil, ErrNoWorkersAvailable
    }

    startIdx := d.next.Add(1)
    for attempt := 0; attempt < len(workers); attempt++ {
        idx := (int(startIdx) + attempt) % len(workers)
        worker := workers[idx]

        resp, err := worker.Client.SubmitTask(ctx, req)
        if err == nil {
            return resp, nil
        }

        st := status.Code(err)
        switch st {
        case codes.ResourceExhausted:
            // Worker 满了，尝试下一个
            continue
        case codes.Unavailable:
            // Worker 不可用，标记并跳过
            d.registry.MarkUnhealthy(worker.ID)
            continue
        default:
            // 其他错误不重试
            return nil, err
        }
    }

    return nil, ErrAllWorkersBusy
}
```

### 10.3 Phase 2 — 最少活跃任务

Gateway 定期轮询 `GetStatus`，维护每个 Worker 的 `active_tasks`。选择 `active_tasks` 最少的 Worker。

```go
func (d *Dispatcher) pickLeastActive() *Worker {
    var best *Worker
    for _, w := range d.registry.ReadyWorkers() {
        if best == nil || w.ActiveTasks < best.ActiveTasks {
            best = w
        }
    }
    return best
}
```

### 10.4 Session 亲和性（可选）

同一 session 的多次请求**优先**发给同一 Worker（利用沙箱缓存），但不强制绑定：

```go
func (d *Dispatcher) pickForSession(sessionKey string) *Worker {
    // 1. 检查上次处理该 session 的 Worker
    if lastWorker := d.sessionAffinity.Get(sessionKey); lastWorker != nil {
        if lastWorker.State == READY && lastWorker.ActiveTasks < lastWorker.MaxTasks {
            return lastWorker  // 亲和命中
        }
    }
    // 2. 回退到最少活跃策略
    return d.pickLeastActive()
}
```

### 10.5 Sticky Affinity — 任务级强制亲和（人机交互）

与 Session 亲和性（软亲和、可降级）不同，`SendInput` 要求**任务级强制亲和**——必须路由到持有该任务的 Worker，否则会收到 `NOT_FOUND`。

**触发条件**：Gateway 从 Event Bus 消费到 `INPUT_REQUIRED` 或 `TOOL_CONFIRM_REQUIRED` 事件时，记录强亲和映射。

**清除条件**：收到该任务的终态事件（`RUN_COMPLETE` / `RUN_ERROR` / `RUN_ABORT`）时清除映射。

```go
// Gateway 任务级强亲和管理
type StickyAffinity struct {
    mu     sync.RWMutex
    sticky map[string]string  // task_id → worker_id
}

func (s *StickyAffinity) MarkSticky(taskID, workerID string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.sticky[taskID] = workerID
}

func (s *StickyAffinity) ClearSticky(taskID string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    delete(s.sticky, taskID)
}

func (s *StickyAffinity) GetWorker(taskID string) (string, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    wid, ok := s.sticky[taskID]
    return wid, ok
}
```

```go
// Gateway Dispatcher.SendInput 路由
func (d *Dispatcher) SendInput(ctx context.Context, req *agentv1.SendInputRequest) (*agentv1.SendInputResponse, error) {
    // 1. 查找强亲和映射
    workerID, ok := d.stickyAffinity.GetWorker(req.TaskId)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "no sticky worker for task %s", req.TaskId)
    }

    // 2. 直接路由到目标 Worker (不走轮询/负载均衡)
    worker := d.registry.GetWorker(workerID)
    if worker == nil || worker.State != READY {
        return nil, status.Errorf(codes.Unavailable, "sticky worker %s not available", workerID)
    }

    return worker.Client.SendInput(ctx, req)
}
```

**三层亲和策略对比**：

| 层级 | 适用场景 | 绑定强度 | 降级行为 |
| --- | --- | --- | --- |
| **无亲和** | SubmitTask (Phase 1) | 无 | Round Robin |
| **Session 亲和** (§10.4) | SubmitTask (Phase 2) | 软 — Worker 满载/不可用时降级 | 回退到 Least Active |
| **Sticky 亲和** (§10.5) | SendInput | 强 — 必须命中目标 Worker | 不降级，返回错误 |

---

## 十一、超时与重试策略

### 11.1 各 RPC 超时配置

| RPC | 默认 Deadline | 说明 |
| --- | --- | --- |
| `SubmitTask` | **5s** | 只等待"接受/拒绝"判定，不等任务执行完 |
| `AbortTask` | **10s** | 等待取消信号发送和确认 |
| `SendInput` | **5s** | 投递用户输入到 Agent 的 input_channel，不等任务恢复执行 |
| `GetTaskStatus` | **3s** | 简单查询 |
| `ListActiveTasks` | **5s** | 可能任务较多 |
| `GetStatus` | **3s** | 轻量级健康查询 |
| `Drain` | **5s** | 只等待排空确认，不等任务完成 |
| `UpdateConfig` | **5s** | 配置更新 |

### 11.2 Deadline 传播

Gateway 设置的 Deadline 通过 gRPC context 自动传播到 Runtime。Runtime **必须**检查：

```python
# Python Runtime 示例
async def SubmitTask(self, request, context):
    # 检查 deadline 是否还有足够时间
    remaining = context.time_remaining()
    if remaining is not None and remaining < 0.5:
        context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "insufficient time")
        return

    # ... 处理逻辑
```

### 11.3 重试策略

| 场景 | 是否重试 | 重试目标 | 最大次数 |
| --- | --- | --- | --- |
| `RESOURCE_EXHAUSTED` | 是 | 下一个 Worker | 遍历所有 Worker |
| `UNAVAILABLE` | 是 | 下一个 Worker | 遍历所有 Worker |
| `DEADLINE_EXCEEDED` | 否 | — | — |
| `INVALID_ARGUMENT` | 否 | — | — |
| `NOT_FOUND` | 否 | — | — |
| `FAILED_PRECONDITION` | 否 | — | — |
| `INTERNAL` | 否 | — | — |
| 网络错误 (连接断开) | 是 | 同一 Worker (1次)，然后下一个 | 2 |

> **SendInput 重试注意**：`SendInput` 使用 Sticky Affinity 路由（§10.5），不走 Worker 轮转。
> `NOT_FOUND` 表示任务已结束或不存在，`FAILED_PRECONDITION` 表示任务未在等待状态，均不应重试。

**gRPC Service Config 重试**（仅对幂等 RPC 启用）：

```json
{
  "methodConfig": [
    {
      "name": [
        {"service": "sahara.worker.v1.WorkerService", "method": "GetStatus"}
      ],
      "retryPolicy": {
        "maxAttempts": 3,
        "initialBackoff": "0.1s",
        "maxBackoff": "1s",
        "backoffMultiplier": 2,
        "retryableStatusCodes": ["UNAVAILABLE"]
      }
    }
  ]
}
```

> **SubmitTask 不使用 gRPC 内建重试**——由 Gateway 调度器在应用层做 Worker 轮转重试，以便感知负载做出更智能的决策。

---

## 十二、Metadata 与上下文传播

### 12.1 标准 Metadata 字段

每个 gRPC 调用**必须**携带以下 metadata（HTTP/2 headers）：

| Key | 值 | 方向 | 用途 |
| --- | --- | --- | --- |
| `x-request-id` | ULID | Gateway → Runtime | 请求级唯一 ID，贯穿日志 |
| `x-gateway-id` | 字符串 | Gateway → Runtime | 发起调用的 Gateway 实例 ID |
| `traceparent` | W3C TraceContext | 双向 | OpenTelemetry 分布式追踪 |
| `x-user-id` | 字符串 | Gateway → Runtime | 已认证的用户 ID |
| `x-channel` | 字符串 | Gateway → Runtime | 来源渠道 (ws/http/telegram/discord) |

### 12.2 Go Gateway 发送 Metadata

```go
import "google.golang.org/grpc/metadata"

func (d *Dispatcher) submit(ctx context.Context, req *agentv1.SubmitTaskRequest) (*agentv1.SubmitTaskResponse, error) {
    md := metadata.Pairs(
        "x-request-id", ulid.Make().String(),
        "x-gateway-id", d.gatewayID,
        "x-user-id",    getUserID(ctx),
        "x-channel",    getChannel(ctx),
    )
    ctx = metadata.NewOutgoingContext(ctx, md)
    return d.client.SubmitTask(ctx, req)
}
```

### 12.3 Python Runtime 接收 Metadata

```python
async def SubmitTask(self, request, context):
    metadata = dict(context.invocation_metadata())
    request_id = metadata.get("x-request-id", "")
    gateway_id = metadata.get("x-gateway-id", "")
    user_id = metadata.get("x-user-id", "")

    # 注入到日志上下文
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        gateway_id=gateway_id,
        user_id=user_id,
    )
```

### 12.4 OpenTelemetry 集成

gRPC interceptor 自动处理 trace context 传播：

```go
// Go: 安装 otel gRPC interceptor
conn, _ := grpc.NewClient(addr,
    grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
)
```

```python
# Python: 安装 otel gRPC interceptor
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer
GrpcInstrumentorServer().instrument()
```

---

## 十三、安全

### 13.1 Phase 1 — 内网信任

Gateway 和 Runtime 在同一 VPC / K8s 集群内通信，使用明文 HTTP/2：
- K8s NetworkPolicy 限制只有 Gateway Pod 可以访问 Runtime 的 50051 端口
- 不对外暴露 gRPC 端口

### 13.2 Phase 2 — mTLS

启用双向 TLS，Gateway 和 Runtime 互相验证证书：

```text
┌──────────────┐       mTLS       ┌──────────────┐
│   Gateway    │◀────────────────▶│   Runtime    │
│   (client    │  Gateway 验证     │   (server    │
│    cert)     │  Runtime 证书     │    cert)     │
└──────────────┘  Runtime 验证     └──────────────┘
                  Gateway 证书
```

证书由 cert-manager 自动签发，K8s Secret 自动挂载。

### 13.3 认证 Token（可选）

如果 mTLS 不可用（如跨网络），可在 metadata 中传递认证 token：

```text
metadata: { "authorization": "Bearer <内部服务 token>" }
```

Runtime 的 gRPC interceptor 验证 token。Token 由共享 Secret 签发（HMAC），不走 JWT 公钥体系。

---

## 十四、版本演进规则

### 14.1 兼容性规则

| 规则 | 约束 |
| --- | --- |
| **字段只增不删** | 新字段使用新编号；已发布字段的编号和类型不可更改 |
| **枚举值只增不删** | 新增枚举值追加在末尾；已有值不重命名 |
| **RPC 只增不删** | 新 RPC 追加到 service 末尾；已有 RPC 不改签名 |
| **废弃标记** | 不再使用的字段/RPC 用 `deprecated = true` 标记，保留至少 2 个大版本 |
| **Breaking Change** | 任何不兼容变更必须创建新版本 package（如 `agent/v2`） |

### 14.2 CI 门禁

```yaml
# .github/workflows/ci.yml (proto 检查部分)
- name: Proto Lint
  run: buf lint

- name: Proto Breaking Change Check
  run: buf breaking --against '.git#branch=main'
  # 对比 main 分支，任何破坏性变更都会阻断 PR
```

### 14.3 版本共存

当需要 v2 时，v1 和 v2 并存，Runtime 同时注册两个版本的 service：

```text
proto/sahara/agent/v1/agent.proto    → AgentService (v1)
proto/sahara/agent/v2/agent.proto    → AgentService (v2)

Runtime 同时暴露两个 service，Gateway 逐步迁移到 v2
```

---

## 十五、性能调优

### 15.1 关键指标基线

| 指标 | 目标 | 说明 |
| --- | --- | --- |
| SubmitTask P50 | < 5ms | 不含任务执行时间，仅提交确认 |
| SubmitTask P99 | < 20ms | 含 Redis 幂等检查和 session 锁获取 |
| GetStatus P99 | < 3ms | 本地内存数据，无 IO |
| gRPC 建连时间 | < 10ms | 内网 HTTP/2 |
| 单连接并发 RPC | > 1000 | HTTP/2 多路复用 |

### 15.2 优化要点

**Go Gateway 侧**：
- 复用 `grpc.ClientConn`（HTTP/2 多路复用，单连接即可）
- 启用 keepalive，避免频繁建连
- SubmitTask 用 `context.WithTimeout`，避免慢 Worker 拖垮整个调度

**Python Runtime 侧**：
- 使用 `grpcio` 的 async server（`aio.server()`）
- 设置合理的 `maximum_concurrent_rpcs` 防止过载
- SubmitTask handler 立即返回（任务用 `asyncio.create_task` 异步执行），不阻塞 gRPC 线程

```python
# Python Runtime server 配置
server = grpc.aio.server(
    options=[
        ("grpc.max_send_message_length", 4 * 1024 * 1024),
        ("grpc.max_receive_message_length", 4 * 1024 * 1024),
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.max_concurrent_streams", 100),
    ],
    maximum_concurrent_rpcs=50,
)
```

### 15.3 序列化优化

Proto 消息的 `bytes` 字段（如附件）使用 zero-copy 传输。大文件（>1MB）不走 gRPC，通过对象存储 URL 传递。

---

## 十六、调试工具链

### 16.1 开发阶段

| 工具 | 用途 | 安装 |
| --- | --- | --- |
| **grpcurl** | 命令行 gRPC 调用 | `brew install grpcurl` |
| **grpcui** | 浏览器交互式 gRPC UI | `go install github.com/fullstorydev/grpcui/cmd/grpcui@latest` |
| **Postman** | GUI gRPC 调试 | 内建支持 |
| **buf** | Proto lint / breaking / generate | `brew install bufbuild/buf/buf` |

**Server Reflection**（开发/测试环境启用）：

```python
# Python Runtime 启用 reflection
from grpc_reflection.v1alpha import reflection
SERVICE_NAMES = (
    agent_pb2.DESCRIPTOR.services_by_name['AgentService'].full_name,
    worker_pb2.DESCRIPTOR.services_by_name['WorkerService'].full_name,
    reflection.SERVICE_NAME,
)
reflection.enable_server_reflection(SERVICE_NAMES, server)
```

**常用调试命令**：

```bash
# 列出服务
grpcurl -plaintext localhost:50051 list

# 查看 service 方法
grpcurl -plaintext localhost:50051 describe sahara.agent.v1.AgentService

# 提交任务
grpcurl -plaintext -d '{
  "task_id": "task-001",
  "session_key": "main:user-123:ws:device-1",
  "agent_id": "main",
  "user_message": {"text": "你好"},
  "idempotency_key": "idem-001"
}' localhost:50051 sahara.agent.v1.AgentService/SubmitTask

# 查询 Worker 状态
grpcurl -plaintext localhost:50051 sahara.worker.v1.WorkerService/GetStatus

# 中止任务
grpcurl -plaintext -d '{
  "task_id": "task-001",
  "run_id": "run-abc",
  "reason": "user cancelled"
}' localhost:50051 sahara.agent.v1.AgentService/AbortTask

# 人机交互：确认工具执行
grpcurl -plaintext -d '{
  "task_id": "task-001",
  "run_id": "run-abc",
  "action": "approve"
}' localhost:50051 sahara.agent.v1.AgentService/SendInput

# 人机交互：文本输入
grpcurl -plaintext -d '{
  "task_id": "task-001",
  "run_id": "run-abc",
  "action": "input",
  "input": "使用 Python 3.12"
}' localhost:50051 sahara.agent.v1.AgentService/SendInput
```

### 16.2 生产阶段

- **Prometheus 指标**：`grpc_server_handled_total`、`grpc_server_handling_seconds` 自动暴露
- **分布式追踪**：通过 OpenTelemetry，在 Jaeger 中查看 Gateway → Runtime 的完整调用链
- **访问日志**：gRPC interceptor 自动记录每个 RPC 的 method、status、duration、metadata

---

## 附录

### 附录 A. Proto 代码生成脚本

```bash
#!/bin/bash
# scripts/proto-gen.sh

set -euo pipefail

PROTO_DIR="proto"
GO_OUT="gateway/gen"
PY_OUT="runtime/gen"

echo "==> Linting proto files..."
buf lint

echo "==> Checking breaking changes..."
buf breaking --against '.git#branch=main' || true

echo "==> Generating Go code..."
buf generate --template buf.gen.yaml --output "$GO_OUT"

echo "==> Generating Python code..."
buf generate --template buf.gen.python.yaml --output "$PY_OUT"

echo "==> Done. Generated files:"
find "$GO_OUT" -name "*.go" -newer "$PROTO_DIR" | head -20
find "$PY_OUT" -name "*_pb2*.py" -newer "$PROTO_DIR" | head -20
```

### 附录 B. buf 配置

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
  except:
    - FIELD_NOT_REQUIRED
  enum_zero_value_suffix: _UNSPECIFIED
breaking:
  use:
    - FILE
```

```yaml
# buf.gen.yaml (Go)
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: gateway/gen
    opt: paths=source_relative
  - remote: buf.build/grpc/go
    out: gateway/gen
    opt: paths=source_relative
```

```yaml
# buf.gen.python.yaml (Python)
version: v2
plugins:
  - remote: buf.build/protocolbuffers/python
    out: runtime/gen
  - remote: buf.build/grpc/python
    out: runtime/gen
```

### 附录 C. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| 第五章 AgentService Proto | P0-4 编写核心 Proto 定义 | Phase 0 |
| 第五章 SubmitTask 实现 | P1-4 (Go) + P1-6 (Python) | Phase 1 |
| 第五章 AbortTask 实现 | P1-4 (Go) + P1-6 (Python) | Phase 1 |
| 第五章 SendInput 实现 | P2-15 人机交互 | Phase 2 |
| 第五章 GetTaskStatus 实现 | P1-4 (Go) + P1-6 (Python) | Phase 1 |
| 第六章 GetStatus 实现 | P0-10 健康检查联通 | Phase 0 |
| 第六章 Drain 实现 | P2-6 Worker 优雅关闭 | Phase 2 |
| 第七章 INPUT_REQUIRED / TOOL_CONFIRM 事件 | P2-15 人机交互 | Phase 2 |
| 第七章 MODEL_FALLBACK 事件 | P2-14 模型降级链 | Phase 2 |
| 第九章 连接管理 | P1-4 gRPC client pool | Phase 1 |
| 第十章 负载均衡 (轮询 + 降级) | P1-4 基础调度 | Phase 1 |
| 第十章 负载均衡 (Sticky Affinity) | P2-15 人机交互 | Phase 2 |
| 第十二章 Metadata | P1-4 + P1-6 | Phase 1 |
| 第十三章 mTLS | P2-12 K8s 部署 | Phase 2 |
| 第十六章 Server Reflection | P0-10 gRPC 通信验证 | Phase 0 |
