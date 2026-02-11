# Agent Runtime 架构设计

> Python Runtime Worker 的内部模块划分、异步并发模型、Agent Loop 核心设计与各子系统接口定义。
> 本文档是 Python 开发组的实现蓝图，将 OpenClaw 八大子系统映射到 Sahara 新架构下的 Python 模块。
>
> 关联文档：
> - [gRPC 协议设计](./GRPC-PROTOCOL-DESIGN.md) — Runtime 作为 gRPC Server 暴露服务（D1）
> - [Event Bus 架构设计](./EVENT-BUS-DESIGN.md) — Runtime 发射事件到 Event Bus（D5）
> - [Sandbox 管理架构](./SANDBOX-DESIGN.md) — 沙箱容器池详细设计（D6）
> - [技术方案 §四](./TECH-PROPOSAL-C-END-REFACTOR.md) — Runtime 八大子系统与选型
> - [OpenClaw AGENT-RUNTIME-v2.md](./openclaw/agent/AGENT-RUNTIME-v2.md) — 原始架构参考

---

## 目录

1. [模块全景](#一模块全景)
2. [Python 包结构](#二python-包结构)
3. [gRPC Server — 入口层](#三grpc-server--入口层)
4. [Agent Loop — 核心交互循环](#四agent-loop--核心交互循环)
5. [事件发射器 (EventEmitter)](#五事件发射器-eventemitter)
6. [模型管理 (Model Router)](#六模型管理-model-router)
7. [工具系统 (Tools)](#七工具系统-tools)
8. [系统提示词构建](#八系统提示词构建)
9. [会话管理 (Session Store)](#九会话管理-session-store)
10. [上下文管理 (Context Manager)](#十上下文管理-context-manager)
11. [沙箱管理 (Sandbox Manager)](#十一沙箱管理-sandbox-manager)
12. [并发模型](#十二并发模型)
13. [错误处理与弹性](#十三错误处理与弹性)
14. [配置管理](#十四配置管理)
15. [可观测性](#十五可观测性)
16. [Phase 1 最小实现范围](#十六phase-1-最小实现范围)

---

## 一、模块全景

### 1.1 OpenClaw 八大子系统 → Sahara Python 模块映射

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  Sahara Runtime Worker (Python)                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─── 入口层 ──────────────────────────────────────────────────────┐   │
│  │  gRPC Server (grpcio async)                                      │   │
│  │  ├── AgentService    ← SubmitTask / AbortTask / GetTaskStatus   │   │
│  │  ├── WorkerService   ← GetStatus / Drain                        │   │
│  │  └── Health          ← 标准健康检查                              │   │
│  └──────────────────────────────┬───────────────────────────────────┘   │
│                                 │ asyncio.create_task                    │
│                                 ▼                                       │
│  ┌─── 任务执行层 ──────────────────────────────────────────────────┐   │
│  │                                                                  │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                │   │
│  │  │ Model      │  │ Session    │  │ Sandbox    │                │   │
│  │  │ Router     │  │ Store      │  │ Manager    │                │   │
│  │  │ (模型选择   │  │ (会话加载   │  │ (沙箱分配   │                │   │
│  │  │  Key 轮换)  │  │  历史恢复)  │  │  容器池)   │                │   │
│  │  └──────┬─────┘  └──────┬─────┘  └──────┬─────┘                │   │
│  │         └───────────────┼───────────────┘                       │   │
│  │                         ▼                                        │   │
│  │  ┌────────────────────────────────────────────────────────────┐ │   │
│  │  │  Prompt Builder (系统提示词组装)                             │ │   │
│  │  └──────────────────────────┬─────────────────────────────────┘ │   │
│  │                             │                                    │   │
│  │  ┌────────────┐             ▼                                    │   │
│  │  │ Tool       │  ┌──────────────────────────────────────────┐  │   │
│  │  │ Registry   │  │  Agent Loop (核心交互循环)                 │  │   │
│  │  │ (工具创建   │─▶│  LLM 调用 → 流式处理 → 工具执行 → 循环   │  │   │
│  │  │  策略过滤)  │  │                                          │  │   │
│  │  └────────────┘  └──────────────────────┬─────────────────────┘  │   │
│  │                                         │                        │   │
│  └─────────────────────────────────────────┼────────────────────────┘   │
│                                            │ events                     │
│  ┌─── 输出层 ─────────────────────────────┼────────────────────────┐   │
│  │                                         ▼                        │   │
│  │  ┌─────────────┐  ┌────────────────────────────────────────┐   │   │
│  │  │ Context     │  │ Event Emitter                           │   │   │
│  │  │ Manager     │  │ (Redis Streams XADD)                   │   │   │
│  │  │ (上下文裁剪) │  │ 每个 delta/tool/lifecycle → Event Bus  │   │   │
│  │  └─────────────┘  └────────────────────────────────────────┘   │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─── 基础设施层 ───────────────────────────────────────────────────┐ │
│  │  Redis Client │ PostgreSQL (asyncpg) │ Docker Client │ Config     │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 子系统职责变化（OpenClaw → Sahara）

| # | 子系统 | OpenClaw 方式 | Sahara 变化 | Python 模块 |
| --- | --- | --- | --- | --- |
| ① | 入口与调度 | 进程内双层队列 | gRPC server + asyncio Semaphore | `server.py` |
| ② | 模型与认证 | 本地配置文件 | Redis 集中配置 + Key 池 | `models/` |
| ③ | 运行环境 | 本地 Docker + 本地文件 | 同机 Docker 池 + 集中存储 | `sandbox/` |
| ④ | 系统提示词 | ~24 节段拼接 | 逻辑不变，数据源从本地→Redis/PG | `prompt/` |
| ⑤ | 工具系统 | 创建 + 9 层过滤 | 保持，沙箱工具走本地容器 | `tools/` |
| ⑥ | Agent Loop | SDK 封装 (streamSimple) | **核心重写**：直接用 LLM SDK + 自建循环 | `agent_loop.py` |
| ⑦ | 事件与流式 | 进程内 emitAgentEvent | **大改**：发布到 Redis Streams | `events/` |
| ⑧ | 上下文管理 | SDK 内置 | Python 重新实现 | `context/` |

---

## 二、Python 包结构

```text
runtime/
├── sahara_runtime/                  # 主包
│   ├── __init__.py
│   ├── main.py                     # 入口：启动 gRPC server + 初始化
│   ├── server.py                   # gRPC servicer 实现
│   ├── worker.py                   # Worker 状态管理 (并发/排空)
│   ├── agent_loop.py               # ★ 核心: LLM 交互循环
│   │
│   ├── models/                     # 模型管理
│   │   ├── __init__.py
│   │   ├── router.py               # 模型选择 + Provider 路由
│   │   ├── providers.py            # Anthropic / OpenAI 客户端封装
│   │   ├── key_pool.py             # API Key 池 + 轮换 + 冷却
│   │   └── fallback.py             # 模型降级链
│   │
│   ├── tools/                      # 工具系统
│   │   ├── __init__.py
│   │   ├── registry.py             # 工具注册表 + 策略过滤
│   │   ├── executor.py             # 工具统一执行入口
│   │   ├── exec_tool.py            # exec (bash 命令)
│   │   ├── file_tools.py           # read / write / edit
│   │   ├── web_tools.py            # web_search / web_fetch
│   │   └── definitions.py          # 工具 schema 定义 (给 LLM)
│   │
│   ├── sandbox/                    # 沙箱管理
│   │   ├── __init__.py
│   │   ├── manager.py              # 容器池管理
│   │   ├── container.py            # 单容器抽象
│   │   └── pool.py                 # 池化 (预创建/分配/回收)
│   │
│   ├── session/                    # 会话管理
│   │   ├── __init__.py
│   │   ├── store.py                # Redis 热 + PG 冷
│   │   ├── lock.py                 # session 级分布式锁
│   │   └── history.py              # 消息历史操作
│   │
│   ├── context/                    # 上下文管理
│   │   ├── __init__.py
│   │   ├── manager.py              # 四层防御协调器
│   │   ├── truncator.py            # Layer 1: 输入截断
│   │   ├── pruner.py               # Layer 2: 历史剪枝
│   │   ├── compactor.py            # Layer 3: 自动压缩
│   │   └── token_counter.py        # tiktoken 封装
│   │
│   ├── prompt/                     # 系统提示词
│   │   ├── __init__.py
│   │   ├── builder.py              # 提示词组装器
│   │   └── segments.py             # 各节段定义
│   │
│   ├── events/                     # 事件系统
│   │   ├── __init__.py
│   │   ├── emitter.py              # Redis Streams 事件发射
│   │   ├── types.py                # 事件类型定义
│   │   └── serializer.py           # Protobuf 序列化
│   │
│   └── config.py                   # pydantic-settings 配置
│
├── gen/                            # Proto 生成的 Python 代码
│   └── sahara/
│       ├── agent/v1/
│       ├── worker/v1/
│       ├── common/v1/
│       └── event/v1/
│
├── tests/                          # 测试
│   ├── test_agent_loop.py
│   ├── test_tools.py
│   ├── test_context.py
│   ├── mock_llm.py                 # LLM Mock Server
│   └── conftest.py
│
├── pyproject.toml                  # 依赖管理 (uv)
├── uv.lock
└── Dockerfile
```

---

## 三、gRPC Server — 入口层

### 3.1 核心职责

gRPC Server 是 Runtime Worker 的入口。接收 Gateway 发来的 `SubmitTask` / `AbortTask` 等请求，转化为 `asyncio.Task` 异步执行。

### 3.2 Servicer 实现

```python
# sahara_runtime/server.py

import asyncio
import grpc
from sahara_runtime.worker import WorkerState
from sahara_runtime.agent_loop import run_agent_loop
from gen.sahara.agent.v1 import agent_pb2, agent_pb2_grpc

class AgentServicer(agent_pb2_grpc.AgentServiceServicer):
    def __init__(self, deps: "Dependencies"):
        self.deps = deps
        self.worker = deps.worker           # WorkerState
        self.active_tasks: dict[str, TaskHandle] = {}

    async def SubmitTask(self, request, context):
        # 1. 检查 Worker 状态
        if self.worker.state == WorkerState.DRAINING:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "worker draining")
            return

        if not self.worker.try_acquire():
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "worker full")
            return

        # 2. 幂等检查
        existing = await self.deps.dedup.check(request.idempotency_key)
        if existing:
            return agent_pb2.SubmitTaskResponse(
                run_id=existing.run_id,
                worker_id=self.worker.id,
            )

        # 3. Session 锁
        lock_acquired = await self.deps.session_lock.acquire(
            request.session_key, timeout=2.0
        )
        if not lock_acquired:
            await context.abort(grpc.StatusCode.ABORTED, "session locked")
            return

        # 4. 生成 run_id, 创建异步任务
        run_id = generate_run_id()
        task = asyncio.create_task(
            self._execute(request, run_id, context)
        )
        self.active_tasks[run_id] = TaskHandle(
            task=task,
            task_id=request.task_id,
            run_id=run_id,
            session_key=request.session_key,
            started_at=time.time(),
        )

        # 5. 记录幂等
        await self.deps.dedup.store(request.idempotency_key, run_id)

        # 6. 返回 accepted
        return agent_pb2.SubmitTaskResponse(
            run_id=run_id,
            worker_id=self.worker.id,
            accepted_at_ms=int(time.time() * 1000),
        )

    async def _execute(self, request, run_id, context):
        """异步执行 Agent 任务（不阻塞 gRPC 线程）"""
        metadata = dict(context.invocation_metadata())
        try:
            await run_agent_loop(
                task_id=request.task_id,
                run_id=run_id,
                session_key=request.session_key,
                user_message=request.user_message.text,
                agent_id=request.agent_id,
                metadata=metadata,
                deps=self.deps,
            )
        except asyncio.CancelledError:
            await self.deps.emitter.emit_abort(run_id, request.session_key, "cancelled")
        except Exception as e:
            await self.deps.emitter.emit_error(run_id, request.session_key, str(e))
            logger.exception("task execution failed", extra={"run_id": run_id})
        finally:
            self.active_tasks.pop(run_id, None)
            self.worker.release()
            await self.deps.session_lock.release(request.session_key)

    async def AbortTask(self, request, context):
        handle = self.active_tasks.get(request.run_id)
        if not handle:
            await context.abort(grpc.StatusCode.NOT_FOUND, "task not found")
            return

        previous_state = "running"
        handle.task.cancel()
        return agent_pb2.AbortTaskResponse(
            previous_state=commonv1.TASK_STATE_RUNNING,
        )
```

### 3.3 Worker 状态

```python
# sahara_runtime/worker.py

class WorkerState(Enum):
    STARTING = "starting"
    READY = "ready"
    DRAINING = "draining"

class Worker:
    def __init__(self, config: RuntimeConfig):
        self.id = config.worker_id
        self.state = WorkerState.STARTING
        self.max_tasks = config.max_concurrent_tasks  # 默认 16
        self._semaphore = asyncio.Semaphore(self.max_tasks)
        self._active_count = 0

    def try_acquire(self) -> bool:
        """尝试获取一个任务槽位（非阻塞）"""
        if self._semaphore.locked():
            return False
        # 这里不 await（非阻塞检查）
        # 实际 acquire 在 _execute 中用 async with
        self._active_count += 1
        return True

    def release(self):
        self._active_count -= 1

    @property
    def active_tasks(self) -> int:
        return self._active_count
```

---

## 四、Agent Loop — 核心交互循环

### 4.1 设计原则

不使用 LangChain/LlamaIndex 等 Agent 框架。**直接用 LLM SDK + 自建循环**。原因：

| 原因 | 详细说明 |
| --- | --- |
| 事件粒度控制 | 需要每个 text_delta 独立发射到 Event Bus，框架通常批量处理 |
| 工具策略 | 9 层策略过滤 + 安全检查，框架的工具抽象太粗 |
| 上下文管理 | 四层防御需要精确的 token 计数和消息操作 |
| Provider 切换 | 热切换 model/key，框架通常绑定单一 provider |
| 可调试性 | 自建循环每一步都可日志/追踪，框架是黑盒 |

### 4.2 完整流程

```text
run_agent_loop()
  │
  ├── 1. 加载 Session 历史 (Redis → messages[])
  ├── 2. 解析模型 + 获取 API Key
  ├── 3. 分配沙箱容器
  ├── 4. 构建系统提示词
  ├── 5. 创建工具集 + 策略过滤
  ├── 6. 注入用户消息到 messages[]
  ├── 7. 发射 RUN_START 事件
  │
  ├── 8. ★ 交互循环 (max N 轮):
  │   │
  │   ├── 8a. 上下文管理 (检查 token → 截断/剪枝/压缩)
  │   ├── 8b. 调用 LLM (流式)
  │   │       ├── 每个 text_delta → 发射 DELTA 事件
  │   │       ├── thinking_delta → 发射 THINKING 事件
  │   │       └── 完整响应 → messages.append(assistant)
  │   ├── 8c. stop_reason == "end_turn" → 跳出循环
  │   ├── 8d. stop_reason == "tool_use":
  │   │       ├── 发射 TOOL_START 事件
  │   │       ├── 执行工具 (沙箱 exec / 文件 read/write)
  │   │       ├── 发射 TOOL_RESULT 事件
  │   │       ├── 发射 USAGE 事件
  │   │       └── messages.append(tool_result) → 继续循环
  │   └── 8e. 异常处理 (API 错误 → 重试/降级/中止)
  │
  ├── 9. 发射 RUN_COMPLETE 事件
  ├── 10. 持久化 Session (messages[] → Redis + PG)
  └── 11. 释放沙箱容器
```

### 4.3 核心实现

```python
# sahara_runtime/agent_loop.py

async def run_agent_loop(
    task_id: str,
    run_id: str,
    session_key: str,
    user_message: str,
    agent_id: str,
    metadata: dict[str, str],
    deps: "Dependencies",
) -> None:
    """Agent 核心交互循环。每个 SubmitTask 调用此函数。"""

    # ── 1. 加载会话历史 ──
    session = await deps.session_store.load(session_key)
    messages = session.messages  # list[dict]

    # ── 2. 模型解析 ──
    model_config = await deps.model_router.resolve(agent_id)
    client = deps.model_router.get_client(model_config.provider)

    # ── 3. 沙箱分配 ──
    sandbox = await deps.sandbox_manager.acquire(session_key)

    # ── 4. 系统提示词 ──
    system_prompt = await deps.prompt_builder.build(
        agent_id=agent_id,
        sandbox=sandbox,
        tools=None,  # 下一步创建
    )

    # ── 5. 工具集 ──
    tools = await deps.tool_registry.create_tools(
        agent_id=agent_id,
        sandbox=sandbox,
        session_key=session_key,
    )
    tool_definitions = [t.to_llm_schema() for t in tools]

    # ── 6. 注入用户消息 ──
    messages.append({"role": "user", "content": user_message})

    # ── 7. RUN_START ──
    emitter = deps.emitter.for_run(run_id, session_key, task_id)
    await emitter.emit_run_start(agent_id=agent_id, model=model_config.model_id)

    # ── 8. 交互循环 ──
    try:
        for iteration in range(model_config.max_iterations):
            # 8a. 上下文管理
            messages = await deps.context_manager.fit(
                messages=messages,
                system_prompt=system_prompt,
                model=model_config,
            )

            # 8b. 调用 LLM (流式)
            response = await _call_llm_streaming(
                client=client,
                model=model_config.model_id,
                system=system_prompt,
                messages=messages,
                tools=tool_definitions,
                emitter=emitter,
                iteration=iteration,
            )

            messages.append({"role": "assistant", "content": response.content})

            # 8c. 自然结束
            if response.stop_reason != "tool_use":
                break

            # 8d. 工具执行
            tool_results = await _execute_tools(
                response=response,
                tools=tools,
                sandbox=sandbox,
                emitter=emitter,
            )
            messages.append({"role": "user", "content": tool_results})

        # ── 9. RUN_COMPLETE ──
        final_text = _extract_final_text(messages)
        await emitter.emit_run_complete(
            final_text=final_text,
            iterations=iteration + 1,
        )

    except asyncio.CancelledError:
        raise  # 由上层 _execute 处理

    except LLMProviderError as e:
        # 模型降级尝试
        if model_config.fallback and e.retryable:
            model_config = await deps.model_router.fallback(model_config)
            # TODO: 重新执行循环 (简化版先不实现)
        await emitter.emit_run_error(str(e), retryable=e.retryable)
        raise

    finally:
        # ── 10. 持久化 ──
        await deps.session_store.save(session_key, messages)
        # ── 11. 释放沙箱 ──
        await deps.sandbox_manager.release(sandbox)


async def _call_llm_streaming(
    client, model, system, messages, tools, emitter, iteration,
):
    """调用 LLM 并处理流式响应，每个 delta 实时发射事件。"""

    async with client.messages.stream(
        model=model,
        system=system,
        messages=messages,
        tools=tools,
        max_tokens=8192,
    ) as stream:
        async for event in stream:
            match event.type:
                case "content_block_delta":
                    if event.delta.type == "text_delta":
                        await emitter.emit_delta(
                            text=event.delta.text,
                            stream="assistant",
                        )
                    elif event.delta.type == "thinking_delta":
                        await emitter.emit_thinking(text=event.delta.thinking)

                case "message_start":
                    pass  # 提取 usage 预估

                case "message_delta":
                    if hasattr(event, "usage"):
                        await emitter.emit_usage(
                            model=model,
                            input_tokens=event.usage.input_tokens,
                            output_tokens=event.usage.output_tokens,
                            iteration=iteration,
                        )

        return await stream.get_final_message()


async def _execute_tools(response, tools, sandbox, emitter):
    """执行所有工具调用，返回 tool_result 列表。"""
    tool_results = []
    tool_map = {t.name: t for t in tools}

    for block in response.content:
        if block.type != "tool_use":
            continue

        tool = tool_map.get(block.name)
        if not tool:
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": f"Unknown tool: {block.name}",
                "is_error": True,
            })
            continue

        # 发射 TOOL_START
        await emitter.emit_tool_start(
            tool_call_id=block.id,
            tool_name=block.name,
            input_json=json.dumps(block.input),
        )

        # 执行
        start = time.monotonic()
        try:
            result = await tool.execute(block.input, sandbox=sandbox)
            success = True
        except ToolExecutionError as e:
            result = str(e)
            success = False

        duration_ms = int((time.monotonic() - start) * 1000)

        # 发射 TOOL_RESULT
        await emitter.emit_tool_result(
            tool_call_id=block.id,
            tool_name=block.name,
            success=success,
            output=_truncate(str(result), max_chars=10_000),
            duration_ms=duration_ms,
        )

        tool_results.append({
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": str(result),
        })

    return tool_results
```

---

## 五、事件发射器 (EventEmitter)

### 5.1 职责

将 Agent 执行过程中的每个 delta/tool/lifecycle 事件序列化为 Protobuf，通过 Redis Streams `XADD` 发布到 Event Bus。

### 5.2 接口设计

```python
# sahara_runtime/events/emitter.py

class RedisEventEmitter:
    def __init__(self, redis: Redis, worker_id: str):
        self.redis = redis
        self.worker_id = worker_id

    def for_run(self, run_id: str, session_key: str, task_id: str) -> "RunEmitter":
        return RunEmitter(self, run_id, session_key, task_id)


class RunEmitter:
    """单次执行的事件发射器，自动维护 seq 递增。"""

    def __init__(self, parent: RedisEventEmitter, run_id, session_key, task_id):
        self._parent = parent
        self._run_id = run_id
        self._session_key = session_key
        self._task_id = task_id
        self._seq = 0
        self._trace_id = get_current_trace_id()

    async def _emit(self, event_type: EventType, **payload_kwargs):
        self._seq += 1
        event = AgentEvent(
            event_id=ulid.new().str,
            run_id=self._run_id,
            session_key=self._session_key,
            task_id=self._task_id,
            type=event_type,
            timestamp_ms=int(time.time() * 1000),
            seq=self._seq,
            trace_id=self._trace_id,
        )
        # 设置 payload oneof
        set_payload(event, event_type, payload_kwargs)

        # Protobuf 序列化
        data = event.SerializeToString()

        # 发布到 Redis Stream
        stream_key = f"events:{self._session_key}"
        await self._parent.redis.xadd(stream_key, {"event": data}, maxlen=5000)

    # ── 便捷方法 ──

    async def emit_run_start(self, agent_id: str, model: str):
        await self._emit(EventType.EVENT_TYPE_RUN_START,
                         agent_id=agent_id, model=model)

    async def emit_delta(self, text: str, stream: str = "assistant"):
        await self._emit(EventType.EVENT_TYPE_DELTA, text=text, stream=stream)

    async def emit_thinking(self, text: str):
        await self._emit(EventType.EVENT_TYPE_THINKING, text=text)

    async def emit_tool_start(self, tool_call_id, tool_name, input_json):
        await self._emit(EventType.EVENT_TYPE_TOOL_START,
                         tool_call_id=tool_call_id, tool_name=tool_name,
                         input_json=input_json)

    async def emit_tool_result(self, tool_call_id, tool_name, success, output, duration_ms):
        await self._emit(EventType.EVENT_TYPE_TOOL_RESULT,
                         tool_call_id=tool_call_id, tool_name=tool_name,
                         success=success, output=output, duration_ms=duration_ms)

    async def emit_run_complete(self, final_text: str, iterations: int):
        await self._emit(EventType.EVENT_TYPE_RUN_COMPLETE,
                         final_text=final_text, iterations=iterations)

    async def emit_run_error(self, error_message: str, retryable: bool = False):
        await self._emit(EventType.EVENT_TYPE_RUN_ERROR,
                         error_message=error_message, retryable=retryable)

    async def emit_abort(self, reason: str):
        await self._emit(EventType.EVENT_TYPE_RUN_ABORT, reason=reason)

    async def emit_usage(self, model, input_tokens, output_tokens, iteration):
        await self._emit(EventType.EVENT_TYPE_USAGE,
                         model=model, input_tokens=input_tokens,
                         output_tokens=output_tokens, iteration=iteration)
```

### 5.3 Redis Stream Key 规范

```text
events:{session_key}    ← 每个 session 一个 stream
                          maxlen=5000 (自动裁剪旧事件)
                          Gateway 用 XREADGROUP 消费
```

---

## 六、模型管理 (Model Router)

### 6.1 职责

将 agent 配置中的模型名解析为具体的 Provider + Model + API Key，支持多 Key 轮换、限流冷却、模型降级。

### 6.2 核心接口

```python
# sahara_runtime/models/router.py

@dataclass
class ModelConfig:
    provider: str             # "anthropic" / "openai"
    model_id: str             # "claude-sonnet-4-20250514"
    max_tokens: int           # 8192
    max_context_tokens: int   # 200000
    max_iterations: int       # 20
    fallback: str | None      # 降级模型名

class ModelRouter:
    def __init__(self, config: RuntimeConfig, key_pool: KeyPool):
        self.config = config
        self.key_pool = key_pool
        self.clients: dict[str, Any] = {}   # provider → SDK client

    async def resolve(self, agent_id: str) -> ModelConfig:
        """解析 agent 配置 → 完整的 ModelConfig"""
        agent_config = await self._load_agent_config(agent_id)
        model_config = KNOWN_MODELS[agent_config.model]

        # 获取可用 Key
        key = await self.key_pool.get_key(model_config.provider)
        if not key:
            raise ModelUnavailableError(f"no keys for {model_config.provider}")

        return model_config

    def get_client(self, provider: str):
        """获取 LLM SDK 客户端 (复用)"""
        if provider not in self.clients:
            key = self.key_pool.current_key(provider)
            if provider == "anthropic":
                self.clients[provider] = anthropic.AsyncAnthropic(api_key=key)
            elif provider == "openai":
                self.clients[provider] = openai.AsyncOpenAI(api_key=key)
        return self.clients[provider]

    async def fallback(self, current: ModelConfig) -> ModelConfig:
        """模型降级：主模型不可用时切换到备用"""
        if not current.fallback:
            raise ModelUnavailableError("no fallback model configured")
        return await self.resolve_by_model_id(current.fallback)
```

### 6.3 Key 池 + 熔断

```python
# sahara_runtime/models/key_pool.py

class KeyPool:
    """API Key 池管理：轮换 + 限流冷却 + 熔断"""

    def __init__(self, redis: Redis):
        self.redis = redis

    async def get_key(self, provider: str) -> str | None:
        """获取一个可用 Key（跳过冷却中和熔断的 Key）"""
        keys = await self.redis.smembers(f"keys:{provider}")
        for key in keys:
            state = await self.redis.hgetall(f"key_state:{key}")
            if state.get("status") == "cooling":
                continue
            if state.get("status") == "circuit_open":
                # 半开检查
                if time.time() - float(state["opened_at"]) > 30:
                    return key  # 试探
                continue
            return key
        return None

    async def report_error(self, key: str, error_code: int):
        """Key 调用失败时上报"""
        if error_code == 429:
            # Rate Limit → 冷却 60s
            await self.redis.hset(f"key_state:{key}", mapping={
                "status": "cooling", "until": str(time.time() + 60)
            })
        elif error_code in (401, 403):
            # 认证失败 → 熔断
            await self.redis.hset(f"key_state:{key}", mapping={
                "status": "circuit_open", "opened_at": str(time.time())
            })
```

---

## 七、工具系统 (Tools)

### 7.1 工具接口

```python
# sahara_runtime/tools/registry.py

class Tool(ABC):
    """所有工具的基类"""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def description(self) -> str: ...

    @abstractmethod
    def parameters_schema(self) -> dict: ...

    @abstractmethod
    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str: ...

    def to_llm_schema(self) -> dict:
        """转换为 LLM API 格式的工具定义"""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.parameters_schema(),
        }
```

### 7.2 exec 工具示例

```python
# sahara_runtime/tools/exec_tool.py

class ExecTool(Tool):
    name = "exec"
    description = "Execute a shell command"

    def parameters_schema(self):
        return {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The bash command to run"},
                "timeout": {"type": "integer", "description": "Timeout in seconds", "default": 30},
            },
            "required": ["command"],
        }

    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str:
        command = args["command"]
        timeout = args.get("timeout", 30)

        if sandbox and sandbox.enabled:
            # 在 Docker 容器内执行
            result = await sandbox.exec(command, timeout=timeout)
        else:
            # 主机执行 (开发环境)
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            result = stdout.decode() + stderr.decode()

        return _truncate(result, max_chars=8000)
```

### 7.3 工具注册与策略过滤

```python
# sahara_runtime/tools/registry.py

class ToolRegistry:
    async def create_tools(self, agent_id, sandbox, session_key) -> list[Tool]:
        # 1. 创建所有工具
        all_tools = [
            ExecTool(),
            ReadTool(),
            WriteTool(),
            EditTool(),
            WebSearchTool(),
            WebFetchTool(),
        ]

        # 2. 加载策略
        policy = await self._load_policy(agent_id)

        # 3. 策略过滤 (简化版: allowlist / blocklist)
        filtered = []
        for tool in all_tools:
            if policy.blocklist and tool.name in policy.blocklist:
                continue
            if policy.allowlist and tool.name not in policy.allowlist:
                continue
            filtered.append(tool)

        return filtered
```

---

## 八、系统提示词构建

```python
# sahara_runtime/prompt/builder.py

class PromptBuilder:
    async def build(self, agent_id: str, sandbox, tools) -> str:
        segments = []

        # 1. 身份定义
        segments.append(self._identity_segment(agent_id))

        # 2. 工具使用指南
        if tools:
            segments.append(self._tool_guide_segment(tools))

        # 3. 安全规则
        segments.append(self._safety_segment())

        # 4. 沙箱环境
        if sandbox and sandbox.enabled:
            segments.append(self._sandbox_segment(sandbox))

        # 5. Agent 上下文文件 (AGENTS.md 等)
        context_files = await self._load_context_files(agent_id)
        for cf in context_files:
            segments.append(f"<context_file name=\"{cf.name}\">\n{cf.content}\n</context_file>")

        # 6. 运行时信息
        segments.append(self._runtime_info_segment())

        return "\n\n".join(segments)
```

---

## 九、会话管理 (Session Store)

### 9.1 热冷分离

```text
┌──────────────────────────┐       ┌──────────────────────────┐
│  Redis (热存储)           │       │  PostgreSQL (冷存储)      │
│                          │       │                          │
│  session:{key}:messages  │       │  sessions 表              │
│  → 最近 N 条消息 (JSON)  │       │  ├── session_key (PK)    │
│  → TTL 24h               │       │  ├── messages (JSONB)    │
│                          │       │  ├── created_at          │
│  session:{key}:meta      │       │  └── updated_at          │
│  → { title, agent_id,   │       │                          │
│      created_at, ... }   │       │  runs 表                 │
│                          │       │  ├── run_id (PK)         │
│  session:{key}:lock      │       │  ├── session_key         │
│  → 分布式锁 (防并发)     │       │  ├── tokens_used         │
│                          │       │  └── duration_ms         │
└──────────────────────────┘       └──────────────────────────┘
```

### 9.2 接口

```python
# sahara_runtime/session/store.py

class SessionStore:
    async def load(self, session_key: str) -> Session:
        """加载会话：先查 Redis，miss 则查 PG 并回填"""
        cached = await self.redis.get(f"session:{session_key}:messages")
        if cached:
            return Session.from_json(cached)

        # 冷加载
        row = await self.pg.fetchrow(
            "SELECT messages FROM sessions WHERE session_key = $1", session_key
        )
        if row:
            session = Session.from_json(row["messages"])
            # 回填 Redis
            await self.redis.setex(
                f"session:{session_key}:messages",
                86400,  # 24h TTL
                session.to_json(),
            )
            return session

        return Session.empty(session_key)

    async def save(self, session_key: str, messages: list[dict]):
        """保存会话：同时写 Redis + PG"""
        data = json.dumps(messages)
        pipe = self.redis.pipeline()
        pipe.setex(f"session:{session_key}:messages", 86400, data)
        await pipe.execute()

        # 异步写 PG (不阻塞主流程)
        asyncio.create_task(self._save_to_pg(session_key, data))
```

---

## 十、上下文管理 (Context Manager)

### 10.1 四层防御

```python
# sahara_runtime/context/manager.py

class ContextManager:
    def __init__(self, token_counter: TokenCounter):
        self.counter = token_counter
        self.truncator = InputTruncator()
        self.pruner = HistoryPruner()
        self.compactor = AutoCompactor()

    async def fit(self, messages, system_prompt, model: ModelConfig) -> list[dict]:
        """确保 messages 不超过模型上下文窗口。"""
        budget = model.max_context_tokens - model.max_tokens  # 留给输出
        system_tokens = self.counter.count(system_prompt)
        available = budget - system_tokens

        # Layer 1: 输入截断 (工具结果过长)
        messages = self.truncator.truncate(messages, max_result_chars=8000)

        # Layer 2: 历史剪枝 (移除旧消息)
        total = self.counter.count_messages(messages)
        if total > available:
            messages = self.pruner.prune(messages, target_tokens=available)

        # Layer 3: 自动压缩 (摘要化旧消息)
        total = self.counter.count_messages(messages)
        if total > available:
            messages = await self.compactor.compact(messages, target_tokens=available)

        return messages
```

### 10.2 Token 计数

```python
# sahara_runtime/context/token_counter.py

class TokenCounter:
    def __init__(self):
        self._encoders = {}

    def count(self, text: str, model: str = "cl100k_base") -> int:
        if model not in self._encoders:
            self._encoders[model] = tiktoken.get_encoding(model)
        return len(self._encoders[model].encode(text))

    def count_messages(self, messages: list[dict]) -> int:
        total = 0
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                total += self.count(content)
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        total += self.count(json.dumps(block))
            total += 4  # message overhead
        return total
```

---

## 十一、沙箱管理 (Sandbox Manager)

> 详细设计见 [SANDBOX-DESIGN.md](./SANDBOX-DESIGN.md)（D6），此处仅概述接口。

```python
# sahara_runtime/sandbox/manager.py

class SandboxManager:
    async def acquire(self, session_key: str) -> Sandbox:
        """从容器池分配一个沙箱"""
        container = await self.pool.checkout()
        return Sandbox(container=container, workspace=f"/workspace/{session_key}")

    async def release(self, sandbox: Sandbox):
        """清理并归还容器到池"""
        await sandbox.cleanup()
        await self.pool.checkin(sandbox.container)


class Sandbox:
    enabled: bool = True

    async def exec(self, command: str, timeout: int = 30) -> str:
        """在容器内执行命令"""
        result = await self.container.exec_run(
            cmd=["sh", "-c", command],
            workdir=self.workspace,
            timeout=timeout,
        )
        return result.output.decode()

    async def read_file(self, path: str) -> str:
        """读取容器内/workspace下的文件"""
        ...

    async def write_file(self, path: str, content: str):
        """写入文件到容器内/workspace"""
        ...
```

---

## 十二、并发模型

### 12.1 asyncio 架构

```text
Python 进程 (单线程 asyncio 事件循环)
  │
  ├── gRPC async server                    1 个事件循环线程
  │   ├── SubmitTask handler               并发 RPC 处理
  │   ├── AbortTask handler
  │   └── GetStatus handler
  │
  ├── Agent Task 1 (asyncio.Task)          并发任务
  │   ├── await client.messages.stream()   IO 等待 (释放事件循环)
  │   ├── await emitter.emit()             IO 等待 (Redis XADD)
  │   └── await sandbox.exec()             IO 等待 (Docker exec)
  │
  ├── Agent Task 2 (asyncio.Task)          并发任务
  │   └── ...
  │
  ├── ...                                  最多 16 个并发任务
  │
  └── Agent Task N (asyncio.Task)

关键: 90%+ 时间在 IO 等待 (LLM API)，GIL 不是问题。
     asyncio 在 IO 等待期间自动切换到其他任务。
```

### 12.2 并发上限

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `max_concurrent_tasks` | 16 | 最大并发 Agent 任务 |
| gRPC `maximum_concurrent_rpcs` | 50 | gRPC 层面的并发上限 |
| `max_iterations` | 20 | 单任务最大 LLM 调用轮数 |
| 单任务超时 | 5 分钟 | 超时自动取消 |

### 12.3 uvloop

```python
# sahara_runtime/main.py

import uvloop

def main():
    uvloop.install()  # 替换默认事件循环，性能提升 2-4x
    asyncio.run(start_server())
```

---

## 十三、错误处理与弹性

### 13.1 错误分类

| 错误类型 | 处理策略 | 示例 |
| --- | --- | --- |
| **LLM 暂时不可用** (429/503) | 重试 + 指数退避 (最多 3 次) | Rate Limit、Provider 过载 |
| **LLM 认证失败** (401/403) | Key 轮换 → 熔断该 Key | API Key 过期/无效 |
| **上下文溢出** | Layer 4 紧急压缩 → 重试 | messages 超过 context window |
| **工具执行失败** | 返回错误文本给 LLM (让 LLM 决策) | exec 命令报错 |
| **沙箱不可用** | 发射 RUN_ERROR → 中止任务 | Docker 容器创建失败 |
| **Session 锁冲突** | gRPC 返回 ABORTED | 同一 session 并发请求 |
| **任务被取消** | asyncio.CancelledError → 清理 | 用户中止 / Gateway 断连 |

### 13.2 LLM 调用重试

```python
async def _call_llm_with_retry(client, model, **kwargs):
    """带重试的 LLM 调用"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await _call_llm_streaming(client, model, **kwargs)
        except RateLimitError:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt + random.uniform(0, 1)
            logger.warning(f"rate limited, retry in {wait:.1f}s", extra={
                "attempt": attempt, "model": model,
            })
            await asyncio.sleep(wait)
        except AuthenticationError:
            # Key 失效 → 轮换
            await kwargs["deps"].key_pool.report_error(key, 401)
            raise LLMProviderError("auth failed", retryable=True)
```

---

## 十四、配置管理

```python
# sahara_runtime/config.py

from pydantic_settings import BaseSettings

class RuntimeConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SAHARA_RT_")

    # Worker
    worker_id: str = "rt-1"
    grpc_port: int = 50051
    max_concurrent_tasks: int = 16

    # Redis
    redis_url: str = "redis://localhost:6379"

    # PostgreSQL
    database_url: str = "postgresql://sahara:dev@localhost:5432/sahara"

    # LLM
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    default_model: str = "claude-sonnet-4-20250514"
    max_iterations: int = 20
    max_tokens: int = 8192

    # Sandbox
    sandbox_enabled: bool = True
    sandbox_pool_size: int = 5
    sandbox_image: str = "sahara-sandbox:latest"

    # Event Bus
    event_stream_maxlen: int = 5000

    # Observability
    log_level: str = "INFO"
    metrics_port: int = 9090
```

---

## 十五、可观测性

### 15.1 structlog 日志

```python
# sahara_runtime/main.py

import structlog

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)

logger = structlog.get_logger()

# 使用
logger.info("task_started", run_id=run_id, session_key=session_key, model=model)
logger.info("llm_call_complete", run_id=run_id, iteration=iteration, tokens=usage.total)
logger.warning("tool_execution_failed", tool=tool_name, error=str(e))
```

### 15.2 Prometheus 指标

```python
from prometheus_client import Counter, Histogram, Gauge

# 任务
tasks_active = Gauge("sahara_rt_tasks_active", "Active agent tasks")
tasks_total = Counter("sahara_rt_tasks_total", "Total tasks", ["status"])

# LLM
llm_call_duration = Histogram("sahara_rt_llm_call_seconds", "LLM call duration",
                               ["provider", "model"])
llm_tokens_total = Counter("sahara_rt_llm_tokens_total", "Tokens used",
                            ["provider", "model", "direction"])
llm_errors_total = Counter("sahara_rt_llm_errors_total", "LLM errors",
                            ["provider", "error_type"])

# 工具
tool_execution_duration = Histogram("sahara_rt_tool_seconds", "Tool execution duration",
                                     ["tool"])
tool_errors_total = Counter("sahara_rt_tool_errors_total", "Tool errors", ["tool"])

# 沙箱
sandbox_pool_idle = Gauge("sahara_rt_sandbox_pool_idle", "Idle sandbox containers")
sandbox_pool_in_use = Gauge("sahara_rt_sandbox_pool_in_use", "In-use sandbox containers")

# 事件
events_emitted_total = Counter("sahara_rt_events_emitted_total", "Events emitted",
                                ["type"])
```

---

## 十六、Phase 1 最小实现范围

| 模块 | Phase 1 范围 | 可推迟 |
| --- | --- | --- |
| **gRPC Server** | ★ SubmitTask + AbortTask + GetStatus + Health | ListActiveTasks, Drain |
| **Agent Loop** | ★ 完整 LLM 交互循环 (Anthropic SDK) | OpenAI SDK 支持 |
| **EventEmitter** | ★ Redis Streams XADD (全部 9 种事件) | — |
| **Model Router** | 简化：单 Provider + 单 Key | Key 池, 轮换, 熔断 |
| **Tools** | ★ exec + read + write (3 个核心工具) | edit, web_search, web_fetch |
| **Prompt Builder** | ★ 基础版 (身份 + 工具 + 安全 + 运行时) | 技能, 上下文文件 |
| **Session Store** | ★ Redis 热存储 | PG 冷存储, 历史分页 |
| **Context Manager** | ★ Layer 1 输入截断 + 简单 Layer 2 剪枝 | Layer 3 压缩, Layer 4 溢出 |
| **Sandbox** | ★ Docker 容器池 (预创建 + 分配 + 回收) | gVisor, Firecracker |
| **Config** | ★ 环境变量 + pydantic-settings | 配置中心热更新 |
| **Observability** | ★ structlog JSON + Prometheus 基础指标 | OpenTelemetry 追踪 |

### Phase 1 开发顺序建议

```text
Week 3-4 (与 Gateway 并行):
  1. gRPC Server 空壳 + Health Check       ← 与 Gateway 联调
  2. EventEmitter (Redis XADD)             ← 与 Gateway 事件消费联调
  3. Agent Loop 骨架 (mock LLM)            ← 端到端帧流通

Week 5-6:
  4. Agent Loop 接入真实 Anthropic SDK      ← 核心
  5. 工具系统 (exec + read + write)
  6. Session Store (Redis)

Week 7-8:
  7. Sandbox 容器池
  8. Context Manager (Layer 1-2)
  9. 端到端集成测试 (LLM mock)
```

---

## 附录

### 附录 A. 依赖清单

| 依赖 | 用途 |
| --- | --- |
| `grpcio[async]` | gRPC async server |
| `anthropic` | Anthropic Claude SDK |
| `openai` | OpenAI SDK |
| `redis[asyncio]` | Redis 异步客户端 |
| `asyncpg` | PostgreSQL 异步客户端 |
| `docker` (docker-py) | Docker 容器管理 |
| `tiktoken` | Token 计数 |
| `pydantic-settings` | 配置管理 |
| `structlog` | 结构化日志 |
| `prometheus-client` | Prometheus 指标 |
| `uvloop` | 高性能事件循环 |
| `ulid-py` | ULID 生成 |
| `opentelemetry-*` | 分布式追踪 (Phase 2) |

### 附录 B. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| §3 gRPC Server | P0-9 + P1-6 | Phase 0-1 |
| §4 Agent Loop | P1-7 LLM 交互循环 | Phase 1 |
| §5 EventEmitter | P1-8 事件发射 | Phase 1 |
| §6 Model Router | P2-14 模型降级 | Phase 2 |
| §7 Tools | P1-13 基础工具 | Phase 1 |
| §9 Session Store | P1-11 会话存储 | Phase 1 |
| §10 Context Manager | — (内含在 Agent Loop 中) | Phase 1 |
| §11 Sandbox | P1-12 + P1-15 | Phase 1 |
