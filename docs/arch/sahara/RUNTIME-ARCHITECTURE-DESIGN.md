# Agent Runtime 架构设计

> Python Runtime Worker 的内部模块划分、异步并发模型、Agent Loop 核心设计与各子系统接口定义。
> 本文档是 Python 开发组的实现蓝图，覆盖从 gRPC 接收任务到发射最后一个事件的完整数据路径。
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
   - [5.1 职责与设计目标](#51-职责与设计目标)
   - [5.2 抽象接口 — EventBackend](#52-抽象接口--eventbackend)
   - [5.3 RunEmitter — 面向 Agent Loop 的统一接口](#53-runemitter--面向-agent-loop-的统一接口)
   - [5.4 后端实现 — Redis Streams (Phase 1)](#54-后端实现--redis-streams-phase-1)
   - [5.5 后端实现 — 其他 MQ (Phase 2+ 预留)](#55-后端实现--其他-mq-phase-2-预留)
   - [5.6 后端选择与多路广播](#56-后端选择与多路广播)
   - [5.7 完整事件类型清单](#57-完整事件类型清单)
   - [5.8 弹性设计](#58-弹性设计)
   - [5.9 各后端特性对比](#59-各后端特性对比)
   - [5.10 迁移策略](#510-迁移策略)
6. [模型管理 (Model Router)](#六模型管理-model-router)
7. [工具系统 (Tools)](#七工具系统-tools)
   - [7.1 工具全生命周期](#71-工具全生命周期)
   - [7.2 工具接口](#72-工具接口)
   - [7.3 工具优先级与分层](#73-工具优先级与分层)
   - [7.4 工具扩展机制](#74-工具扩展机制)
   - [7.5 工具定义详解](#75-工具定义详解)
   - [7.6 工具注册与策略过滤](#76-工具注册与策略过滤)
   - [7.7 LLM Schema 注入详解](#77-llm-schema-注入详解)
   - [7.8 工具执行流程详解](#78-工具执行流程详解)
   - [7.9 输出截断策略](#79-输出截断策略)
   - [7.10 完整消息流示例](#710-完整消息流示例)
   - [7.11 安全策略](#711-安全策略)
8. [系统提示词构建](#八系统提示词构建)
   - [8.1 职责与设计原则](#81-职责与设计原则)
   - [8.2 段落组成与排列顺序](#82-段落组成与排列顺序)
   - [8.3 各段落详解](#83-各段落详解)
   - [8.4 完整构建流程](#84-完整构建流程)
   - [8.5 构建流程时序图](#85-构建流程时序图)
   - [8.6 与 Agent Loop 的集成](#86-与-agent-loop-的集成)
   - [8.7 缓存策略与 LLM Prompt Cache 的关系](#87-缓存策略与-llm-prompt-cache-的关系)
   - [8.8 完整输出示例](#88-完整输出示例)
   - [8.9 Phase 规划](#89-phase-规划)
9. [沙箱管理 (Sandbox Manager)](#九沙箱管理-sandbox-manager)
   - [9.1 设计目标](#91-设计目标)
   - [9.2 Sandbox 抽象接口](#92-sandbox-抽象接口)
   - [9.3 SandboxManager 抽象接口](#93-sandboxmanager-抽象接口)
   - [9.4 后端实现概览](#94-后端实现概览)
   - [9.5 后端选择工厂](#95-后端选择工厂)
   - [9.6 各后端特性对比](#96-各后端特性对比)
   - [9.7 迁移策略](#97-迁移策略)
   - [9.8 包结构](#98-包结构)
10. [Skills 管理](#十skills-管理)
11. [上下文管理 (Context Manager)](#十一上下文管理-context-manager)
12. [会话管理 (Session Store)](#十二会话管理-session-store)
13. [Dependencies 注入与启动流程](#十三dependencies-注入与启动流程)
14. [并发模型](#十四并发模型)
15. [错误处理与弹性](#十五错误处理与弹性)
16. [配置管理](#十六配置管理)
17. [可观测性](#十七可观测性)
18. [Phase 1 最小实现范围](#十八phase-1-最小实现范围)

---

## 一、模块全景

### 1.1 八大子系统模块映射

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

### 1.2 子系统职责与定位

| # | 子系统 | 核心职责 | 技术选型 | Python 模块 |
| --- | --- | --- | --- | --- |
| ① | 入口与调度 | 接收 Gateway gRPC 请求 | grpcio async + asyncio Semaphore | `server.py` |
| ② | 模型与认证 | 模型选择、Key 轮换、降级 | Redis 集中配置 + Key 池 | `models/` |
| ③ | 运行环境 | 沙箱容器分配与回收 | 同机 Docker 池 + 集中存储 | `sandbox/` |
| ④ | 系统提示词 | 多段落动态拼接 | Redis/PG 数据源 | `prompt/` |
| ⑤ | 工具系统 | 工具创建、策略过滤、执行 | 沙箱隔离 + 安全确认 | `tools/` |
| ⑥ | Agent Loop | **核心**: LLM 流式交互 + 工具循环 | LLM SDK 直调 + 自建循环 | `agent_loop.py` |
| ⑦ | 事件与流式 | 实时发布执行事件 | Redis Streams XADD | `events/` |
| ⑧ | 上下文管理 | 四层防御确保不超窗口 | tiktoken + LLM 摘要 | `context/` |

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
│   ├── sandbox/                    # 沙箱管理 (可插拔后端)
│   │   ├── __init__.py
│   │   ├── base.py                 # Sandbox(ABC), ExecResult, 异常体系
│   │   ├── manager.py              # SandboxManager(ABC), SandboxOptions
│   │   ├── factory.py              # create_sandbox_manager()
│   │   ├── backends/
│   │   │   ├── docker.py           # Docker 容器池 (Phase 1)
│   │   │   ├── gvisor.py           # gVisor (Phase 2, 继承 Docker)
│   │   │   ├── firecracker.py      # Firecracker microVM (Phase 3)
│   │   │   ├── remote.py           # 远程沙箱 (Phase 3+)
│   │   │   └── noop.py             # 开发/测试用
│   │   └── pool.py                 # 池化 (预创建/分配/回收)
│   │
│   ├── skills/                     # 技能管理
│   │   ├── __init__.py
│   │   ├── types.py                # SkillEntry, SkillMetadata 等定义
│   │   ├── loader.py               # 技能加载 (多目录, 优先级合并)
│   │   ├── filter.py               # 7 层过滤 (OS/二进制/环境变量)
│   │   ├── prompt.py               # 技能提示词生成 (<available_skills>)
│   │   └── sync.py                 # 技能同步到沙箱
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
│   │   ├── emitter.py              # EventEmitterFactory + RunEmitter
│   │   ├── backend.py             # EventBackend 抽象接口
│   │   ├── backends/
│   │   │   ├── redis_streams.py   # Redis Streams 实现 (Phase 1)
│   │   │   ├── kafka.py           # Kafka 实现 (Phase 2+)
│   │   │   ├── nats_jetstream.py  # NATS JetStream 实现 (Phase 2+)
│   │   │   ├── composite.py       # 多路广播 (迁移双写)
│   │   │   └── in_memory.py       # 测试/开发用
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

    async def SendInput(self, request, context):
        """人机交互回源 — 接收用户输入并唤醒挂起的 Agent 任务。
        Gateway Dispatcher 通过强制亲和确保请求路由到持有该任务的 Worker。
        """
        handle = self.active_tasks.get(request.run_id)
        if not handle:
            await context.abort(grpc.StatusCode.NOT_FOUND, "task not found or completed")
            return

        if not handle.input_channel:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                                "task is not waiting for input")
            return

        # 通过 asyncio.Queue 将用户输入传递给挂起的 Agent Loop
        user_input = UserInput(
            action=request.action,   # "approve" / "reject" / "input"
            text=request.input,
            task_id=request.task_id,
        )
        try:
            handle.input_channel.put_nowait(user_input)
        except asyncio.QueueFull:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,
                                "input channel full")
            return

        return agent_pb2.SendInputResponse(delivered=True)

    async def GetTaskStatus(self, request, context):
        """查询单个任务的执行状态。"""
        handle = self.active_tasks.get(request.run_id)
        if not handle:
            # 任务可能已结束, 查 Redis 历史
            status_info = await self.deps.task_status.get(request.run_id)
            if not status_info:
                await context.abort(grpc.StatusCode.NOT_FOUND, "task not found")
                return
            return status_info.to_proto()

        return agent_pb2.GetTaskStatusResponse(
            run_id=handle.run_id,
            task_id=handle.task_id,
            state=commonv1.TASK_STATE_RUNNING,
            started_at_ms=int(handle.started_at * 1000),
            session_key=handle.session_key,
            waiting_for_input=handle.input_channel is not None
                              and handle.waiting_for_input,
        )

    async def ListActiveTasks(self, request, context):
        """列出当前 Worker 上所有活跃任务 (用于排空监控和故障恢复)。"""
        tasks = []
        for handle in self.active_tasks.values():
            tasks.append(agent_pb2.ActiveTask(
                run_id=handle.run_id,
                task_id=handle.task_id,
                session_key=handle.session_key,
                started_at_ms=int(handle.started_at * 1000),
                waiting_for_input=handle.waiting_for_input,
            ))
        return agent_pb2.ListActiveTasksResponse(tasks=tasks)
```

### 3.3 WorkerService — Worker 生命周期

```python
# sahara_runtime/server.py (续)

class WorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    def __init__(self, deps: "Dependencies"):
        self.worker = deps.worker
        self.agent_servicer = deps.agent_servicer  # 引用 AgentServicer 获取活跃任务
        self.deps = deps

    async def GetStatus(self, request, context):
        """Gateway 每 5 秒轮询一次, 用于调度决策和健康检查。
        必须极快返回 (< 3ms), 所有数据来自本地内存。
        """
        w = self.worker
        sandbox = self.deps.sandbox_manager

        return worker_pb2.GetStatusResponse(
            worker_id=w.id,
            # 容量信息
            active_tasks=w.active_tasks,
            max_tasks=w.max_tasks,
            queued_tasks=0,  # 当前无本地队列, 直接拒绝
            # 资源信息 (由 ResourceMonitor 每秒更新)
            cpu_usage_percent=w.resource_monitor.cpu_percent,
            memory_usage_percent=w.resource_monitor.memory_percent,
            memory_used_bytes=w.resource_monitor.memory_used_bytes,
            # 沙箱池
            sandbox_pool_idle=sandbox.pool.idle_count,
            sandbox_pool_in_use=sandbox.pool.in_use_count,
            sandbox_pool_total=sandbox.pool.total_count,
            # 状态
            state=w.state.to_proto(),
            uptime_seconds=int(time.time() - w.started_at),
            version=w.version,
        )

    async def Drain(self, request, context):
        """排空 Worker: 停止接受新任务, 等待活跃任务完成。
        K8s preStop hook 或运维手动触发。
        """
        timeout = request.timeout_seconds or 60
        remaining = self.worker.active_tasks

        # 1. 标记排空状态 → SubmitTask 开始拒绝新任务
        self.worker.state = WorkerState.DRAINING
        logger.info("drain_started", remaining_tasks=remaining, timeout=timeout)

        # 2. 异步等待所有任务完成 (不阻塞 gRPC 返回)
        asyncio.create_task(self._drain_wait(timeout))

        return worker_pb2.DrainResponse(
            remaining_tasks=remaining,
            estimated_complete_at_ms=int((time.time() + timeout) * 1000),
        )

    async def _drain_wait(self, timeout: int):
        """等待所有活跃任务完成, 超时后强制取消。"""
        deadline = time.time() + timeout
        while self.worker.active_tasks > 0 and time.time() < deadline:
            await asyncio.sleep(1)
            logger.info("drain_progress",
                        remaining=self.worker.active_tasks,
                        timeout_remaining=int(deadline - time.time()))

        if self.worker.active_tasks > 0:
            # 超时: 强制取消剩余任务
            logger.warning("drain_timeout_force_cancel",
                           remaining=self.worker.active_tasks)
            for handle in list(self.agent_servicer.active_tasks.values()):
                handle.task.cancel()
            await asyncio.sleep(1)  # 等待取消完成

        logger.info("drain_complete")
        # 发送 SIGTERM 给自身, 触发优雅退出
        os.kill(os.getpid(), signal.SIGTERM)
```

### 3.4 TaskHandle — 任务句柄

```python
# sahara_runtime/server.py

@dataclass
class UserInput:
    action: str      # "approve" / "reject" / "input"
    text: str        # 用户输入内容
    task_id: str

@dataclass
class TaskHandle:
    """一个活跃任务的完整句柄, 在 Agent 生命周期内持有。"""
    task: asyncio.Task           # 可取消的 asyncio 任务
    task_id: str
    run_id: str
    session_key: str
    started_at: float

    # ── 人机交互 ──
    input_channel: asyncio.Queue[UserInput] | None = None
    # 当 Agent Loop 挂起等待输入时设为 True
    waiting_for_input: bool = False
```

### 3.5 Worker 状态与资源监控

```python
# sahara_runtime/worker.py

import psutil
import os
import asyncio
from enum import Enum

class WorkerState(Enum):
    STARTING = "starting"
    READY = "ready"
    DRAINING = "draining"

    def to_proto(self):
        return {
            "starting": worker_pb2.WORKER_STATE_STARTING,
            "ready": worker_pb2.WORKER_STATE_READY,
            "draining": worker_pb2.WORKER_STATE_DRAINING,
        }[self.value]

class ResourceMonitor:
    """每秒采集一次系统资源, 供 GetStatus 零 IO 读取。"""

    def __init__(self):
        self.cpu_percent: float = 0.0
        self.memory_percent: float = 0.0
        self.memory_used_bytes: int = 0
        self._process = psutil.Process(os.getpid())

    async def run(self):
        """后台协程, 每秒更新资源指标。"""
        while True:
            self.cpu_percent = self._process.cpu_percent()
            mem = self._process.memory_info()
            self.memory_used_bytes = mem.rss
            total = psutil.virtual_memory().total
            self.memory_percent = (mem.rss / total) * 100 if total else 0
            await asyncio.sleep(1)


class Worker:
    def __init__(self, config: RuntimeConfig):
        self.id = config.worker_id
        self.state = WorkerState.STARTING
        self.max_tasks = config.max_concurrent_tasks  # 默认 16
        self._active_count = 0
        self.started_at = time.time()
        self.version = config.version or "dev"
        self.resource_monitor = ResourceMonitor()

    def try_acquire(self) -> bool:
        """尝试获取一个任务槽位 (非阻塞)"""
        if self.state == WorkerState.DRAINING:
            return False
        if self._active_count >= self.max_tasks:
            return False
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
run_agent_loop(task_handle)
  │
  ├── 1. 加载 Session 历史 (Redis → messages[])
  ├── 2. 解析模型 + 获取 API Key (Session 级亲和 §6.2)
  ├── 3. 分配沙箱容器
  ├── 4. 加载 + 过滤技能, 同步到沙箱 (§10)
  ├── 5. 创建工具集 + 策略过滤 (§7.6)
  ├── 6. 构建系统提示词 (§8.4: 8 段落组装, tools+skills 都已就绪)
  ├── 7. 注入用户消息到 messages[]
  ├── 8. 发射 RUN_START 事件 + 创建 ToolExecutor (§7.8)
  │
  ├── 9. ★ 交互循环 (max N 轮, 整体超时 5min):
  │   │
  │   ├── 9a. 上下文管理 (检查 token → 截断/剪枝/压缩)
  │   ├── 9b. 调用 LLM (流式, 含重试)
  │   │       ├── 每个 text_delta → 发射 DELTA 事件
  │   │       ├── thinking_delta → 发射 THINKING 事件
  │   │       └── 完整响应 → messages.append(assistant)
  │   ├── 9c. stop_reason == "end_turn" → 跳出循环
  │   ├── 9d. stop_reason == "tool_use" → ToolExecutor 执行:
  │   │       ├── 查找工具 → 安全检查 → 是否需要确认?
  │   │       ├── 需要确认 → 人机交互挂起 (§4.4):
  │   │       │       ├── 发射 INPUT_REQUIRED 事件
  │   │       │       │   (Gateway 收到后升级亲和为 Sticky)
  │   │       │       ├── await input_channel.get() (挂起等待)
  │   │       │       ├── 收到 approve → 执行工具
  │   │       │       ├── 收到 reject → 生成拒绝结果给 LLM
  │   │       │       └── 超时 120s → 自动拒绝
  │   │       ├── 不需要确认 → 直接执行:
  │   │       │       ├── 发射 TOOL_START 事件
  │   │       │       ├── 执行工具 (沙箱 exec / 文件 read/write)
  │   │       │       ├── 输出截断 (§7.9) + 发射 TOOL_RESULT 事件
  │   │       │       └── 封装 tool_result (§7.10)
  │   │       └── messages.append(tool_result) → 继续循环
  │   └── 9e. 异常处理 (API 错误 → 重试/降级/中止)
  │
  ├── 10. 发射 RUN_COMPLETE 事件
  ├── 11. 持久化 Session (messages[] → Redis + PG)
  └── 12. 释放沙箱容器
```

### 4.3 核心实现

```python
# sahara_runtime/agent_loop.py

TASK_TIMEOUT = 300  # 5 分钟, 可通过配置覆盖
INPUT_WAIT_TIMEOUT = 120  # 等待用户输入最多 120 秒

async def run_agent_loop(
    task_id: str,
    run_id: str,
    session_key: str,
    user_message: str,
    agent_id: str,
    metadata: dict[str, str],
    deps: "Dependencies",
    task_handle: "TaskHandle",   # ← 持有 input_channel, 支持人机交互
) -> None:
    """Agent 核心交互循环。每个 SubmitTask 调用此函数。
    整体受 TASK_TIMEOUT 保护, 超时自动取消。
    """

    # ── 1. 加载会话历史 ──
    session = await deps.session_store.load(session_key)
    messages = session.messages  # list[dict]

    # ── 2. 模型解析 (Session 级亲和: 复用同一 Model + Key → 命中 LLM 缓存) ──
    model_config = await deps.model_router.resolve_for_session(agent_id, session_key)
    client = deps.model_router.get_client_for_session(session_key)

    # ── 3. 沙箱分配 ──
    sandbox = await deps.sandbox_manager.acquire(session_key)

    # ── 4. 加载 + 过滤技能 ──
    all_skills = await deps.skill_loader.load_all()
    filtered_skills = deps.skill_filter.filter(all_skills, deps.config)

    # ── 4b. 同步技能到沙箱 (LLM 的 read 工具需要读到 SKILL.md) ──
    if sandbox and sandbox.enabled and filtered_skills:
        await sync_skills_to_sandbox(filtered_skills, sandbox)

    # ── 5. 工具集 ──
    tools = await deps.tool_registry.create_tools(
        agent_id=agent_id,
        sandbox=sandbox,
        session_key=session_key,
    )
    tool_definitions = [t.to_llm_schema() for t in tools]  # → 传入 LLM API

    # ── 6. 构建系统提示词 (需要 tools + skills 都准备好, 详见 §8.4) ──
    system_prompt = await deps.prompt_builder.build(
        agent_config=agent_config,
        tools=tools,                  # → 段落 3: 工具指南
        skill_entries=filtered_skills, # → 段落 4: 技能列表
        sandbox=sandbox,              # → 段落 5: 沙箱环境
        session_meta=session_meta,
        user_info=user_info,
        user_prefs=user_prefs,
    )

    # ── 7. 注入用户消息 ──
    messages.append({"role": "user", "content": user_message})

    # ── 8. RUN_START + 工具执行器 ──
    emitter = deps.emitter.for_run(run_id, session_key, task_id)

    # 创建 ToolExecutor (封装安全检查、沙箱调用、事件发射, 详见 §7.8)
    tool_executor = ToolExecutor(
        tools=tools,
        sandbox=sandbox,
        emitter=emitter,
        policy=await deps.tool_registry.load_policy(agent_id),
    )
    await emitter.emit_run_start(agent_id=agent_id, model=model_config.model_id)

    # ── 9. 交互循环 (整体受 TASK_TIMEOUT 保护) ──
    try:
        async with asyncio.timeout(TASK_TIMEOUT):
            for iteration in range(model_config.max_iterations):
                # 9a. 上下文管理
                messages = await deps.context_manager.fit(
                    messages=messages,
                    system_prompt=system_prompt,
                    model=model_config,
                )

                # 9b. 调用 LLM (流式, 含自动重试)
                response = await _call_llm_with_retry(
                    client=client,
                    model=model_config.model_id,
                    system=system_prompt,
                    messages=messages,
                    tools=tool_definitions,
                    emitter=emitter,
                    iteration=iteration,
                    key_pool=deps.model_router.key_pool,
                )

                messages.append({"role": "assistant", "content": response.content})

                # 9c. 自然结束
                if response.stop_reason != "tool_use":
                    break

                # 9d. 工具执行 (含安全检查、确认、沙箱调用, 详见 §7.8)
                tool_results = await tool_executor.execute_tool_calls(
                    response=response,
                    task_handle=task_handle,
                )
                messages.append({"role": "user", "content": tool_results})

        # ── 10. RUN_COMPLETE ──
        final_text = _extract_final_text(messages)
        await emitter.emit_run_complete(
            final_text=final_text,
            iterations=iteration + 1,
        )

    except asyncio.TimeoutError:
        await emitter.emit_run_error("task timeout exceeded", retryable=False)
        raise

    except asyncio.CancelledError:
        raise  # 由上层 _execute 处理

    except LLMProviderError as e:
        # 模型降级尝试
        if model_config.fallback and e.retryable:
            model_config = await deps.model_router.fallback(model_config)
            # TODO: 重新执行循环 (Phase 2 实现完整降级重试)
        await emitter.emit_run_error(str(e), retryable=e.retryable)
        raise

    finally:
        # ── 11. 持久化 ──
        await deps.session_store.save(session_key, messages)
        # ── 12. 释放沙箱 ──
        await deps.sandbox_manager.release(sandbox)
        # ── 释放模型绑定 (注意: 不在此处释放, session 下次请求还需复用)
        # deps.model_router.release_session(session_key)
        # ↑ 仅在 session 被删除/过期时由外部清理


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


async def _execute_tools(response, tool_executor, task_handle):
    """执行所有工具调用，返回 tool_result 列表。
    完整实现详见 §7.8 ToolExecutor.execute_tool_calls()。
    """
    return await tool_executor.execute_tool_calls(response, task_handle)
```

### 4.4 人机交互 — 工具确认与用户输入

Agent 执行过程中，某些高危工具（如 `exec rm -rf`、`write` 覆盖关键文件）需要用户确认后才能执行。整个人机交互链路涉及 Runtime、Event Bus、Gateway、Client 四方协作：

```text
人机交互数据流:

  Runtime (Agent Loop)          Event Bus          Gateway           Client
       │                           │                  │                │
       │  工具需要确认              │                  │                │
       │  emit INPUT_REQUIRED ────▶│                  │                │
       │                           │  Broadcaster 消费 │                │
       │                           │ ────────────────▶│                │
       │                           │                  │  event push    │
       │                           │                  │ ──────────────▶│
       │                           │                  │                │ 用户看到确认弹窗
       │  await input_channel      │                  │                │ 用户点击 [允许]
       │  (挂起, 释放事件循环)     │                  │                │
       │                           │                  │  agent.input   │
       │                           │                  │ ◀──────────────│
       │                           │   Dispatcher 强制亲和路由         │
       │                           │  (Sticky → 同一 Worker)          │
       │  gRPC SendInput ◀─────────┼──────────────────│                │
       │                           │                  │                │
       │  input_channel.put()      │                  │                │
       │  Agent 被唤醒             │                  │                │
       │  继续执行工具             │                  │                │
       └───────────────────────────┘──────────────────┘────────────────┘
```

**核心机制：`asyncio.Queue` 作为暂停/唤醒桥梁**

```python
# sahara_runtime/agent_loop.py — 人机交互工具执行

async def _execute_tools_with_confirmation(
    response, tools, sandbox, emitter, task_handle, deps,
):
    """执行工具调用, 对需要确认的工具走人机交互流程。
    注: 此函数的逻辑已整合进 ToolExecutor (§7.8), 此处保留
    详细注释以说明人机交互的 asyncio.Queue 桥接机制。
    """
    tool_results = []
    tool_map = {t.name: t for t in tools}
    policy = deps.tool_policy

    for block in response.content:
        if block.type != "tool_use":
            continue

        tool = tool_map.get(block.name)
        if not tool:
            tool_results.append(_tool_error(block, f"Unknown tool: {block.name}"))
            continue

        # ── 检查是否需要用户确认 ──
        needs_confirm = policy.requires_confirmation(
            tool_name=block.name,
            args=block.input,
        )

        if needs_confirm:
            # 人机交互: 挂起等待用户输入
            result = await _wait_for_user_confirmation(
                block=block,
                tool=tool,
                sandbox=sandbox,
                emitter=emitter,
                task_handle=task_handle,
            )
        else:
            # 直接执行
            result = await _execute_single_tool(
                block=block, tool=tool, sandbox=sandbox, emitter=emitter,
            )

        tool_results.append(result)

    return tool_results


async def _wait_for_user_confirmation(block, tool, sandbox, emitter, task_handle):
    """挂起 Agent 等待用户确认, 通过 asyncio.Queue 桥接 gRPC SendInput。

    关键设计:
    1. 初始化 input_channel (asyncio.Queue, maxsize=1)
    2. 发射 INPUT_REQUIRED 事件 → Gateway 升级亲和为 Sticky
    3. await queue.get() → Agent 协程挂起, 释放事件循环
    4. 用户通过 WS agent.input → Gateway gRPC SendInput → queue.put()
    5. Agent 被唤醒, 根据 action 决定执行或拒绝
    """

    # 1. 创建输入通道
    if not task_handle.input_channel:
        task_handle.input_channel = asyncio.Queue(maxsize=1)
    task_handle.waiting_for_input = True

    # 2. 发射 INPUT_REQUIRED 事件
    await emitter.emit_input_required(
        tool_call_id=block.id,
        tool_name=block.name,
        input_preview=json.dumps(block.input, ensure_ascii=False)[:500],
        prompt=f"Agent 请求执行 {block.name}，是否允许？",
        input_type="tool_confirm",  # "tool_confirm" / "text_input"
    )

    # 3. 等待用户输入 (超时保护)
    try:
        user_input: UserInput = await asyncio.wait_for(
            task_handle.input_channel.get(),
            timeout=INPUT_WAIT_TIMEOUT,  # 120s
        )
    except asyncio.TimeoutError:
        task_handle.waiting_for_input = False
        # 超时 → 自动拒绝, 告知 LLM
        return {
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": "用户未在规定时间内响应, 工具执行已被自动拒绝。",
            "is_error": True,
        }

    task_handle.waiting_for_input = False

    # 4. 根据用户决策处理
    if user_input.action == "approve":
        # 用户批准 → 执行工具
        return await _execute_single_tool(
            block=block, tool=tool, sandbox=sandbox, emitter=emitter,
        )
    elif user_input.action == "reject":
        # 用户拒绝 → 返回拒绝信息给 LLM
        reason = user_input.text or "用户拒绝了此操作"
        return {
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": f"工具执行被用户拒绝: {reason}",
            "is_error": True,
        }
    elif user_input.action == "input":
        # 用户提供了额外输入 → 注入到工具参数
        block.input["user_provided_input"] = user_input.text
        return await _execute_single_tool(
            block=block, tool=tool, sandbox=sandbox, emitter=emitter,
        )


async def _execute_single_tool(block, tool, sandbox, emitter):
    """执行单个工具, 发射事件, 返回结果。"""
    await emitter.emit_tool_start(
        tool_call_id=block.id,
        tool_name=block.name,
        input_json=json.dumps(block.input),
    )

    start = time.monotonic()
    try:
        result = await tool.execute(block.input, sandbox=sandbox)
        success = True
    except ToolExecutionError as e:
        result = str(e)
        success = False

    duration_ms = int((time.monotonic() - start) * 1000)

    await emitter.emit_tool_result(
        tool_call_id=block.id,
        tool_name=block.name,
        success=success,
        output=_truncate(str(result), max_chars=10_000),
        duration_ms=duration_ms,
    )

    return {
        "type": "tool_result",
        "tool_use_id": block.id,
        "content": str(result),
    }
```

**工具确认策略 (ToolPolicy)**：

```python
# sahara_runtime/tools/policy.py

class ToolPolicy:
    """决定哪些工具调用需要用户确认。"""

    # 默认需要确认的工具和参数模式
    DANGEROUS_PATTERNS = {
        "exec": [
            r"rm\s+-rf",           # 危险删除
            r"sudo\s+",           # 提权操作
            r"chmod\s+777",       # 权限放开
            r"curl\s+.*\|\s*sh",  # 远程执行
        ],
        "write": [
            r"\.(env|key|pem|cert)$",  # 敏感文件
        ],
    }

    def requires_confirmation(self, tool_name: str, args: dict) -> bool:
        patterns = self.DANGEROUS_PATTERNS.get(tool_name, [])
        if not patterns:
            return False

        # 检查参数是否匹配危险模式
        text = json.dumps(args)
        return any(re.search(p, text) for p in patterns)
```

### 4.5 并行工具执行

当 LLM 在一次响应中返回多个 `tool_use` 块时，支持并行执行以减少总延迟：

```python
async def _execute_tools_parallel(blocks, tools, sandbox, emitter):
    """并行执行不需要确认的多个工具调用。"""
    tool_map = {t.name: t for t in tools}
    tasks = []

    for block in blocks:
        if block.type != "tool_use":
            continue
        tool = tool_map.get(block.name)
        if tool:
            tasks.append(_execute_single_tool(block, tool, sandbox, emitter))

    # asyncio.gather 并行执行, return_exceptions=True 避免单个失败影响其他
    results = await asyncio.gather(*tasks, return_exceptions=True)

    tool_results = []
    for result in results:
        if isinstance(result, Exception):
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": "unknown",
                "content": f"Tool execution error: {result}",
                "is_error": True,
            })
        else:
            tool_results.append(result)

    return tool_results
```

> **注意**：并行执行仅适用于不需要确认的工具。需要确认的工具仍按顺序处理（因为用户需要逐个审批）。沙箱内的文件操作可能存在顺序依赖，Phase 1 先串行执行，Phase 2 根据工具类型判断是否可并行。

---

## 五、事件发射器 (EventEmitter)

### 5.1 职责与设计目标

将 Agent 执行过程中的每个 delta/tool/lifecycle 事件序列化后发布到消息通道，供 Gateway 消费并推送给客户端。

**核心设计目标：可插拔后端。** EventEmitter 的上层接口（`RunEmitter`）对 Agent Loop 完全屏蔽底层传输细节。未来从 Redis Streams 迁移到 Kafka / NATS / Pulsar 等 MQ 时，**只需替换 `EventBackend` 实现，Agent Loop 代码零修改。**

```text
可插拔架构:

  Agent Loop / ToolExecutor / ...
       │
       │  调用 RunEmitter.emit_delta(), emit_tool_start(), ...
       │  (不感知底层传输)
       ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  RunEmitter (事件构造 + seq 管理 + 弹性策略)                │
  │                                                             │
  │  职责:                                                      │
  │  - 构造 AgentEvent (Protobuf)                               │
  │  - 维护 seq 自增                                            │
  │  - 按事件分类执行弹性策略 (重试/丢弃)                      │
  │  - 委托给 EventBackend 实际发送                            │
  └──────────────────────────┬──────────────────────────────────┘
                             │
                             │  backend.publish(topic, data)
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  EventBackend (抽象接口)                                    │
  │                                                             │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
  │  │ Redis Streams │  │ Kafka        │  │ NATS         │     │
  │  │ (Phase 1)     │  │ (Phase 2+)   │  │ JetStream    │     │
  │  │               │  │              │  │ (Phase 2+)   │     │
  │  │ XADD          │  │ produce()    │  │ publish()    │     │
  │  └──────────────┘  └──────────────┘  └──────────────┘     │
  │                                                             │
  │  ┌──────────────┐  ┌──────────────┐                       │
  │  │ InMemory     │  │ Composite    │                       │
  │  │ (测试/开发)   │  │ (多路广播)    │                       │
  │  └──────────────┘  └──────────────┘                       │
  └─────────────────────────────────────────────────────────────┘
```

### 5.2 抽象接口 — EventBackend

```python
# sahara_runtime/events/backend.py

class EventBackend(ABC):
    """事件后端抽象接口。所有 MQ 实现必须实现此接口。"""

    @abstractmethod
    async def publish(self, topic: str, data: bytes) -> None:
        """发布一条序列化后的事件到指定 topic。

        Args:
            topic: 事件路由标识 (如 "events:{session_key}")
            data: Protobuf 序列化后的 AgentEvent 字节流

        Raises:
            EventPublishError: 发布失败 (网络/超时/容量)
        """
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """检查后端连接是否健康。"""
        ...

    @abstractmethod
    async def close(self) -> None:
        """优雅关闭连接。"""
        ...

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """后端名称, 用于日志和指标标签。如 "redis_streams", "kafka", "nats"。"""
        ...


class EventPublishError(Exception):
    """事件发布失败。"""
    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable
```

### 5.3 RunEmitter — 面向 Agent Loop 的统一接口

`RunEmitter` 是 Agent Loop 唯一直接交互的类。它负责事件构造和 seq 管理，**完全不关心底层是 Redis 还是 Kafka**。

```python
# sahara_runtime/events/emitter.py

class EventEmitterFactory:
    """事件发射器工厂。根据配置创建不同后端的发射器。"""

    def __init__(self, backend: EventBackend, worker_id: str):
        self._backend = backend
        self._worker_id = worker_id

    def for_run(self, run_id: str, session_key: str, task_id: str) -> "RunEmitter":
        return RunEmitter(
            backend=self._backend,
            worker_id=self._worker_id,
            run_id=run_id,
            session_key=session_key,
            task_id=task_id,
        )


class RunEmitter:
    """单次执行的事件发射器。

    职责:
    - 构造 AgentEvent Protobuf 消息
    - 维护 seq 自增 (客户端用于检测空洞和排序)
    - 计算 topic (基于 session_key)
    - 委托给 EventBackend.publish() 实际发送
    """

    def __init__(self, backend: EventBackend, worker_id: str,
                 run_id: str, session_key: str, task_id: str):
        self._backend = backend
        self._worker_id = worker_id
        self._run_id = run_id
        self._session_key = session_key
        self._task_id = task_id
        self._seq = 0
        self._trace_id = get_current_trace_id()
        self._topic = f"events:{session_key}"  # topic 格式独立于后端

    async def _emit(self, event_type: EventType, **payload_kwargs):
        self._seq += 1

        # 1. 构造 Protobuf 事件
        event = AgentEvent(
            event_id=ulid.new().str,
            run_id=self._run_id,
            session_key=self._session_key,
            task_id=self._task_id,
            worker_id=self._worker_id,
            type=event_type,
            timestamp_ms=int(time.time() * 1000),
            seq=self._seq,
            trace_id=self._trace_id,
        )
        set_payload(event, event_type, payload_kwargs)

        # 2. Protobuf 序列化
        data = event.SerializeToString()

        # 3. 委托给后端发送 (不关心是 Redis/Kafka/NATS)
        await self._backend.publish(self._topic, data)

    # ── 便捷方法 (Agent Loop 直接调用) ──

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

    # ── 人机交互事件 ──

    async def emit_input_required(
        self, tool_call_id: str, tool_name: str,
        input_preview: str, prompt: str, input_type: str,
    ):
        await self._emit(EventType.EVENT_TYPE_INPUT_REQUIRED,
                         tool_call_id=tool_call_id, tool_name=tool_name,
                         input_preview=input_preview, prompt=prompt,
                         input_type=input_type)

    async def emit_tool_confirm_required(
        self, tool_call_id: str, tool_name: str,
        input_json: str, reason: str,
    ):
        await self._emit(EventType.EVENT_TYPE_TOOL_CONFIRM_REQUIRED,
                         tool_call_id=tool_call_id, tool_name=tool_name,
                         input_json=input_json, reason=reason)
```

### 5.4 后端实现 — Redis Streams (Phase 1)

```python
# sahara_runtime/events/backends/redis_streams.py

class RedisStreamsBackend(EventBackend):
    """基于 Redis Streams 的事件后端。Phase 1 默认实现。

    特性:
    - XADD 写入, Gateway 用 XREADGROUP 消费
    - maxlen 自动裁剪旧事件
    - 天然支持消费者组 (多 Gateway 实例消费同一 stream)
    """

    def __init__(self, redis: "Redis", maxlen: int = 5000):
        self._redis = redis
        self._maxlen = maxlen

    async def publish(self, topic: str, data: bytes) -> None:
        try:
            await self._redis.xadd(
                topic,
                {"event": data},
                maxlen=self._maxlen,
            )
        except RedisError as e:
            raise EventPublishError(str(e), retryable=True) from e

    async def health_check(self) -> bool:
        try:
            await self._redis.ping()
            return True
        except RedisError:
            return False

    async def close(self) -> None:
        await self._redis.close()

    @property
    def backend_name(self) -> str:
        return "redis_streams"
```

### 5.5 后端实现 — 其他 MQ (Phase 2+ 预留)

```python
# sahara_runtime/events/backends/kafka.py

class KafkaBackend(EventBackend):
    """基于 Kafka 的事件后端。

    适用场景: 事件量极大 (>10K msg/s), 需要持久化回放, 多消费者组。
    topic 映射: "events:{session_key}" → Kafka topic "sahara-events",
               session_key 作为 partition key 保证同 session 有序。
    """

    def __init__(self, bootstrap_servers: str, topic_prefix: str = "sahara-events"):
        self._producer: AIOKafkaProducer | None = None
        self._bootstrap_servers = bootstrap_servers
        self._topic_prefix = topic_prefix

    async def publish(self, topic: str, data: bytes) -> None:
        # topic "events:sess_abc123" → Kafka topic "sahara-events"
        # partition key = "sess_abc123" → 同 session 同分区 → 保序
        session_key = topic.split(":", 1)[1] if ":" in topic else topic
        try:
            await self._producer.send_and_wait(
                self._topic_prefix,
                value=data,
                key=session_key.encode(),
            )
        except KafkaError as e:
            raise EventPublishError(str(e), retryable=True) from e

    async def health_check(self) -> bool:
        return self._producer is not None and self._producer._sender.sender_task is not None

    async def close(self) -> None:
        if self._producer:
            await self._producer.stop()

    @property
    def backend_name(self) -> str:
        return "kafka"


# sahara_runtime/events/backends/nats_jetstream.py

class NATSJetStreamBackend(EventBackend):
    """基于 NATS JetStream 的事件后端。

    适用场景: 低延迟、轻量级部署、云原生。
    subject 映射: "events:{session_key}" → NATS subject "sahara.events.{session_key}"
    """

    def __init__(self, nats_url: str, stream_name: str = "SAHARA_EVENTS"):
        self._nc: nats.NATS | None = None
        self._js: nats.JetStreamContext | None = None
        self._nats_url = nats_url
        self._stream_name = stream_name

    async def publish(self, topic: str, data: bytes) -> None:
        subject = topic.replace(":", ".")   # "events:sess_abc" → "events.sess_abc"
        nats_subject = f"sahara.{subject}"
        try:
            await self._js.publish(nats_subject, data)
        except Exception as e:
            raise EventPublishError(str(e), retryable=True) from e

    async def health_check(self) -> bool:
        return self._nc is not None and self._nc.is_connected

    async def close(self) -> None:
        if self._nc:
            await self._nc.close()

    @property
    def backend_name(self) -> str:
        return "nats_jetstream"


# sahara_runtime/events/backends/in_memory.py

class InMemoryBackend(EventBackend):
    """内存后端, 用于单元测试和开发环境。

    事件存储在内存 dict 中, 可直接读取断言。
    """

    def __init__(self):
        self.events: dict[str, list[bytes]] = {}  # topic → [event_data]

    async def publish(self, topic: str, data: bytes) -> None:
        self.events.setdefault(topic, []).append(data)

    async def health_check(self) -> bool:
        return True

    async def close(self) -> None:
        self.events.clear()

    @property
    def backend_name(self) -> str:
        return "in_memory"

    # ── 测试辅助方法 ──

    def get_events(self, topic: str) -> list["AgentEvent"]:
        """反序列化指定 topic 的所有事件, 用于断言。"""
        from sahara_runtime.proto import AgentEvent
        result = []
        for data in self.events.get(topic, []):
            event = AgentEvent()
            event.ParseFromString(data)
            result.append(event)
        return result
```

### 5.6 后端选择与多路广播

```python
# sahara_runtime/events/backends/composite.py

class CompositeBackend(EventBackend):
    """多路广播后端: 同时发布到多个后端。

    用途:
    - 迁移期间双写 (Redis + Kafka 同时写入, 逐步切换消费端)
    - 同时写入事件总线 + 可观测性系统
    """

    def __init__(self, primary: EventBackend, secondaries: list[EventBackend]):
        self._primary = primary
        self._secondaries = secondaries

    async def publish(self, topic: str, data: bytes) -> None:
        # Primary 必须成功
        await self._primary.publish(topic, data)

        # Secondaries 尽力而为 (失败不影响主链路)
        for secondary in self._secondaries:
            try:
                await secondary.publish(topic, data)
            except EventPublishError:
                logger.warning("secondary_backend_publish_failed",
                               backend=secondary.backend_name)

    async def health_check(self) -> bool:
        return await self._primary.health_check()

    async def close(self) -> None:
        await self._primary.close()
        for s in self._secondaries:
            await s.close()

    @property
    def backend_name(self) -> str:
        names = [self._primary.backend_name] + [s.backend_name for s in self._secondaries]
        return f"composite({'+'.join(names)})"


# sahara_runtime/events/factory.py

def create_event_backend(config: "RuntimeConfig") -> EventBackend:
    """根据配置创建事件后端。

    配置示例:
    - EVENT_BACKEND=redis_streams   (默认)
    - EVENT_BACKEND=kafka
    - EVENT_BACKEND=nats_jetstream
    - EVENT_BACKEND=redis_streams+kafka  (双写, 用于迁移)
    - EVENT_BACKEND=in_memory       (测试)
    """
    backend_name = config.event_backend  # "redis_streams" / "kafka" / ...

    if "+" in backend_name:
        # 多路广播模式: "redis_streams+kafka"
        names = backend_name.split("+")
        backends = [_create_single_backend(n, config) for n in names]
        return CompositeBackend(primary=backends[0], secondaries=backends[1:])

    return _create_single_backend(backend_name, config)


def _create_single_backend(name: str, config: "RuntimeConfig") -> EventBackend:
    match name:
        case "redis_streams":
            from sahara_runtime.events.backends.redis_streams import RedisStreamsBackend
            return RedisStreamsBackend(
                redis=config.redis_client,
                maxlen=config.event_stream_maxlen,
            )
        case "kafka":
            from sahara_runtime.events.backends.kafka import KafkaBackend
            return KafkaBackend(
                bootstrap_servers=config.kafka_bootstrap_servers,
            )
        case "nats_jetstream":
            from sahara_runtime.events.backends.nats_jetstream import NATSJetStreamBackend
            return NATSJetStreamBackend(nats_url=config.nats_url)
        case "in_memory":
            from sahara_runtime.events.backends.in_memory import InMemoryBackend
            return InMemoryBackend()
        case _:
            raise ValueError(f"Unknown event backend: {name}")
```

### 5.7 完整事件类型清单

| 事件类型 | 发射时机 | Gateway 处理 | 客户端呈现 |
| --- | --- | --- | --- |
| `RUN_START` | Agent Loop 开始 | 推送 → 客户端 | 显示"思考中..." |
| `DELTA` | LLM 流式文本 | 聚合 + 推送 | 逐字显示回复 |
| `THINKING` | LLM 思考过程 | 推送 | 显示思考泡泡 |
| `TOOL_START` | 工具开始执行 | 推送 | 显示"正在执行..." |
| `TOOL_RESULT` | 工具执行完成 | 推送 | 显示执行结果 |
| `USAGE` | LLM 调用计量 | 推送 + 配额扣减 | 显示 token 用量 |
| `INPUT_REQUIRED` | Agent 需要用户输入 | **推送 + 升级亲和** | 显示输入框/确认弹窗 |
| `TOOL_CONFIRM_REQUIRED` | 高危工具需确认 | **推送 + 升级亲和** | 显示确认弹窗 |
| `RUN_COMPLETE` | Agent Loop 结束 | 推送 + 清除亲和 | 显示完成状态 |
| `RUN_ERROR` | 执行出错 | 推送 + 清除亲和 | 显示错误信息 |
| `RUN_ABORT` | 任务被取消 | 推送 + 清除亲和 | 显示取消状态 |

> **待同步**：`INPUT_REQUIRED` 和 `TOOL_CONFIRM_REQUIRED` 事件类型需要同步添加到 [gRPC 协议设计](./GRPC-PROTOCOL-DESIGN.md) 的 `EventType` 枚举和 [Event Bus 架构设计](./EVENT-BUS-DESIGN.md) 的事件定义中。

### 5.8 弹性设计

事件发射位于 Agent 执行的关键路径上（每个 delta 都需要发布），其稳定性直接影响用户体验。弹性策略在 `ResilientEmitter` 中实现，**与底层后端无关**。

```python
# sahara_runtime/events/emitter.py

class ResilientEmitter(RunEmitter):
    """带弹性保护的事件发射器。包装 RunEmitter, 按事件分类执行不同策略。"""

    CRITICAL_EVENTS = {
        EventType.EVENT_TYPE_RUN_START,
        EventType.EVENT_TYPE_RUN_COMPLETE,
        EventType.EVENT_TYPE_RUN_ERROR,
        EventType.EVENT_TYPE_RUN_ABORT,
        EventType.EVENT_TYPE_INPUT_REQUIRED,
    }

    IMPORTANT_EVENTS = {
        EventType.EVENT_TYPE_TOOL_START,
        EventType.EVENT_TYPE_TOOL_RESULT,
        EventType.EVENT_TYPE_TOOL_CONFIRM_REQUIRED,
    }

    async def _emit(self, event_type, **payload_kwargs):
        try:
            await asyncio.wait_for(
                super()._emit(event_type, **payload_kwargs),
                timeout=2.0,
            )
        except (EventPublishError, asyncio.TimeoutError) as e:
            await self._handle_failure(event_type, payload_kwargs, e)

    async def _handle_failure(self, event_type, payload_kwargs, error):
        if event_type in self.CRITICAL_EVENTS:
            # 生命周期事件: 重试 3 次, 仍失败则中止任务
            await self._retry(event_type, payload_kwargs, retries=3)

        elif event_type in self.IMPORTANT_EVENTS:
            # 工具事件: 重试 1 次, 仍失败则丢弃
            try:
                await self._retry(event_type, payload_kwargs, retries=1)
            except EventDeliveryError:
                self._log_dropped(event_type, error)

        else:
            # DELTA / THINKING / USAGE: 直接丢弃
            self._log_dropped(event_type, error)

    async def _retry(self, event_type, payload_kwargs, retries):
        for attempt in range(retries):
            try:
                await asyncio.sleep(0.5 * (2 ** attempt))
                await super()._emit(event_type, **payload_kwargs)
                return
            except (EventPublishError, asyncio.TimeoutError):
                if attempt == retries - 1:
                    raise EventDeliveryError(
                        f"critical event {event_type.name} delivery failed "
                        f"after {retries} retries"
                    )

    def _log_dropped(self, event_type, error):
        logger.warning("event_emit_dropped",
                       event_type=event_type.name,
                       backend=self._backend.backend_name,
                       error=str(error))
        metrics.events_dropped_total.labels(
            type=event_type.name,
            backend=self._backend.backend_name,
        ).inc()
```

**弹性策略总结**：

| 事件分类 | 后端不可用时的策略 | 理由 |
| --- | --- | --- |
| **Critical** (RUN_START/COMPLETE/ERROR/ABORT/INPUT_REQUIRED) | 重试 3 次 (指数退避), 仍失败则中止任务 | 丢失会导致 Gateway 状态不一致或 Agent 永久挂起 |
| **Important** (TOOL_START/RESULT/CONFIRM) | 重试 1 次, 仍失败丢弃并记录 | 丢失影响可观测性但不影响正确性 |
| **Best-effort** (DELTA/THINKING/USAGE) | 直接丢弃并记录 | 流式文本丢几个 delta 不影响最终结果; 用量可事后补偿 |

### 5.9 各后端特性对比

| 特性 | Redis Streams | Kafka | NATS JetStream | InMemory |
| --- | --- | --- | --- | --- |
| **延迟** | ~1ms | ~5-10ms | ~2-3ms | ~0ms |
| **吞吐** | 10K msg/s (单节点) | 100K+ msg/s | 50K+ msg/s | 无限 |
| **持久化** | 可选 (AOF/RDB) | 强持久化 | 可选 | 无 |
| **消费者组** | 原生 XREADGROUP | 原生 ConsumerGroup | 原生 Pull Consumer | 不支持 |
| **回放** | 有限 (maxlen 裁剪) | 按 offset 任意回放 | 按 seq 回放 | 不支持 |
| **部署复杂度** | 低 (已有 Redis) | 高 (ZK/KRaft) | 中 | 无 |
| **适用阶段** | Phase 1 | Phase 2+ (高吞吐) | Phase 2+ (低延迟) | 开发/测试 |
| **Topic 映射** | `events:{session_key}` | `sahara-events` (session_key 为 partition key) | `sahara.events.{session_key}` | `events:{session_key}` |

### 5.10 迁移策略

从 Redis Streams 迁移到其他 MQ 的推荐步骤：

```text
迁移路径 (以 Redis → Kafka 为例):

  Phase A: 双写 (1-2 周)
  ┌─────────────────────────────────────────────────────────┐
  │  config: EVENT_BACKEND=redis_streams+kafka               │
  │                                                         │
  │  CompositeBackend:                                      │
  │    primary: RedisStreamsBackend    ← Gateway 仍然消费这里│
  │    secondary: KafkaBackend        ← 写入但无消费者      │
  │                                                         │
  │  验证: Kafka 中事件数量 == Redis 中事件数量             │
  └─────────────────────────────────────────────────────────┘

  Phase B: 切换消费端 (灰度)
  ┌─────────────────────────────────────────────────────────┐
  │  config: EVENT_BACKEND=redis_streams+kafka               │
  │                                                         │
  │  Gateway 灰度切换:                                      │
  │    10% Gateway 实例改为消费 Kafka                       │
  │    90% 仍消费 Redis Streams                             │
  │                                                         │
  │  验证: 灰度用户事件延迟、丢失率正常                     │
  └─────────────────────────────────────────────────────────┘

  Phase C: 全量切换
  ┌─────────────────────────────────────────────────────────┐
  │  config: EVENT_BACKEND=kafka                             │
  │                                                         │
  │  所有 Gateway 消费 Kafka                                │
  │  停止 Redis Streams 写入                                │
  │                                                         │
  │  Runtime 代码改动: 仅修改 EVENT_BACKEND 配置, 零代码变更 │
  └─────────────────────────────────────────────────────────┘
```

---

## 六、模型管理 (Model Router)

### 6.1 职责

将 agent 配置中的模型名解析为具体的 Provider + Model + API Key，支持多 Key 轮换、限流冷却、模型降级。

### 6.2 核心策略：Session 级模型与 Key 亲和

> **这是 Model Router 最重要的设计决策。**

主流 LLM 提供商都提供了 Prompt Cache 机制：

| 提供商 | 缓存机制 | 命中条件 | 缓存收益 |
| --- | --- | --- | --- |
| **Anthropic** | Prompt Caching | 相同 API Key + 相同 Model + 消息前缀完全匹配 | 输入 token 价格降低 **90%**，延迟降低 **~85%** |
| **OpenAI** | Prefix Caching | 相同 API Key + 相同 Model + 前 1024+ token 相同 | 缓存部分 token 价格降低 **50%** |

一个典型的 Agent 会话会产生 10-50 轮 LLM 交互（用户消息 → LLM 回复 → 工具调用 → LLM 回复 → ...），每轮都会携带完整的系统提示词 + 历史消息作为上下文前缀。如果会话过程中切换了 Key 或模型，**之前所有轮次积累的缓存全部失效**，等于白花了前 N 轮的 Input Token 费用。

**Session 级亲和规则**：

```text
同一 session 内的所有 LLM 调用:
  ├── 使用相同的 Model (除非发生不可恢复的降级)
  ├── 使用相同的 API Key (即使该 Key 遇到 429, 也优先等待/重试)
  └── 使用相同的 Provider (Anthropic ↔ OpenAI 切换必然丢失缓存)

Key 切换仅在以下情况发生:
  ├── 当前 Key 遭遇 401/403 (认证失效) → 被迫换 Key
  ├── 当前 Key 被管理员从 Key 池移除 → 被迫换 Key
  └── 新会话开始 → 可重新选择 Key (此时没有缓存可丢失)
```

```text
缓存命中的经济效益 (以 Claude Sonnet 为例):

  假设: 系统提示词 3000 token, 每轮增量 ~500 token, 共 20 轮

  ┌── 无缓存亲和 (每轮换 Key) ──────────────────────────────────┐
  │  轮次 1: 输入  3,500 token  (全价)                            │
  │  轮次 2: 输入  4,000 token  (全价 ← Key 变了, 缓存失效)      │
  │  ...                                                          │
  │  轮次 20: 输入 13,000 token (全价)                            │
  │  总输入: ~165,000 token × $3/MTok = $0.495                    │
  └───────────────────────────────────────────────────────────────┘

  ┌── Session 级亲和 (同一 Key) ─────────────────────────────────┐
  │  轮次 1: 输入  3,500 token  (全价, 建立缓存)                 │
  │  轮次 2: 输入  3,500 缓存 + 500 新 (缓存 90% 折扣)          │
  │  ...                                                          │
  │  轮次 20: 输入 13,000 缓存 + 500 新                          │
  │  总输入: ~3,500 全价 + ~161,500 缓存 × $0.3/MTok = $0.059   │
  └───────────────────────────────────────────────────────────────┘

  节省: ~88% 的 Input Token 费用
```

### 6.3 核心接口

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

@dataclass
class SessionModelBinding:
    """Session 级的模型+Key 绑定, 确保缓存命中。"""
    session_key: str
    provider: str
    model_id: str
    api_key: str
    client: Any              # SDK client (已绑定 Key)
    created_at: float
    call_count: int = 0      # 累计调用次数 (评估缓存价值)

class ModelRouter:
    def __init__(self, config: RuntimeConfig, key_pool: KeyPool):
        self.config = config
        self.key_pool = key_pool
        self._client_keys: dict[str, str] = {}
        # ── Session 级绑定缓存 ──
        self._session_bindings: dict[str, SessionModelBinding] = {}

    async def resolve_for_session(self, agent_id: str, session_key: str) -> ModelConfig:
        """为 session 解析模型配置。
        核心策略: 同一 session 尽量复用相同的 model + key。
        """
        # ── 1. 检查已有绑定 ──
        binding = self._session_bindings.get(session_key)
        if binding:
            # 验证 Key 仍然可用
            if await self.key_pool.is_key_available(binding.api_key):
                binding.call_count += 1
                return self._binding_to_config(binding, agent_id)
            else:
                # Key 不可用 (401/被移除) → 被迫解绑
                logger.warning("session_key_unavailable",
                               session_key=session_key,
                               old_key=binding.api_key[:8] + "...",
                               call_count=binding.call_count)
                del self._session_bindings[session_key]
                # ↓ 走新绑定流程

        # ── 2. 新绑定: 解析模型 + 选择 Key ──
        agent_config = await self._load_agent_config(agent_id)
        model_config = KNOWN_MODELS[agent_config.model]

        key = await self.key_pool.get_key(model_config.provider)
        if not key:
            raise ModelUnavailableError(f"no keys for {model_config.provider}")

        # ── 3. 创建绑定 ──
        client = self._create_client(model_config.provider, key)
        self._session_bindings[session_key] = SessionModelBinding(
            session_key=session_key,
            provider=model_config.provider,
            model_id=model_config.model_id,
            api_key=key,
            client=client,
            created_at=time.time(),
            call_count=1,
        )

        return model_config

    def get_client_for_session(self, session_key: str) -> Any:
        """获取 session 绑定的 SDK 客户端 (已锁定 Key)。"""
        binding = self._session_bindings.get(session_key)
        if not binding:
            raise RuntimeError(f"no model binding for session {session_key}")
        return binding.client

    def release_session(self, session_key: str):
        """会话结束时释放绑定 (供 GC 清理)。"""
        binding = self._session_bindings.pop(session_key, None)
        if binding:
            logger.info("session_model_released",
                        session_key=session_key,
                        call_count=binding.call_count,
                        duration=int(time.time() - binding.created_at))

    async def fallback(self, current: ModelConfig, session_key: str) -> ModelConfig:
        """模型降级: 主模型不可恢复时切换到备用。
        注意: 降级会丢失缓存, 仅在不可恢复错误时触发。
        """
        if not current.fallback:
            raise ModelUnavailableError("no fallback model configured")

        # 解绑旧的 session binding
        self.release_session(session_key)
        logger.warning("session_model_fallback",
                       session_key=session_key,
                       from_model=current.model_id,
                       to_model=current.fallback)

        # 创建新绑定 (新模型, 新 Key)
        return await self.resolve_for_session(
            agent_id="__fallback__",  # 直接用 fallback model
            session_key=session_key,
        )

    def _create_client(self, provider: str, key: str) -> Any:
        if provider == "anthropic":
            return anthropic.AsyncAnthropic(api_key=key)
        elif provider == "openai":
            return openai.AsyncOpenAI(api_key=key)
        raise ValueError(f"unknown provider: {provider}")
```

### 6.4 429 Rate Limit 处理策略

429 (Rate Limit) 是最常遇到的错误，但**不应该触发 Key 切换**——切换 Key 会丢失缓存，代价远大于等待几秒重试：

```python
# sahara_runtime/models/retry.py

async def call_llm_with_session_affinity(
    client, model: str, session_key: str,
    key_pool: KeyPool, api_key: str,
    **kwargs,
):
    """LLM 调用 + Session 亲和的重试策略。
    429 → 等待重试 (保持同一 Key)
    401/403 → 上报并让上层切换 Key
    5xx → 重试, 超过 3 次上报
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await client.messages.create(model=model, **kwargs)
            return response
        except RateLimitError as e:
            # ★ 429: 不换 Key, 等待后重试 (保护缓存)
            retry_after = float(e.response.headers.get("retry-after", 2))
            wait = min(retry_after, 30)  # 最多等 30s
            logger.info("rate_limited_retry_same_key",
                        session_key=session_key,
                        attempt=attempt, wait=wait)
            await asyncio.sleep(wait)
            continue

        except AuthenticationError:
            # 401/403: Key 失效, 必须切换
            await key_pool.report_error(api_key, 401)
            raise  # 上层 catch 后调用 fallback 或重新 resolve

        except APIStatusError as e:
            if e.status_code >= 500:
                # 5xx: 服务端错误, 短暂重试
                wait = 2 ** attempt
                await asyncio.sleep(wait)
                if attempt == max_retries - 1:
                    raise
                continue
            raise

    raise LLMProviderError("max retries exceeded", retryable=False)
```

**重试策略对缓存的影响**：

| 错误类型 | 操作 | 是否换 Key | 缓存影响 |
| --- | --- | --- | --- |
| **429 Rate Limit** | 等待 `retry-after` 秒后重试 | **否** | 缓存保留 ✓ |
| **5xx Server Error** | 指数退避重试 (2s, 4s, 8s) | **否** | 缓存保留 ✓ |
| **超时** | 重试一次 | **否** | 缓存保留 ✓ |
| **401/403 Auth** | 上报 Key, 解绑 session | **是** | 缓存丢失 ✗ |
| **模型降级** | 切换到 fallback model | **是** | 缓存丢失 ✗ |

### 6.5 Key 池 + 熔断

```python
# sahara_runtime/models/key_pool.py

class KeyPool:
    """API Key 池管理: Session 亲和优先, 跨 Session 轮换。

    设计原则:
    - 同一 session 内锁定同一 Key (缓存亲和)
    - 新 session 在可用 Key 间轮换 (均摊负载)
    - 429 的 Key 不标记为"不可用" (只是暂时被限)
    - 401/403 的 Key 标记为熔断
    """

    def __init__(self, redis: Redis):
        self.redis = redis
        self._round_robin_idx = 0

    async def get_key(self, provider: str) -> str | None:
        """为新 session 选择一个可用 Key (轮换策略)。"""
        keys = list(await self.redis.smembers(f"keys:{provider}"))
        if not keys:
            return None

        # Round-Robin 遍历, 跳过熔断的 Key
        for i in range(len(keys)):
            idx = (self._round_robin_idx + i) % len(keys)
            key = keys[idx]
            state = await self.redis.hgetall(f"key_state:{key}")

            if state.get("status") == "circuit_open":
                # 熔断中: 检查是否可半开试探
                if time.time() - float(state.get("opened_at", 0)) > 60:
                    self._round_robin_idx = idx + 1
                    return key  # 半开试探
                continue

            # 注意: cooling (429) 的 Key 仍然可选
            # → 因为 429 是短暂的, session 亲和比避开 429 更重要
            self._round_robin_idx = idx + 1
            return key

        return None

    async def is_key_available(self, key: str) -> bool:
        """检查 Key 是否仍可用 (未被熔断/移除)。"""
        state = await self.redis.hgetall(f"key_state:{key}")
        if state.get("status") == "circuit_open":
            return False
        # 确认 Key 仍在池中
        return await self.redis.sismember(f"keys:{state.get('provider', '')}", key)

    async def report_error(self, key: str, error_code: int):
        """Key 调用失败时上报。仅 401/403 触发熔断。"""
        if error_code in (401, 403):
            # 认证失败 → 熔断 (这是唯一让 session 被迫换 Key 的情况)
            await self.redis.hset(f"key_state:{key}", mapping={
                "status": "circuit_open", "opened_at": str(time.time())
            })
            logger.error("key_circuit_open", key=key[:8] + "...", code=error_code)
        # 429 不标记 → session 内等待重试即可
```

### 6.6 Session 绑定生命周期

```text
Session 绑定的创建与销毁:

  ┌──────────────────────────────────────────────────────────────┐
  │  1. 首次 LLM 调用                                            │
  │     resolve_for_session()                                    │
  │     → Key 池选择 Key (Round-Robin)                           │
  │     → 创建 SessionModelBinding                               │
  │     → 创建专属 SDK Client (锁定 Key)                         │
  │                                                              │
  │  2. 后续 LLM 调用 (同一 session)                             │
  │     resolve_for_session()                                    │
  │     → 命中 binding 缓存 ✓                                   │
  │     → 复用同一 Key + Client                                  │
  │     → LLM Provider 侧命中 Prompt Cache ✓                    │
  │                                                              │
  │  3. 会话结束 / Agent Loop 完成                               │
  │     release_session()                                        │
  │     → 清除 binding                                           │
  │     → 记录统计 (call_count, duration)                        │
  │                                                              │
  │  异常路径:                                                    │
  │  4a. 当前 Key 被 401/403 → 被迫解绑                         │
  │      → 重新 resolve → 新 Key (缓存丢失, 不可避免)           │
  │  4b. 模型降级 → 被迫解绑                                     │
  │      → fallback model + 新 Key (缓存丢失)                   │
  └──────────────────────────────────────────────────────────────┘
```

> **注意**：`_session_bindings` 是 Worker 内存中的缓存。由于 Gateway Dispatcher 的 Session 亲和策略（§7.2 in Gateway 文档），同一 session 的请求会尽量路由到同一 Worker，因此这个内存缓存的命中率很高。即使偶尔路由到不同 Worker，仅需重新创建一个 binding（首次调用无缓存），不影响正确性。

---

## 七、工具系统 (Tools)

### 7.1 工具全生命周期

工具系统是 Agent 与外部世界交互的唯一桥梁。一次完整的工具调用经历 **6 个阶段**，跨越 Runtime、LLM Provider、沙箱三方：

```text
工具全生命周期:

  ┌─────────────────────────────────────────────────────────────────────┐
  │  阶段 1: 工具注册 & 策略过滤 (Agent Loop 步骤 5)                    │
  │                                                                     │
  │  ToolRegistry.create_tools(agent_id, sandbox)                       │
  │    │                                                                │
  │    ├── 创建 [exec, read, write, edit, web_search, web_fetch]       │
  │    ├── 加载 Agent 策略 → allowlist / blocklist 过滤                │
  │    └── 产出: tools = [exec, read, write]  (过滤后的可用工具集)      │
  │                                                                     │
  └──────────────────────────────────────┬──────────────────────────────┘
                                         │
                                         ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  阶段 2: 工具 Schema 注入 LLM 请求                                  │
  │                                                                     │
  │  tool_definitions = [t.to_llm_schema() for t in tools]             │
  │                                                                     │
  │  → 生成 JSON Schema 数组, 随 LLM API 请求发送:                     │
  │    client.messages.create(                                          │
  │        model="claude-sonnet-4-20250514",                            │
  │        system=system_prompt,                                        │
  │        messages=messages,                                           │
  │        tools=tool_definitions,  ← 工具定义在这里                    │
  │    )                                                                │
  │                                                                     │
  │  LLM 收到的工具定义示例:                                            │
  │  [                                                                  │
  │    { "name": "exec",                                                │
  │      "description": "Execute a shell command in the sandbox...",    │
  │      "input_schema": {                                              │
  │        "type": "object",                                            │
  │        "properties": { "command": {"type":"string"}, ... },         │
  │        "required": ["command"] } },                                 │
  │    { "name": "read", ... },                                        │
  │    { "name": "write", ... },                                       │
  │  ]                                                                  │
  │                                                                     │
  └──────────────────────────────────────┬──────────────────────────────┘
                                         │
                                         ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  阶段 3: LLM 决策 — 是否调用工具                                    │
  │                                                                     │
  │  LLM 流式返回 response, stop_reason 有两种:                        │
  │                                                                     │
  │  ├── stop_reason == "end_turn"                                     │
  │  │   → LLM 直接给出文本回复, 不调用工具 → 循环结束                 │
  │  │                                                                  │
  │  └── stop_reason == "tool_use"                                     │
  │      → response.content 包含一个或多个 tool_use 块:                │
  │        [                                                            │
  │          { "type": "text", "text": "我来查看一下文件..." },        │
  │          { "type": "tool_use",                                     │
  │            "id": "toolu_01ABC",       ← LLM 生成的调用 ID          │
  │            "name": "exec",            ← 要调用的工具名              │
  │            "input": {                 ← LLM 生成的参数              │
  │              "command": "cat /workspace/main.py"                    │
  │            }                                                        │
  │          }                                                          │
  │        ]                                                            │
  │                                                                     │
  └──────────────────────────────────────┬──────────────────────────────┘
                                         │
                                         ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  阶段 4: 安全检查 + 确认 (可选)                                     │
  │                                                                     │
  │  for each tool_use block:                                           │
  │    ├── 工具名合法? → 未知工具返回 is_error                         │
  │    ├── ToolPolicy.requires_confirmation(name, args)?                │
  │    │   ├── 是 → 发射 INPUT_REQUIRED 事件, 挂起等待 (§4.4)         │
  │    │   └── 否 → 直接执行                                           │
  │    └── 发射 TOOL_START 事件                                        │
  │                                                                     │
  └──────────────────────────────────────┬──────────────────────────────┘
                                         │
                                         ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  阶段 5: 沙箱执行                                                   │
  │                                                                     │
  │  tool.execute(args, sandbox=sandbox)                                │
  │    │                                                                │
  │    ├── exec → sandbox.exec("cat /workspace/main.py", timeout=30)   │
  │    │          → Docker: container.exec_run(cmd, workdir, timeout)   │
  │    │          → 返回 stdout + stderr                                │
  │    │                                                                │
  │    ├── read → sandbox.read_file("/workspace/src/app.py")           │
  │    │          → Docker: cp 或 exec cat                              │
  │    │          → 返回文件内容 (含行号)                               │
  │    │                                                                │
  │    ├── write → sandbox.write_file(path, content)                   │
  │    │           → Docker: 写入容器文件系统                           │
  │    │           → 返回 "File written: {path}"                       │
  │    │                                                                │
  │    ├── edit → sandbox.read_file → 应用 diff → sandbox.write_file   │
  │    │          → 返回 diff 结果                                      │
  │    │                                                                │
  │    └── web_search / web_fetch → HTTP 请求 (不走沙箱)               │
  │                                                                     │
  │  输出截断: result = _truncate(raw_output, max_chars=8000)          │
  │                                                                     │
  │  发射 TOOL_RESULT 事件 (含 success, output, duration_ms)           │
  │                                                                     │
  └──────────────────────────────────────┬──────────────────────────────┘
                                         │
                                         ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  阶段 6: 结果回传 LLM                                               │
  │                                                                     │
  │  工具结果封装为 tool_result 消息, 追加到 messages:                  │
  │                                                                     │
  │  messages.append({                                                  │
  │    "role": "user",              ← Anthropic 协议: tool_result 在    │
  │    "content": [                    user 消息中                      │
  │      {                                                              │
  │        "type": "tool_result",                                       │
  │        "tool_use_id": "toolu_01ABC",  ← 对应 LLM 的 tool_use.id   │
  │        "content": "#!/usr/bin/env python\nimport os\n...",         │
  │        "is_error": false,       ← 执行成功/失败                    │
  │      }                                                              │
  │    ]                                                                │
  │  })                                                                 │
  │                                                                     │
  │  → 继续循环: 带着 tool_result 再次调用 LLM                        │
  │  → LLM 看到工具返回的内容, 决定:                                   │
  │    ├── 继续调用其他工具 (stop_reason == "tool_use") → 回到阶段 3   │
  │    └── 给出最终回复 (stop_reason == "end_turn") → 循环结束         │
  │                                                                     │
  └─────────────────────────────────────────────────────────────────────┘
```

### 7.2 工具接口

```python
# sahara_runtime/tools/base.py

class Tool(ABC):
    """所有工具的基类。"""

    @property
    @abstractmethod
    def name(self) -> str:
        """工具唯一标识, 也是 LLM 调用时使用的名称。"""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """工具描述, LLM 根据此描述决定是否调用。
        描述质量直接影响 LLM 的工具选择准确率。
        """
        ...

    @abstractmethod
    def parameters_schema(self) -> dict:
        """JSON Schema 格式的参数定义。LLM 按此 Schema 生成调用参数。"""
        ...

    @abstractmethod
    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str:
        """执行工具, 返回字符串结果。
        所有结果统一为字符串 — LLM 只理解文本。
        """
        ...

    def to_llm_schema(self) -> dict:
        """转换为 LLM API 格式的工具定义 (Anthropic Tool Use 协议)。
        此 dict 直接传入 client.messages.create(tools=[...])。
        """
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.parameters_schema(),
        }
```

### 7.3 工具优先级与分层

工具集内部有明确的**优先级分层**，影响三个维度：

1. **LLM Schema 排列顺序** — 排在前面的工具，LLM 更倾向优先使用
2. **预算裁剪顺序** — 上下文紧张时，低优先级工具的 Schema 可被移除
3. **冲突解决** — 同类功能多个工具时，优先使用高优先级的

```text
工具优先级分层:

  ┌───────────────────────────────────────────────────────────────┐
  │  Tier 0: 核心工具 (Core)                                      │
  │  永远可用, 不可被 blocklist 禁用, Schema 排列最前              │
  │                                                               │
  │  exec   — 命令执行, Agent 最基本的"手"                        │
  │  read   — 文件读取, Agent 最基本的"眼"                        │
  │  write  — 文件写入, Agent 最基本的"笔"                        │
  │                                                               │
  ├───────────────────────────────────────────────────────────────┤
  │  Tier 1: 增强工具 (Enhanced)                                   │
  │  默认启用, 可被 blocklist 禁用, Schema 排列在核心工具之后      │
  │                                                               │
  │  edit   — 精确编辑 (比 write 更适合小修改)                    │
  │  glob   — 文件搜索 (按模式匹配文件名)         [Phase 2]       │
  │  grep   — 内容搜索 (在文件中搜索文本)         [Phase 2]       │
  │                                                               │
  ├───────────────────────────────────────────────────────────────┤
  │  Tier 2: 扩展工具 (Extended)                                   │
  │  按需启用, 默认可能关闭, 受 Agent 策略控制                    │
  │                                                               │
  │  web_search  — 搜索引擎查询                   [Phase 2]       │
  │  web_fetch   — 抓取 URL 内容                  [Phase 2]       │
  │                                                               │
  ├───────────────────────────────────────────────────────────────┤
  │  Tier 3: 插件工具 (Plugin)                    [Phase 3]        │
  │  第三方 / 用户自定义工具, 动态加载, 最低优先级                 │
  │                                                               │
  │  image_gen  — 图片生成 (调用外部 API)                          │
  │  db_query   — 数据库查询 (受权限控制)                          │
  │  custom_*   — 用户自定义工具                                   │
  │                                                               │
  └───────────────────────────────────────────────────────────────┘

  LLM tools 参数中的排列顺序:
  [exec, read, write, edit, glob, grep, web_search, web_fetch, ...]
   ├── Tier 0 ──┤├── Tier 1 ───────┤├── Tier 2 ────────┤├ Tier 3
```

**优先级数据模型：**

```python
# sahara_runtime/tools/base.py

class ToolTier(IntEnum):
    """工具优先级层级。数值越小优先级越高。"""
    CORE = 0        # 核心工具: 永远可用
    ENHANCED = 1    # 增强工具: 默认启用, 可禁用
    EXTENDED = 2    # 扩展工具: 按需启用
    PLUGIN = 3      # 插件工具: 第三方/自定义

class Tool(ABC):
    """所有工具的基类。"""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def description(self) -> str: ...

    @property
    def tier(self) -> ToolTier:
        """工具优先级层级。子类可覆盖。默认 ENHANCED。"""
        return ToolTier.ENHANCED

    @property
    def tags(self) -> list[str]:
        """工具标签, 用于分组和过滤。如 ["filesystem", "sandbox"]。"""
        return []

    @abstractmethod
    def parameters_schema(self) -> dict: ...

    @abstractmethod
    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str: ...

    def to_llm_schema(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.parameters_schema(),
        }
```

**各内置工具的 Tier 定义：**

| 工具 | 名称 | Tier | 标签 | 执行位置 | 说明 | Phase |
| --- | --- | --- | --- | --- | --- | --- |
| **exec** | `exec` | Core (0) | `sandbox`, `shell` | 沙箱 Docker | 执行 bash 命令 | Phase 1 |
| **read** | `read` | Core (0) | `sandbox`, `filesystem` | 沙箱 Docker | 读取文件内容 | Phase 1 |
| **write** | `write` | Core (0) | `sandbox`, `filesystem` | 沙箱 Docker | 写入/创建文件 | Phase 1 |
| **edit** | `edit` | Enhanced (1) | `sandbox`, `filesystem` | 沙箱 Docker | 精确编辑文件 | Phase 2 |
| **glob** | `glob` | Enhanced (1) | `sandbox`, `filesystem` | 沙箱 Docker | 文件名模式搜索 | Phase 2 |
| **grep** | `grep` | Enhanced (1) | `sandbox`, `search` | 沙箱 Docker | 文件内容搜索 | Phase 2 |
| **web_search** | `web_search` | Extended (2) | `network`, `search` | Runtime 主机 | 搜索引擎查询 | Phase 2 |
| **web_fetch** | `web_fetch` | Extended (2) | `network`, `fetch` | Runtime 主机 | 抓取 URL 内容 | Phase 2 |

**Tier 对策略过滤的影响：**

```python
# ToolPolicy 中的 Tier 相关规则

class ToolPolicy:
    allowlist: list[str] | None = None
    blocklist: list[str] | None = None
    disabled_tiers: list[ToolTier] | None = None  # 按层级禁用
    confirmation_patterns: dict[str, list[str]] | None = None

    # 示例: 一个"只读 Agent"的策略
    # {
    #   "allowlist": None,
    #   "blocklist": ["write", "edit", "exec"],
    #   "disabled_tiers": [ToolTier.EXTENDED, ToolTier.PLUGIN],
    # }
    #
    # 效果: 只剩 read + glob + grep (Tier 0/1 中未被 blocklist 的)
```

### 7.4 工具扩展机制

支持三种方式扩展工具集，满足不同场景需求：

```text
工具来源:

  ┌─────────────────────────────────────────────────────────┐
  │  内置工具 (Built-in)                                     │
  │  随 Runtime 代码发布, 直接 import                        │
  │  exec, read, write, edit, glob, grep, web_search, ...   │
  │  优先级: Tier 0-2                                        │
  ├─────────────────────────────────────────────────────────┤
  │  Agent 级工具 (Agent-scoped)            [Phase 2]        │
  │  Agent 配置中指定的额外工具                              │
  │  如: Agent "数据分析师" 额外启用 db_query               │
  │  从 Agent 配置 (PG) 加载                                 │
  │  优先级: Tier 2-3                                        │
  ├─────────────────────────────────────────────────────────┤
  │  插件工具 (Plugin)                      [Phase 3]        │
  │  用户上传/市场安装的自定义工具                           │
  │  通过标准接口注册, 在独立进程/容器中执行                 │
  │  优先级: Tier 3                                          │
  └─────────────────────────────────────────────────────────┘
```

**插件工具接口 (Phase 3 预留)：**

```python
# sahara_runtime/tools/plugin.py

@dataclass
class PluginToolSpec:
    """插件工具规格, 从配置或市场元数据加载。"""
    name: str
    description: str
    input_schema: dict                  # JSON Schema
    execution_mode: str = "sandbox"     # "sandbox" | "sidecar" | "remote"
    image: str | None = None            # Docker 镜像 (sidecar 模式)
    endpoint: str | None = None         # HTTP 端点 (remote 模式)
    timeout: int = 30
    requires_confirmation: bool = False # 是否默认需要确认
    tags: list[str] = field(default_factory=list)

class PluginTool(Tool):
    """基于 PluginToolSpec 动态创建的工具。"""

    def __init__(self, spec: PluginToolSpec):
        self._spec = spec

    @property
    def name(self) -> str:
        return self._spec.name

    @property
    def description(self) -> str:
        return self._spec.description

    @property
    def tier(self) -> ToolTier:
        return ToolTier.PLUGIN

    def parameters_schema(self) -> dict:
        return self._spec.input_schema

    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str:
        match self._spec.execution_mode:
            case "sandbox":
                # 在当前沙箱中执行 (如调用沙箱内的 CLI 工具)
                return await sandbox.exec(
                    self._build_command(args),
                    timeout=self._spec.timeout,
                )
            case "sidecar":
                # 在独立容器中执行 (如需要特殊环境)
                return await self._exec_sidecar(args)
            case "remote":
                # 调用远程 HTTP 端点
                return await self._exec_remote(args)
```

**ToolRegistry 的完整加载流程 (支持扩展后)：**

```python
class ToolRegistry:

    BUILTIN_TOOLS = [
        ExecTool, ReadTool, WriteTool,    # Tier 0: Core
        # Phase 2:
        # EditTool, GlobTool, GrepTool,   # Tier 1: Enhanced
        # WebSearchTool, WebFetchTool,    # Tier 2: Extended
    ]

    async def create_tools(
        self, agent_id: str, sandbox: Sandbox, session_key: str,
    ) -> list[Tool]:
        """创建工具集: 内置 + Agent 级 + 插件, 按 Tier 排序。"""

        # 1. 内置工具
        tools = [cls() for cls in self.BUILTIN_TOOLS]

        # 2. Agent 级工具 [Phase 2]
        # agent_tools = await self._load_agent_tools(agent_id)
        # tools.extend(agent_tools)

        # 3. 插件工具 [Phase 3]
        # plugin_specs = await self._load_plugin_specs(agent_id)
        # tools.extend(PluginTool(spec) for spec in plugin_specs)

        # 4. 策略过滤
        policy = await self._load_policy(agent_id)
        filtered = self._apply_policy(tools, policy)

        # 5. ★ 按 Tier 排序 (优先级高的排前面 → LLM 更倾向使用)
        filtered.sort(key=lambda t: (t.tier, t.name))

        # 6. 上下文预算裁剪 [Phase 3]
        # 如果工具 Schema 总 token 超出预算, 从 Tier 最高的开始移除
        # filtered = self._apply_budget(filtered, max_tool_tokens=4000)

        return filtered
```

### 7.5 工具定义详解

#### exec — 命令执行

```python
# sahara_runtime/tools/exec_tool.py

class ExecTool(Tool):
    name = "exec"
    description = (
        "Execute a shell command in the sandbox environment. "
        "The command runs in a bash shell with access to the /workspace directory. "
        "Returns stdout and stderr combined. Use for running scripts, "
        "installing packages, compiling code, running tests, etc."
    )

    def parameters_schema(self):
        return {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The bash command to execute",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in seconds (default 30, max 300)",
                    "default": 30,
                },
            },
            "required": ["command"],
        }

    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str:
        command = args["command"]
        timeout = min(args.get("timeout", 30), 300)  # 上限 5 分钟

        if sandbox and sandbox.enabled:
            result = await sandbox.exec(command, timeout=timeout)
        else:
            result = await self._exec_local(command, timeout)

        return _truncate(result, max_chars=8000)

    async def _exec_local(self, command: str, timeout: int) -> str:
        """开发模式: 本地执行 (受白名单保护)。"""
        cmd_prefix = command.strip().split()[0] if command.strip() else ""
        if cmd_prefix not in DEV_MODE_ALLOWED_PREFIXES:
            raise ToolExecutionError(
                f"Command '{cmd_prefix}' not allowed in dev mode (no sandbox)"
            )
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout,
        )
        return stdout.decode() + stderr.decode()

# 开发模式命令白名单
DEV_MODE_ALLOWED_PREFIXES = [
    "ls", "cat", "head", "tail", "wc", "grep", "find",
    "echo", "pwd", "whoami", "date", "python", "pip",
    "node", "npm", "git",
]
```

#### read — 文件读取

```python
# sahara_runtime/tools/file_tools.py

class ReadTool(Tool):
    name = "read"
    description = (
        "Read the contents of a file at the specified path. "
        "Returns the file content with line numbers. "
        "For large files, use the offset and limit parameters."
    )

    def parameters_schema(self):
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the file to read",
                },
                "offset": {
                    "type": "integer",
                    "description": "Starting line number (1-based). Optional.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of lines to read. Optional.",
                },
            },
            "required": ["path"],
        }

    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str:
        path = args["path"]
        offset = args.get("offset")
        limit = args.get("limit")

        if sandbox and sandbox.enabled:
            content = await sandbox.read_file(path)
        else:
            content = open(path, "r").read()

        # 添加行号 (LLM 需要行号来精确引用和编辑)
        lines = content.splitlines()
        if offset:
            lines = lines[offset - 1:]
        if limit:
            lines = lines[:limit]

        numbered = [f"{i+1:6d}|{line}" for i, line in enumerate(lines, start=offset or 1)]
        return _truncate("\n".join(numbered), max_chars=8000)
```

#### write — 文件写入

```python
class WriteTool(Tool):
    name = "write"
    description = (
        "Write content to a file at the specified path. "
        "Creates the file if it doesn't exist, overwrites if it does. "
        "Parent directories are created automatically."
    )

    def parameters_schema(self):
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Absolute path to write to"},
                "content": {"type": "string", "description": "The content to write"},
            },
            "required": ["path", "content"],
        }

    async def execute(self, args: dict, sandbox: Sandbox | None = None) -> str:
        path = args["path"]
        content = args["content"]

        if sandbox and sandbox.enabled:
            # 确保父目录存在
            parent = os.path.dirname(path)
            await sandbox.exec(f"mkdir -p {parent}")
            await sandbox.write_file(path, content)
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(content)

        line_count = content.count("\n") + 1
        return f"File written: {path} ({line_count} lines)"
```

### 7.6 工具注册与策略过滤

```python
# sahara_runtime/tools/registry.py

@dataclass
class ToolPolicy:
    """Agent 级别的工具访问策略。"""
    allowlist: list[str] | None = None         # None = 允许全部
    blocklist: list[str] | None = None         # 明确禁用的工具
    disabled_tiers: list[ToolTier] | None = None  # 按层级禁用 (如禁用所有 Plugin)
    confirmation_patterns: dict[str, list[str]] | None = None  # 工具 → 危险模式

class ToolRegistry:
    """工具注册中心: 创建、过滤、排序、输出 LLM Schema。"""

    BUILTIN_TOOLS = [
        ExecTool, ReadTool, WriteTool,    # Tier 0: Core
        # Phase 2:
        # EditTool, GlobTool, GrepTool,   # Tier 1: Enhanced
        # WebSearchTool, WebFetchTool,    # Tier 2: Extended
    ]

    async def create_tools(
        self, agent_id: str, sandbox: Sandbox, session_key: str,
    ) -> list[Tool]:
        """为一次 Agent 运行创建工具集。"""

        # 1. 实例化所有内置工具
        all_tools = [cls() for cls in self.BUILTIN_TOOLS]

        # 2. Agent 级工具 [Phase 2]
        # agent_tools = await self._load_agent_tools(agent_id)
        # all_tools.extend(agent_tools)

        # 3. 插件工具 [Phase 3]
        # plugin_specs = await self._load_plugin_specs(agent_id)
        # all_tools.extend(PluginTool(spec) for spec in plugin_specs)

        # 4. 加载 Agent 策略 (从 Redis/PG)
        policy = await self._load_policy(agent_id)

        # 5. 策略过滤 (blocklist + allowlist + disabled_tiers)
        filtered = self._apply_policy(all_tools, policy)

        # 6. ★ 按 Tier 排序 (优先级高的排前面 → LLM 更倾向使用)
        filtered.sort(key=lambda t: (t.tier, t.name))

        logger.info("tools_created",
                     agent_id=agent_id,
                     total=len(all_tools),
                     filtered=len(filtered),
                     names=[t.name for t in filtered],
                     tiers={t.name: t.tier.name for t in filtered})

        return filtered

    def _apply_policy(self, tools: list[Tool], policy: ToolPolicy) -> list[Tool]:
        result = []
        for tool in tools:
            # Tier 0 (Core) 不可被 blocklist 禁用, 只能被 allowlist 排除
            if tool.tier != ToolTier.CORE:
                if policy.blocklist and tool.name in policy.blocklist:
                    continue
            # disabled_tiers 按层级禁用
            if policy.disabled_tiers and tool.tier in policy.disabled_tiers:
                continue
            # allowlist 非空时, 必须在列表中
            if policy.allowlist and tool.name not in policy.allowlist:
                continue
            result.append(tool)
        return result
```

### 7.7 LLM Schema 注入详解

工具定义在每次 LLM 调用时作为 `tools` 参数传入。由于 Session 级缓存亲和（§6.2），**工具 Schema 是 Prompt 前缀的一部分，会被 LLM Provider 缓存**：

```python
# Agent Loop 步骤 5 + 8b 的关联

# ── 步骤 5: 生成工具 Schema (一次性) ──
tools = await deps.tool_registry.create_tools(agent_id, sandbox, session_key)
tool_definitions = [t.to_llm_schema() for t in tools]

# ── 步骤 8b: 每次 LLM 调用都携带相同的 tools ──
response = await client.messages.create(
    model=model_config.model_id,
    system=system_prompt,          # ← 缓存前缀的一部分
    messages=messages,             # ← 前缀 (历史) + 增量 (新消息)
    tools=tool_definitions,        # ← 缓存前缀的一部分 (不变)
    max_tokens=8192,
    stream=True,
)
```

**缓存关系**：

```text
LLM Provider 的 Prompt Cache 范围:

  ┌──────────────────────────────────────────────────────┐
  │  缓存前缀 (每轮不变, 命中缓存 90% 折扣)             │
  │                                                      │
  │  system_prompt                                       │
  │    ├── 身份定义                                      │
  │    ├── 工具使用指南 (含 Skills)                      │
  │    ├── 安全规则                                      │
  │    └── 运行时信息                                    │
  │                                                      │
  │  tools (工具 Schema 定义)                            │
  │    ├── { name: "exec", input_schema: {...} }        │
  │    ├── { name: "read", input_schema: {...} }        │
  │    └── { name: "write", input_schema: {...} }       │
  │                                                      │
  │  messages[0..n-1] (历史消息)                         │
  │    ├── 轮次 1: user → assistant                     │
  │    ├── 轮次 2: user → assistant → tool → assistant  │
  │    └── ...                                           │
  │                                                      │
  ├──────────────────────────────────────────────────────┤
  │  增量部分 (本轮新增, 全价计费)                       │
  │                                                      │
  │  messages[n] (当前轮次的新消息)                      │
  │    └── 用户最新消息 或 工具返回结果                  │
  │                                                      │
  └──────────────────────────────────────────────────────┘

  → 工具 Schema 固定不变 → 被纳入缓存前缀 → 零边际成本
```

### 7.8 工具执行流程详解

LLM 返回 `stop_reason == "tool_use"` 后，Runtime 执行以下流程：

```python
# sahara_runtime/tools/executor.py

class ToolExecutor:
    """工具执行引擎。协调安全检查、沙箱调用、事件发射、结果封装。"""

    def __init__(self, tools: list[Tool], sandbox: Sandbox,
                 emitter: RunEmitter, policy: ToolPolicy):
        self.tool_map = {t.name: t for t in tools}
        self.sandbox = sandbox
        self.emitter = emitter
        self.policy = policy

    async def execute_tool_calls(
        self, response, task_handle: "TaskHandle | None" = None,
    ) -> list[dict]:
        """处理 LLM 返回的所有 tool_use 块, 返回 tool_result 列表。

        返回值直接追加到 messages 中, 格式:
        {"role": "user", "content": [tool_result_1, tool_result_2, ...]}
        """
        results = []

        for block in response.content:
            if block.type != "tool_use":
                continue

            result = await self._execute_one(block, task_handle)
            results.append(result)

        return results

    async def _execute_one(self, block, task_handle) -> dict:
        """执行单个工具调用。"""
        tool_call_id = block.id
        tool_name = block.name
        tool_args = block.input

        # ── 1. 查找工具 ──
        tool = self.tool_map.get(tool_name)
        if not tool:
            return self._error_result(
                tool_call_id, f"Unknown tool: {tool_name}",
            )

        # ── 2. 安全检查: 是否需要用户确认 (§4.4) ──
        if self.policy.requires_confirmation(tool_name, tool_args):
            if task_handle:
                user_input = await self._wait_for_confirmation(
                    block, task_handle,
                )
                if user_input.action == "reject":
                    return self._error_result(
                        tool_call_id,
                        f"Tool execution rejected by user: {user_input.text or '无原因'}",
                    )
            # 无 task_handle (测试模式) → 跳过确认

        # ── 3. 发射 TOOL_START 事件 ──
        await self.emitter.emit_tool_start(
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            input_json=json.dumps(tool_args, ensure_ascii=False),
        )

        # ── 4. 执行 (含超时保护) ──
        timeout = min(tool_args.get("timeout", 30), 300)
        start = time.monotonic()

        try:
            raw_result = await asyncio.wait_for(
                tool.execute(tool_args, sandbox=self.sandbox),
                timeout=timeout,
            )
            success = True
        except asyncio.TimeoutError:
            raw_result = f"Tool {tool_name} timed out after {timeout}s"
            success = False
        except ToolExecutionError as e:
            raw_result = str(e)
            success = False
        except Exception as e:
            raw_result = f"Unexpected error: {type(e).__name__}: {e}"
            success = False
            logger.exception("tool_unexpected_error", tool=tool_name)

        duration_ms = int((time.monotonic() - start) * 1000)

        # ── 5. 输出截断 (防止撑爆上下文) ──
        truncated_result = _truncate(raw_result, max_chars=8000)
        was_truncated = len(raw_result) > 8000

        # ── 6. 发射 TOOL_RESULT 事件 ──
        await self.emitter.emit_tool_result(
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            success=success,
            output=_truncate(raw_result, max_chars=10_000),  # 事件中保留更多
            duration_ms=duration_ms,
        )

        # ── 7. 封装为 LLM tool_result 格式 ──
        content = truncated_result
        if was_truncated:
            content += "\n\n[Output truncated. Use offset/limit for large files.]"

        logger.info("tool_executed",
                     tool=tool_name, success=success,
                     duration_ms=duration_ms,
                     output_chars=len(raw_result),
                     truncated=was_truncated)

        return {
            "type": "tool_result",
            "tool_use_id": tool_call_id,
            "content": content,
            "is_error": not success,
        }

    def _error_result(self, tool_call_id: str, message: str) -> dict:
        return {
            "type": "tool_result",
            "tool_use_id": tool_call_id,
            "content": message,
            "is_error": True,
        }

    # ── 并行执行模式 (Phase 2) ──

    async def execute_tool_calls_parallel(
        self, response, task_handle=None,
    ) -> list[dict]:
        """并行执行不需要确认的工具, 串行处理需要确认的工具。

        LLM 可能一次返回多个 tool_use (如同时 read 多个文件),
        并行执行可以大幅降低总延迟。
        """
        confirm_blocks = []
        parallel_blocks = []

        for block in response.content:
            if block.type != "tool_use":
                continue
            if self.policy.requires_confirmation(block.name, block.input):
                confirm_blocks.append(block)
            else:
                parallel_blocks.append(block)

        # 1. 并行执行不需确认的工具
        parallel_tasks = [
            self._execute_one(block, task_handle)
            for block in parallel_blocks
        ]
        parallel_results = await asyncio.gather(
            *parallel_tasks, return_exceptions=True,
        )

        results = []
        for i, r in enumerate(parallel_results):
            if isinstance(r, Exception):
                results.append(self._error_result(
                    parallel_blocks[i].id,
                    f"Parallel execution error: {r}",
                ))
            else:
                results.append(r)

        # 2. 串行处理需确认的工具 (用户需逐个审批)
        for block in confirm_blocks:
            result = await self._execute_one(block, task_handle)
            results.append(result)

        return results
```

### 7.9 输出截断策略

工具输出直接进入 LLM 上下文，过大的输出会浪费 token 甚至超出窗口。截断策略需要在**保留有用信息**和**控制大小**之间平衡：

```python
# sahara_runtime/tools/truncate.py

def _truncate(text: str, max_chars: int = 8000) -> str:
    """智能截断: 保留头尾, 中间用省略号。

    为什么保留头尾?
    - 头部: 通常是程序输出的 header、错误信息的类型
    - 尾部: 通常是最终结果、错误的 stack trace 根因
    - 中间: 通常是重复的日志行 (价值最低)
    """
    if len(text) <= max_chars:
        return text

    half = max_chars // 2
    head = text[:half]
    tail = text[-half:]
    omitted = len(text) - max_chars

    return (
        f"{head}\n\n"
        f"... [{omitted:,} characters omitted] ...\n\n"
        f"{tail}"
    )
```

**各工具的截断上限**：

| 场景 | 上限 | 理由 |
| --- | --- | --- |
| **工具返回 → LLM 上下文** | 8,000 字符 | LLM 上下文寸土寸金, 过大的结果会挤占历史消息空间 |
| **工具返回 → Event Bus 事件** | 10,000 字符 | 事件可存档, 保留更多细节供调试 |
| **read 工具** | 8,000 字符 | 大文件应使用 offset+limit 分段读取 |
| **exec 工具** | 8,000 字符 | 命令输出; 过大时建议重定向到文件再 read |

### 7.10 完整消息流示例

以一个"读取文件并修复 bug"的场景展示完整的消息流：

```text
messages 数组的演变:

─── 轮次 1: 用户提交任务 ───────────────────────────────────────────

  messages = [
    { role: "user",
      content: "请修复 main.py 中的 TypeError" }
  ]

  → LLM 调用 (tools=[exec, read, write], system=..., messages=上面)
  → LLM 返回: stop_reason="tool_use"

  messages = [
    { role: "user", content: "请修复 main.py 中的 TypeError" },
    { role: "assistant",
      content: [
        { type: "text", text: "让我先查看一下文件内容。" },
        { type: "tool_use", id: "toolu_001", name: "read",
          input: { path: "/workspace/main.py" } },
      ] },
  ]

─── 轮次 2: 执行 read 工具, 回传结果 ──────────────────────────────

  Runtime: read("/workspace/main.py") → "     1|import os\n     2|..."

  messages = [
    ...(上面的),
    { role: "user",                              ← tool_result 在 user 角色
      content: [
        { type: "tool_result", tool_use_id: "toolu_001",
          content: "     1|import os\n     2|def main():\n     3|    x = '1' + 2  # TypeError\n..." },
      ] },
  ]

  → LLM 调用 (同一 tools, 同一 system, messages=上面)
  → LLM 返回: stop_reason="tool_use"

  messages = [
    ...(上面的),
    { role: "assistant",
      content: [
        { type: "text", text: "找到了第 3 行的 TypeError，`'1' + 2` 应改为 `int('1') + 2`。" },
        { type: "tool_use", id: "toolu_002", name: "write",
          input: { path: "/workspace/main.py",
                   content: "import os\ndef main():\n    x = int('1') + 2\n..." } },
      ] },
  ]

─── 轮次 3: 执行 write 工具, 回传结果 ─────────────────────────────

  Runtime: write("/workspace/main.py", ...) → "File written: /workspace/main.py (5 lines)"

  messages = [
    ...(上面的),
    { role: "user",
      content: [
        { type: "tool_result", tool_use_id: "toolu_002",
          content: "File written: /workspace/main.py (5 lines)" },
      ] },
  ]

  → LLM 调用
  → LLM 返回: stop_reason="end_turn"  ← 任务完成, 不再调用工具

  messages = [
    ...(上面的),
    { role: "assistant",
      content: "已修复 main.py 第 3 行的 TypeError。将 `'1' + 2` 改为 `int('1') + 2`。" },
  ]

─── 循环结束, 发射 RUN_COMPLETE 事件 ──────────────────────────────
```

### 7.11 安全策略

| 安全层 | 措施 | 说明 |
| --- | --- | --- |
| **沙箱隔离** | 生产环境工具全部在 Docker 容器内执行 | 即使命令恶意也只影响容器 |
| **超时保护** | 每个工具调用独立超时 (默认 30s, 上限 5min) | 防止工具挂死阻塞 Agent Loop |
| **输出截断** | 头尾保留策略, 上限 8000 字符 | 防止巨大输出撑爆上下文 |
| **确认机制** | 高危操作需用户确认 (§4.4 ToolPolicy) | `rm -rf`、`sudo` 等需要审批 |
| **策略过滤** | allowlist/blocklist 控制可用工具集 | 不同 Agent 可配置不同工具权限 |
| **开发模式保护** | 非沙箱模式下限制命令白名单 | 开发环境无 Docker 时的最低保护 |
| **路径限制** | read/write 限制在 /workspace 目录下 | 防止读写容器系统文件 |

```python
# sahara_runtime/tools/file_tools.py — 路径安全检查

WORKSPACE_ROOT = "/workspace"

def _validate_path(path: str) -> str:
    """确保路径在 /workspace 内, 防止路径遍历攻击。"""
    resolved = os.path.realpath(path)
    if not resolved.startswith(WORKSPACE_ROOT):
        raise ToolExecutionError(
            f"Access denied: path must be within {WORKSPACE_ROOT}"
        )
    return resolved
```

---

## 八、系统提示词构建

### 8.1 职责与设计原则

系统提示词是 Agent 行为的"宪法"——决定了 Agent 的身份、能力边界和行为规范。`PromptBuilder` 负责将多个**段落 (Segment)** 动态组装为一个完整的 `system` 字符串。

**核心设计原则：**

| 原则 | 说明 |
| --- | --- |
| **段落有序** | 段落顺序固定——LLM 从上往下阅读，越靠前的指令权重越高 |
| **前缀稳定** | 靠前的段落尽量不变，最大化 LLM Provider Prompt Cache 命中率（§6.2） |
| **工具优先于技能** | 工具是原子能力（LLM 直接调用），技能是组合指南（LLM 读后再用工具执行）；系统提示词中工具指南在前、技能列表在后 |
| **关注分离** | 每个段落只负责一件事，便于独立更新和缓存 |
| **输出可预测** | 同一 Agent + 同一工具集 + 同一技能集 → 应该生成一致的系统提示词 |

### 8.2 段落组成与排列顺序

```text
系统提示词全景:

  ┌─────────────────────────────────────────────────────────────────────┐
  │  system_prompt (传入 LLM API 的 system 参数)                       │
  │                                                                     │
  │  ┌───────────────────────────────────────────────────────────────┐ │
  │  │  § 段落 1: 身份与角色 (Identity)                    [稳定]    │ │
  │  │  "你是 Sahara AI 助手，一个..."                               │ │
  │  ├───────────────────────────────────────────────────────────────┤ │
  │  │  § 段落 2: 安全与行为规则 (Safety)                  [静态]    │ │
  │  │  "<safety_rules>...</safety_rules>"                           │ │
  │  ├───────────────────────────────────────────────────────────────┤ │
  │  │  § 段落 3: 工具使用指南 (Tool Guide)                [较稳定]  │ │
  │  │  "<tool_instructions>...</tool_instructions>"                 │ │
  │  ├───────────────────────────────────────────────────────────────┤ │
  │  │  § 段落 4: 技能列表 (Skills)                        [较稳定]  │ │
  │  │  "<available_skills>...</available_skills>"                   │ │
  │  ├───────────────────────────────────────────────────────────────┤ │
  │  │  § 段落 5: 沙箱环境信息 (Sandbox)                   [较稳定]  │ │
  │  │  "<sandbox_info>...</sandbox_info>"                           │ │
  │  ├───────────────────────────────────────────────────────────────┤ │
  │  │  § 段落 6: Agent 上下文文件 (Context Files)         [每 Agent]│ │
  │  │  "<context_file name='AGENTS.md'>...</context_file>"         │ │
  │  ├───────────────────────────────────────────────────────────────┤ │
  │  │  § 段落 7: 用户自定义指令 (Custom Instructions)     [每 Agent]│ │
  │  │  "<custom_instructions>...</custom_instructions>"             │ │
  │  ├───────────────────────────────────────────────────────────────┤ │
  │  │  § 段落 8: 运行时信息 (Runtime Info)                [每次变]  │ │
  │  │  "<runtime_info>日期、用户、会话元数据</runtime_info>"        │ │
  │  └───────────────────────────────────────────────────────────────┘ │
  │                                                                     │
  │  排列原则: 稳定的在前 (缓存友好) → 动态的在后                      │
  └─────────────────────────────────────────────────────────────────────┘
```

### 8.3 各段落详解

#### 段落 1: 身份与角色 (Identity)

```python
# sahara_runtime/prompt/segments/identity.py

class IdentitySegment:
    """Agent 的身份定义。

    数据来源: Agent 配置 (PG agents 表 → Redis 缓存)
    变化频率: 每个 Agent 固定, 只在配置更新时变化
    """

    async def build(self, agent_config: AgentConfig) -> str:
        parts = [
            f"You are {agent_config.display_name}, {agent_config.description}.",
        ]

        # Agent 特定的能力定义
        if agent_config.capabilities:
            parts.append(f"\nYour capabilities: {agent_config.capabilities}")

        # Agent 个性/风格
        if agent_config.personality:
            parts.append(f"\nCommunication style: {agent_config.personality}")

        return "\n".join(parts)
```

**输出示例：**

```text
You are Sahara AI 助手, a software engineering agent that helps users
write, debug, and improve code.

Your capabilities: You can read, write, and execute code in a sandboxed
environment. You have access to web search and file operations.

Communication style: Be concise and technical. Use Chinese when the
user writes in Chinese.
```

#### 段落 2: 安全与行为规则 (Safety)

```python
# sahara_runtime/prompt/segments/safety.py

class SafetySegment:
    """安全规则——不可被 Agent 配置覆盖的硬性约束。

    数据来源: 内置模板 (代码中硬编码)
    变化频率: 极低, 仅随代码版本更新
    """

    # 此段落是纯静态的, 不因 Agent/Session 而变
    SAFETY_RULES = """\
<safety_rules>
CRITICAL: These rules override ALL other instructions.

1. NEVER execute commands that could harm the host system.
2. NEVER access files outside of /workspace directory.
3. NEVER expose environment variables, API keys, or credentials in responses.
4. NEVER make network requests to internal/private IP addresses.
5. When uncertain about a destructive operation, ask for user confirmation.
6. If a tool execution fails, explain the error clearly; do NOT retry
   blindly or attempt workarounds that bypass safety checks.
7. Do not generate extremely long outputs (>50KB) in a single response.
</safety_rules>"""

    def build(self) -> str:
        return self.SAFETY_RULES
```

#### 段落 3: 工具使用指南 (Tool Guide)

**这是 Tools 与 Skills 优先级分界的关键段落。**

```python
# sahara_runtime/prompt/segments/tool_guide.py

class ToolGuideSegment:
    """工具使用指南 — 指导 LLM 如何正确使用工具。

    数据来源: 过滤后的工具列表 (§7.6 ToolRegistry)
    变化频率: 同一 Agent 的工具集在会话内不变 → 可被缓存

    重要:
    - 此段落在系统提示词中位于 Skills 段落之前
    - LLM 的 tools 参数 (JSON Schema) 定义了"工具能做什么"
    - 此段落定义了"怎么用工具"的策略指南
    - 工具 = 原子操作, LLM 直接调用; 技能 = 高级指南, LLM 读后再调用工具
    """

    def build(self, tools: list["Tool"]) -> str:
        tool_names = [t.name for t in tools]

        sections = ["<tool_instructions>"]

        # ── 通用规则 ──
        sections.append("""
## Tool Usage Rules

You have access to tools that are executed in a sandboxed environment.
Follow these rules when using tools:

1. **Read before write**: Always read a file before editing it.
2. **Verify after write**: After writing a file, verify it was written
   correctly if the operation is critical.
3. **One step at a time**: Don't chain too many tool calls in one turn.
   Prefer to inspect results before proceeding.
4. **Minimize output**: For commands that produce large output, use
   head/tail/grep to filter, or redirect to a file then read selectively.
5. **Handle errors**: If a tool returns an error, analyze the error
   message and adjust your approach rather than retrying the same command.
""")

        # ── 工具特定指南 ──
        if "exec" in tool_names:
            sections.append("""
### exec (Shell Command)
- Working directory: /workspace
- Commands run in bash.
- Long-running commands: set a reasonable timeout.
- Install packages with pip/npm as needed.
- Use `set -e` for multi-command scripts to fail fast.
""")

        if "read" in tool_names:
            sections.append("""
### read (File Read)
- Returns file content with line numbers (e.g., "  1|import os").
- For large files (>500 lines), use offset + limit to read in chunks.
- Line numbers in output are metadata — do not include them when writing.
""")

        if "write" in tool_names:
            sections.append("""
### write (File Write)
- Overwrites the entire file. Make sure to include ALL content.
- Parent directories are created automatically.
- For small edits, prefer using the edit tool (when available).
""")

        if "edit" in tool_names:
            sections.append("""
### edit (Precise Edit)
- Replaces old_string with new_string in a file.
- old_string must match exactly (including whitespace/indentation).
- Include enough context in old_string to make the match unique.
""")

        sections.append("</tool_instructions>")
        return "\n".join(sections)
```

#### 段落 4: 技能列表 (Skills)

```python
# sahara_runtime/prompt/segments/skills.py

class SkillsSegment:
    """技能列表 — 可选的高级能力。

    数据来源: 过滤后的技能列表 (§10.5 SkillFilter)
    变化频率: 同一 Agent 的技能集在会话内不变 → 可被缓存

    重要:
    - 此段落排在工具指南之后
    - 技能不是工具: LLM 不会直接"调用"技能
    - 技能是指南: LLM 先用 read 工具读取 SKILL.md, 再按指南用工具执行
    """

    def build(self, skill_entries: list["SkillEntry"]) -> str:
        # 调用 §10.6 的 build_skills_prompt
        from sahara_runtime.skills.prompt import build_skills_prompt
        return build_skills_prompt(skill_entries)
```

#### 段落 5: 沙箱环境信息 (Sandbox)

```python
# sahara_runtime/prompt/segments/sandbox.py

class SandboxSegment:
    """沙箱环境描述——告诉 LLM 它的运行环境。

    数据来源: Sandbox 对象 (容器元信息)
    变化频率: 同一沙箱生命周期内不变
    """

    async def build(self, sandbox: "Sandbox") -> str:
        if not sandbox or not sandbox.enabled:
            return "<sandbox_info>No sandbox available. Running in local mode.</sandbox_info>"

        # 获取沙箱环境信息
        env_info = await sandbox.get_environment_info()

        return f"""\
<sandbox_info>
You are running inside a sandboxed Docker container.

- OS: {env_info.os} ({env_info.arch})
- Working directory: /workspace
- User: {env_info.user}
- Available languages: {', '.join(env_info.languages)}
- Available package managers: {', '.join(env_info.package_managers)}
- Network: {'enabled' if env_info.network_enabled else 'disabled (air-gapped)'}
- Writable paths: /workspace, /tmp
- Read-only paths: /usr, /bin, /lib (system directories)

Files you create/modify persist within this session's sandbox.
</sandbox_info>"""
```

**输出示例：**

```xml
<sandbox_info>
You are running inside a sandboxed Docker container.

- OS: Ubuntu 22.04 (amd64)
- Working directory: /workspace
- User: sandbox
- Available languages: python3.11, node20, go1.22
- Available package managers: pip, npm, apt
- Network: enabled
- Writable paths: /workspace, /tmp
- Read-only paths: /usr, /bin, /lib (system directories)

Files you create/modify persist within this session's sandbox.
</sandbox_info>
```

#### 段落 6: Agent 上下文文件 (Context Files)

```python
# sahara_runtime/prompt/segments/context_files.py

class ContextFilesSegment:
    """Agent 的上下文文件 — 如 AGENTS.md、.cursorrules 等。

    数据来源: Agent 配置中指定的上下文文件列表 → 从沙箱/工作空间读取
    变化频率: 文件内容可能随时变化, 每次构建时重新读取
    """

    MAX_CONTEXT_FILE_SIZE = 10_000  # 单个文件最大字符数

    async def build(self, agent_config: AgentConfig, sandbox: "Sandbox") -> str:
        file_paths = agent_config.context_files or []  # ["AGENTS.md", ".cursorrules"]

        if not file_paths:
            return ""

        parts = []
        for path in file_paths:
            try:
                if sandbox and sandbox.enabled:
                    content = await sandbox.read_file(f"/workspace/{path}")
                else:
                    content = open(path, "r").read()

                # 截断过大的文件
                if len(content) > self.MAX_CONTEXT_FILE_SIZE:
                    content = content[:self.MAX_CONTEXT_FILE_SIZE] + \
                        f"\n\n[Truncated: file exceeds {self.MAX_CONTEXT_FILE_SIZE} chars]"

                parts.append(
                    f'<context_file name="{path}">\n{content}\n</context_file>'
                )
            except FileNotFoundError:
                logger.warning("context_file_not_found", path=path)
                continue

        return "\n\n".join(parts)
```

#### 段落 7: 用户自定义指令 (Custom Instructions)

```python
# sahara_runtime/prompt/segments/custom_instructions.py

class CustomInstructionsSegment:
    """用户级别的自定义指令 — 来自 User 配置或 Agent 配置。

    数据来源: User 配置 (user_preferences.custom_instructions) + Agent 配置
    变化频率: 低, 用户手动修改时变化
    """

    async def build(self, agent_config: AgentConfig, user_prefs: "UserPreferences | None") -> str:
        parts = []

        # Agent 级自定义指令
        if agent_config.custom_instructions:
            parts.append(agent_config.custom_instructions)

        # 用户级自定义指令 (追加, 不覆盖)
        if user_prefs and user_prefs.custom_instructions:
            parts.append(user_prefs.custom_instructions)

        if not parts:
            return ""

        combined = "\n\n".join(parts)
        return f"<custom_instructions>\n{combined}\n</custom_instructions>"
```

**输出示例：**

```xml
<custom_instructions>
- 始终使用简体中文进行回复。
- 默认以"资深工程负责人 / CTO"视角回答。
- 代码注释使用英文。
</custom_instructions>
```

#### 段落 8: 运行时信息 (Runtime Info)

```python
# sahara_runtime/prompt/segments/runtime_info.py

class RuntimeInfoSegment:
    """运行时动态信息——每次请求都不同。

    数据来源: 系统时间、请求上下文
    变化频率: 每次请求
    注意: 此段落放在最后, 对 Prompt Cache 影响最小
    """

    def build(self, session_meta: "SessionMeta", user_info: "UserInfo | None") -> str:
        now = datetime.now(timezone.utc)

        parts = [
            "<runtime_info>",
            f"Current date: {now.strftime('%A, %B %d, %Y')}",
            f"Current time (UTC): {now.strftime('%H:%M:%S')}",
        ]

        if user_info:
            parts.append(f"User: {user_info.display_name or user_info.user_id}")
            if user_info.timezone:
                local_time = now.astimezone(ZoneInfo(user_info.timezone))
                parts.append(f"User local time: {local_time.strftime('%H:%M %Z')}")
            if user_info.locale:
                parts.append(f"User locale: {user_info.locale}")

        parts.append(f"Session: {session_meta.session_id}")
        parts.append(f"Workspace: {session_meta.workspace_path or '/workspace'}")

        parts.append("</runtime_info>")
        return "\n".join(parts)
```

### 8.4 完整构建流程

```python
# sahara_runtime/prompt/builder.py

@dataclass
class PromptSegment:
    """一个提示词段落。"""
    name: str           # 段落标识 (用于缓存 key 和日志)
    content: str        # 段落内容
    priority: int       # 段落排序优先级 (越小越靠前)
    cacheable: bool     # 是否可缓存 (与 LLM Prompt Cache 无关, 是 Runtime 本地缓存)
    char_count: int     # 字符数 (用于预算控制)


class PromptBuilder:
    """系统提示词构建器。组装所有段落, 控制总大小, 输出完整 system prompt。"""

    # 段落顺序 (priority 值)
    SEGMENT_ORDER = {
        "identity": 10,
        "safety": 20,
        "tool_guide": 30,
        "skills": 40,
        "sandbox": 50,
        "context_files": 60,
        "custom_instructions": 70,
        "runtime_info": 80,
    }

    # 系统提示词总字符上限 (约 ~25K tokens)
    MAX_SYSTEM_PROMPT_CHARS = 100_000

    def __init__(
        self,
        identity_segment: IdentitySegment,
        safety_segment: SafetySegment,
        tool_guide_segment: ToolGuideSegment,
        skills_segment: SkillsSegment,
        sandbox_segment: SandboxSegment,
        context_files_segment: ContextFilesSegment,
        custom_instructions_segment: CustomInstructionsSegment,
        runtime_info_segment: RuntimeInfoSegment,
        cache: "SegmentCache | None" = None,
    ):
        self._segments_builders = {
            "identity": identity_segment,
            "safety": safety_segment,
            "tool_guide": tool_guide_segment,
            "skills": skills_segment,
            "sandbox": sandbox_segment,
            "context_files": context_files_segment,
            "custom_instructions": custom_instructions_segment,
            "runtime_info": runtime_info_segment,
        }
        self._cache = cache

    async def build(
        self,
        agent_config: AgentConfig,
        tools: list["Tool"],
        skill_entries: list["SkillEntry"],
        sandbox: "Sandbox | None",
        session_meta: "SessionMeta",
        user_info: "UserInfo | None" = None,
        user_prefs: "UserPreferences | None" = None,
    ) -> str:
        """构建完整的系统提示词。

        流程:
        1. 逐段构建 (可缓存段落走缓存)
        2. 按优先级排序
        3. 预算控制 (超出上限时裁剪低优先级段落)
        4. 拼接输出
        """

        segments: list[PromptSegment] = []

        # ── 1. 构建各段落 ──

        # 段落 1: 身份 [可缓存, key=agent_id]
        segments.append(await self._build_segment(
            "identity",
            self._segments_builders["identity"].build(agent_config),
            cacheable=True,
            cache_key=f"prompt:identity:{agent_config.agent_id}",
        ))

        # 段落 2: 安全规则 [可缓存, 全局]
        segments.append(await self._build_segment(
            "safety",
            self._segments_builders["safety"].build(),
            cacheable=True,
            cache_key="prompt:safety:global",
        ))

        # 段落 3: 工具指南 [可缓存, key=工具列表hash]
        tool_hash = self._hash_tool_names(tools)
        segments.append(await self._build_segment(
            "tool_guide",
            self._segments_builders["tool_guide"].build(tools),
            cacheable=True,
            cache_key=f"prompt:tool_guide:{tool_hash}",
        ))

        # 段落 4: 技能列表 [可缓存, key=技能列表hash]
        if skill_entries:
            skill_hash = self._hash_skill_names(skill_entries)
            segments.append(await self._build_segment(
                "skills",
                self._segments_builders["skills"].build(skill_entries),
                cacheable=True,
                cache_key=f"prompt:skills:{skill_hash}",
            ))

        # 段落 5: 沙箱环境 [可缓存, key=sandbox_id]
        if sandbox and sandbox.enabled:
            segments.append(await self._build_segment(
                "sandbox",
                await self._segments_builders["sandbox"].build(sandbox),
                cacheable=True,
                cache_key=f"prompt:sandbox:{sandbox.sandbox_id}",
            ))

        # 段落 6: 上下文文件 [不缓存, 文件内容可能变化]
        ctx_content = await self._segments_builders["context_files"].build(
            agent_config, sandbox,
        )
        if ctx_content:
            segments.append(PromptSegment(
                name="context_files",
                content=ctx_content,
                priority=self.SEGMENT_ORDER["context_files"],
                cacheable=False,
                char_count=len(ctx_content),
            ))

        # 段落 7: 自定义指令 [不缓存]
        custom_content = await self._segments_builders["custom_instructions"].build(
            agent_config, user_prefs,
        )
        if custom_content:
            segments.append(PromptSegment(
                name="custom_instructions",
                content=custom_content,
                priority=self.SEGMENT_ORDER["custom_instructions"],
                cacheable=False,
                char_count=len(custom_content),
            ))

        # 段落 8: 运行时信息 [不缓存, 每次请求不同]
        segments.append(PromptSegment(
            name="runtime_info",
            content=self._segments_builders["runtime_info"].build(
                session_meta, user_info,
            ),
            priority=self.SEGMENT_ORDER["runtime_info"],
            cacheable=False,
            char_count=0,  # 稍后计算
        ))

        # ── 2. 排序 ──
        segments.sort(key=lambda s: s.priority)

        # ── 3. 预算控制 ──
        segments = self._apply_budget(segments)

        # ── 4. 拼接 ──
        result = "\n\n".join(s.content for s in segments if s.content)

        logger.info("system_prompt_built",
                     agent_id=agent_config.agent_id,
                     segments=[s.name for s in segments],
                     total_chars=len(result),
                     segment_chars={s.name: s.char_count for s in segments})

        return result

    def _apply_budget(self, segments: list[PromptSegment]) -> list[PromptSegment]:
        """预算控制: 超出上限时, 从最低优先级开始裁剪。

        策略:
        - 身份、安全、工具指南: 不可裁剪 (核心功能)
        - 技能、沙箱: 可截断
        - 上下文文件、自定义指令、运行时信息: 可裁剪
        """
        total = sum(s.char_count for s in segments)
        if total <= self.MAX_SYSTEM_PROMPT_CHARS:
            return segments

        # 从优先级最低的段落开始裁剪
        overflow = total - self.MAX_SYSTEM_PROMPT_CHARS
        non_critical = ["runtime_info", "custom_instructions", "context_files", "skills"]

        for name in non_critical:
            if overflow <= 0:
                break
            seg = next((s for s in segments if s.name == name), None)
            if seg and seg.char_count > 0:
                # 截断或移除
                if seg.char_count <= overflow:
                    overflow -= seg.char_count
                    seg.content = ""
                    seg.char_count = 0
                    logger.warning("prompt_segment_removed", segment=name)
                else:
                    seg.content = seg.content[:seg.char_count - overflow]
                    seg.content += f"\n[Truncated: system prompt budget exceeded]"
                    seg.char_count = len(seg.content)
                    overflow = 0
                    logger.warning("prompt_segment_truncated", segment=name)

        return [s for s in segments if s.content]

    async def _build_segment(
        self, name: str, content_or_coro, cacheable: bool, cache_key: str,
    ) -> PromptSegment:
        """构建段落, 支持缓存。"""
        # 尝试从缓存读取
        if cacheable and self._cache:
            cached = await self._cache.get(cache_key)
            if cached:
                return PromptSegment(
                    name=name, content=cached,
                    priority=self.SEGMENT_ORDER[name],
                    cacheable=True, char_count=len(cached),
                )

        # 构建
        if asyncio.iscoroutine(content_or_coro):
            content = await content_or_coro
        else:
            content = content_or_coro

        # 写入缓存
        if cacheable and self._cache and content:
            await self._cache.set(cache_key, content, ttl=300)  # 5 分钟 TTL

        return PromptSegment(
            name=name, content=content,
            priority=self.SEGMENT_ORDER[name],
            cacheable=cacheable, char_count=len(content) if content else 0,
        )
```

### 8.5 构建流程时序图

```text
Agent Loop 步骤 4 — 构建系统提示词:

  run_agent_loop()
       │
       │  ── 前置步骤 ──
       │  1. 加载 Session
       │  2. 解析 Model
       │  3. 分配沙箱
       │
       ▼
  PromptBuilder.build(agent_config, tools, skills, sandbox, ...)
       │
       ├─→ [并行] 构建可缓存段落 (走 Redis 缓存)
       │    ├── IdentitySegment.build(agent_config)
       │    ├── SafetySegment.build()
       │    ├── ToolGuideSegment.build(tools)
       │    ├── SkillsSegment.build(skill_entries)
       │    └── SandboxSegment.build(sandbox)
       │
       ├─→ [并行] 构建动态段落
       │    ├── ContextFilesSegment.build(agent_config, sandbox)
       │    │    └── sandbox.read_file("AGENTS.md") → 读沙箱文件
       │    ├── CustomInstructionsSegment.build(agent_config, user_prefs)
       │    └── RuntimeInfoSegment.build(session_meta, user_info)
       │
       ├─→ 按 priority 排序
       │
       ├─→ 预算控制 (总计 ≤ 100K 字符)
       │    └── 超出时从低优先级段落裁剪
       │
       └─→ 返回完整 system_prompt 字符串
              │
              ▼
       传入 LLM API:
       client.messages.create(
           system=system_prompt,    ← 这里
           messages=messages,
           tools=tool_definitions,  ← §7.7 工具 Schema
       )
```

### 8.6 与 Agent Loop 的集成

```python
# sahara_runtime/agent_loop.py — 步骤 4 (更新后)

async def run_agent_loop(deps, task_handle, ...):
    ...

    # ── 3. 沙箱分配 ──
    sandbox = await deps.sandbox_manager.acquire(session_key)

    # ── 4a. 加载 + 过滤技能 ──
    all_skills = await deps.skill_loader.load_all()
    filtered_skills = deps.skill_filter.filter(all_skills, deps.config)

    # ── 4b. 同步技能到沙箱 (LLM 的 read 工具需要能读到 SKILL.md) ──
    if sandbox and sandbox.enabled and filtered_skills:
        await sync_skills_to_sandbox(filtered_skills, sandbox)

    # ── 5. 工具集 ──
    tools = await deps.tool_registry.create_tools(
        agent_id=agent_id, sandbox=sandbox, session_key=session_key,
    )
    tool_definitions = [t.to_llm_schema() for t in tools]

    # ── 4c. 构建系统提示词 (需要 tools + skills 都准备好) ──
    system_prompt = await deps.prompt_builder.build(
        agent_config=agent_config,
        tools=tools,                  # → 段落 3: 工具指南
        skill_entries=filtered_skills, # → 段落 4: 技能列表
        sandbox=sandbox,              # → 段落 5: 沙箱环境
        session_meta=session_meta,
        user_info=user_info,
        user_prefs=user_prefs,
    )

    # ── 6. 注入用户消息 ──
    messages.append({"role": "user", "content": user_message})

    # ── 7. RUN_START + 工具执行器 ──
    ...
```

### 8.7 缓存策略与 LLM Prompt Cache 的关系

**两层缓存，分别解决不同问题：**

```text
┌──────────────────────────────────────────────────────────────────┐
│  Layer 1: Runtime 本地段落缓存 (SegmentCache → Redis)            │
│                                                                  │
│  目的: 减少段落构建开销 (避免重复读 PG/沙箱)                     │
│  Key:  prompt:{segment}:{hash}                                   │
│  TTL:  5 分钟                                                    │
│  命中场景: 同一 Agent 的多次请求, 段落内容不变                   │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  identity:agent_123  → "You are Sahara AI..."  (5min TTL) │ │
│  │  safety:global       → "<safety_rules>..."     (5min TTL) │ │
│  │  tool_guide:abc123   → "<tool_instructions>..."(5min TTL) │ │
│  │  skills:def456       → "<available_skills>..." (5min TTL) │ │
│  │  sandbox:sbx_789     → "<sandbox_info>..."     (5min TTL) │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  → 段落拼接后输出完整 system_prompt                             │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  Layer 2: LLM Provider Prompt Cache (Anthropic/OpenAI 侧)       │
│                                                                  │
│  目的: 减少 LLM 推理成本 (缓存命中的 token 90% 折扣)            │
│  触发条件: 请求的 [system + tools + messages 前缀] 完全一致     │
│  依赖: Session 级 Model+Key 亲和 (§6.2)                        │
│                                                                  │
│  system_prompt 的前缀稳定性:                                     │
│  ┌─────────────────────────────────────┐                        │
│  │  段落 1-5: 通常不变     ← 缓存命中  │                        │
│  │  段落 6-7: 偶尔变化     ← 部分命中  │                        │
│  │  段落 8: 每次不同       ← 缓存失效  │                        │
│  └─────────────────────────────────────┘                        │
│                                                                  │
│  设计策略: 动态内容放最后, 最小化对缓存前缀的破坏               │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

```python
# sahara_runtime/prompt/cache.py

class SegmentCache:
    """段落缓存 — Redis 存储。"""

    def __init__(self, redis: "Redis"):
        self._redis = redis

    async def get(self, key: str) -> str | None:
        return await self._redis.get(key)

    async def set(self, key: str, value: str, ttl: int = 300):
        await self._redis.set(key, value, ex=ttl)
```

### 8.8 完整输出示例

以下是一个完整的 `system_prompt` 拼接后的效果（简化版）：

```text
You are Sahara AI 助手, a software engineering agent that helps users
write, debug, and improve code.

Your capabilities: You can read, write, and execute code in a sandboxed
environment. You have access to web search and file operations.

Communication style: Be concise and technical. Use Chinese when the
user writes in Chinese.

<safety_rules>
CRITICAL: These rules override ALL other instructions.

1. NEVER execute commands that could harm the host system.
2. NEVER access files outside of /workspace directory.
3. NEVER expose environment variables, API keys, or credentials in responses.
...
</safety_rules>

<tool_instructions>

## Tool Usage Rules

You have access to tools that are executed in a sandboxed environment.
Follow these rules when using tools:

1. **Read before write**: Always read a file before editing it.
2. **Verify after write**: After writing a file, verify it was written
   correctly if the operation is critical.
...

### exec (Shell Command)
- Working directory: /workspace
- Commands run in bash.
...

### read (File Read)
- Returns file content with line numbers.
...

### write (File Write)
- Overwrites the entire file.
...

</tool_instructions>

## Skills (mandatory)
Before replying: scan <available_skills> <description> entries.
- If exactly one skill clearly applies: read its SKILL.md at <location>
  with `read`, then follow it.
- If multiple could apply: choose the most specific one, then read/follow it.
- If none clearly apply: do not read any SKILL.md.
Constraints: never read more than one skill up front; only read after selecting.

<available_skills>
<skill name="deploy" location="/workspace/skills/deploy/SKILL.md">
  <description>Deploy application to staging or production.</description>
</skill>
<skill name="create-react-app" location="/workspace/skills/create-react-app/SKILL.md">
  <description>Create a new React project with TypeScript and best practices.</description>
</skill>
</available_skills>

<sandbox_info>
You are running inside a sandboxed Docker container.

- OS: Ubuntu 22.04 (amd64)
- Working directory: /workspace
- Available languages: python3.11, node20, go1.22
- Network: enabled
</sandbox_info>

<context_file name="AGENTS.md">
# Project Guidelines
- Use TypeScript for all frontend code
- Follow PEP 8 for Python code
...
</context_file>

<custom_instructions>
- 始终使用简体中文进行回复。
- 代码注释使用英文。
</custom_instructions>

<runtime_info>
Current date: Monday, February 9, 2026
Current time (UTC): 14:30:00
User: Zhang Wei
User local time: 22:30 CST
Session: sess_abc123
Workspace: /workspace
</runtime_info>
```

### 8.9 Phase 规划

| Phase | 范围 | 说明 |
| --- | --- | --- |
| **Phase 1** | ★ 基础版 PromptBuilder: 身份 + 安全 + 工具指南 + 运行时信息 | 4 个段落, 无缓存, 无预算控制 |
| **Phase 2** | + 技能段落 + 沙箱段落 + 上下文文件 + 自定义指令 | 完整 8 段落, Redis 段落缓存 |
| **Phase 3** | + 预算控制 + 段落 A/B 测试 + 提示词效果度量 | 根据工具选择准确率优化段落措辞 |

---

## 九、沙箱管理 (Sandbox Manager)

> 底层容器池、安全限制、gVisor/Firecracker 演进等详细设计见 [SANDBOX-DESIGN.md](./SANDBOX-DESIGN.md)（D6）。
> 本节聚焦于 **Runtime 内部的抽象接口设计**，确保 Agent Loop 和 Tools 完全不感知底层隔离技术。

### 9.1 设计目标

**核心原则：面向抽象编程。** Agent Loop、ToolExecutor、Skills 等上层模块只依赖 `Sandbox` 和 `SandboxManager` 两个抽象接口。底层从 Docker 切换到 gVisor、Firecracker、远程沙箱甚至 WASM 时，**上层代码零修改。**

```text
可插拔架构:

  Agent Loop / ToolExecutor / Skills
       │
       │  sandbox.exec(), sandbox.read_file(), sandbox.write_file()
       │  (不感知底层隔离技术)
       ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  Sandbox (抽象接口)                                             │
  │  - exec(command, timeout) → ExecResult                         │
  │  - read_file(path) → str                                       │
  │  - write_file(path, content) → None                            │
  │  - list_files(path) → list[FileInfo]                           │
  │  - upload(local_path, remote_path) → None                      │
  │  - cleanup() → None                                            │
  └──────────────────────────┬──────────────────────────────────────┘
                             │
  ┌──────────────────────────▼──────────────────────────────────────┐
  │  SandboxManager (抽象接口)                                      │
  │  - acquire(session_key, options) → Sandbox                     │
  │  - release(sandbox) → None                                     │
  │  - warmup(pool_size) → None                                    │
  │  - health_check() → HealthStatus                               │
  │                                                                 │
  │  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐        │
  │  │ Docker        │ │ gVisor        │ │ Firecracker   │        │
  │  │ (Phase 1)     │ │ (Phase 2)     │ │ (Phase 3)     │        │
  │  │               │ │               │ │               │        │
  │  │ docker-py     │ │ docker-py     │ │ FC API        │        │
  │  │ ContainerPool │ │ +--runtime=   │ │ microVM Pool  │        │
  │  │               │ │   runsc       │ │               │        │
  │  └───────────────┘ └───────────────┘ └───────────────┘        │
  │                                                                 │
  │  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐        │
  │  │ Remote        │ │ WASM          │ │ Noop          │        │
  │  │ (Phase 3+)    │ │ (实验性)       │ │ (开发/测试)    │        │
  │  └───────────────┘ └───────────────┘ └───────────────┘        │
  └─────────────────────────────────────────────────────────────────┘
```

### 9.2 Sandbox 抽象接口

```python
# sahara_runtime/sandbox/base.py

@dataclass
class ExecResult:
    """命令执行结果。所有后端实现必须统一返回此结构。"""
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    duration_ms: int

@dataclass
class FileInfo:
    """文件元信息。"""
    name: str
    path: str
    is_dir: bool
    size: int
    modified_at: float | None = None


class Sandbox(ABC):
    """沙箱操作接口 — Agent Loop 和 Tools 的唯一依赖。

    所有上层模块 (ToolExecutor, ExecTool, ReadTool, WriteTool, Skills)
    只通过此接口与沙箱交互, 完全不感知底层实现。

    约定:
    - 所有文件操作路径基于 /workspace 根目录
    - exec 在 /workspace 下以非 root 用户执行
    - 异常统一为 SandboxError 子类
    """

    @property
    @abstractmethod
    def enabled(self) -> bool:
        """沙箱是否可用。"""
        ...

    @property
    @abstractmethod
    def sandbox_id(self) -> str:
        """沙箱唯一标识 (容器 ID / VM ID / 实例 ID)。"""
        ...

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """后端类型标识, 用于日志和指标。如 "docker", "gvisor", "firecracker", "noop"。"""
        ...

    # ── 核心操作 ──

    @abstractmethod
    async def exec(self, command: str, timeout: int = 30,
                   workdir: str = "/workspace") -> ExecResult:
        """在沙箱内执行 shell 命令。

        Args:
            command: 要执行的 shell 命令
            timeout: 超时秒数, 超时后 kill 进程
            workdir: 工作目录

        Returns:
            ExecResult: 包含 exit_code, stdout, stderr, timed_out, duration_ms
        """
        ...

    @abstractmethod
    async def read_file(self, path: str) -> str:
        """读取沙箱内文件内容。

        Raises:
            SandboxFileNotFoundError: 文件不存在
            SandboxPermissionError: 无权限
        """
        ...

    @abstractmethod
    async def write_file(self, path: str, content: str) -> None:
        """写入文件到沙箱。

        Raises:
            SandboxPermissionError: 路径不在 /workspace 范围内
            SandboxQuotaError: 磁盘空间不足
        """
        ...

    @abstractmethod
    async def list_files(self, path: str = ".") -> list[FileInfo]:
        """列出沙箱内指定路径的文件和目录。"""
        ...

    # ── 文件传输 ──

    @abstractmethod
    async def upload(self, local_path: str, remote_path: str) -> None:
        """从 Runtime 宿主机上传文件到沙箱内。

        用途: 同步 SKILL.md、配置文件等到 /workspace
        """
        ...

    @abstractmethod
    async def download(self, remote_path: str, local_path: str) -> None:
        """从沙箱下载文件到 Runtime 宿主机。

        用途: 提取构建产物、日志等
        """
        ...

    # ── 生命周期 ──

    @abstractmethod
    async def cleanup(self) -> None:
        """清理沙箱状态 (删除工作文件, kill 残余进程)。"""
        ...


# ── 统一异常体系 ──

class SandboxError(Exception):
    """沙箱操作基础异常。"""
    pass

class SandboxTimeoutError(SandboxError):
    """命令执行超时。"""
    pass

class SandboxFileNotFoundError(SandboxError):
    """沙箱内文件不存在。"""
    pass

class SandboxPermissionError(SandboxError):
    """沙箱权限错误 (路径越界等)。"""
    pass

class SandboxQuotaError(SandboxError):
    """沙箱资源配额超限 (磁盘、进程数等)。"""
    pass

class SandboxUnavailableError(SandboxError):
    """沙箱不可用 (容器崩溃、VM 异常等)。"""
    pass
```

### 9.3 SandboxManager 抽象接口

```python
# sahara_runtime/sandbox/manager.py

@dataclass
class SandboxOptions:
    """沙箱分配选项。不同任务可能需要不同的资源配置。"""
    memory_limit: str | None = None   # 覆盖默认值, 如 "512m"
    cpu_quota: int | None = None      # 覆盖默认值
    timeout: int | None = None        # 单任务最长生存时间 (秒)
    network_enabled: bool = False     # 是否允许网络 (默认隔离)
    extra_mounts: list[str] | None = None  # 额外挂载点 (仅特定后端支持)

@dataclass
class HealthStatus:
    """沙箱管理器健康状态。"""
    healthy: bool
    backend_type: str
    pool_idle: int                  # 空闲沙箱数
    pool_in_use: int                # 使用中沙箱数
    pool_total: int                 # 总沙箱数
    details: dict | None = None     # 后端特定详情


class SandboxManager(ABC):
    """沙箱管理器抽象接口。

    职责:
    - 沙箱生命周期管理 (创建/分配/回收/销毁)
    - 资源池化 (预热、弹性伸缩)
    - 健康检查

    Agent Loop 通过 acquire/release 获取和归还沙箱,
    不感知底层是 Docker 容器池、gVisor、Firecracker microVM 还是远程服务。
    """

    @abstractmethod
    async def acquire(self, session_key: str,
                      options: SandboxOptions | None = None) -> Sandbox:
        """从池中分配一个沙箱。

        Args:
            session_key: 会话标识, 用于隔离工作目录
            options: 可选的资源配置覆盖

        Returns:
            Sandbox: 可用的沙箱实例

        Raises:
            SandboxUnavailableError: 池耗尽或后端不可用
        """
        ...

    @abstractmethod
    async def release(self, sandbox: Sandbox) -> None:
        """清理并归还沙箱到池。

        实现应:
        1. 调用 sandbox.cleanup() 清理工作文件
        2. 重置沙箱状态
        3. 归还到池 (或销毁异常实例)
        """
        ...

    @abstractmethod
    async def warmup(self, pool_size: int) -> None:
        """预热沙箱池。Worker 启动时调用。"""
        ...

    @abstractmethod
    async def shutdown(self) -> None:
        """优雅关闭: 等待使用中沙箱完成, 销毁所有实例。"""
        ...

    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """检查沙箱管理器和后端健康状态。"""
        ...

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """后端类型名称。如 "docker", "gvisor", "firecracker", "noop"。"""
        ...
```

### 9.4 后端实现概览

#### 9.4.1 Docker 容器池 (Phase 1)

```python
# sahara_runtime/sandbox/backends/docker.py

class DockerSandboxManager(SandboxManager):
    """基于 Docker 容器池的沙箱管理器。Phase 1 默认实现。

    特性:
    - ContainerPool 预创建 + checkout/checkin
    - 每个沙箱 = 一个独立 Docker 容器
    - 非 root 用户执行, seccomp + capabilities 限制
    详细设计见 SANDBOX-DESIGN.md §3-§7
    """

    def __init__(self, config: "SandboxConfig"):
        self._config = config
        self._pool = ContainerPool(config)

    async def acquire(self, session_key: str,
                      options: SandboxOptions | None = None) -> Sandbox:
        container = await self._pool.checkout(session_key)
        return DockerSandbox(
            container=container,
            workspace=f"/workspace/{session_key}",
        )

    async def release(self, sandbox: Sandbox) -> None:
        assert isinstance(sandbox, DockerSandbox)
        await sandbox.cleanup()
        await self._pool.checkin(sandbox._container)

    async def warmup(self, pool_size: int) -> None:
        await self._pool.initialize(pool_size)

    async def shutdown(self) -> None:
        await self._pool.shutdown()

    async def health_check(self) -> HealthStatus:
        stats = self._pool.stats()
        return HealthStatus(
            healthy=stats["idle"] > 0 or stats["total"] < self._config.pool_max_total,
            backend_type=self.backend_type,
            pool_idle=stats["idle"],
            pool_in_use=stats["in_use"],
            pool_total=stats["total"],
        )

    @property
    def backend_type(self) -> str:
        return "docker"


class DockerSandbox(Sandbox):
    """Docker 容器沙箱。实现细节见 SANDBOX-DESIGN.md §6。"""

    def __init__(self, container: "Container", workspace: str):
        self._container = container
        self._workspace = workspace

    @property
    def enabled(self) -> bool:
        return True

    @property
    def sandbox_id(self) -> str:
        return self._container.id

    @property
    def backend_type(self) -> str:
        return "docker"

    async def exec(self, command: str, timeout: int = 30,
                   workdir: str = "/workspace") -> ExecResult:
        # 完整实现见 SANDBOX-DESIGN.md §6.2
        ...

    async def read_file(self, path: str) -> str: ...
    async def write_file(self, path: str, content: str) -> None: ...
    async def list_files(self, path: str = ".") -> list[FileInfo]: ...
    async def upload(self, local_path: str, remote_path: str) -> None: ...
    async def download(self, remote_path: str, local_path: str) -> None: ...
    async def cleanup(self) -> None: ...
```

#### 9.4.2 gVisor (Phase 2)

```python
# sahara_runtime/sandbox/backends/gvisor.py

class GVisorSandboxManager(DockerSandboxManager):
    """gVisor 沙箱管理器。继承 Docker 实现, 仅覆盖容器 runtime。

    差异: 容器创建时指定 --runtime=runsc, 提供内核级隔离。
    接口完全一致, 切换仅需修改配置: sandbox.runtime = "gvisor"
    """

    def _container_options(self) -> dict:
        opts = super()._container_options()
        opts["runtime"] = "runsc"  # gVisor 运行时
        return opts

    @property
    def backend_type(self) -> str:
        return "gvisor"
```

#### 9.4.3 Firecracker microVM (Phase 3)

```python
# sahara_runtime/sandbox/backends/firecracker.py

class FirecrackerSandboxManager(SandboxManager):
    """Firecracker microVM 沙箱管理器。

    差异:
    - 底层从 Docker 容器变为 Firecracker microVM
    - 文件传输从 docker cp 变为 virtio-vsock / SSH
    - 每个 VM 独立内核, 隔离级别最高
    - 启动 ~125ms, 内存 ~5MB (极轻量)

    但对上层暴露的 Sandbox 接口完全一致。
    """
    ...
```

#### 9.4.4 远程沙箱 (Phase 3+)

```python
# sahara_runtime/sandbox/backends/remote.py

class RemoteSandboxManager(SandboxManager):
    """远程沙箱管理器。沙箱运行在独立的沙箱集群中。

    适用场景:
    - Runtime 与沙箱分离部署 (安全隔离)
    - 沙箱集群独立伸缩
    - 多 Runtime 共享沙箱池

    通信方式: gRPC (Runtime → Sandbox Service)
    """

    def __init__(self, sandbox_service_url: str):
        self._stub: SandboxServiceStub | None = None
        self._url = sandbox_service_url

    async def acquire(self, session_key: str,
                      options: SandboxOptions | None = None) -> Sandbox:
        resp = await self._stub.Acquire(
            AcquireRequest(session_key=session_key, options=options)
        )
        return RemoteSandbox(
            stub=self._stub,
            sandbox_id=resp.sandbox_id,
        )

    @property
    def backend_type(self) -> str:
        return "remote"


class RemoteSandbox(Sandbox):
    """远程沙箱代理。所有操作通过 gRPC 转发到远程沙箱服务。

    接口一致, Agent Loop 无感知。
    """

    async def exec(self, command: str, timeout: int = 30,
                   workdir: str = "/workspace") -> ExecResult:
        resp = await self._stub.Exec(
            ExecRequest(sandbox_id=self._id, command=command,
                        timeout=timeout, workdir=workdir)
        )
        return ExecResult(
            exit_code=resp.exit_code,
            stdout=resp.stdout,
            stderr=resp.stderr,
            timed_out=resp.timed_out,
            duration_ms=resp.duration_ms,
        )
    ...
```

#### 9.4.5 Noop 沙箱 (开发/测试)

```python
# sahara_runtime/sandbox/backends/noop.py

class NoopSandboxManager(SandboxManager):
    """空操作沙箱管理器, 用于不需要沙箱的开发和测试环境。"""

    async def acquire(self, session_key: str,
                      options: SandboxOptions | None = None) -> Sandbox:
        return NoopSandbox(session_key)

    async def release(self, sandbox: Sandbox) -> None:
        pass

    async def warmup(self, pool_size: int) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def health_check(self) -> HealthStatus:
        return HealthStatus(
            healthy=True, backend_type="noop",
            pool_idle=0, pool_in_use=0, pool_total=0,
        )

    @property
    def backend_type(self) -> str:
        return "noop"


class NoopSandbox(Sandbox):
    """空操作沙箱。直接在宿主机 /tmp 下执行 (仅限开发环境)。"""

    def __init__(self, session_key: str):
        self._id = f"noop-{session_key}"
        self._workspace = f"/tmp/sahara-sandbox/{session_key}"

    @property
    def enabled(self) -> bool:
        return False  # 标记为未启用, Tools 可据此调整行为

    async def exec(self, command: str, timeout: int = 30,
                   workdir: str = "/workspace") -> ExecResult:
        # 直接在宿主机执行 (不安全, 仅限开发)
        proc = await asyncio.create_subprocess_shell(
            command, cwd=self._workspace,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        return ExecResult(
            exit_code=proc.returncode or 0,
            stdout=stdout.decode(),
            stderr=stderr.decode(),
            timed_out=False,
            duration_ms=0,
        )
    ...
```

### 9.5 后端选择工厂

```python
# sahara_runtime/sandbox/factory.py

def create_sandbox_manager(config: "RuntimeConfig") -> SandboxManager:
    """根据配置创建沙箱管理器。

    配置示例:
    - SAHARA_RT_SANDBOX_RUNTIME=docker       (Phase 1 默认)
    - SAHARA_RT_SANDBOX_RUNTIME=gvisor       (Phase 2, 需安装 runsc)
    - SAHARA_RT_SANDBOX_RUNTIME=firecracker  (Phase 3, 需 KVM)
    - SAHARA_RT_SANDBOX_RUNTIME=remote       (Phase 3+, 沙箱独立部署)
    - SAHARA_RT_SANDBOX_RUNTIME=noop         (开发/测试, 无隔离)
    """
    match config.sandbox_runtime:
        case "docker":
            from .backends.docker import DockerSandboxManager
            return DockerSandboxManager(config.sandbox_config)
        case "gvisor":
            from .backends.gvisor import GVisorSandboxManager
            return GVisorSandboxManager(config.sandbox_config)
        case "firecracker":
            from .backends.firecracker import FirecrackerSandboxManager
            return FirecrackerSandboxManager(config.sandbox_config)
        case "remote":
            from .backends.remote import RemoteSandboxManager
            return RemoteSandboxManager(config.sandbox_service_url)
        case "noop":
            from .backends.noop import NoopSandboxManager
            return NoopSandboxManager()
        case _:
            raise ValueError(f"Unknown sandbox runtime: {config.sandbox_runtime}")
```

### 9.6 各后端特性对比

| 特性 | Docker | gVisor | Firecracker | Remote | Noop |
| --- | --- | --- | --- | --- | --- |
| **隔离级别** | 进程级 (namespace/cgroup) | 内核级 (用户态内核) | VM 级 (独立内核) | 取决于远端实现 | 无 |
| **启动延迟** | ~50ms (预热池) | ~50ms (预热池) | ~125ms | ~200ms (网络) | ~0ms |
| **内存开销** | ~10MB/容器 | ~15MB/容器 | ~5MB/VM | N/A (远端) | 0 |
| **安全性** | ★★★ | ★★★★ | ★★★★★ | ★★★★+ | ★ |
| **文件传输** | docker cp | docker cp | virtio-vsock/SSH | gRPC stream | 本地文件系统 |
| **前置条件** | Docker daemon | Docker + runsc | KVM 支持 | 沙箱服务集群 | 无 |
| **适用阶段** | Phase 1 | Phase 2 | Phase 3 | Phase 3+ | 开发/测试 |
| **适用场景** | 通用 | 高安全要求 | 最高安全 + 大规模 | 分离部署 | 本地开发 |

### 9.7 迁移策略

```text
迁移路径 (Runtime 代码零修改):

  Phase 1: Docker
  ┌──────────────────────────────────────────────────────┐
  │  config: SANDBOX_RUNTIME=docker                       │
  │  SandboxManager = DockerSandboxManager               │
  │  Agent Loop 调用 sandbox.exec() / read_file() / ...  │
  └──────────────────────────────────────────────────────┘
        │
        │  仅改配置 + 安装 runsc
        ▼
  Phase 2: gVisor
  ┌──────────────────────────────────────────────────────┐
  │  config: SANDBOX_RUNTIME=gvisor                       │
  │  SandboxManager = GVisorSandboxManager               │
  │  Agent Loop 代码不变, 同样调 sandbox.exec() / ...    │
  └──────────────────────────────────────────────────────┘
        │
        │  实现 FirecrackerSandboxManager + 准备 KVM 环境
        ▼
  Phase 3: Firecracker / Remote
  ┌──────────────────────────────────────────────────────┐
  │  config: SANDBOX_RUNTIME=firecracker                  │
  │       或 SANDBOX_RUNTIME=remote                       │
  │  Agent Loop 代码不变                                  │
  └──────────────────────────────────────────────────────┘

  关键保证:
  ✓ Agent Loop 从始至终只依赖 Sandbox(ABC) 接口
  ✓ 切换后端 = 修改一行配置
  ✓ 无需灰度双写 (与 EventEmitter 不同, 沙箱是独占资源)
```

### 9.8 包结构

```text
sahara_runtime/
  sandbox/
  ├── __init__.py
  ├── base.py                   # Sandbox(ABC), ExecResult, FileInfo, 异常体系
  ├── manager.py                # SandboxManager(ABC), SandboxOptions, HealthStatus
  ├── factory.py                # create_sandbox_manager()
  └── backends/
      ├── __init__.py
      ├── docker.py             # DockerSandboxManager + DockerSandbox (Phase 1)
      ├── gvisor.py             # GVisorSandboxManager (Phase 2, 继承 Docker)
      ├── firecracker.py        # FirecrackerSandboxManager (Phase 3)
      ├── remote.py             # RemoteSandboxManager (Phase 3+)
      └── noop.py               # NoopSandboxManager + NoopSandbox (开发/测试)
```

---

## 十、Skills 管理

### 10.1 概述

Skills（技能）是扩展 Agent 能力的核心机制。每个技能本质上是一个包含 `SKILL.md` 文件的目录，其中定义了技能的描述、使用方法和调用策略。

**关键设计原则：技能 ≠ 工具**。系统中没有以技能命名的工具（如 `weather`、`deploy` 等）。技能是通过**提示词驱动**的两阶段过程——LLM 先读取 `SKILL.md`，再按其中的指示使用已有工具（如 `exec`、`read`、`write`）完成任务。

```text
Skills 在 Agent Loop 中的位置:

  run_agent_loop()
       │
       ├── 3. 分配沙箱
       ├── 4. 构建系统提示词
       │       │
       │       ├── load_skills()             ← 加载技能
       │       ├── filter_skills()           ← 过滤可用技能
       │       ├── sync_skills_to_sandbox()  ← 同步到沙箱 (如需)
       │       └── build_skills_prompt()     ← 生成 <available_skills> 段落
       │               │
       │               └── 注入到系统提示词 §8 段落 4 "技能列表"
       │
       ├── 5. 创建工具集
       └── 8. 交互循环
               │
               LLM 根据 <available_skills> 自主决策:
               ├── read(SKILL.md)    ← 第一阶段: 读取技能指南
               └── exec/write/...   ← 第二阶段: 按指南执行工具
```

### 10.2 技能定义

```python
# sahara_runtime/skills/types.py

class SkillTier(IntEnum):
    """技能优先级层级。数值越小优先级越高。"""
    BUILTIN = 0       # 内置技能: 随 Runtime 发布
    CURATED = 1       # 精选技能: 平台官方审核推荐
    MANAGED = 2       # 托管技能: 平台市场安装
    WORKSPACE = 3     # 工作空间技能: Agent/用户本地
    USER = 4          # 用户自定义: 用户上传

@dataclass
class SkillMetadata:
    """技能元数据，从 SKILL.md frontmatter 解析。"""
    always: bool = False             # 始终启用 (跳过过滤)
    skill_key: str | None = None     # 技能标识键
    primary_env: str | None = None   # 主环境变量 (如 API Key 名称)
    emoji: str | None = None
    homepage: str | None = None
    os: list[str] | None = None      # 支持的操作系统 ["linux", "darwin"]
    tier: SkillTier = SkillTier.WORKSPACE  # 优先级层级
    priority: int = 100              # 同 Tier 内的排序权重 (越小越靠前)
    tags: list[str] | None = None    # 分类标签, 如 ["devops", "deploy"]
    requires: SkillRequirements | None = None
    install: list[SkillInstallSpec] | None = None

@dataclass
class SkillRequirements:
    bins: list[str] | None = None       # 必需的所有二进制
    any_bins: list[str] | None = None   # 必需的任意一个二进制
    env: list[str] | None = None        # 必需的环境变量
    config: list[str] | None = None     # 必需的配置路径

@dataclass
class SkillInstallSpec:
    kind: str        # "pip" / "npm" / "go" / "download"
    package: str | None = None
    url: str | None = None
    bins: list[str] | None = None

@dataclass
class InvocationPolicy:
    """技能调用策略。"""
    user_invocable: bool = True          # 是否可被用户 /命令 触发
    disable_model_invocation: bool = False  # 是否禁止 LLM 自动调用

@dataclass
class SkillEntry:
    """加载后的完整技能条目。"""
    name: str
    description: str
    base_dir: str                  # SKILL.md 所在目录
    file_path: str                 # SKILL.md 完整路径
    source: str = "bundled"        # 来源: "bundled" / "managed" / "workspace"
    metadata: SkillMetadata | None = None
    invocation: InvocationPolicy | None = None

    @property
    def effective_tier(self) -> SkillTier:
        """有效 Tier, 来源覆盖 + SKILL.md 声明取其低。"""
        source_tier = {
            "bundled": SkillTier.BUILTIN,
            "managed": SkillTier.MANAGED,
            "workspace": SkillTier.WORKSPACE,
        }.get(self.source, SkillTier.USER)
        declared_tier = self.metadata.tier if self.metadata else SkillTier.WORKSPACE
        return min(source_tier, declared_tier)

    @property
    def sort_key(self) -> tuple[int, int, str]:
        """排序键: (tier, priority, name)。"""
        meta = self.metadata or SkillMetadata()
        return (self.effective_tier, meta.priority, self.name)
```

### 10.3 技能优先级与排序

技能集内部有明确的优先级分层，影响 **`<available_skills>` 段落中的排列顺序**（LLM 更倾向选择排在前面的技能）和**上下文预算裁剪**（超出预算时先移除低优先级技能）。

```text
技能优先级分层:

  ┌───────────────────────────────────────────────────────────────┐
  │  Tier 0: 内置技能 (Builtin)                                   │
  │  随 Runtime 发布, 经过充分测试, 始终排在最前                   │
  │                                                               │
  │  create-rule     — 创建 Cursor/Agent 规则文件                 │
  │  create-skill    — 创建新技能                                 │
  │                                                               │
  ├───────────────────────────────────────────────────────────────┤
  │  Tier 1: 精选技能 (Curated)                      [Phase 2]    │
  │  平台官方审核推荐, 质量有保证                                  │
  │                                                               │
  │  deploy-k8s     — K8s 部署                                    │
  │  setup-ci       — CI/CD 配置                                  │
  │                                                               │
  ├───────────────────────────────────────────────────────────────┤
  │  Tier 2: 托管技能 (Managed)                      [Phase 2]    │
  │  从技能市场安装, 平台托管                                      │
  │                                                               │
  │  weather         — 天气查询                                   │
  │  translate       — 翻译                                       │
  │                                                               │
  ├───────────────────────────────────────────────────────────────┤
  │  Tier 3: 工作空间技能 (Workspace)                              │
  │  Agent 或项目本地技能, 开发者自行维护                          │
  │                                                               │
  │  my-deploy       — 项目专属部署脚本                           │
  │  code-review     — 团队代码审查规范                           │
  │                                                               │
  ├───────────────────────────────────────────────────────────────┤
  │  Tier 4: 用户自定义 (User)                       [Phase 3]    │
  │  用户上传, 最低优先级, 需要安全审核                            │
  │                                                               │
  └───────────────────────────────────────────────────────────────┘

  <available_skills> 中的排列顺序:
  [create-rule, create-skill, deploy-k8s, weather, my-deploy, ...]
   ├─ Tier 0 ───────────┤├─ Tier 1 ──┤├ Tier 2 ┤├ Tier 3 ──────┤
```

**同 Tier 内的排序：** 由 `priority` 字段决定（默认 100，越小越靠前）。同 `priority` 按名称字母序。

```yaml
# SKILL.md frontmatter 中声明 tier 和 priority
---
name: deploy-k8s
description: Deploy to Kubernetes cluster.
metadata:
  tier: 1          # Tier 1: Curated
  priority: 10     # 同 Tier 内排在较前
  tags: ["devops", "deploy", "kubernetes"]
  requires:
    bins: ["kubectl"]
---
```

**排序算法：**

```python
# sahara_runtime/skills/sorter.py

def sort_skills(entries: list[SkillEntry]) -> list[SkillEntry]:
    """按 (effective_tier, priority, name) 排序。
    效果: 内置在前, 用户自定义在后; 同层内按 priority 再按名称。
    """
    return sorted(entries, key=lambda e: e.sort_key)
```

**优先级对各环节的影响：**

| 环节 | 高优先级技能 | 低优先级技能 |
| --- | --- | --- |
| **`<available_skills>` 排列** | 排在前面, LLM 更易匹配 | 排在后面 |
| **上下文预算裁剪 (§8.4)** | 最后被裁剪 | 最先被裁剪 |
| **同名冲突** | 低 Tier 覆盖高 Tier (来源优先级反转: workspace > managed > bundled) | 被高优先级来源覆盖 |
| **多技能匹配** | LLM 系统提示词引导选择"最具体的" | 作为备选 |

> **同名冲突的特殊处理**：`SkillLoader` 中同名技能按来源优先级 **workspace > managed > bundled** 覆盖（用户可以用自己的版本替换内置技能），这与 `<available_skills>` 排列顺序（按 Tier 排列）是两个独立维度。来源优先级决定"用谁的代码"，Tier 决定"在 LLM 视角中排第几"。

### 10.4 技能加载

```python
# sahara_runtime/skills/loader.py

class SkillLoader:
    """从多个目录加载技能，按优先级合并。"""

    def __init__(self, config: RuntimeConfig):
        self.config = config

    async def load_all(self) -> list[SkillEntry]:
        """加载并合并所有技能。同名技能按来源优先级覆盖 (workspace > managed > bundled)。
        返回的列表按 sort_key 排序 (tier → priority → name)。
        """
        merged: dict[str, SkillEntry] = {}

        # 来源覆盖优先级从低到高: bundled < managed < workspace
        sources = [
            ("bundled", self.config.bundled_skills_dir),
            ("managed", self.config.managed_skills_dir),
            ("workspace", self.config.workspace_skills_dir),
        ]

        for source_name, skills_dir in sources:
            if not skills_dir or not os.path.isdir(skills_dir):
                continue
            entries = self._load_from_dir(skills_dir, source_name)
            for entry in entries:
                merged[entry.name] = entry  # 同名覆盖 (后来的 source 优先)

        # ★ 按优先级排序后返回
        result = list(merged.values())
        result.sort(key=lambda e: e.sort_key)
        return result

    def _load_from_dir(self, dir_path: str, source: str) -> list[SkillEntry]:
        """扫描目录下的 SKILL.md 文件，解析技能定义。"""
        entries = []
        for item in os.listdir(dir_path):
            skill_md = os.path.join(dir_path, item, "SKILL.md")
            if not os.path.isfile(skill_md):
                continue

            raw = open(skill_md, "r").read()
            frontmatter, body = self._parse_frontmatter(raw)

            entries.append(SkillEntry(
                name=frontmatter.get("name", item),
                description=frontmatter.get("description", ""),
                base_dir=os.path.join(dir_path, item),
                file_path=skill_md,
                source=source,  # ← "bundled" / "managed" / "workspace"
                metadata=self._parse_metadata(frontmatter),
                invocation=self._parse_invocation(frontmatter),
            ))

        return entries
```

**技能目录优先级**：

| 优先级 | 目录 | 说明 |
| --- | --- | --- |
| 1 (最低) | `bundled_skills_dir` | 随 Runtime 分发的内置技能 |
| 2 | `managed_skills_dir` | 平台安装的托管技能 |
| 3 (最高) | `workspace_skills_dir` | Agent 工作空间内的本地技能 |

### 10.5 技能过滤

加载后的技能需要根据运行环境过滤，只保留当前可用的技能：

```python
# sahara_runtime/skills/filter.py

class SkillFilter:
    """7 层过滤检查，确保只启用满足条件的技能。"""

    def filter(self, entries: list[SkillEntry], config: RuntimeConfig) -> list[SkillEntry]:
        return [e for e in entries if self._should_include(e, config)]

    def _should_include(self, entry: SkillEntry, config: RuntimeConfig) -> bool:
        meta = entry.metadata or SkillMetadata()

        # 1. 显式禁用检查
        if entry.name in config.disabled_skills:
            return False

        # 2. 操作系统兼容性
        if meta.os and platform.system().lower() not in [o.lower() for o in meta.os]:
            return False

        # 3. always=true 始终启用 (跳过后续检查)
        if meta.always:
            return True

        # 4. 必需二进制文件
        if meta.requires and meta.requires.bins:
            for bin_name in meta.requires.bins:
                if not shutil.which(bin_name):
                    return False

        # 5. 任意一个二进制
        if meta.requires and meta.requires.any_bins:
            if not any(shutil.which(b) for b in meta.requires.any_bins):
                return False

        # 6. 必需环境变量
        if meta.requires and meta.requires.env:
            for env_name in meta.requires.env:
                if not os.environ.get(env_name):
                    return False

        # 7. 必需配置路径
        if meta.requires and meta.requires.config:
            for cfg_path in meta.requires.config:
                if not config.has_config(cfg_path):
                    return False

        return True
```

```text
过滤决策流程:

  SkillEntry
       │
       ▼
  ┌─────────────┐    是
  │ 显式禁用?    ├──────► 排除
  └──────┬──────┘
         │ 否
         ▼
  ┌─────────────┐    是
  │ OS 不兼容?   ├──────► 排除
  └──────┬──────┘
         │ 否
         ▼
  ┌─────────────┐    是
  │ always=true? ├──────► 包含 ✓
  └──────┬──────┘
         │ 否
         ▼
  ┌─────────────┐    是
  │ 缺少必需      ├──────► 排除
  │ 二进制/环境/  │
  │ 配置?         │
  └──────┬──────┘
         │ 否
         ▼
      包含 ✓
```

### 10.6 技能提示词生成

过滤后的技能被格式化为 XML 段落，注入到系统提示词中：

```python
# sahara_runtime/skills/prompt.py

def build_skills_prompt(entries: list[SkillEntry]) -> str:
    """生成技能提示词段落，注入到系统提示词 §8 段落 4。

    关键: 技能按 sort_key 排序后输出 — 高优先级技能排在前面,
    LLM 更倾向选择排在前面的技能。
    """

    # 1. 排除禁止 LLM 自动调用的技能
    prompt_entries = [
        e for e in entries
        if not (e.invocation and e.invocation.disable_model_invocation)
    ]

    if not prompt_entries:
        return ""

    # 2. ★ 按优先级排序 (tier → priority → name)
    prompt_entries = sorted(prompt_entries, key=lambda e: e.sort_key)

    lines = [
        "## Skills (mandatory)",
        'Before replying: scan <available_skills> <description> entries.',
        '- If exactly one skill clearly applies: read its SKILL.md at <location> with `read`, then follow it.',
        '- If multiple could apply: choose the most specific one, then read/follow it.',
        '- If none clearly apply: do not read any SKILL.md.',
        'Constraints: never read more than one skill up front; only read after selecting.',
        '',
        '<available_skills>',
    ]

    for entry in prompt_entries:
        # location 指向沙箱内的路径 (供 LLM 的 read 工具使用)
        location = f"/workspace/skills/{entry.name}/SKILL.md"
        # 可选: 带标签帮助 LLM 理解技能分类
        tag_attr = ""
        if entry.metadata and entry.metadata.tags:
            tag_attr = f' tags="{",".join(entry.metadata.tags)}"'
        lines.append(
            f'<skill name="{entry.name}" location="{location}"{tag_attr}>'
            f'\n  <description>{entry.description}</description>'
            f'\n</skill>'
        )

    lines.append('</available_skills>')
    return '\n'.join(lines)
```

**生成的提示词示例**：

```xml
## Skills (mandatory)
Before replying: scan <available_skills> <description> entries.
- If exactly one skill clearly applies: read its SKILL.md at <location> with `read`, then follow it.
- If multiple could apply: choose the most specific one, then read/follow it.
- If none clearly apply: do not read any SKILL.md.
Constraints: never read more than one skill up front; only read after selecting.

<available_skills>
<skill name="weather" location="/workspace/skills/weather/SKILL.md">
  <description>Get current weather and forecasts (no API key required).</description>
</skill>
<skill name="deploy" location="/workspace/skills/deploy/SKILL.md">
  <description>Deploy application to staging or production environment.</description>
</skill>
</available_skills>
```

### 10.7 技能同步到沙箱

当沙箱启用时，技能的 `SKILL.md` 文件必须同步到沙箱内，使 LLM 的 `read` 工具能够访问：

```python
# sahara_runtime/skills/sync.py

async def sync_skills_to_sandbox(
    entries: list[SkillEntry],
    sandbox: Sandbox,
):
    """将技能目录同步到沙箱的 /workspace/skills/。
    在 Agent Loop 步骤 3 (沙箱分配) 之后、步骤 4 (系统提示词) 之前执行。
    """
    target_dir = "/workspace/skills"

    # 1. 清空目标目录
    await sandbox.exec(f"rm -rf {target_dir} && mkdir -p {target_dir}")

    # 2. 逐个写入技能文件
    for entry in entries:
        skill_dir = f"{target_dir}/{entry.name}"
        await sandbox.exec(f"mkdir -p {skill_dir}")

        # 读取主机上的 SKILL.md
        content = open(entry.file_path, "r").read()
        await sandbox.write_file(f"{skill_dir}/SKILL.md", content)

        # 如果技能目录有额外文件 (脚本、模板等)，也一并同步
        for extra_file in _list_extra_files(entry.base_dir):
            extra_content = open(extra_file, "r").read()
            rel_path = os.path.relpath(extra_file, entry.base_dir)
            await sandbox.write_file(f"{skill_dir}/{rel_path}", extra_content)
```

### 10.8 技能调用全流程详解

技能的调用本质上是 **LLM 自主编排的多轮工具调用**。Runtime 不感知"技能"概念——它只看到一连串标准的 `tool_use` 请求。技能的"智能"完全来自 LLM 阅读 `SKILL.md` 后的行为改变。

#### 10.8.1 全流程时序图

以 "帮我部署到 staging 环境" 为例，展示完整的技能调用链路（涉及 5 轮 Agent Loop 迭代）：

```text
┌──────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌───────────┐
│  Client   │    │  Gateway          │    │  Runtime          │    │  沙箱      │
│  (用户)   │    │  (WebSocket)      │    │  (Agent Loop)     │    │  (Docker)  │
└────┬─────┘    └────────┬─────────┘    └────────┬─────────┘    └─────┬─────┘
     │                    │                       │                     │
     │  "帮我部署到       │                       │                     │
     │   staging"         │                       │                     │
     │───────────────────▶│   gRPC SubmitTask     │                     │
     │                    │──────────────────────▶│                     │
     │                    │                       │                     │
     │                    │            ┌──────────┴──────────┐         │
     │                    │            │  Agent Loop 迭代 1   │         │
     │                    │            │                      │         │
     │                    │            │  LLM 接收:           │         │
     │                    │            │  - system_prompt      │         │
     │                    │            │    (含 <available_    │         │
     │                    │            │     skills>)          │         │
     │                    │            │  - tools=[exec,read,  │         │
     │                    │            │          write]       │         │
     │                    │            │  - user: "帮我部署    │         │
     │                    │            │    到 staging"        │         │
     │                    │            │                      │         │
     │                    │            │  LLM 决策:           │         │
     │                    │            │  "deploy 技能匹配,    │         │
     │                    │            │   先读取 SKILL.md"    │         │
     │                    │            │                      │         │
     │                    │            │  → stop_reason:       │         │
     │                    │            │    "tool_use"         │         │
     │                    │            │  → tool_use: read(    │         │
     │                    │            │    "/workspace/skills/ │         │
     │                    │            │     deploy/SKILL.md") │         │
     │                    │            └──────────┬──────────┘         │
     │                    │                       │                     │
     │  ◄─ DELTA 事件 ───│◄──── emit_delta ──────│                     │
     │  "让我查看部署指南"│                       │                     │
     │                    │                       │ ToolExecutor        │
     │  ◄─ TOOL_START ───│◄──── emit_tool_start ─│  .execute_one()     │
     │   (read, SKILL.md) │                       │────read────────────▶│
     │                    │                       │                     │
     │                    │                       │◄───SKILL.md 内容────│
     │  ◄─ TOOL_RESULT ──│◄──── emit_tool_result │                     │
     │   (SKILL.md 内容)  │                       │                     │
     │                    │                       │                     │
     │                    │            ┌──────────┴──────────┐         │
     │                    │            │  Agent Loop 迭代 2   │         │
     │                    │            │                      │         │
     │                    │            │  LLM 接收:           │         │
     │                    │            │  - 历史 + SKILL.md   │         │
     │                    │            │    tool_result        │         │
     │                    │            │                      │         │
     │                    │            │  LLM 阅读 SKILL.md:  │         │
     │                    │            │  "1. 先运行测试       │         │
     │                    │            │   2. 构建镜像         │         │
     │                    │            │   3. 推送部署"        │         │
     │                    │            │                      │         │
     │                    │            │  → tool_use: exec(    │         │
     │                    │            │   "npm test")         │         │
     │                    │            └──────────┬──────────┘         │
     │                    │                       │                     │
     │  ◄─ TOOL_START ───│◄──────────────────────│────exec────────────▶│
     │   (exec, npm test) │                       │    "npm test"       │
     │                    │                       │                     │
     │                    │                       │◄─"All tests pass"──│
     │  ◄─ TOOL_RESULT ──│◄──────────────────────│                     │
     │   (pass)           │                       │                     │
     │                    │                       │                     │
     │                    │            ┌──────────┴──────────┐         │
     │                    │            │  Agent Loop 迭代 3   │         │
     │                    │            │                      │         │
     │                    │            │  LLM: 测试通过,      │         │
     │                    │            │  开始构建镜像         │         │
     │                    │            │                      │         │
     │                    │            │  → tool_use: exec(    │         │
     │                    │            │   "docker build -t    │         │
     │                    │            │    app:staging .")    │         │
     │                    │            └──────────┬──────────┘         │
     │                    │                       │                     │
     │  ◄─ TOOL_START ───│◄──────────────────────│────exec────────────▶│
     │  (exec, docker...) │                       │  "docker build..."  │
     │                    │                       │                     │
     │                    │                       │◄─"Successfully..."──│
     │  ◄─ TOOL_RESULT ──│◄──────────────────────│                     │
     │                    │                       │                     │
     │                    │            ┌──────────┴──────────┐         │
     │                    │            │  Agent Loop 迭代 4   │         │
     │                    │            │                      │         │
     │                    │            │  → tool_use: exec(    │         │
     │                    │            │   "kubectl apply -f   │         │
     │                    │            │    k8s/staging.yaml") │         │
     │                    │            └──────────┬──────────┘         │
     │                    │                       │                     │
     │  ◄─ TOOL_START ───│◄──────────────────────│────exec────────────▶│
     │  (exec, kubectl..) │                       │                     │
     │                    │                       │◄─"deployed..."──────│
     │  ◄─ TOOL_RESULT ──│◄──────────────────────│                     │
     │                    │                       │                     │
     │                    │            ┌──────────┴──────────┐         │
     │                    │            │  Agent Loop 迭代 5   │         │
     │                    │            │                      │         │
     │                    │            │  → stop_reason:       │         │
     │                    │            │    "end_turn"         │         │
     │                    │            │  "部署完成!..."       │         │
     │                    │            └──────────┬──────────┘         │
     │                    │                       │                     │
     │  ◄─ DELTA 事件 ───│◄──── emit_delta ──────│                     │
     │  "部署完成! 测试   │                       │                     │
     │   通过后镜像已推送 │  ◄─ RUN_COMPLETE ─────│                     │
     │   到 staging..."   │                       │                     │
```

#### 10.8.2 messages 数组演变

**关键理解：Runtime 不区分"技能调用"和"普通工具调用"。** `messages` 中只有标准的 `tool_use` / `tool_result` 对。LLM 之所以能按技能指南行事，是因为 `SKILL.md` 的内容已经作为 `tool_result` 进入了上下文。

```text
messages 数组在技能调用过程中的演变:

─── 初始状态 ─────────────────────────────────────────────

  messages = [
    { role: "user", content: "帮我部署到 staging 环境" }
  ]

─── 迭代 1: LLM 决定读取技能 ────────────────────────────

  messages = [
    { role: "user", content: "帮我部署到 staging 环境" },
    { role: "assistant", content: [
      { type: "text", text: "让我查看部署指南。" },
      { type: "tool_use", id: "toolu_001", name: "read",
        input: { path: "/workspace/skills/deploy/SKILL.md" } },
    ]},
  ]

  ┌───────────────────────────────────────────────────────┐
  │  ToolExecutor 处理 read 工具:                          │
  │  1. TOOL_START 事件 → Event Bus → Gateway → Client    │
  │  2. ReadTool.execute({path: ".../SKILL.md"})          │
  │     → sandbox.read_file("/workspace/skills/deploy/    │
  │       SKILL.md")                                       │
  │     → Docker: exec cat /workspace/skills/deploy/      │
  │       SKILL.md                                         │
  │     → 返回 SKILL.md 完整内容 (含 frontmatter + 指南)  │
  │  3. 输出截断 (如超过 8000 字符)                        │
  │  4. TOOL_RESULT 事件 → Event Bus → Gateway → Client   │
  └───────────────────────────────────────────────────────┘

  messages = [
    ...(上面的),
    { role: "user", content: [
      { type: "tool_result", tool_use_id: "toolu_001",
        content: "---\nname: deploy\ndescription: ...\n---\n
                  # Deploy\n\n## Steps\n
                  1. Run tests: `npm test`\n
                  2. Build: `docker build -t app:staging .`\n
                  3. Deploy: `kubectl apply -f k8s/staging.yaml`\n
                  ..." },
    ]},
  ]

  → 此刻 SKILL.md 的内容已经在 messages 中
  → LLM 下一轮会"看到"这些指南并按步骤执行

─── 迭代 2: 按技能指南执行步骤 1 ────────────────────────

  messages = [
    ...(上面的),
    { role: "assistant", content: [
      { type: "text", text: "按照部署指南, 先运行测试。" },
      { type: "tool_use", id: "toolu_002", name: "exec",
        input: { command: "npm test" } },
    ]},
  ]

  ┌───────────────────────────────────────────────────────┐
  │  ToolExecutor 处理 exec 工具:                          │
  │  1. TOOL_START 事件                                    │
  │  2. ExecTool.execute({command: "npm test"})            │
  │     → sandbox.exec("npm test", timeout=30)             │
  │     → Docker: container.exec_run("npm test",           │
  │       workdir="/workspace")                             │
  │     → stdout: "PASS  src/app.test.js\n                 │
  │              Tests: 12 passed\n                         │
  │              Time: 3.2s"                                │
  │  3. 截断 + TOOL_RESULT 事件                            │
  └───────────────────────────────────────────────────────┘

  messages = [
    ...(上面的),
    { role: "user", content: [
      { type: "tool_result", tool_use_id: "toolu_002",
        content: "PASS  src/app.test.js\nTests: 12 passed\nTime: 3.2s" },
    ]},
  ]

─── 迭代 3-4: 继续按指南执行步骤 2、3 ──────────────────

  (同上模式: LLM 生成 tool_use → ToolExecutor 在沙箱执行 → 结果追加回 messages)

─── 迭代 5: 全部步骤完成, LLM 总结 ─────────────────────

  messages = [
    ...(上面的),
    { role: "assistant",
      content: "部署完成! 测试全部通过 (12/12), 镜像 app:staging 已构建并推送,
                kubectl 已将 staging.yaml 部署到集群。" },
  ]

  → stop_reason == "end_turn" → Agent Loop 结束
  → 发射 RUN_COMPLETE 事件
```

#### 10.8.3 事件流与客户端感知

技能调用过程中，客户端通过 WebSocket 收到的事件序列：

```text
事件时间线 (Client 视角):

  t=0s   ◄── DELTA: "让我查看部署指南。"
  t=0.1s ◄── TOOL_START: { tool: "read", input: "...deploy/SKILL.md" }
  t=0.3s ◄── TOOL_RESULT: { tool: "read", success: true, duration: 200ms }
                           (输出内容: SKILL.md 全文, 客户端可选择展示)

  t=1.0s ◄── DELTA: "按照部署指南, 先运行测试。"
  t=1.1s ◄── TOOL_START: { tool: "exec", input: "npm test" }
  t=4.5s ◄── TOOL_RESULT: { tool: "exec", success: true, duration: 3400ms }
                           (输出: "Tests: 12 passed")

  t=5.5s ◄── DELTA: "测试通过, 开始构建 Docker 镜像。"
  t=5.6s ◄── TOOL_START: { tool: "exec", input: "docker build..." }
  t=35s  ◄── TOOL_RESULT: { tool: "exec", success: true, duration: 29400ms }

  t=36s  ◄── DELTA: "镜像构建完成, 推送到 staging。"
  t=36.1s◄── TOOL_START: { tool: "exec", input: "kubectl apply..." }
  t=38s  ◄── TOOL_RESULT: { tool: "exec", success: true, duration: 1900ms }

  t=39s  ◄── DELTA: "部署完成! ..."
  t=39.5s◄── RUN_COMPLETE

  客户端 UI 可以:
  - 折叠 SKILL.md 读取步骤 (用户不关心)
  - 展开每个 exec 步骤的输入/输出 (用户关心执行细节)
  - 显示每步的耗时
```

#### 10.8.4 沙箱内的文件视角

技能执行前后，沙箱 `/workspace` 目录的变化：

```text
沙箱文件系统变化:

  执行前 (§10.7 sync_skills_to_sandbox 之后):
  /workspace/
  ├── skills/
  │   ├── deploy/
  │   │   ├── SKILL.md              ← 技能指南 (LLM 会 read 这个)
  │   │   └── k8s/
  │   │       └── staging.yaml      ← 技能附带的模板文件
  │   └── weather/
  │       └── SKILL.md
  ├── src/                           ← 用户项目代码
  │   ├── app.js
  │   └── app.test.js
  ├── package.json
  └── Dockerfile

  技能执行过程中, LLM 按 SKILL.md 指引:
  1. exec("npm test")              → 读取 src/, 运行测试
  2. exec("docker build ...")      → 读取 Dockerfile, 构建镜像
  3. exec("kubectl apply ...")     → 使用 skills/deploy/k8s/staging.yaml

  执行后:
  /workspace/                        (无新增文件, 只是在沙箱内执行了命令)
```

#### 10.8.5 技能执行中的错误处理

技能执行中的失败处理与普通工具调用完全一致——**Runtime 不需要任何特殊逻辑**。LLM 根据 `tool_result.is_error` 自行决策：

```text
错误场景 1: 技能 SKILL.md 不存在 (同步失败)

  LLM → read("/workspace/skills/deploy/SKILL.md")
  ToolExecutor → tool_result: { is_error: true,
                  content: "File not found: /workspace/skills/deploy/SKILL.md" }
  LLM → "抱歉, 未找到部署技能。我可以尝试手动帮你部署..."
  (LLM 自动降级为直接使用工具)

错误场景 2: 技能指南中的命令执行失败

  LLM → exec("npm test")
  ToolExecutor → tool_result: { is_error: false,  ← exec 本身成功
                  content: "FAIL  src/app.test.js\n3 tests failed" }
  LLM → "测试未通过 (3 个失败)。让我查看失败详情..."
  LLM → read("src/app.test.js")  ← LLM 自主决定排查, 不是 SKILL.md 要求的
  (LLM 按自身判断处理, 可能偏离 SKILL.md 的步骤)

错误场景 3: 命令超时

  LLM → exec("docker build ...", timeout=30)
  ToolExecutor → tool_result: { is_error: true,
                  content: "Tool exec timed out after 30s" }
  LLM → "构建超时, 尝试增加超时时间..."
  LLM → exec("docker build ...", timeout=120)

错误场景 4: 上下文窗口不足

  SKILL.md 非常长 (>5000 字符) + 已有大量历史消息
  → Context Manager (§11) 在下一轮迭代前可能剪枝历史消息
  → 但 SKILL.md 的 tool_result 作为最近消息, 通常被保留
  → 风险: 如果 SKILL.md 被剪掉, LLM 会"忘记"指南
  → 缓解: SKILL.md 应尽量简短 (<2000 字符), 复杂逻辑放脚本里
```

#### 10.8.6 调用策略控制

| 属性 | 默认值 | 说明 | 影响 |
| --- | --- | --- | --- |
| `user_invocable` | `true` | 可被用户 `/命令` 触发 | Phase 2 实现用户命令注册 |
| `disable_model_invocation` | `false` | 禁止 LLM 自动调用 | 不出现在 `<available_skills>` 中 |

**与普通工具调用的对比**：

| 维度 | 普通工具调用 | 技能驱动的工具调用 |
| --- | --- | --- |
| **Runtime 感知** | ToolExecutor 执行单个工具 | 完全相同——Runtime 不知道 LLM 在"用技能" |
| **LLM 迭代次数** | 通常 1 次 (单个 tool_use) | 至少 2 次 (read SKILL.md + 至少 1 次工具) |
| **上下文消耗** | 仅工具参数和结果 | 额外包含 SKILL.md 全文 (tool_result) |
| **沙箱交互** | 单次 sandbox.exec/read/write | 多次沙箱交互, 可能涉及多个工具 |
| **事件数量** | 1 对 TOOL_START/RESULT | 多对 TOOL_START/RESULT |
| **错误处理** | ToolExecutor 返回 is_error | 完全相同; LLM 自行决策重试/放弃 |
| **计费** | 1 次 LLM 调用 | 多次 LLM 调用 (每轮迭代都计费) |

### 10.9 SKILL.md 规范

````markdown
---
name: weather
description: Get current weather and forecasts (no API key required).
metadata:
  always: false
  tier: 2               # Tier 2: Managed
  priority: 50          # 同 Tier 内排在较前
  tags: ["utility", "weather"]
  emoji: "🌤️"
  homepage: https://wttr.in/:help
  requires:
    bins: ["curl"]
invocation:
  user_invocable: true
  disable_model_invocation: false
---

# Weather

Two free services, no API keys needed.

## wttr.in (primary)

```bash
curl -s "wttr.in/{city}?format=3"
```

## Open-Meteo (fallback)

```bash
curl -s "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
```
````

**Frontmatter 字段说明**：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `name` | string | ✓ | 技能唯一标识 |
| `description` | string | ✓ | 一句话描述，供 LLM 匹配判断 |
| `metadata.always` | bool | — | 始终启用，跳过过滤 |
| `metadata.tier` | int (0-4) | — | 优先级层级 (默认 3, 详见 §10.3) |
| `metadata.priority` | int | — | 同 Tier 内排序权重 (默认 100, 越小越靠前) |
| `metadata.tags` | string[] | — | 分类标签, 如 `["devops", "deploy"]` |
| `metadata.requires.bins` | string[] | — | 必需的二进制文件 |
| `metadata.requires.env` | string[] | — | 必需的环境变量 |
| `metadata.primary_env` | string | — | 主 API Key 环境变量名 |
| `invocation.user_invocable` | bool | — | 是否可被 `/命令` 触发 |
| `invocation.disable_model_invocation` | bool | — | 是否禁止 LLM 自动调用 |

### 10.10 技能扩展机制

支持三种方式扩展技能集：

```text
技能来源:

  ┌─────────────────────────────────────────────────────────┐
  │  内置技能 (Bundled)                                      │
  │  随 Runtime 代码发布, bundled_skills_dir                 │
  │  经过充分测试, 稳定可靠                                  │
  │  Tier 0, 最高可信度                                      │
  ├─────────────────────────────────────────────────────────┤
  │  托管技能 (Managed)                     [Phase 2]        │
  │  通过 /skill install 命令安装到 managed_skills_dir      │
  │  来源: 技能市场 (平台审核)                               │
  │  Tier 1-2, 由市场标签确定                                │
  ├─────────────────────────────────────────────────────────┤
  │  工作空间技能 (Workspace)                                │
  │  Agent 项目目录中的 .skills/ 文件夹                     │
  │  开发者自行维护, 随项目版本管理                          │
  │  Tier 3, 同名时覆盖内置和托管                            │
  ├─────────────────────────────────────────────────────────┤
  │  用户上传技能 (User)                    [Phase 3]        │
  │  用户通过 API/UI 上传                                    │
  │  需要安全审核 (检查 SKILL.md 内容是否有注入风险)        │
  │  Tier 4, 最低优先级                                      │
  └─────────────────────────────────────────────────────────┘
```

**安全审核要点 (Phase 3)：**

| 检查项 | 说明 |
| --- | --- |
| **SKILL.md 注入检测** | 检查是否包含企图覆盖安全规则的指令（如 "ignore all previous instructions"） |
| **命令白名单** | 检查 SKILL.md 中引用的命令是否在安全范围内 |
| **外部 URL 检查** | 检查引用的 URL 是否为已知恶意域名 |
| **权限声明** | 检查技能是否声明了与其功能不匹配的权限需求 |

### 10.11 Phase 规划

| 阶段 | 范围 |
| --- | --- |
| **Phase 1** | 内置技能 (Tier 0) 加载 + 过滤 + 按 sort_key 排序 + 提示词生成 + 沙箱同步 |
| **Phase 2** | 精选/托管技能 (Tier 1-2) + `/命令` 调用 + 技能安装 + 环境变量注入 + tags 过滤 |
| **Phase 3** | 用户上传技能 (Tier 3-4) + 技能市场 + 版本管理 + 安全审核 + 热更新 |

---

## 十一、上下文管理 (Context Manager)

### 11.1 四层防御

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

### 11.2 各层策略详解

| 层 | 名称 | 触发条件 | 策略 | 代价 |
| --- | --- | --- | --- | --- |
| **Layer 1** | 输入截断 | 工具结果 > 8000 字符 | 截断为 `头部 4000 + "..." + 尾部 4000` | 丢失中间内容 |
| **Layer 2** | 历史剪枝 | messages token > 可用预算 | 从最旧的**非系统消息**开始移除 | 丢失早期对话上下文 |
| **Layer 3** | 自动压缩 | Layer 2 后仍超标 | 将被裁的旧消息用 LLM 生成摘要 | 额外 LLM 调用开销 |
| **Layer 4** | 紧急溢出 | Layer 3 后仍超标 | 只保留最近 2 轮对话 + 当前用户消息 | 几乎丢失所有历史 |

```python
# sahara_runtime/context/pruner.py

class HistoryPruner:
    def prune(self, messages: list[dict], target_tokens: int) -> list[dict]:
        """Layer 2: 从最旧的消息开始移除, 直到总 token 不超过 target。
        始终保留: 第一条用户消息 (任务上下文) 和最后一条用户消息 (当前请求)。
        """
        if len(messages) <= 2:
            return messages

        # 保护首尾
        first_user = messages[0]
        last_user = messages[-1]
        middle = messages[1:-1]

        # 从最旧的 middle 消息开始移除
        while middle and self.counter.count_messages(
            [first_user] + middle + [last_user]
        ) > target_tokens:
            middle.pop(0)  # 移除最旧

        return [first_user] + middle + [last_user]
```

```python
# sahara_runtime/context/compactor.py

class AutoCompactor:
    async def compact(self, messages: list[dict], target_tokens: int) -> list[dict]:
        """Layer 3: 将旧消息压缩为摘要。
        调用 LLM 生成摘要, 替换被压缩的消息。
        Phase 1 先不实现, 直接回退到 Layer 4。
        """
        # Phase 2: 调用 LLM 生成摘要
        # old_messages = messages[:split_point]
        # summary = await self._summarize(old_messages)
        # return [{"role": "system", "content": f"[对话历史摘要]\n{summary}"}] + messages[split_point:]

        # Phase 1: 回退到 Layer 4 紧急策略
        return self._emergency_truncate(messages, target_tokens)

    def _emergency_truncate(self, messages, target_tokens):
        """Layer 4: 紧急策略 — 只保留最近 2 轮 + 当前用户消息。"""
        if len(messages) <= 4:
            return messages
        # 保留最后 4 条 (2 轮对话)
        return messages[-4:]
```

### 11.3 Token 计数

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

## 十二、会话管理 (Session Store)

### 12.1 热冷分离

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

### 12.2 接口

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

        # 异步写 PG (不阻塞主流程, 带重试)
        asyncio.create_task(self._save_to_pg_with_retry(session_key, data))

    async def _save_to_pg_with_retry(self, session_key: str, data: str, retries: int = 3):
        """PG 异步写入, 失败重试, 最终失败记录日志 (不影响用户体验)。"""
        for attempt in range(retries):
            try:
                await self.pg.execute(
                    """INSERT INTO sessions (session_key, messages, updated_at)
                       VALUES ($1, $2::jsonb, NOW())
                       ON CONFLICT (session_key) DO UPDATE SET
                       messages = $2::jsonb, updated_at = NOW()""",
                    session_key, data,
                )
                return
            except Exception as e:
                if attempt == retries - 1:
                    logger.error("session_pg_save_failed",
                                 session_key=session_key, error=str(e))
                    # 不抛异常: Redis 已持久化, PG 可通过后续任务补偿
                else:
                    await asyncio.sleep(1)
```

### 12.3 Session 分布式锁

同一 session 同一时刻只允许一个 Agent 任务执行，通过 Redis 分布式锁保证：

```python
# sahara_runtime/session/lock.py

class SessionLock:
    """基于 Redis SET NX + TTL 的分布式锁。"""

    def __init__(self, redis: Redis, ttl: int = 300):
        self.redis = redis
        self.ttl = ttl  # 锁最长持有时间 (5min, 与任务超时对齐)

    async def acquire(self, session_key: str, timeout: float = 2.0) -> bool:
        """获取 session 锁。timeout 内未获取则返回 False。
        gRPC SubmitTask 在获取锁失败时返回 ABORTED。
        """
        lock_key = f"session:{session_key}:lock"
        lock_value = f"{config.worker_id}:{time.time()}"
        deadline = time.time() + timeout

        while time.time() < deadline:
            acquired = await self.redis.set(
                lock_key, lock_value, nx=True, ex=self.ttl,
            )
            if acquired:
                self._lock_value = lock_value
                return True
            await asyncio.sleep(0.1)  # 短暂退避

        return False

    async def release(self, session_key: str):
        """释放锁 (只释放自己持有的锁, 防止误释放)。"""
        lock_key = f"session:{session_key}:lock"
        # Lua 脚本保证原子性: 只删除 value 匹配的锁
        script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
        """
        await self.redis.eval(script, 1, lock_key, self._lock_value)
```

---

## 十三、Dependencies 注入与启动流程

### 13.1 Dependencies — 依赖容器

Runtime 的所有子系统通过一个 `Dependencies` 容器组装。构造时注入，不使用全局变量，便于测试 mock。

```python
# sahara_runtime/deps.py

@dataclass
class Dependencies:
    """Runtime Worker 的全部依赖, 在 main.py 中一次性组装。"""

    # ── 基础设施 ──
    config: RuntimeConfig
    redis: Redis
    pg_pool: asyncpg.Pool

    # ── 核心模块 ──
    worker: Worker
    event_backend: EventBackend        # 可插拔后端 (Redis/Kafka/NATS/InMemory)
    emitter: EventEmitterFactory       # 工厂, for_run() 创建 RunEmitter
    session_store: SessionStore
    session_lock: SessionLock
    model_router: ModelRouter
    context_manager: ContextManager
    prompt_builder: PromptBuilder
    tool_registry: ToolRegistry
    tool_policy: ToolPolicy
    sandbox_manager: SandboxManager    # 可插拔后端 (Docker/gVisor/Firecracker/Remote/Noop)
    skill_loader: SkillLoader
    skill_filter: SkillFilter
    dedup: IdempotencyStore

    # ── gRPC Servicers (启动后赋值) ──
    agent_servicer: AgentServicer | None = None
```

### 13.2 启动流程

```python
# sahara_runtime/main.py

import uvloop
import asyncio
import signal
from grpc_health.v1 import health_pb2_grpc, health

async def start_server():
    config = RuntimeConfig()  # 从环境变量加载

    # ── Phase 1: 基础设施连接 ──
    redis = Redis.from_url(config.redis_url)
    await redis.ping()  # 快速失败: Redis 不可用则启动失败
    logger.info("redis_connected", url=config.redis_url)

    pg_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=10)
    logger.info("pg_connected", url=config.database_url)

    # ── Phase 2: 模块初始化 ──
    worker = Worker(config)
    event_backend = create_event_backend(config)  # 可插拔: Redis/Kafka/NATS/InMemory
    emitter = EventEmitterFactory(event_backend, config.worker_id)
    session_store = SessionStore(redis, pg_pool)
    session_lock = SessionLock(redis)
    key_pool = KeyPool(redis)
    model_router = ModelRouter(config, key_pool)
    context_manager = ContextManager(TokenCounter())
    prompt_builder = PromptBuilder(config, redis)
    tool_registry = ToolRegistry()
    tool_policy = ToolPolicy()
    sandbox_manager = create_sandbox_manager(config)  # 可插拔: Docker/gVisor/Firecracker/Remote/Noop
    dedup = RedisIdempotencyStore(redis)

    deps = Dependencies(
        config=config, redis=redis, pg_pool=pg_pool,
        worker=worker, emitter=emitter, session_store=session_store,
        session_lock=session_lock, model_router=model_router,
        context_manager=context_manager, prompt_builder=prompt_builder,
        tool_registry=tool_registry, tool_policy=tool_policy,
        sandbox_manager=sandbox_manager, dedup=dedup,
    )

    # ── Phase 3: 沙箱预热 ──
    if config.sandbox_enabled:
        await sandbox_manager.warmup(pool_size=config.sandbox_pool_size)
        logger.info("sandbox_pool_ready", size=config.sandbox_pool_size)

    # ── Phase 4: gRPC Server 启动 ──
    server = grpc.aio.server(
        options=[
            ("grpc.max_concurrent_rpcs", 50),
            ("grpc.max_receive_message_length", 4 * 1024 * 1024),  # 4MB
        ],
    )

    agent_servicer = AgentServicer(deps)
    deps.agent_servicer = agent_servicer
    agent_pb2_grpc.add_AgentServiceServicer_to_server(agent_servicer, server)

    worker_servicer = WorkerServicer(deps)
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)

    # 标准健康检查
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    server.add_insecure_port(f"[::]:{config.grpc_port}")
    await server.start()
    logger.info("grpc_server_started", port=config.grpc_port)

    # ── Phase 5: 标记就绪 + 后台任务 ──
    worker.state = WorkerState.READY
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    # 启动资源监控后台协程
    monitor_task = asyncio.create_task(worker.resource_monitor.run())

    # ── Phase 6: 等待退出信号 ──
    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    await shutdown_event.wait()

    # ── Phase 7: 优雅关闭 ──
    logger.info("graceful_shutdown_start")
    worker.state = WorkerState.DRAINING
    health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)

    # 等待活跃任务完成 (最多 60s)
    deadline = time.time() + 60
    while worker.active_tasks > 0 and time.time() < deadline:
        await asyncio.sleep(1)

    monitor_task.cancel()
    await server.stop(grace=5)  # gRPC 5s 优雅关闭
    await pg_pool.close()
    await redis.close()
    logger.info("graceful_shutdown_complete")


def main():
    uvloop.install()
    asyncio.run(start_server())
```

**启动时序图**：

```text
t=0s   main() 启动
       │
t=0-1s Phase 1: Redis ping + PG connect
       │  失败 → 进程直接退出 (exit 1), K8s 重启
       │
t=1-2s Phase 2: 模块初始化 (纯内存, 极快)
       │
t=2-5s Phase 3: 沙箱预热 (Docker pull + 容器创建)
       │  这是最慢的阶段, K8s startupProbe 应覆盖
       │
t=5s   Phase 4: gRPC Server 开始监听
       │
t=5s   Phase 5: worker.state = READY
       │  Health Check → SERVING
       │  Gateway GetStatus 轮询成功 → Worker 加入调度列表
       │
       ──── 正常运行 ────
       │
t=N    收到 SIGTERM
       │  Phase 7: DRAINING → 等待任务 → 关闭
```

---

## 十四、并发模型

### 14.1 asyncio 架构

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
  │   ├── await emitter.emit()             IO 等待 (EventBackend.publish)
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

### 14.2 并发上限

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `max_concurrent_tasks` | 16 | 最大并发 Agent 任务 |
| gRPC `maximum_concurrent_rpcs` | 50 | gRPC 层面的并发上限 |
| `max_iterations` | 20 | 单任务最大 LLM 调用轮数 |
| 单任务超时 | 5 分钟 | 超时自动取消 |

### 14.3 uvloop

```python
# sahara_runtime/main.py

import uvloop

def main():
    uvloop.install()  # 替换默认事件循环，性能提升 2-4x
    asyncio.run(start_server())
```

---

## 十五、错误处理与弹性

### 15.1 错误分类

| 错误类型 | 处理策略 | 示例 |
| --- | --- | --- |
| **LLM 暂时不可用** (429/503) | 重试 + 指数退避 (最多 3 次) | Rate Limit、Provider 过载 |
| **LLM 认证失败** (401/403) | Key 轮换 → 熔断该 Key | API Key 过期/无效 |
| **上下文溢出** | Layer 4 紧急压缩 → 重试 | messages 超过 context window |
| **工具执行失败** | 返回错误文本给 LLM (让 LLM 决策) | exec 命令报错 |
| **沙箱不可用** | 发射 RUN_ERROR → 中止任务 | Docker 容器创建失败 |
| **Session 锁冲突** | gRPC 返回 ABORTED | 同一 session 并发请求 |
| **任务被取消** | asyncio.CancelledError → 清理 | 用户中止 / Gateway 断连 |

### 15.2 LLM 调用重试

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

## 十六、配置管理

```python
# sahara_runtime/config.py

from pydantic_settings import BaseSettings

class RuntimeConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SAHARA_RT_")

    # Worker
    worker_id: str = "rt-1"
    grpc_port: int = 50051
    max_concurrent_tasks: int = 16
    task_timeout: int = 300           # 单任务超时 (秒)
    input_wait_timeout: int = 120     # 等待用户输入超时 (秒)
    version: str = "dev"

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

    # Sandbox (可插拔后端, 详见 §9)
    sandbox_enabled: bool = True
    sandbox_runtime: str = "docker"   # "docker" / "gvisor" / "firecracker" / "remote" / "noop"
    sandbox_pool_size: int = 5
    sandbox_image: str = "sahara-sandbox:latest"
    sandbox_service_url: str = ""     # Remote 后端专用

    # Skills
    bundled_skills_dir: str = "/app/skills/bundled"
    managed_skills_dir: str = ""          # 平台托管技能目录 (可选)
    workspace_skills_dir: str = ""        # 工作空间技能目录 (可选)
    disabled_skills: list[str] = []       # 显式禁用的技能名列表

    # Event Bus (可插拔后端, 详见 §5)
    event_backend: str = "redis_streams"  # "redis_streams" / "kafka" / "nats_jetstream" / "in_memory"
    event_stream_maxlen: int = 5000       # Redis Streams 专用: maxlen
    kafka_bootstrap_servers: str = ""     # Kafka 专用
    nats_url: str = ""                    # NATS 专用

    # Observability
    log_level: str = "INFO"
    metrics_port: int = 9090
```

---

## 十七、可观测性

### 17.1 structlog 日志

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

### 17.2 Prometheus 指标

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

## 十八、Phase 1 最小实现范围

| 模块 | Phase 1 范围 | 可推迟 |
| --- | --- | --- |
| **gRPC Server** | ★ SubmitTask + AbortTask + GetStatus + GetTaskStatus + Health | SendInput, ListActiveTasks, Drain |
| **Agent Loop** | ★ 完整 LLM 交互循环 + 任务超时 + 重试 (Anthropic SDK) | 人机交互(§4.4), OpenAI SDK, 并行工具执行 |
| **EventEmitter** | ★ EventBackend 抽象 + RedisStreamsBackend (11 种事件 + ResilientEmitter 弹性) + InMemoryBackend (测试) | Kafka/NATS 后端, CompositeBackend 双写迁移 |
| **Model Router** | 简化：单 Provider + 单 Key + Key 刷新 | Key 池, 轮换, 熔断, 降级重试 |
| **Tools** | ★ exec + read + write (Tier 0) + ToolExecutor (串行) + 超时 + 截断 + 路径安全 + ToolTier 排序 | Tier 1 (edit/glob/grep), Tier 2 (web_*), Tier 3 (Plugin), 确认, 并行执行, 预算裁剪 |
| **Prompt Builder** | ★ 4 段落 (身份 + 安全 + 工具指南 + 运行时), PromptBuilder 基础骨架, 无缓存 | 技能段落, 沙箱段落, 上下文文件, 自定义指令, 段落缓存, 预算控制 |
| **Session Store** | ★ Redis 热存储 + 分布式锁 + PG 异步写入 | 历史分页 |
| **Context Manager** | ★ Layer 1 输入截断 + Layer 2 剪枝 + Layer 4 紧急回退 | Layer 3 LLM 压缩 |
| **Sandbox** | ★ Sandbox/SandboxManager 抽象 + DockerSandboxManager + NoopSandbox (测试) | gVisor, Firecracker, Remote, WASM |
| **Dependencies** | ★ 完整启动流程 + 优雅关闭 | — |
| **Config** | ★ 环境变量 + pydantic-settings | 配置中心热更新 |
| **Worker 管理** | ★ GetStatus (资源监控) + ResourceMonitor | Drain (Phase 2), UpdateConfig (Phase 3) |
| **Observability** | ★ structlog JSON + Prometheus 基础指标 | OpenTelemetry 追踪 |

### Phase 1 开发顺序建议

```text
Week 3-4 (与 Gateway 并行):
  1. gRPC Server 空壳 + Health Check       ← 与 Gateway 联调
  2. EventBackend 抽象 + RedisStreamsBackend ← 与 Gateway 事件消费联调
  3. Agent Loop 骨架 (mock LLM)            ← 端到端帧流通

Week 5-6:
  4. Agent Loop 接入真实 Anthropic SDK      ← 核心
  5. 工具系统 (exec + read + write)
  6. Session Store (Redis)

Week 7-8:
  7. Sandbox 抽象 + DockerSandboxManager
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
| `psutil` | 系统资源监控 (CPU/内存, 供 GetStatus) |
| `opentelemetry-*` | 分布式追踪 (Phase 2) |

### 附录 B. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| §3 gRPC Server (SubmitTask/AbortTask) | P0-9 + P1-6 | Phase 0-1 |
| §3 gRPC Server (GetStatus) | P0-10 健康检查联通 | Phase 0 |
| §3 gRPC Server (SendInput) | P2-15 人机交互 | Phase 2 |
| §3 gRPC Server (Drain) | P2-6 Worker 优雅关闭 | Phase 2 |
| §4 Agent Loop (核心循环) | P1-7 LLM 交互循环 | Phase 1 |
| §4 Agent Loop (人机交互) | P2-15 人机交互 | Phase 2 |
| §5 EventEmitter | P1-8 事件发射 | Phase 1 |
| §6 Model Router | P2-14 模型降级 | Phase 2 |
| §7 Tools | P1-13 基础工具 | Phase 1 |
| §9 Session Store | P1-11 会话存储 | Phase 1 |
| §10 Context Manager | — (内含在 Agent Loop 中) | Phase 1 |
| §11 Sandbox | P1-12 + P1-15 | Phase 1 |
| §12 Dependencies 启动流程 | P0-9 基础骨架 | Phase 0 |
