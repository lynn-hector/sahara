# Agent Memory 架构设计

> Agent 的记忆体系：三层记忆层次（工作记忆 / 短期记忆 / 长期记忆）、Session Store、向量记忆、记忆召回管线。
> 使 Agent 在单次对话中保持连贯，在跨会话中"记住"用户。
>
> 关联文档：
> - [Runtime 架构设计 §12](./RUNTIME-ARCHITECTURE-DESIGN.md) — Agent Memory 接口概述
> - [Runtime 架构设计 §4](./RUNTIME-ARCHITECTURE-DESIGN.md) — Agent Loop (记忆加载/保存时机)
> - [上下文管理架构](./CONTEXT-MANAGEMENT-DESIGN.md) — 工作记忆的优化策略（D7）
> - [Runtime 架构设计 §8](./RUNTIME-ARCHITECTURE-DESIGN.md) — System Prompt Builder (记忆注入)
> - [OpenClaw AGENT-RUNTIME-SESSION.md](../openclaw/agent/AGENT-RUNTIME-SESSION.md) — 原始会话系统参考
> - [OpenClaw AGENT-RUNTIME-MEMORY.md](../openclaw/agent/AGENT-RUNTIME-MEMORY.md) — 原始长期记忆参考

---

## 目录

1. [核心理念：三层记忆层次](#一核心理念三层记忆层次)
   - [1.1 问题定义](#11-问题定义)
   - [1.2 三层记忆模型](#12-三层记忆模型)
   - [1.3 与 OpenClaw 的关键差异](#13-与-openclaw-的关键差异)
2. [短期记忆：Session Store](#二短期记忆session-store)
   - [2.1 职责定义](#21-职责定义)
   - [2.2 数据模型](#22-数据模型)
   - [2.3 热冷分离存储](#23-热冷分离存储)
   - [2.4 Session 生命周期](#24-session-生命周期)
   - [2.5 分布式锁](#25-分布式锁)
   - [2.6 SessionStore 接口](#26-sessionstore-接口)
3. [长期记忆：Knowledge Store](#三长期记忆knowledge-store)
   - [3.1 设计思想](#31-设计思想)
   - [3.2 记忆条目模型](#32-记忆条目模型)
   - [3.3 记忆来源](#33-记忆来源)
   - [3.4 索引管线](#34-索引管线)
   - [3.5 混合搜索](#35-混合搜索)
   - [3.6 存储层](#36-存储层)
   - [3.7 记忆工具](#37-记忆工具)
4. [记忆召回管线](#四记忆召回管线)
   - [4.1 端到端流程](#41-端到端流程)
   - [4.2 与 Agent Loop 的集成](#42-与-agent-loop-的集成)
   - [4.3 与 System Prompt 的集成](#43-与-system-prompt-的集成)
   - [4.4 与 Context Manager 的协作](#44-与-context-manager-的协作)
5. [记忆隔离与多租户](#五记忆隔离与多租户)
6. [C 端规模化设计](#六c-端规模化设计)
7. [可观测性](#七可观测性)
8. [配置管理](#八配置管理)
9. [Phase 规划](#九phase-规划)
10. [包结构](#十包结构)

---

## 一、核心理念：三层记忆层次

### 1.1 问题定义

一个有用的 Agent 需要两种"记忆"能力：

```text
能力 1 — 单次对话连贯:
  用户说 "用 TypeScript 写", Agent 在第 15 轮仍然记得这个偏好。
  → 这需要管理好对话历史 (messages[]) 在上下文窗口中的呈现。

能力 2 — 跨会话延续:
  用户昨天说 "我的项目用 pnpm 而不是 npm", 今天开新会话时 Agent 仍然知道。
  → 这需要将关键知识持久化到对话之外, 并在新会话中召回。

原来的 "Session Store" 只解决了能力 1 的一半 (持久化 messages),
而上下文管理 (D7) 解决了另一半 (优化 messages 在上下文中的呈现)。
能力 2 完全缺失。
```

### 1.2 三层记忆模型

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                        Agent Memory 三层记忆模型                              │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Layer 1: 工作记忆 (Working Memory)                                    │  │
│  │                                                                        │  │
│  │  = LLM 上下文窗口中的 messages[]                                       │  │
│  │  容量: model.max_context_tokens (e.g. 200K)                           │  │
│  │  生命周期: 单次 LLM 调用                                               │  │
│  │  管理者: Context Manager (D7)                                          │  │
│  │  特点: LLM 直接可见, 每次调用前由四种策略优化                           │  │
│  │                                                                        │  │
│  │  类比: 人类的"此刻在想什么" (注意力焦点)                               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                              ▲ 加载                                          │
│                              │                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Layer 2: 短期记忆 (Short-term Memory) — Session Store                │  │
│  │                                                                        │  │
│  │  = 完整的、未裁剪的会话历史 (messages[])                               │  │
│  │  容量: 无限 (持久化存储)                                               │  │
│  │  生命周期: 一次会话 (从创建到关闭/过期)                                 │  │
│  │  存储: Redis (热, 24h TTL) + PostgreSQL (冷, 永久)                     │  │
│  │  特点: 保留所有原始消息, 会话恢复时重新加载到工作记忆                   │  │
│  │                                                                        │  │
│  │  类比: 人类的"今天和某人聊了什么" (短期回忆)                           │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                              ▲ 提取 / ▼ 召回                                │
│                              │                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Layer 3: 长期记忆 (Long-term Memory) — Knowledge Store               │  │
│  │                                                                        │  │
│  │  = 从会话中提取的关键知识 + 用户显式保存的信息                         │  │
│  │  容量: 无限                                                            │  │
│  │  生命周期: 跨会话持久 (直到用户删除)                                    │  │
│  │  存储: PostgreSQL + pgvector (向量索引)                                 │  │
│  │  检索: 混合搜索 (向量语义 + 关键词 BM25)                               │  │
│  │  特点: LLM 通过 memory_search 工具按需召回                             │  │
│  │                                                                        │  │
│  │  类比: 人类的"我知道这个用户喜欢 TypeScript" (长期知识)                │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

三层协作:
  ① 新会话开始 → Session Store 加载历史 → 送入 Context Manager → 工作记忆
  ② 每次 LLM 调用 → Context Manager.fit() 优化工作记忆
  ③ LLM 需要跨会话知识 → 调用 memory_search → Knowledge Store 检索 → 结果进入工作记忆
  ④ 会话结束 → Session Store 保存完整历史 + 提取关键知识 → Knowledge Store
```

### 1.3 与 OpenClaw 的关键差异

| 维度 | OpenClaw | Sahara |
| --- | --- | --- |
| 短期记忆存储 | JSONL 文件 (单机) | Redis + PostgreSQL (分布式) |
| 长期记忆存储 | SQLite + sqlite-vec (单机) | PostgreSQL + pgvector (分布式) |
| 记忆隔离 | 按 Agent ID | **按 (user_id, agent_id) 二元组** |
| 并发控制 | 文件锁 | Redis 分布式锁 |
| 记忆来源 | MEMORY.md 文件 + 会话转录 | **会话提取 + 用户保存 + Agent 配置 + 运营注入** |
| 检索方式 | SQLite FTS5 + sqlite-vec | pgvector + pg_trgm (或未来独立向量服务) |
| 规模 | 单用户/单机 | **C 端多租户 / 百万级用户** |

---

## 二、短期记忆：Session Store

### 2.1 职责定义

```text
Session Store 的职责:

  1. 持久化对话历史
     messages[] 的完整、未裁剪版本, 即使上下文管理裁剪了工作记忆,
     Session Store 保留的始终是原始数据。

  2. 会话生命周期管理
     创建 → 恢复 → 追加 → 关闭/过期

  3. 并发控制
     同一 session 同一时刻只允许一个 Agent 任务执行。

  4. 会话元数据管理
     title, agent_id, user_id, model, token 统计等。

  不是 Session Store 的职责:
  ✗ 裁剪 messages (→ Context Manager)
  ✗ 跨会话知识检索 (→ Knowledge Store)
  ✗ LLM 交互 (→ Agent Loop)
```

### 2.2 数据模型

```python
# sahara_runtime/memory/session/models.py

@dataclass
class SessionMeta:
    """会话元数据。"""
    session_key: str              # 全局唯一: "{user_id}:{agent_id}:{session_id}"
    session_id: str               # UUID
    user_id: str
    agent_id: str
    title: str | None = None      # 会话标题 (可由 LLM 自动生成)
    model: str | None = None      # 最近使用的模型
    status: str = "active"        # "active" | "closed" | "expired"
    created_at: float = 0.0
    updated_at: float = 0.0
    message_count: int = 0
    total_input_tokens: int = 0   # 累计输入 token
    total_output_tokens: int = 0  # 累计输出 token


@dataclass
class Session:
    """完整会话对象。"""
    meta: SessionMeta
    messages: list[dict]          # 完整的 messages[] (未裁剪)

    @classmethod
    def empty(cls, session_key: str, user_id: str, agent_id: str) -> "Session":
        return cls(
            meta=SessionMeta(
                session_key=session_key,
                session_id=str(uuid.uuid4()),
                user_id=user_id,
                agent_id=agent_id,
                created_at=time.time(),
                updated_at=time.time(),
            ),
            messages=[],
        )
```

### 2.3 热冷分离存储

```text
┌──────────────────────────────────────┐    ┌──────────────────────────────────┐
│  Redis (热存储)                       │    │  PostgreSQL (冷存储)              │
│                                      │    │                                  │
│  session:{key}:messages              │    │  sessions 表                      │
│  → 完整 messages JSON                │    │  ├── session_key  VARCHAR PK     │
│  → TTL 24h (活跃会话自动续期)        │    │  ├── session_id   UUID           │
│                                      │    │  ├── user_id      VARCHAR        │
│  session:{key}:meta                  │    │  ├── agent_id     VARCHAR        │
│  → SessionMeta JSON                  │    │  ├── title        VARCHAR        │
│  → TTL 7d                            │    │  ├── model        VARCHAR        │
│                                      │    │  ├── status       VARCHAR        │
│  session:{key}:lock                  │    │  ├── messages     JSONB          │
│  → 分布式锁 (防并发)                 │    │  ├── message_count INT           │
│                                      │    │  ├── total_input_tokens  BIGINT  │
│                                      │    │  ├── total_output_tokens BIGINT  │
│                                      │    │  ├── created_at   TIMESTAMPTZ   │
│                                      │    │  └── updated_at   TIMESTAMPTZ   │
│                                      │    │                                  │
│                                      │    │  索引:                           │
│                                      │    │  ├── idx_sessions_user_id        │
│                                      │    │  ├── idx_sessions_agent_id       │
│                                      │    │  └── idx_sessions_updated_at     │
└──────────────────────────────────────┘    └──────────────────────────────────┘

写入策略:
  Agent Loop 结束 → 同时写 Redis + 异步写 PG
  Redis 写入是同步的 (保证下次请求能读到)
  PG 写入是异步的 (不阻塞用户体验, 失败重试)

读取策略:
  Redis hit → 直接返回 (99%+ 命中率)
  Redis miss → 查 PG → 回填 Redis → 返回
```

### 2.4 Session 生命周期

```text
┌───────────────────────────────────────────────────────────────────────────┐
│                        Session 生命周期                                    │
│                                                                           │
│  ① 创建                                                                   │
│  ─────                                                                    │
│  触发: 用户首次发消息 / API 显式创建                                       │
│  动作: 生成 session_key, 创建空 Session                                    │
│  状态: active                                                             │
│                                                                           │
│  ② 活跃                                                                   │
│  ─────                                                                    │
│  触发: 每次 Agent Loop 执行                                                │
│  动作: 加载 messages → Agent 执行 → 保存更新的 messages                    │
│        自动续期 Redis TTL                                                  │
│  状态: active                                                             │
│                                                                           │
│  ③ 恢复                                                                   │
│  ─────                                                                    │
│  触发: Redis 过期后用户再次请求                                            │
│  动作: 从 PG 冷加载 → 回填 Redis → 继续对话                               │
│  状态: active                                                             │
│                                                                           │
│  ④ 关闭                                                                   │
│  ─────                                                                    │
│  触发: 用户显式关闭 / 管理 API 调用                                       │
│  动作: 提取关键知识 → Knowledge Store [Phase 2]                           │
│        标记 status = "closed"                                              │
│        清除 Redis 缓存                                                     │
│  状态: closed                                                             │
│                                                                           │
│  ⑤ 过期清理                                                               │
│  ─────                                                                    │
│  触发: 后台任务 (session.updated_at < now() - retention_days)              │
│  动作: 确保已提取知识 → 归档或删除 PG 记录                                │
│  状态: expired → 删除                                                     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

### 2.5 分布式锁

```python
# sahara_runtime/memory/session/lock.py

class SessionLock:
    """基于 Redis SET NX + TTL 的分布式锁。

    保证同一 session 同一时刻只有一个 Agent 任务在执行。
    gRPC SubmitTask 在获取锁失败时返回 ABORTED。
    """

    def __init__(self, redis: "Redis", ttl: int = 300):
        self.redis = redis
        self.ttl = ttl  # 5min, 与任务超时对齐

    async def acquire(self, session_key: str, timeout: float = 2.0) -> bool:
        lock_key = f"session:{session_key}:lock"
        lock_value = f"{config.worker_id}:{time.time()}"
        deadline = time.time() + timeout
        while time.time() < deadline:
            acquired = await self.redis.set(lock_key, lock_value, nx=True, ex=self.ttl)
            if acquired:
                self._lock_value = lock_value
                return True
            await asyncio.sleep(0.1)
        return False

    async def release(self, session_key: str):
        lock_key = f"session:{session_key}:lock"
        script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
        """
        await self.redis.eval(script, 1, lock_key, self._lock_value)
```

### 2.6 SessionStore 接口

```python
# sahara_runtime/memory/session/store.py

class SessionStore:
    """短期记忆存储。热冷分离, Redis + PostgreSQL。"""

    def __init__(self, redis: "Redis", pg: "asyncpg.Pool"):
        self._redis = redis
        self._pg = pg

    async def load(self, session_key: str) -> Session:
        """加载会话: Redis → PG → 空 Session。"""
        # 热读
        cached = await self._redis.get(f"session:{session_key}:messages")
        if cached:
            meta = await self._load_meta(session_key)
            return Session(meta=meta, messages=json.loads(cached))

        # 冷读 + 回填
        row = await self._pg.fetchrow(
            "SELECT * FROM sessions WHERE session_key = $1", session_key
        )
        if row:
            session = self._row_to_session(row)
            await self._warm_cache(session)
            return session

        return None  # 不存在

    async def create(self, user_id: str, agent_id: str) -> Session:
        """创建新会话。"""
        session = Session.empty(
            session_key=f"{user_id}:{agent_id}:{uuid.uuid4()}",
            user_id=user_id,
            agent_id=agent_id,
        )
        await self._save(session)
        return session

    async def save(self, session: Session):
        """保存会话: 同步写 Redis + 异步写 PG。"""
        session.meta.updated_at = time.time()
        session.meta.message_count = len(session.messages)

        # 同步写 Redis
        pipe = self._redis.pipeline()
        pipe.setex(f"session:{session.meta.session_key}:messages",
                   86400, json.dumps(session.messages))
        pipe.setex(f"session:{session.meta.session_key}:meta",
                   604800, session.meta.to_json())  # 7d TTL
        await pipe.execute()

        # 异步写 PG
        asyncio.create_task(self._save_to_pg(session))

    async def close(self, session_key: str):
        """关闭会话。标记状态 + 清除缓存。"""
        await self._pg.execute(
            "UPDATE sessions SET status = 'closed', updated_at = NOW() WHERE session_key = $1",
            session_key,
        )
        await self._redis.delete(
            f"session:{session_key}:messages",
            f"session:{session_key}:meta",
        )

    async def list_sessions(self, user_id: str, agent_id: str,
                            status: str = "active",
                            limit: int = 20, offset: int = 0) -> list[SessionMeta]:
        """列出用户的会话。供 API Service 调用。"""
        rows = await self._pg.fetch(
            """SELECT session_key, session_id, user_id, agent_id, title, model,
                      status, created_at, updated_at, message_count
               FROM sessions
               WHERE user_id = $1 AND agent_id = $2 AND status = $3
               ORDER BY updated_at DESC LIMIT $4 OFFSET $5""",
            user_id, agent_id, status, limit, offset,
        )
        return [self._row_to_meta(r) for r in rows]

    async def update_token_usage(self, session_key: str,
                                 input_tokens: int, output_tokens: int):
        """累加 token 使用统计。每次 LLM 调用后由 Agent Loop 调用。"""
        await self._pg.execute(
            """UPDATE sessions SET
                 total_input_tokens = total_input_tokens + $2,
                 total_output_tokens = total_output_tokens + $3,
                 updated_at = NOW()
               WHERE session_key = $1""",
            session_key, input_tokens, output_tokens,
        )
```

---

## 三、长期记忆：Knowledge Store

### 3.1 设计思想

```text
长期记忆的核心价值:

  "Agent 记住用户" — C 端产品的核心体验差异化

  场景:
  - 用户偏好: "我喜欢简洁的代码风格", "项目用 pnpm 不用 npm"
  - 项目知识: "数据库密码在 .env.local 里", "部署用 Vercel"
  - 历史决策: "上次选了方案 A 因为性能更好"
  - 待办追踪: "auth 模块的 bug 下周修"

  没有长期记忆:
  每次新会话, Agent 都是"失忆"状态, 用户需要反复交代背景。

  有长期记忆:
  新会话开始, Agent 已经知道用户的项目背景和偏好,
  直接进入高效工作状态。
```

> **Phase 1 不实现长期记忆**。Phase 1 专注 Session Store（短期记忆）。
> Phase 2 实现长期记忆的核心流程。以下为完整架构设计，指导 Phase 2 实现。

### 3.2 记忆条目模型

```python
# sahara_runtime/memory/knowledge/models.py

@dataclass
class MemoryEntry:
    """长期记忆条目。"""
    memory_id: str                    # UUID
    user_id: str                      # 所属用户
    agent_id: str                     # 所属 Agent
    source: str                       # "user_save" | "session_extract" | "agent_config" | "admin"
    category: str                     # "preference" | "fact" | "decision" | "todo" | "general"
    content: str                      # 记忆内容 (纯文本)
    embedding: list[float] | None     # 向量嵌入
    metadata: dict                    # 附加元数据 (来源 session_id, 置信度等)
    created_at: float
    updated_at: float
    access_count: int = 0             # 被召回次数 (用于 LRU/重要性排序)
    last_accessed_at: float = 0.0


@dataclass
class MemoryChunk:
    """记忆分块 (大型记忆内容被分块索引)。"""
    chunk_id: str
    memory_id: str                    # 所属 MemoryEntry
    content: str                      # 分块内容
    embedding: list[float]            # 向量嵌入
    chunk_index: int                  # 在原始内容中的位置
```

### 3.3 记忆来源

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                          记忆来源                                           │
│                                                                            │
│  ① 用户显式保存 (user_save) [Phase 2]                                      │
│  ─────────────────────────────                                             │
│  LLM 调用 memory_save 工具, 将用户明确要求记住的信息写入长期记忆。          │
│  示例: 用户说 "记住: 我的项目用 TypeScript + pnpm"                         │
│        → LLM 调用 memory_save("用户项目使用 TypeScript + pnpm")            │
│                                                                            │
│  ② 会话自动提取 (session_extract) [Phase 2]                                │
│  ─────────────────────────────────                                         │
│  会话结束时, 用 LLM 从对话历史中提取值得长期记忆的知识。                    │
│  提取内容: 用户偏好、项目配置、关键决策、待办事项。                         │
│  成本控制: 仅在会话 >= 5 轮时触发, 使用低成本模型。                        │
│                                                                            │
│  ③ Agent 配置 (agent_config) [Phase 2]                                     │
│  ─────────────────────────────                                             │
│  Agent 定义中预设的知识, 作为所有用户共享的"基础记忆"。                     │
│  示例: Agent 的领域知识、公司规范、常见 FAQ。                               │
│  特点: 只读, 管理员维护, 不参与用户级别的记忆搜索排序。                    │
│                                                                            │
│  ④ 运营注入 (admin) [Phase 3]                                              │
│  ─────────────────────                                                     │
│  运营人员为特定用户或全局注入记忆。                                         │
│  示例: VIP 用户的特殊偏好、临时通知。                                      │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### 3.4 索引管线

```python
# sahara_runtime/memory/knowledge/indexer.py

class MemoryIndexer:
    """记忆索引管线: 内容 → 分块 → 嵌入 → 存储。"""

    def __init__(self, embedder: "Embedder", store: "KnowledgeStore"):
        self._embedder = embedder
        self._store = store

    async def index(self, entry: MemoryEntry) -> None:
        """索引一条记忆。

        流程:
        1. 内容分块 (长内容按 ~400 tokens 分块, 短内容直接索引)
        2. 生成向量嵌入 (批量调用 Embedding API)
        3. 写入存储 (PostgreSQL + pgvector)
        """
        chunks = self._chunk_content(entry.content)

        # 批量嵌入
        texts = [c.content for c in chunks]
        embeddings = await self._embedder.embed_batch(texts)

        for chunk, embedding in zip(chunks, embeddings):
            chunk.embedding = embedding

        # 存储
        await self._store.upsert_memory(entry, chunks)

    def _chunk_content(self, content: str, max_tokens: int = 400,
                       overlap_tokens: int = 80) -> list[MemoryChunk]:
        """按 token 数分块, 在行边界处切分。"""
        if len(content) < max_tokens * 4:  # 短内容不分块
            return [MemoryChunk(
                chunk_id=str(uuid.uuid4()),
                memory_id="",
                content=content,
                embedding=[],
                chunk_index=0,
            )]

        chunks = []
        lines = content.split("\n")
        current = []
        current_chars = 0

        for line in lines:
            current.append(line)
            current_chars += len(line) + 1
            if current_chars >= max_tokens * 4:
                chunk_text = "\n".join(current)
                chunks.append(MemoryChunk(
                    chunk_id=str(uuid.uuid4()),
                    memory_id="",
                    content=chunk_text,
                    embedding=[],
                    chunk_index=len(chunks),
                ))
                # 保留 overlap
                overlap_chars = overlap_tokens * 4
                overlap_lines = []
                overlap_len = 0
                for l in reversed(current):
                    if overlap_len + len(l) > overlap_chars:
                        break
                    overlap_lines.insert(0, l)
                    overlap_len += len(l) + 1
                current = overlap_lines
                current_chars = overlap_len

        if current:
            chunks.append(MemoryChunk(
                chunk_id=str(uuid.uuid4()),
                memory_id="",
                content="\n".join(current),
                embedding=[],
                chunk_index=len(chunks),
            ))

        return chunks
```

### 3.5 混合搜索

```python
# sahara_runtime/memory/knowledge/search.py

@dataclass
class SearchResult:
    """搜索结果。"""
    memory_id: str
    content: str
    score: float                  # 综合得分
    vector_score: float           # 向量相似度
    keyword_score: float          # 关键词得分
    category: str
    source: str
    created_at: float


class MemorySearch:
    """混合搜索: 向量语义 + 关键词 BM25。"""

    DEFAULT_MAX_RESULTS = 6
    DEFAULT_MIN_SCORE = 0.35
    VECTOR_WEIGHT = 0.7
    KEYWORD_WEIGHT = 0.3

    def __init__(self, embedder: "Embedder", store: "KnowledgeStore"):
        self._embedder = embedder
        self._store = store

    async def search(self, query: str, user_id: str, agent_id: str,
                     max_results: int = DEFAULT_MAX_RESULTS,
                     min_score: float = DEFAULT_MIN_SCORE,
                     category: str | None = None) -> list[SearchResult]:
        """混合搜索。

        1. 生成 query embedding
        2. 并行: 向量搜索 + 关键词搜索
        3. 合并 + 排序 + 过滤
        """
        query_embedding = await self._embedder.embed(query)
        candidate_count = max_results * 4  # 扩大候选集

        # 并行搜索
        vector_task = self._store.vector_search(
            query_embedding, user_id, agent_id,
            limit=candidate_count, category=category,
        )
        keyword_task = self._store.keyword_search(
            query, user_id, agent_id,
            limit=candidate_count, category=category,
        )
        vector_results, keyword_results = await asyncio.gather(vector_task, keyword_task)

        # 合并
        merged = self._merge_results(vector_results, keyword_results)

        # 过滤 + 截取
        filtered = [r for r in merged if r.score >= min_score]
        top = sorted(filtered, key=lambda r: r.score, reverse=True)[:max_results]

        # 更新访问计数
        memory_ids = [r.memory_id for r in top]
        if memory_ids:
            asyncio.create_task(self._store.bump_access_count(memory_ids))

        return top

    def _merge_results(self, vector_results, keyword_results) -> list[SearchResult]:
        """加权合并两路结果。"""
        by_id: dict[str, SearchResult] = {}

        for r in vector_results:
            by_id[r.memory_id] = SearchResult(
                memory_id=r.memory_id, content=r.content,
                score=r.vector_score * self.VECTOR_WEIGHT,
                vector_score=r.vector_score, keyword_score=0.0,
                category=r.category, source=r.source, created_at=r.created_at,
            )

        for r in keyword_results:
            if r.memory_id in by_id:
                by_id[r.memory_id].keyword_score = r.keyword_score
                by_id[r.memory_id].score += r.keyword_score * self.KEYWORD_WEIGHT
            else:
                by_id[r.memory_id] = SearchResult(
                    memory_id=r.memory_id, content=r.content,
                    score=r.keyword_score * self.KEYWORD_WEIGHT,
                    vector_score=0.0, keyword_score=r.keyword_score,
                    category=r.category, source=r.source, created_at=r.created_at,
                )

        return list(by_id.values())
```

### 3.6 存储层

```sql
-- PostgreSQL DDL (需要 pgvector 扩展)

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- 关键词搜索加速

-- 记忆主表
CREATE TABLE memories (
    memory_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         VARCHAR(128) NOT NULL,
    agent_id        VARCHAR(128) NOT NULL,
    source          VARCHAR(32)  NOT NULL,  -- "user_save" | "session_extract" | ...
    category        VARCHAR(32)  NOT NULL,  -- "preference" | "fact" | ...
    content         TEXT         NOT NULL,
    metadata        JSONB        DEFAULT '{}',
    access_count    INT          DEFAULT 0,
    last_accessed_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_memories_user_agent ON memories(user_id, agent_id);
CREATE INDEX idx_memories_category ON memories(user_id, agent_id, category);
CREATE INDEX idx_memories_content_trgm ON memories USING gin(content gin_trgm_ops);

-- 记忆分块表 (含向量)
CREATE TABLE memory_chunks (
    chunk_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    memory_id       UUID NOT NULL REFERENCES memories(memory_id) ON DELETE CASCADE,
    content         TEXT NOT NULL,
    embedding       vector(1536),  -- text-embedding-3-small 维度
    chunk_index     INT  DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_chunks_memory ON memory_chunks(memory_id);
CREATE INDEX idx_chunks_embedding ON memory_chunks
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);  -- IVFFlat 索引, lists 数随数据量调整
```

```python
# sahara_runtime/memory/knowledge/store.py

class KnowledgeStore:
    """长期记忆存储层 (PostgreSQL + pgvector)。"""

    def __init__(self, pg: "asyncpg.Pool"):
        self._pg = pg

    async def vector_search(self, query_embedding: list[float],
                            user_id: str, agent_id: str,
                            limit: int = 24,
                            category: str | None = None) -> list[SearchResult]:
        """向量相似度搜索。"""
        sql = """
            SELECT m.memory_id, m.content, m.category, m.source, m.created_at,
                   1 - (mc.embedding <=> $1::vector) AS vector_score
            FROM memory_chunks mc
            JOIN memories m ON mc.memory_id = m.memory_id
            WHERE m.user_id = $2 AND m.agent_id = $3
        """
        params = [query_embedding, user_id, agent_id]

        if category:
            sql += " AND m.category = $4"
            params.append(category)

        sql += " ORDER BY mc.embedding <=> $1::vector LIMIT $" + str(len(params) + 1)
        params.append(limit)

        rows = await self._pg.fetch(sql, *params)
        return [SearchResult(
            memory_id=str(r["memory_id"]),
            content=r["content"],
            score=0.0,
            vector_score=float(r["vector_score"]),
            keyword_score=0.0,
            category=r["category"],
            source=r["source"],
            created_at=r["created_at"].timestamp(),
        ) for r in rows]

    async def keyword_search(self, query: str,
                             user_id: str, agent_id: str,
                             limit: int = 24,
                             category: str | None = None) -> list[SearchResult]:
        """关键词相似度搜索 (pg_trgm)。"""
        sql = """
            SELECT memory_id, content, category, source, created_at,
                   similarity(content, $1) AS keyword_score
            FROM memories
            WHERE user_id = $2 AND m.agent_id = $3
              AND content %% $1
        """
        params = [query, user_id, agent_id]
        if category:
            sql += " AND category = $4"
            params.append(category)

        sql += " ORDER BY keyword_score DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)

        rows = await self._pg.fetch(sql, *params)
        return [SearchResult(
            memory_id=str(r["memory_id"]),
            content=r["content"],
            score=0.0,
            vector_score=0.0,
            keyword_score=float(r["keyword_score"]),
            category=r["category"],
            source=r["source"],
            created_at=r["created_at"].timestamp(),
        ) for r in rows]

    async def bump_access_count(self, memory_ids: list[str]):
        """更新访问计数和最后访问时间。"""
        await self._pg.execute(
            """UPDATE memories
               SET access_count = access_count + 1, last_accessed_at = NOW()
               WHERE memory_id = ANY($1::uuid[])""",
            memory_ids,
        )
```

### 3.7 记忆工具

```python
# sahara_runtime/memory/knowledge/tools.py  [Phase 2]

class MemorySearchTool:
    """memory_search — LLM 可调用的长期记忆搜索工具。"""

    name = "memory_search"
    description = (
        "Search long-term memory for relevant knowledge about the user, "
        "their preferences, past decisions, project information, and todos. "
        "Use this before answering questions about prior work or user preferences."
    )
    parameters = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query describing what you want to recall",
            },
            "category": {
                "type": "string",
                "enum": ["preference", "fact", "decision", "todo", "general"],
                "description": "Optional: filter by memory category",
            },
        },
        "required": ["query"],
    }

    async def execute(self, args: dict, context: "ToolContext") -> str:
        results = await self._search.search(
            query=args["query"],
            user_id=context.user_id,
            agent_id=context.agent_id,
            category=args.get("category"),
        )
        if not results:
            return "No relevant memories found."

        lines = []
        for r in results:
            lines.append(f"[{r.category}] (score: {r.score:.2f}) {r.content}")
        return "\n\n".join(lines)


class MemorySaveTool:
    """memory_save — LLM 可调用的记忆保存工具。"""

    name = "memory_save"
    description = (
        "Save important information to long-term memory. Use when the user "
        "explicitly asks you to remember something, or when you learn a key "
        "preference, decision, or fact worth preserving across sessions."
    )
    parameters = {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "The information to remember",
            },
            "category": {
                "type": "string",
                "enum": ["preference", "fact", "decision", "todo", "general"],
                "description": "Category of the memory",
            },
        },
        "required": ["content", "category"],
    }

    async def execute(self, args: dict, context: "ToolContext") -> str:
        entry = MemoryEntry(
            memory_id=str(uuid.uuid4()),
            user_id=context.user_id,
            agent_id=context.agent_id,
            source="user_save",
            category=args["category"],
            content=args["content"],
            embedding=None,
            metadata={"session_key": context.session_key},
            created_at=time.time(),
            updated_at=time.time(),
        )
        await self._indexer.index(entry)
        return f"Saved to long-term memory: [{entry.category}] {entry.content[:100]}..."
```

---

## 四、记忆召回管线

### 4.1 端到端流程

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                    完整记忆召回流程 (Agent Loop 一次执行)                      │
│                                                                              │
│  1. 加载短期记忆                                                              │
│  ─────────────────                                                           │
│  Session Store → load(session_key) → messages[]                              │
│  所有原始消息加载到内存                                                       │
│                                                                              │
│  2. 注入长期记忆指引 [Phase 2]                                                │
│  ─────────────────────                                                       │
│  System Prompt Builder 注入 Memory Recall 段落:                              │
│  "在回答关于用户偏好、项目信息的问题前, 先用 memory_search 查询"             │
│  + memory_search / memory_save 工具加入工具集                                │
│                                                                              │
│  3. 上下文优化                                                                │
│  ─────────────                                                               │
│  Context Manager.fit() → 四种策略优化 messages[]                             │
│  messages[] 从"完整短期记忆"变为"优化的工作记忆"                             │
│                                                                              │
│  4. LLM 推理 (可能触发记忆搜索)                                              │
│  ─────────────────────────────                                               │
│  LLM 接收 system_prompt + 优化后的 messages + 工具定义                       │
│  → LLM 自主决定是否调用 memory_search (基于系统提示引导)                     │
│  → 搜索结果作为 tool_result 进入 messages                                    │
│  → LLM 结合记忆结果继续推理                                                  │
│                                                                              │
│  5. 保存短期记忆                                                              │
│  ─────────────────                                                           │
│  Agent Loop 结束 → Session Store.save(messages[]) → 保存完整原始消息         │
│                                                                              │
│  6. 提取长期记忆 [Phase 2, 会话关闭时]                                       │
│  ─────────────────────────────────────                                       │
│  会话关闭时 → LLM 从对话历史中提取关键知识                                   │
│  → Knowledge Store 索引新记忆                                                │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 与 Agent Loop 的集成

```python
# Agent Loop (§4) 中的记忆相关步骤:

async def run_agent_loop(...):
    # ── 1. 加载短期记忆 ──
    session = await deps.session_store.load(session_key)
    if session is None:
        session = await deps.session_store.create(user_id, agent_id)
    messages = session.messages

    # ── 2-8. (模型、沙箱、技能、工具、提示词等, 同原设计) ──
    ...

    # ── 5b. 注入记忆工具 [Phase 2] ──
    if deps.knowledge_store:
        tools.extend([
            MemorySearchTool(deps.knowledge_store),
            MemorySaveTool(deps.knowledge_store),
        ])

    # ── 9. 交互循环 ──
    while True:
        messages = await deps.context_manager.fit(messages, system_prompt, model_config)
        response = await _call_llm_streaming(...)
        ...

    # ── 11. 保存短期记忆 ──
    await deps.session_store.save(session)

    # ── 11b. 更新 token 统计 ──
    await deps.session_store.update_token_usage(
        session_key, total_input_tokens, total_output_tokens,
    )
```

### 4.3 与 System Prompt 的集成

```text
当 memory_search 工具可用时, System Prompt Builder (§8) 注入以下段落:

  ## Memory Recall

  You have access to long-term memory about this user. Before answering
  questions about the user's preferences, project setup, past decisions,
  or todos, use `memory_search` to recall relevant information.

  Guidelines:
  - Search before assuming — the user may have stated preferences before.
  - Use `memory_save` when the user explicitly asks you to remember something,
    or when you discover an important preference/fact worth preserving.
  - Don't save trivial or transient information.
  - Memory categories: preference, fact, decision, todo, general.

此段落在 PromptBuilder 的段落序列中排在 Context Files 之后、Custom Instructions 之前。
```

### 4.4 与 Context Manager 的协作

```text
关键原则: Context Manager 不感知长期记忆, 但尊重记忆搜索结果。

  memory_search 的 tool_result 与普通 tool_result 的处理完全相同:
  - Compaction: 超大搜索结果会被压实
  - Eviction: 旧的搜索结果可能被卸载
  - Filtering: 过时的搜索结果可能被过滤

  但有一个特殊规则:
  最近一次 memory_search 的结果应受保护, 不被 Eviction/Filtering。
  (因为 LLM 刚搜索的记忆信息很可能是当前任务所需的)
```

---

## 五、记忆隔离与多租户

```text
C 端多租户记忆隔离:

  ┌──────────────────────────────────────────────────────────────────┐
  │  记忆命名空间 = (user_id, agent_id)                              │
  │                                                                  │
  │  用户 A + Agent X → 记忆空间 A-X (只有 A 与 X 交互时可访问)    │
  │  用户 A + Agent Y → 记忆空间 A-Y (不同 Agent 的记忆完全隔离)   │
  │  用户 B + Agent X → 记忆空间 B-X (不同用户的记忆完全隔离)      │
  │                                                                  │
  │  特殊: Agent 配置级记忆 (source = "agent_config")               │
  │  → 全局共享, 所有用户都能搜索到, 但不可写                       │
  └──────────────────────────────────────────────────────────────────┘

  Session Store 隔离:
  session_key = "{user_id}:{agent_id}:{session_id}"
  用户只能访问自己的 session (API Service 层鉴权)

  Knowledge Store 隔离:
  所有查询都带 WHERE user_id = $1 AND agent_id = $2
  行级安全策略 (RLS) 作为额外保障 [可选]
```

---

## 六、C 端规模化设计

### 6.1 容量估算

```text
假设: 100 万用户, 每用户平均 50 条记忆, 每条记忆平均 2 个 chunk

  memories 表: 50M 行
  memory_chunks 表: 100M 行
  向量维度: 1536 (text-embedding-3-small)
  向量存储: 100M × 1536 × 4 bytes = ~600 GB

  sessions 表: 假设每用户 20 个会话 = 20M 行
  messages JSONB: 平均 100KB/会话 = ~2 TB
```

### 6.2 扩展策略

| 规模 | Session Store | Knowledge Store | Embedding |
| --- | --- | --- | --- |
| **Phase 1** (< 10K 用户) | 单 Redis + 单 PG | N/A | N/A |
| **Phase 2** (< 100K 用户) | Redis Cluster + PG | 单 PG + pgvector | 云 API (OpenAI) |
| **Phase 3** (100K+ 用户) | Redis Cluster + PG 分片 (user_id) | PG 分片 or 独立向量服务 (Qdrant/Milvus) | 自部署 embedding 模型 |

### 6.3 Embedding 抽象

```python
# sahara_runtime/memory/knowledge/embedder.py

class Embedder(ABC):
    """Embedding 提供商抽象。"""

    @abstractmethod
    async def embed(self, text: str) -> list[float]:
        """单条文本嵌入。"""
        ...

    @abstractmethod
    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """批量文本嵌入。"""
        ...

    @abstractmethod
    def dimension(self) -> int:
        """向量维度。"""
        ...


class OpenAIEmbedder(Embedder):
    """OpenAI text-embedding-3-small。"""
    MODEL = "text-embedding-3-small"

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        response = await self._client.embeddings.create(
            model=self.MODEL, input=texts,
        )
        return [e.embedding for e in response.data]

    def dimension(self) -> int:
        return 1536


class SelfHostedEmbedder(Embedder):
    """自部署 embedding 模型 (e.g. BGE, GTE)。[Phase 3]"""
    ...
```

### 6.4 成本控制

```text
Embedding 成本 (OpenAI text-embedding-3-small: $0.02 / 1M tokens):

  每条记忆平均 200 tokens:
  - 索引: 200 tokens × $0.02/M = $0.000004 / 条
  - 搜索: query ~50 tokens = $0.000001 / 次

  100 万用户 × 50 条记忆:
  - 索引总成本: 50M × 200 tokens = 10B tokens = $200 (一次性)
  - 每日搜索: 假设 500K 次 = $0.5 / 天

  结论: Embedding 成本可忽略, 主要成本在 LLM 调用。

会话自动提取成本:
  每次提取 ~2K input tokens + ~500 output tokens (低成本模型)
  = ~$0.001 / 次
  每日 10K 会话关闭 = $10 / 天
```

---

## 七、可观测性

### 7.1 核心指标

```python
# Session Store 指标
session_load_duration = Histogram("sahara_session_load_seconds", ...)
session_save_duration = Histogram("sahara_session_save_seconds", ...)
session_cache_hit = Counter("sahara_session_cache_hit_total",
    "Redis cache hits", ["result"])  # "hit" / "miss"
session_count = Gauge("sahara_session_count",
    "Active sessions", ["status"])

# Knowledge Store 指标 [Phase 2]
memory_search_duration = Histogram("sahara_memory_search_seconds", ...)
memory_search_results = Histogram("sahara_memory_search_results_count",
    "Number of results returned", buckets=[0, 1, 2, 3, 5, 10])
memory_index_duration = Histogram("sahara_memory_index_seconds", ...)
memory_total = Gauge("sahara_memory_total",
    "Total memory entries", ["source", "category"])
embedding_api_duration = Histogram("sahara_embedding_api_seconds", ...)
```

### 7.2 关键日志

| 事件 | 级别 | 说明 |
| --- | --- | --- |
| `session_loaded` | INFO | 会话加载, 包含 source (redis/pg/new), message_count |
| `session_saved` | INFO | 会话保存, 包含 message_count, token_usage |
| `session_pg_save_failed` | ERROR | PG 异步写入失败 |
| `session_lock_timeout` | WARN | 获取分布式锁超时 |
| `memory_search` | INFO | 记忆搜索, 包含 query, result_count, top_score [Phase 2] |
| `memory_saved` | INFO | 新记忆保存, 包含 source, category [Phase 2] |
| `memory_extracted` | INFO | 会话自动提取, 包含 count [Phase 2] |

---

## 八、配置管理

```python
# sahara_runtime/memory/config.py

@dataclass
class SessionConfig:
    redis_url: str = "redis://localhost:6379"
    redis_messages_ttl: int = 86_400          # 24h
    redis_meta_ttl: int = 604_800             # 7d
    pg_dsn: str = "postgresql://localhost/sahara"
    lock_ttl: int = 300                       # 5min
    lock_acquire_timeout: float = 2.0         # 2s
    session_retention_days: int = 90          # PG 数据保留天数

@dataclass
class KnowledgeConfig:
    enabled: bool = False                     # Phase 2 启用
    pg_dsn: str = "postgresql://localhost/sahara"
    embedding_provider: str = "openai"        # "openai" | "self_hosted"
    embedding_model: str = "text-embedding-3-small"
    embedding_dimension: int = 1536
    search_max_results: int = 6
    search_min_score: float = 0.35
    search_vector_weight: float = 0.7
    search_keyword_weight: float = 0.3
    chunk_max_tokens: int = 400
    chunk_overlap_tokens: int = 80
    auto_extract_enabled: bool = False        # 会话自动提取
    auto_extract_min_turns: int = 5           # 至少 5 轮才提取

@dataclass
class MemoryConfig:
    session: SessionConfig = field(default_factory=SessionConfig)
    knowledge: KnowledgeConfig = field(default_factory=KnowledgeConfig)
```

---

## 九、Phase 规划

| 阶段 | 范围 |
| --- | --- |
| **Phase 1** | **Session Store 完整实现**: SessionStore (load/save/create/close/list) + SessionLock (Redis 分布式锁) + SessionMeta (元数据管理) + 热冷分离 (Redis + PG) + Token 使用统计 + 基础指标 |
| **Phase 2** | **长期记忆核心**: KnowledgeStore (pgvector) + Embedder (OpenAI) + MemoryIndexer + MemorySearch (混合搜索) + memory_search / memory_save 工具 + System Prompt Memory Recall 段落 + 会话自动提取 (LLM) + 记忆管理 API (CRUD) |
| **Phase 3** | **规模化与智能化**: Embedding 自部署 + PG 分片 or 独立向量服务 + 记忆去重与合并 + 记忆重要性衰减 (access_count + 时间) + 记忆推荐 (主动在系统提示中注入高频记忆) + 运营注入 |

---

## 十、包结构

```text
sahara_runtime/
  memory/
  ├── __init__.py
  ├── config.py                       # MemoryConfig (Session + Knowledge)
  │
  ├── session/                        # Layer 2: 短期记忆
  │   ├── __init__.py
  │   ├── models.py                   # Session, SessionMeta
  │   ├── store.py                    # SessionStore (Redis + PG)
  │   ├── lock.py                     # SessionLock (分布式锁)
  │   └── metrics.py                  # 会话指标
  │
  ├── knowledge/                      # Layer 3: 长期记忆 [Phase 2]
  │   ├── __init__.py
  │   ├── models.py                   # MemoryEntry, MemoryChunk, SearchResult
  │   ├── store.py                    # KnowledgeStore (pgvector)
  │   ├── search.py                   # MemorySearch (混合搜索)
  │   ├── indexer.py                  # MemoryIndexer (分块 + 嵌入)
  │   ├── extractor.py               # SessionExtractor (会话自动提取) [Phase 2]
  │   ├── embedder.py                # Embedder ABC + OpenAIEmbedder
  │   ├── tools.py                   # memory_search / memory_save 工具定义
  │   └── metrics.py                 # 记忆指标
  │
  └── migration/                      # 数据库迁移
      ├── 001_sessions.sql
      └── 002_knowledge.sql           # [Phase 2]
```
