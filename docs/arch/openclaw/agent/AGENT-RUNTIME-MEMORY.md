# Agent Runtime 记忆系统

> 本文档详解 Agent Runtime 的长期记忆能力：如何对 `MEMORY.md`、`memory/*.md` 文件和会话转录进行向量索引，并通过混合搜索（向量 + 关键词）实现语义召回。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、记忆源与文件发现](#二记忆源与文件发现)
- [三、索引管线](#三索引管线)
- [四、Embedding 提供商](#四embedding-提供商)
- [五、存储层 (SQLite)](#五存储层-sqlite)
- [六、搜索机制](#六搜索机制)
- [七、同步策略](#七同步策略)
- [八、记忆工具](#八记忆工具)
- [九、系统提示集成](#九系统提示集成)
- [十、配置参考](#十配置参考)
- [十一、关键源文件索引](#十一关键源文件索引)

---

## 一、全局视角

### 1.1 记忆系统解决的问题

LLM 的上下文窗口有限，无法容纳所有历史信息。记忆系统让 Agent 拥有**跨会话的长期知识**——用户的偏好、项目决策、待办事项、过往讨论等被持久化索引，在需要时通过语义搜索召回。

### 1.2 架构总览

```text
┌───────────────────────────────────────────────────────────────────────┐
│                          记忆系统架构                                   │
│                                                                       │
│  ┌───────────────────┐                                                │
│  │   记忆源文件        │                                                │
│  │                   │                                                │
│  │  MEMORY.md        │     ┌──────────────┐     ┌──────────────────┐ │
│  │  memory/*.md      │────→│  索引管线     │────→│  SQLite 存储      │ │
│  │  extraPaths       │     │              │     │                  │ │
│  │  sessions/*.jsonl │     │  分块 → 嵌入  │     │  chunks 表       │ │
│  │  (实验性)         │     │              │     │  chunks_vec (向量)│ │
│  └───────────────────┘     └──────────────┘     │  chunks_fts (FTS) │ │
│                                                  │  embedding_cache  │ │
│                                                  └────────┬─────────┘ │
│                                                           │           │
│  ┌───────────────────┐     ┌──────────────┐              │           │
│  │  LLM 调用         │     │  混合搜索     │              │           │
│  │  memory_search()  │────→│              │←─────────────┘           │
│  │  memory_get()     │     │  向量相似度   │                          │
│  └───────────────────┘     │  + BM25 关键词│                          │
│                             └──────────────┘                          │
│                                                                       │
│  同步策略: watch(文件变更) / onSearch / onSessionStart / interval     │
└───────────────────────────────────────────────────────────────────────┘
```

### 1.3 关键设计决策

| 决策 | 选择 | 理由 |
| ---- | ---- | ---- |
| 存储引擎 | SQLite + sqlite-vec | 零部署、嵌入式、支持向量搜索 |
| 搜索方式 | 向量 + BM25 混合 | 语义理解 + 精确关键词互补 |
| 嵌入提供商 | 自动检测 (OpenAI/Gemini/本地) | 最大兼容性，无需手动配置 |
| 同步方式 | 文件监听 + 增量更新 | 低延迟、低开销 |
| 分块策略 | 按 token 数固定分块 + overlap | 平衡召回粒度和上下文完整性 |

---

## 二、记忆源与文件发现

### 2.1 记忆源类型

| 源类型 | 文件 | 默认启用 | 说明 |
| ---- | ---- | ---- | ---- |
| `memory` | `MEMORY.md`, `memory.md`, `memory/*.md` | 是 | 工作区内的知识文件 |
| `memory` (extraPaths) | 配置指定的额外路径 | 否 | 额外的 `.md` 文件或目录 |
| `sessions` | `~/.openclaw/agents/<id>/sessions/*.jsonl` | 否 (实验性) | 会话转录索引 |

### 2.2 文件发现规则

```text
扫描工作区:
    ├── MEMORY.md        ← 主记忆文件
    ├── memory.md        ← 别名
    ├── memory/          ← 记忆目录（递归扫描 .md 文件）
    │   ├── decisions.md
    │   ├── people.md
    │   └── todos.md
    └── (extraPaths 配置的额外路径)

过滤规则:
    • 只处理 .md 文件
    • 忽略符号链接
    • 按 realpath 去重
```

### 2.3 会话转录提取 (实验性)

当 `sources` 包含 `"sessions"` 时，系统会索引会话转录：

```text
sessions/abc123.jsonl
    │
    ▼  逐行解析 JSON
    │
    ├── type: "message", role: "user"   → "User: 你好，帮我看看代码"
    ├── type: "message", role: "assistant" → "Assistant: 好的，我来看看"
    ├── (跳过 toolResult 和其他类型)
    │
    ▼  拼接为纯文本
    │
    "User: 你好，帮我看看代码\nAssistant: 好的，我来看看\n..."
    │
    ▼  进入标准索引管线 (分块 → 嵌入 → 存储)
```

---

## 三、索引管线

### 3.1 分块策略

> 源文件: `src/memory/internal.ts` — `chunkMarkdown()`

```text
原始文件 (可能数千行)
    │
    ▼  按行分割
    │
    ▼  逐行累积，直到达到 maxChars (tokens × 4)
    │
    ├── 达到阈值 → 切分为一个 chunk
    │               携带最后 overlapChars 进入下一个 chunk
    │
    └── 文件末尾 → 剩余部分作为最后一个 chunk
```

**配置参数**:

| 参数 | 默认值 | 说明 |
| ---- | ---- | ---- |
| `chunking.tokens` | 400 | 每个 chunk 的目标 token 数 |
| `chunking.overlap` | 80 | chunk 之间的重叠 token 数 |

**每个 chunk 的数据**:

```typescript
{
  text: string;       // chunk 文本内容
  startLine: number;  // 起始行号 (1-based)
  endLine: number;    // 结束行号
  hash: string;       // 内容哈希 (用于变更检测)
}
```

**行边界保护**: 分块永远在行边界处切分，不会在一行中间断开。

### 3.2 索引流程

```text
文件变更检测 (sync)
    │
    ▼
indexFile(path, source)
    │
    ├── ① 读取文件内容
    │
    ├── ② chunkMarkdown() → chunks[]
    │
    ├── ③ 查找 embedding cache (按 chunk hash)
    │      ├── 命中 → 复用缓存的 embedding
    │      └── 未命中 → 调用 Embedding API
    │
    ├── ④ 删除该文件的旧数据 (chunks/vectors/FTS)
    │
    ├── ⑤ 插入新数据:
    │      ├── chunks 表 (文本 + 元数据)
    │      ├── chunks_vec 表 (向量索引)
    │      └── chunks_fts 表 (全文索引)
    │
    └── ⑥ 更新 files 表 (hash/mtime/size)
```

### 3.3 全量重索引

当 embedding 提供商、模型或分块参数变更时，触发**原子化全量重索引**：

```text
检测到配置变更
    │
    ▼
runSafeReindex()
    │
    ├── ① 创建临时 SQLite 数据库
    ├── ② 从旧数据库迁移 embedding cache (seedEmbeddingCache)
    ├── ③ 在临时库中完成全量索引
    ├── ④ 原子交换: 临时库 → 正式库
    └── ⑤ 删除旧库文件
```

---

## 四、Embedding 提供商

### 4.1 支持的提供商

| 提供商 | 默认模型 | API Key 来源 | 特点 |
| ---- | ---- | ---- | ---- |
| `openai` | `text-embedding-3-small` | `OPENAI_API_KEY` / 配置 | 批量 API、高质量 |
| `gemini` | `gemini-embedding-001` | `GEMINI_API_KEY` / 配置 | 批量 API、免费额度 |
| `local` | `embeddinggemma-300M` (GGUF) | 无需 Key | 离线运行、自动下载 |
| `auto` | (自动选择) | 检测可用的 Key | 优先 local → OpenAI → Gemini |

### 4.2 自动检测逻辑

```text
provider = "auto"
    │
    ├── local.modelPath 存在且为文件?
    │   → 尝试 local 提供商
    │
    ├── OPENAI_API_KEY 存在?
    │   → 尝试 OpenAI 提供商
    │
    ├── GEMINI_API_KEY 存在?
    │   → 尝试 Gemini 提供商
    │
    └── 全部失败 → 抛出错误
```

### 4.3 批量嵌入

远程提供商（OpenAI/Gemini）支持批量 API：

- 每批最多 8000 tokens
- 并发数: `batch.concurrency`（默认 2）
- 速率限制重试: 指数退避，最多 3 次
- 批量 API 连续失败 2 次后自动降级为逐条请求

### 4.4 Embedding 缓存

```text
生成 embedding 前:
    │
    ▼
查找 embedding_cache 表 (provider + model + provider_key + chunk_hash)
    │
    ├── 命中 → 直接使用缓存的向量
    └── 未命中 → 调用 API → 结果写入缓存

缓存淘汰:
    └── maxEntries 超限时 → 删除最旧的条目 (LRU by updated_at)
```

**Provider Key 指纹**: 由 `(provider, model, baseUrl, headers)` 计算的哈希。如果切换了 API endpoint，指纹变更会触发重索引，但缓存中的 embedding 会被迁移。

---

## 五、存储层 (SQLite)

### 5.1 数据库位置

默认: `~/.openclaw/memory/<agentId>.sqlite`

可通过 `memorySearch.store.path` 配置（支持 `{agentId}` 占位符）。

### 5.2 表结构

```text
┌─────────────────────────────────────────────────────────────┐
│  SQLite 数据库                                               │
│                                                             │
│  ┌─────────┐   ┌──────────────┐   ┌──────────────────────┐ │
│  │  meta    │   │   files      │   │   chunks             │ │
│  │         │   │              │   │                      │ │
│  │  配置快照│   │  文件追踪    │   │  文本 + 元数据 +     │ │
│  │  (JSON) │   │  (hash/mtime)│   │  embedding (JSON)    │ │
│  └─────────┘   └──────────────┘   └──────────┬───────────┘ │
│                                               │             │
│                          ┌────────────────────┼──────────┐  │
│                          │                    │          │  │
│                          ▼                    ▼          │  │
│               ┌──────────────────┐  ┌────────────────┐  │  │
│               │  chunks_vec      │  │  chunks_fts    │  │  │
│               │  (sqlite-vec)    │  │  (FTS5)        │  │  │
│               │                  │  │                │  │  │
│               │  向量相似度搜索   │  │  BM25 关键词   │  │  │
│               └──────────────────┘  └────────────────┘  │  │
│                                                          │  │
│               ┌──────────────────┐                       │  │
│               │  embedding_cache │  ← chunk hash → 向量  │  │
│               │  (LRU 缓存)      │                       │  │
│               └──────────────────┘                       │  │
└─────────────────────────────────────────────────────────────┘
```

### 5.3 容错降级

| 组件不可用 | 降级行为 |
| ---- | ---- |
| sqlite-vec 扩展 | JS 内计算余弦相似度（加载全部 chunk 到内存） |
| FTS5 | 仅向量搜索（跳过关键词分支） |
| 两者都不可用 | JS 余弦相似度，无关键词加权 |

---

## 六、搜索机制

### 6.1 混合搜索流程

```text
memory_search({ query: "上次讨论的数据库方案" })
    │
    ▼
① 生成 query embedding (embedQuery)
    │
    ▼
② 并行执行两路搜索:
    │
    ├── 路径 A: 向量搜索
    │   → chunks_vec 表 cosine 距离
    │   → 取 top (maxResults × candidateMultiplier) 候选
    │   → 返回 { chunkId, vectorScore }
    │
    └── 路径 B: 关键词搜索 (BM25)
        → chunks_fts 表 MATCH 查询
        → BM25 排名 → 转换为分数: textScore = 1 / (1 + max(0, rank))
        → 取 top (maxResults × candidateMultiplier) 候选
        → 返回 { chunkId, textScore }
    │
    ▼
③ 合并两路结果 (按 chunkId):
    finalScore = vectorWeight × vectorScore + textWeight × textScore
    │
    ▼
④ 按 finalScore 降序排序 → 取 top maxResults
    │
    ▼
⑤ 过滤 minScore → 返回结果
```

### 6.2 默认搜索参数

| 参数 | 默认值 | 说明 |
| ---- | ---- | ---- |
| `maxResults` | 6 | 最大返回结果数 |
| `minScore` | 0.35 | 最低分数阈值 |
| `vectorWeight` | 0.7 | 向量分数权重 |
| `textWeight` | 0.3 | 关键词分数权重 |
| `candidateMultiplier` | 4 | 候选数 = maxResults × 此值 |

**权重归一化**: `vectorWeight` 和 `textWeight` 会自动归一化使其和为 1.0。

---

## 七、同步策略

### 7.1 四种同步触发方式

```text
┌──────────────────────────────────────────────────────────────────┐
│  触发方式               何时                  说明               │
│                                                                  │
│  onSessionStart        首次搜索前            会话级预热          │
│  (默认开启)            warmSession()         每个 sessionKey     │
│                                              只触发一次          │
│                                                                  │
│  onSearch              每次搜索前            dirty 标志检查       │
│  (默认开启)            如果 dirty → sync     文件变更后生效       │
│                                                                  │
│  watch                 文件变更时            chokidar 监听       │
│  (默认开启)            add/change/unlink     防抖 1500ms          │
│                                                                  │
│  interval              定时                  intervalMinutes      │
│  (默认关闭)            setInterval()         设为 0 = 禁用        │
└──────────────────────────────────────────────────────────────────┘
```

### 7.2 会话转录同步 (实验性)

```text
会话中产生新消息 → onSessionTranscriptUpdate()
    │
    ├── 累计 pendingBytes += 新内容大小
    ├── 累计 pendingMessages += 新消息数
    │
    ├── pendingBytes ≥ 100KB 或 pendingMessages ≥ 50?
    │   → 标记 dirty → 5s 防抖 → 触发增量 sync
    │
    └── 未达阈值 → 继续累积
```

### 7.3 增量 vs 全量

| 场景 | 行为 |
| ---- | ---- |
| 文件内容变更 (hash 不同) | 增量更新该文件 |
| 文件被删除 | 删除该文件的所有 chunks |
| 新文件出现 | 新建索引 |
| Provider/Model/分块参数变更 | 全量重索引 (原子交换) |

---

## 八、记忆工具

### 8.1 memory_search

LLM 使用的语义搜索工具。

| 参数 | 类型 | 必需 | 说明 |
| ---- | ---- | ---- | ---- |
| `query` | string | 是 | 搜索查询 |
| `maxResults` | number | 否 | 覆盖默认最大结果数 |
| `minScore` | number | 否 | 覆盖默认最低分数 |

**返回格式**:

```json
{
  "results": [
    {
      "path": "memory/decisions.md",
      "startLine": 12,
      "endLine": 25,
      "score": 0.82,
      "snippet": "## 数据库方案\n选择 PostgreSQL...",
      "source": "memory"
    }
  ],
  "provider": "openai",
  "model": "text-embedding-3-small",
  "fallback": false
}
```

### 8.2 memory_get

按路径和行号精确读取记忆文件片段。

| 参数 | 类型 | 必需 | 说明 |
| ---- | ---- | ---- | ---- |
| `path` | string | 是 | 工作区相对路径 |
| `from` | number | 否 | 起始行号 (1-indexed) |
| `lines` | number | 否 | 读取行数 |

**典型使用模式**: LLM 先用 `memory_search` 找到相关片段（包含 path + startLine），再用 `memory_get` 读取完整上下文。

### 8.3 工具创建条件

- `resolveMemorySearchConfig()` 返回非 null（即 `enabled: true`）
- `getMemorySearchManager()` 能正常获取管理器实例
- 子 Agent 默认**不**创建记忆工具（在默认拒绝列表中）

---

## 九、系统提示集成

> 源文件: `src/agents/system-prompt.ts`

当 `memory_search` 或 `memory_get` 工具可用时，系统提示中会注入：

```text
## Memory Recall

Before answering anything about prior work, decisions, dates,
people, preferences, or todos: run memory_search on MEMORY.md +
memory/*.md; then use memory_get to pull only the needed lines.
If low confidence after search, say you checked.
```

**注入条件**: 仅在 `promptMode = "full"` 时注入（子 Agent 的 `"minimal"` 模式跳过）。

---

## 十、配置参考

### 完整配置示例

```yaml
agents:
  defaults:
    memorySearch:
      enabled: true
      sources: ["memory"]                    # ["memory", "sessions"]
      extraPaths: ["docs/notes"]             # 额外扫描路径

      provider: "auto"                       # "openai" | "gemini" | "local" | "auto"
      model: "text-embedding-3-small"        # 嵌入模型
      fallback: "none"                       # 嵌入失败时的备用: "openai" | "gemini" | "local" | "none"

      remote:
        baseUrl: "https://api.openai.com/v1" # 自定义 endpoint
        apiKey: "sk-..."                     # 覆盖环境变量
        batch:
          enabled: true                      # 批量 API
          concurrency: 2                     # 并发数

      local:
        modelPath: "/path/to/model.gguf"     # 本地模型路径
        modelCacheDir: "~/.cache/openclaw"   # 模型缓存目录

      store:
        driver: "sqlite"
        path: "~/.openclaw/memory/{agentId}.sqlite"
        vector:
          enabled: true                      # sqlite-vec 向量索引
          extensionPath: ""                  # 自定义扩展路径

      chunking:
        tokens: 400                          # 每 chunk token 数
        overlap: 80                          # 重叠 token 数

      sync:
        onSessionStart: true                 # 会话首次搜索前同步
        onSearch: true                       # 每次搜索前检查 dirty
        watch: true                          # 文件变更监听
        watchDebounceMs: 1500                # 监听防抖 (ms)
        intervalMinutes: 0                   # 定时同步 (0=禁用)
        sessions:
          deltaBytes: 100000                 # 转录同步阈值 (字节)
          deltaMessages: 50                  # 转录同步阈值 (消息数)

      query:
        maxResults: 6                        # 最大结果数
        minScore: 0.35                       # 最低分数
        hybrid:
          enabled: true                      # 混合搜索
          vectorWeight: 0.7                  # 向量权重
          textWeight: 0.3                    # 关键词权重
          candidateMultiplier: 4             # 候选倍数

      cache:
        enabled: true                        # embedding 缓存
        maxEntries: 10000                    # 最大缓存条目

      experimental:
        sessionMemory: false                 # 会话转录索引
```

---

## 十一、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/agents/memory-search.ts` | 配置解析、`MemoryIndexManager.get()`、搜索入口 |
| `src/agents/tools/memory-tool.ts` | `memory_search` / `memory_get` 工具定义 |
| `src/memory/manager.ts` | `MemoryIndexManager`：索引、同步、搜索、重索引 |
| `src/memory/internal.ts` | `chunkMarkdown()`：分块策略 |
| `src/memory/embeddings.ts` | 嵌入提供商工厂、自动检测 |
| `src/memory/embeddings-openai.ts` | OpenAI 嵌入实现（批量 API） |
| `src/memory/embeddings-gemini.ts` | Gemini 嵌入实现（批量 API） |
| `src/memory/memory-schema.ts` | SQLite 表结构定义 |
| `src/memory/sqlite-vec.ts` | sqlite-vec 扩展加载 |
| `src/memory/sync-memory-files.ts` | 文件发现与增量同步 |
| `src/agents/system-prompt.ts` | Memory Recall 提示注入 |
