# 上下文管理架构设计

> Runtime 中的 LLM 上下文窗口管理：在有限的上下文窗口中，最大化信息的知识比特率，同时缓解注意力衰减问题。
> 通过四种核心策略——卸载 (Eviction)、压实 (Compaction)、摘要 (Summarization)、过滤 (Filtering)——
> 确保每一个 Token 都承载对当前任务最有价值的信息，且关键信息处于 LLM 注意力高效区。
>
> 关联文档：
> - [Runtime 架构设计 §11](./RUNTIME-ARCHITECTURE-DESIGN.md) — Context Manager 接口概述
> - [Runtime 架构设计 §8](./RUNTIME-ARCHITECTURE-DESIGN.md) — System Prompt Builder (上下文文件注入)
> - [Runtime 架构设计 §7](./RUNTIME-ARCHITECTURE-DESIGN.md) — Tools 输出截断 (§7.9)
> - [Runtime 架构设计 §6](./RUNTIME-ARCHITECTURE-DESIGN.md) — Model Router (上下文窗口参数)
> - [OpenClaw AGENT-RUNTIME-SYSTEM-PROMPT.md §6](../openclaw/agent/AGENT-RUNTIME-SYSTEM-PROMPT.md) — 原始上下文管理参考

---

## 目录

1. [核心理念：知识比特率与注意力衰减](#一核心理念知识比特率与注意力衰减)
2. [上下文窗口预算模型](#二上下文窗口预算模型)
3. [四种核心策略总览](#三四种核心策略总览)
4. [策略一：卸载 (Eviction)](#四策略一卸载-eviction)
   - [4.1 设计思想](#41-设计思想)
   - [4.2 卸载目标与存储](#42-卸载目标与存储)
   - [4.3 消息卸载机制](#43-消息卸载机制)
   - [4.4 卸载索引与按需召回](#44-卸载索引与按需召回)
   - [4.5 卸载策略配置](#45-卸载策略配置)
5. [策略二：压实 (Compaction)](#五策略二压实-compaction)
   - [5.1 设计思想](#51-设计思想)
   - [5.2 工具结果压实](#52-工具结果压实)
   - [5.3 结构化数据压实](#53-结构化数据压实)
   - [5.4 上下文文件压实](#54-上下文文件压实)
   - [5.5 两阶段压实](#55-两阶段压实softhard)
6. [策略三：摘要 (Summarization)](#六策略三摘要-summarization)
   - [6.1 设计思想](#61-设计思想)
   - [6.2 分阶段摘要](#62-分阶段摘要)
   - [6.3 增量摘要](#63-增量摘要)
   - [6.4 摘要 Prompt 设计](#64-摘要-prompt-设计)
   - [6.5 摘要质量保障](#65-摘要质量保障)
7. [策略四：过滤 (Filtering)](#七策略四过滤-filtering)
   - [7.1 设计思想](#71-设计思想)
   - [7.2 消息相关性评分](#72-消息相关性评分)
   - [7.3 上下文文件过滤](#73-上下文文件过滤)
   - [7.4 工具结果过滤](#74-工具结果过滤)
8. [策略编排引擎](#八策略编排引擎)
   - [8.1 ContextManager 核心接口](#81-contextmanager-核心接口)
   - [8.2 策略执行流水线](#82-策略执行流水线)
   - [8.3 上下文窗口守卫](#83-上下文窗口守卫)
9. [Token 计数器](#九token-计数器)
10. [多模型上下文窗口适配](#十多模型上下文窗口适配)
11. [与其他子系统的协作](#十一与其他子系统的协作)
12. [C 端成本控制](#十二c-端成本控制)
13. [可观测性](#十三可观测性)
14. [配置管理](#十四配置管理)
15. [Phase 规划](#十五phase-规划)
16. [包结构](#十六包结构)

---

## 一、核心理念：知识比特率与注意力衰减

### 1.1 双重问题

上下文管理面对的不是一个问题，而是**两个相互叠加的问题**：

```text
问题 1 — 窗口容量有限:
  Agent 多轮工具调用产生的信息量轻松超过上下文窗口上限。
  这是"装不下"的问题。

问题 2 — 注意力衰减 (Attention Degradation):
  即使窗口装得下，LLM 也无法均匀利用所有 Token。
  由于 Transformer 注意力机制的固有特性, 上下文越长:
  - 对中间位置信息的关注度下降 ("Lost in the Middle" 效应)
  - 关键指令被噪声稀释, 遵循度降低
  - 推理质量整体退化
  这是"装得下但用不好"的问题。

上下文管理必须同时解决这两个问题。
```

**知识比特率**是统一度量：

```text
知识比特率 = 对当前任务有价值的信息量 / 实际消耗的 Token 数

目标: 在 available_tokens 约束下, 最大化知识比特率
       ─────────── 解决问题 1 (容量)
同时: 缩短有效上下文长度, 让关键信息处于注意力高效区
       ─────────── 解决问题 2 (衰减)
```

### 1.2 注意力衰减：被低估的核心问题

#### 1.2.1 Lost in the Middle 效应

学术研究 (Liu et al., 2023) 和工程实践反复证实：LLM 对上下文中不同位置的信息关注度呈 **U 型分布**——开头和结尾关注度最高，中间部分被系统性忽略。

```text
LLM 注意力分布 (U-Shape Attention):

  注意力
  强度
   ▲
   │ █                                                    █ █
   │ █ █                                                █ █ █
   │ █ █ █                                            █ █ █ █
   │ █ █ █ █                                        █ █ █ █ █
   │ █ █ █ █ █ ░                              ░ █ █ █ █ █ █ █
   │ █ █ █ █ █ ░ ░ ░                    ░ ░ ░ ░ █ █ █ █ █ █ █
   │ █ █ █ █ █ ░ ░ ░ ░ ░ ░ ░ ░ ░ ░ ░ ░ ░ ░ ░ ░ █ █ █ █ █ █
   └──────────────────────────────────────────────────────────→ 位置
     System     旧 Tool      中间对话历史      最近     User
     Prompt     Results       (注意力盲区)      Tool    Message
                                               Results

     ◀── 高关注 ──▶  ◀──── 低关注 (Lost) ────▶  ◀── 高关注 ──▶
```

#### 1.2.2 注意力衰减对 Agent 的具体影响

| 场景 | 退化表现 | 根因 |
| --- | --- | --- |
| **关键指令被忽略** | system_prompt 中的安全规则被违反 | 安全规则在 system_prompt 中部，被大量工具定义稀释 |
| **重复执行相同工具** | LLM 再次 read 已经读过的文件 | 历史 tool_result 在中间位置，LLM "看不见" |
| **丢失用户偏好** | 用户第 2 轮说 "用 TypeScript"，第 15 轮 LLM 用 Python 实现 | 偏好指令被后续大量工具结果挤到中间 |
| **逻辑不连贯** | 前面分析出方案 A 最优，后面却实现方案 B | 决策推理链在中间，被后续交互淹没 |
| **错误重复** | 同一个命令报错 3 次，LLM 第 4 次仍尝试 | 错误信息在中间位置的旧 tool_result 中 |

#### 1.2.3 上下文长度 vs. 推理质量

```text
LLM 输出质量与上下文长度的关系:

  质量
   ▲
   │ ██████
   │       ████
   │           ████
   │               ████
   │                   ████
   │                       ████                    ← 质量拐点
   │                           ████████
   │                                   ████████
   │                                           ████████
   └──────────────────────────────────────────────────→ 上下文 Token 数
     0     10K    20K    30K    50K    80K    128K   200K

  观察:
  - 0~30K:  质量几乎不衰减, 信息被充分利用
  - 30~80K: 开始出现信息忽略, 指令遵循度下降
  - 80K+:   显著退化, "Lost in the Middle" 效应明显
  - 128K+:  即使窗口支持, 推理质量已大幅低于短上下文

  ⟹ 上下文管理的目标不只是 "不超 200K",
     而是尽量将有效长度控制在质量拐点以内。
```

#### 1.2.4 四种策略对注意力衰减的缓解作用

```text
策略                对注意力衰减的作用
─────────────────────────────────────────────────────────────────
Filtering (过滤)    直接移除无关信息 → 缩短有效上下文 →
                    关键信息占比提升 → 注意力更集中于有价值内容

Compaction (压实)   压缩冗长输出 → 减少中间区域的噪声 Token →
                    关键信息在中间区域不被淹没

Eviction (卸载)     大块旧信息移出 → 替换为几十 Token 的索引 →
                    上下文物理长度大幅缩短 → 远离质量拐点
                    + 索引摘要本身是高密度信息, 比原始内容更易被注意

Summarization       多轮旧对话 → 短摘要 →
(摘要)              历史知识以高密度形式保留在上下文开头 →
                    利用 U 型注意力的开头高关注区

综合效果:
  未优化: 150K tokens, 大量噪声, 关键信息在中间 → 质量差
  优化后:  30K tokens, 高密度, 关键信息在首尾 → 质量接近最优
```

### 1.3 低比特率的根源

典型 Agent 执行过程中，上下文中大量 Token 承载的是低价值信息，这些信息既浪费窗口容量，又稀释注意力：

| 低价值信息 | 示例 | Token 浪费 | 注意力稀释 |
| --- | --- | --- | --- |
| **冗余工具输出** | `ls -la` 返回 500 行目录列表，实际只需知道 3 个文件 | 90%+ | 严重: 中间区域噪声 |
| **过时的中间结果** | 10 轮前的 `grep` 搜索结果，当前任务已转向 | 100% | 严重: 无关信息占据注意力 |
| **冗长的错误堆栈** | 完整 Java stacktrace 50KB，关键信息只在前 3 行 | 95%+ | 中等: 关键行被埋没 |
| **重复的文件内容** | 同一文件被 `read` 多次，每次都完整保留 | 50-80% | 严重: LLM 可能反复处理 |
| **已完成的子任务** | 前 5 轮完成的配置修改，现在在做测试 | 70-90% | 高: 干扰当前任务焦点 |

### 1.4 四种策略的信息论视角

```text
                      知识比特率优化

  ┌────────────────────────────────────────────────────────┐
  │                                                        │
  │  卸载 (Eviction)                                       │
  │  ──────────────                                        │
  │  将低访问频率的信息移出上下文窗口，存入外部文件。         │
  │  LLM 在需要时可通过 read 工具召回。                     │
  │  类比: 操作系统的虚拟内存 / CPU 缓存分级               │
  │  效果: 不丢失信息，但释放窗口空间                      │
  │                                                        │
  │  压实 (Compaction)                                     │
  │  ──────────────                                        │
  │  在保留语义的前提下，减少信息的物理体积。                │
  │  去除冗余、截断、结构化紧缩。                           │
  │  类比: 数据库的页面压缩 / 文件系统的碎片整理            │
  │  效果: 信息不变，Token 数下降                          │
  │                                                        │
  │  摘要 (Summarization)                                  │
  │  ──────────────                                        │
  │  用 LLM 将大量低密度信息转化为高密度摘要。              │
  │  有损压缩，但保留对当前任务最关键的知识。                │
  │  类比: 有损压缩 (JPEG) / 信息蒸馏                     │
  │  效果: 大幅降低 Token 数，可能丢失细节                 │
  │                                                        │
  │  过滤 (Filtering)                                      │
  │  ──────────────                                        │
  │  根据与当前任务的相关性，选择性地包含/排除上下文。       │
  │  基于规则或语义评分，只保留对当前决策最有帮助的信息。     │
  │  类比: 注意力机制 / 信息检索                           │
  │  效果: 直接提升知识比特率                              │
  │                                                        │
  └────────────────────────────────────────────────────────┘
```

### 1.5 与 OpenClaw 的关键差异

| 维度 | OpenClaw | Sahara |
| --- | --- | --- |
| 设计哲学 | 被动防御（四层降级） | **主动优化（知识比特率 + 注意力友好）** |
| 注意力衰减 | 未考虑 | **核心设计目标：缩短有效上下文，控制在质量拐点以内** |
| 卸载 | 无 | **支持消息卸载到文件 + 按需召回** |
| 压实 | 简单截断 | **两阶段压实 (Soft/Hard) + 结构化紧缩** |
| 摘要 | Pi SDK 内置单次摘要 | **分阶段摘要 + 增量摘要 (摘要置于开头高注意力区)** |
| 过滤 | 无 | **基于相关性的消息过滤** [Phase 3] |
| 核心指标 | "不超标就行" | **知识比特率 + 有效上下文长度** |

---

## 二、上下文窗口预算模型

### 2.1 窗口组成结构

```text
┌─────────────────────────────────────────────────────────────────┐
│                  LLM 上下文窗口 (e.g. 200K tokens)              │
│                                                                 │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  System Prompt (§8)                      [相对稳定]    │     │
│  │  Identity + Safety + Tool Guide + Skills + Sandbox    │     │
│  │  + Context Files + Custom Instructions + Runtime Info │     │
│  └───────────────────────────────────────────────────────┘     │
│                                                                 │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  Conversation History (messages[])       [动态增长]    │     │
│  │                                                       │     │
│  │  [user]          初始任务描述                          │     │
│  │  [assistant]     思考 + 调用工具 A                     │     │
│  │  [tool_result]   工具 A 的输出  ← 膨胀主因            │     │
│  │  [assistant]     分析 + 调用工具 B                     │     │
│  │  [tool_result]   工具 B 的输出  ← 膨胀主因            │     │
│  │  ...                                                   │     │
│  │  [eviction_ref]  已卸载消息的索引摘要  ← 卸载策略      │     │
│  │  [user]          最新一条用户消息                       │     │
│  │                                                       │     │
│  │  ← 四种策略的主要作用对象                              │     │
│  └───────────────────────────────────────────────────────┘     │
│                                                                 │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  Output Reserve (max_tokens)              [固定预留]   │     │
│  └───────────────────────────────────────────────────────┘     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 预算分配公式

```python
context_window   = model.max_context_tokens        # e.g. 200,000
output_reserve   = model.max_tokens                 # e.g. 8,192
system_tokens    = token_counter.count(system_prompt)  # e.g. ~3,000

available_for_messages = context_window - output_reserve - system_tokens
# 示例: 200,000 - 8,192 - 3,000 = 188,808 tokens
```

### 2.3 使用率区间与策略触发

```text
消息 Token 使用率 = messages_tokens / available_for_messages

  0%           30%            50%            75%          100%
  ├─────────────┼──────────────┼──────────────┼─────────────┤
  │   绿色区     │   黄色区     │   橙色区      │   红色区     │
  │             │              │              │             │
  │  无操作     │  Compaction  │  Compaction  │ Summarize   │
  │  Filtering  │  (Soft)      │  (Hard)      │ + Eviction  │
  │  可选       │  + Filtering │  + Eviction  │ + 紧急截断  │
  └─────────────┴──────────────┴──────────────┴─────────────┘
```

---

## 三、四种核心策略总览

```text
┌────────────────────────────────────────────────────────────────────┐
│                     上下文管理策略体系                               │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ① 卸载 (Eviction)                                                │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ 将低频访问的消息（旧工具结果、已完成的子任务）写入沙箱文件。  │ │
│  │ 上下文中只保留一条轻量索引摘要。                              │ │
│  │ LLM 需要时可通过 read 工具召回原始内容。                     │ │
│  │                                                              │ │
│  │ 信息损失: 零 (可召回)     Token 节省: 70-95%                 │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ② 压实 (Compaction)                                              │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ 在保留语义的前提下，物理压缩消息体积。                        │ │
│  │ 包括: 工具结果头尾截断、结构化数据紧缩、冗余去除。            │ │
│  │                                                              │ │
│  │ Soft: > 4K 字符的 tool_result → 保留头 1.5K + 尾 1.5K       │ │
│  │ Hard: 替换为占位符 "[Tool result compacted]"                 │ │
│  │                                                              │ │
│  │ 信息损失: 低-中          Token 节省: 30-80%                  │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ③ 摘要 (Summarization)                                           │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ 用 LLM 将一组旧消息生成高密度摘要，替换原始消息。             │ │
│  │ 有损压缩，但通过精心设计的 Prompt 保留关键知识。              │ │
│  │                                                              │ │
│  │ 分阶段: 分块摘要 → 合并摘要                                  │ │
│  │ 增量式: 每 N 轮追加新摘要，不重新生成                        │ │
│  │                                                              │ │
│  │ 信息损失: 中             Token 节省: 80-95%                  │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ④ 过滤 (Filtering)                                               │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ 根据与当前任务的相关性，选择性保留上下文内容。                 │ │
│  │ 基于规则 (Phase 1) 或语义评分 (Phase 3) 决定去留。            │ │
│  │                                                              │ │
│  │ 规则过滤: 相同文件多次 read → 只保留最后一次                  │ │
│  │ 语义过滤: 当前任务是"部署" → 过滤掉"代码格式化"相关上下文    │ │
│  │                                                              │ │
│  │ 信息损失: 低 (精准过滤)   Token 节省: 20-60%                 │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

**策略对比矩阵**：

| 维度 | 卸载 Eviction | 压实 Compaction | 摘要 Summarization | 过滤 Filtering |
| --- | --- | --- | --- | --- |
| **信息损失** | 零 (可召回) | 低-中 | 中 | 低 |
| **Token 节省** | 70-95% | 30-80% | 80-95% | 20-60% |
| **注意力改善** | ★★★★★ 大幅缩短上下文 | ★★★ 减少中间噪声 | ★★★★ 高密度摘要置于开头 | ★★★★ 移除注意力干扰源 |
| **延迟开销** | < 5ms (文件写入) | < 1ms | 2-5s (LLM 调用) | < 5ms |
| **召回能力** | 完整召回 | 不可恢复 | 不可恢复 | 不可恢复 |
| **适用对象** | 旧消息、大型工具结果 | 工具结果、结构化数据 | 多轮旧对话 | 重复/无关内容 |
| **Phase** | Phase 1 | Phase 1 | Phase 2 | Phase 1 (规则) / Phase 3 (语义) |

---

## 四、策略一：卸载 (Eviction)

### 4.1 设计思想

卸载是上下文管理中最优雅的策略——**零信息损失**。其核心思想借鉴操作系统的虚拟内存：

```text
类比:
  操作系统: 物理内存不足 → 将不活跃的页面换出到 swap → 需要时 page fault 换入
  上下文管理: 上下文窗口不足 → 将旧消息卸载到沙箱文件 → LLM 需要时 read 召回

关键:
  1. 卸载的消息写入沙箱文件 (/workspace/.context/)
  2. 上下文中只保留一条轻量索引 (文件路径 + 内容摘要)
  3. LLM 在后续推理中如果需要已卸载的信息, 可自主决定 read 该文件
  4. 信息始终可访问, 只是从"热"变为"温"
```

### 4.2 卸载目标与存储

```python
# sahara_runtime/context/eviction.py

@dataclass
class EvictedMessage:
    """已卸载消息的元数据。"""
    original_index: int           # 在原始 messages 中的位置
    role: str                     # "assistant" / "tool"
    tool_name: str | None         # 如果是 tool_result, 工具名称
    file_path: str                # 卸载到的文件路径
    original_tokens: int          # 原始 token 数
    summary: str                  # 内容摘要 (1-2 句话)
    evicted_at: float             # 卸载时间戳


class MessageEviction:
    """消息卸载策略。将低频访问的消息写入沙箱文件。"""

    EVICTION_DIR = "/workspace/.context/evicted"

    async def evict(self, messages: list[dict], sandbox: "Sandbox",
                    candidates: list[int]) -> tuple[list[dict], list[EvictedMessage]]:
        """将指定索引的消息卸载到沙箱文件。

        Args:
            messages: 完整消息列表
            sandbox: 沙箱实例
            candidates: 要卸载的消息索引列表

        Returns:
            (新消息列表, 卸载记录)
        """
        await sandbox.exec(f"mkdir -p {self.EVICTION_DIR}")
        evicted_records: list[EvictedMessage] = []
        new_messages = []

        for i, msg in enumerate(messages):
            if i not in candidates:
                new_messages.append(msg)
                continue

            # 1. 写入沙箱文件
            file_name = f"msg_{i:04d}_{msg.get('role', 'unknown')}.md"
            file_path = f"{self.EVICTION_DIR}/{file_name}"
            content = self._serialize_message(msg)
            await sandbox.write_file(file_path, content)

            # 2. 生成轻量摘要
            summary = self._generate_summary(msg)

            # 3. 替换为索引引用
            ref_msg = {
                "role": msg["role"],
                "content": (
                    f"[已卸载到 {file_path}]\n"
                    f"摘要: {summary}\n"
                    f"如需查看完整内容, 请使用 read 工具读取该文件。"
                ),
            }
            # 如果是 tool 消息, 保留 tool_call_id
            if "tool_call_id" in msg:
                ref_msg["tool_call_id"] = msg["tool_call_id"]

            new_messages.append(ref_msg)

            evicted_records.append(EvictedMessage(
                original_index=i,
                role=msg.get("role", ""),
                tool_name=msg.get("name"),
                file_path=file_path,
                original_tokens=self._estimate_tokens(msg),
                summary=summary,
                evicted_at=time.time(),
            ))

        return new_messages, evicted_records

    def _serialize_message(self, msg: dict) -> str:
        """将消息序列化为可读的 Markdown 文件。"""
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        header = f"# Evicted Message\n\n- Role: {role}\n"

        if msg.get("name"):
            header += f"- Tool: {msg['name']}\n"
        if msg.get("tool_call_id"):
            header += f"- Tool Call ID: {msg['tool_call_id']}\n"

        header += f"\n---\n\n"
        return header + (content if isinstance(content, str) else json.dumps(content, indent=2))

    def _generate_summary(self, msg: dict) -> str:
        """生成消息的轻量摘要。

        Phase 1: 规则生成 (截取前 100 字符)
        Phase 2: LLM 生成 (1-2 句话概括)
        """
        content = msg.get("content", "")
        if isinstance(content, str):
            if len(content) > 100:
                return content[:100] + "..."
            return content
        return str(content)[:100] + "..."
```

### 4.3 消息卸载机制

**哪些消息适合卸载？**

```python
class EvictionCandidateSelector:
    """选择卸载候选消息。"""

    def select(self, messages: list[dict],
               protected_indices: set[int],
               min_age_iterations: int = 3) -> list[int]:
        """选择适合卸载的消息索引。

        卸载候选条件:
        1. 不在保护范围内 (首条 user、末条 user、最近 N 轮)
        2. 年龄 >= min_age_iterations (不卸载太新的消息)
        3. 是 tool_result 或 assistant 消息 (不卸载 user 消息)
        4. 内容较大 (> 2000 字符, 卸载小消息收益太低)

        优先卸载:
        - 最旧的消息优先
        - 大型 tool_result 优先
        - 已被 Compaction Soft Trim 过的消息优先 (已降级, 不如卸载)
        """
        candidates = []
        for i, msg in enumerate(messages):
            if i in protected_indices:
                continue
            if msg.get("role") == "user":
                continue
            content = msg.get("content", "")
            if isinstance(content, str) and len(content) > 2000:
                candidates.append(i)
        return candidates
```

### 4.4 卸载索引与按需召回

卸载后，上下文中保留的索引引用让 LLM 知道有哪些信息可以召回：

```text
卸载前 messages:
  [0] user:      "请帮我修复 auth.py 中的登录 bug"
  [1] assistant:  "让我先读取文件看看..."
  [2] tool:       "# auth.py 完整内容 (500 行, ~15K tokens)"    ← 大量 Token
  [3] assistant:  "看到问题了, 让我检查测试..."
  [4] tool:       "test_auth.py 完整内容 (300 行, ~9K tokens)"  ← 大量 Token
  [5] assistant:  "现在修复 bug..."
  [6] tool:       "exec: patch applied successfully"
  [7] user:       "请运行测试验证一下"

卸载后 messages (卸载 [2] 和 [4]):
  [0] user:      "请帮我修复 auth.py 中的登录 bug"
  [1] assistant:  "让我先读取文件看看..."
  [2] tool:       "[已卸载到 /workspace/.context/evicted/msg_0002_tool.md]
                   摘要: auth.py 源代码, 包含 login(), verify_token() 等函数
                   如需查看完整内容, 请使用 read 工具读取该文件。"
  [3] assistant:  "看到问题了, 让我检查测试..."
  [4] tool:       "[已卸载到 /workspace/.context/evicted/msg_0004_tool.md]
                   摘要: test_auth.py 测试文件, 包含 test_login(), test_token() 等
                   如需查看完整内容, 请使用 read 工具读取该文件。"
  [5] assistant:  "现在修复 bug..."
  [6] tool:       "exec: patch applied successfully"
  [7] user:       "请运行测试验证一下"

Token 节省: ~24K → ~300 tokens (索引引用), 节省 98%
信息损失: 零 — LLM 如需重新查看 auth.py, 可随时 read 该文件
```

### 4.5 卸载策略配置

```python
@dataclass
class EvictionConfig:
    """卸载策略配置。"""
    enabled: bool = True
    eviction_dir: str = "/workspace/.context/evicted"
    min_message_chars: int = 2_000    # 只卸载 > 2K 字符的消息
    min_age_iterations: int = 3       # 至少 3 轮迭代后才考虑卸载
    max_evicted_messages: int = 50    # 最多卸载 50 条消息
    keep_last_assistants: int = 3     # 保护最近 3 轮 assistant
```

---

## 五、策略二：压实 (Compaction)

### 5.1 设计思想

压实是对消息内容的**无损/低损物理压缩**——在保留关键语义的前提下减少 Token 数。

```text
类比:
  数据库: 页面压缩, 同样的行数, 更少的磁盘空间
  上下文: 同样的消息条数, 更少的 Token

与截断的区别:
  截断: 直接砍掉尾部 → 丢失信息
  压实: 保留头部+尾部, 去除冗余/中间部分 → 信息损失最小化
```

### 5.2 工具结果压实

工具结果是上下文膨胀的最大来源。压实策略按"头尾保留"原则：

```python
# sahara_runtime/context/compaction.py

class ToolResultCompactor:
    """工具结果压实。保留语义关键部分, 去除中间冗余。"""

    # ── 工具结果压实参数 ──
    TOOL_RESULT_MAX_CHARS = 8_000
    TOOL_RESULT_HEAD_CHARS = 4_000
    TOOL_RESULT_TAIL_CHARS = 4_000

    # ── 工具错误压实参数 ──
    TOOL_ERROR_MAX_CHARS = 400

    def compact_tool_result(self, content: str, tool_name: str = "") -> str:
        """压实工具结果。

        保留策略:
        - 头部: 命令输出开头 (通常包含成功/失败状态)
        - 尾部: 命令输出结尾 (通常包含最终结果、错误信息)
        - 中间: 用压实标记替代
        """
        if len(content) <= self.TOOL_RESULT_MAX_CHARS:
            return content

        head = content[:self.TOOL_RESULT_HEAD_CHARS]
        tail = content[-self.TOOL_RESULT_TAIL_CHARS:]
        removed = len(content) - self.TOOL_RESULT_HEAD_CHARS - self.TOOL_RESULT_TAIL_CHARS

        return (
            f"{head}\n\n"
            f"... [{removed} chars compacted from {tool_name or 'tool result'}] ...\n\n"
            f"{tail}"
        )

    def compact_tool_error(self, error: str) -> str:
        """压实工具错误信息。错误的关键信息通常在前几行。"""
        if len(error) <= self.TOOL_ERROR_MAX_CHARS:
            return error
        return error[:self.TOOL_ERROR_MAX_CHARS] + "... [error truncated]"
```

### 5.3 结构化数据压实

对于可解析的结构化输出，可以进行更智能的压实：

```python
class StructuredCompactor:
    """结构化数据的智能压实。[Phase 2]"""

    def compact_json(self, content: str, max_chars: int) -> str:
        """JSON 数据: 去除缩进、压缩数组。"""
        try:
            data = json.loads(content)
            compact = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
            if len(compact) <= max_chars:
                return compact
            # 仍然太大 → 截断数组元素
            return self._truncate_collections(data, max_chars)
        except json.JSONDecodeError:
            return content[:max_chars]

    def compact_directory_listing(self, content: str, max_lines: int = 50) -> str:
        """目录列表: 只保留前 N 行 + 文件计数。"""
        lines = content.split("\n")
        if len(lines) <= max_lines:
            return content
        kept = lines[:max_lines]
        omitted = len(lines) - max_lines
        return "\n".join(kept) + f"\n... [{omitted} more lines]"

    def compact_stacktrace(self, content: str) -> str:
        """错误堆栈: 保留异常类型 + 消息 + 最后 5 帧。"""
        lines = content.split("\n")
        # 找到异常行和最后几帧
        error_line = next((l for l in lines if "Error" in l or "Exception" in l), "")
        frames = [l for l in lines if l.strip().startswith(("File ", "at "))]
        if not frames:
            return content[:2000]
        last_frames = frames[-5:]
        return f"{error_line}\n" + "\n".join(last_frames)
```

### 5.4 上下文文件压实

Agent 配置中指定的上下文文件在加载时进行压实：

```python
class ContextFileCompactor:
    """上下文文件压实。"""

    SINGLE_FILE_MAX_CHARS = 20_000    # 单文件上限
    TOTAL_MAX_CHARS = 50_000          # 所有文件总上限
    HEAD_RATIO = 0.7                  # 头部保留 70%
    TAIL_RATIO = 0.2                  # 尾部保留 20%

    def compact(self, content: str, filename: str) -> str:
        """压实单个上下文文件。

        头部保留 70% (通常包含最重要的规则定义)
        尾部保留 20% (通常包含最新添加的内容)
        中间 10% 被压实标记替代
        """
        if len(content) <= self.SINGLE_FILE_MAX_CHARS:
            return content

        max_chars = self.SINGLE_FILE_MAX_CHARS
        head_chars = int(max_chars * self.HEAD_RATIO)
        tail_chars = int(max_chars * self.TAIL_RATIO)
        head = content[:head_chars]
        tail = content[-tail_chars:]
        removed = len(content) - head_chars - tail_chars

        return (
            f"{head}\n\n"
            f"... [{removed} chars compacted from {filename},"
            f" read full file for complete content] ...\n\n"
            f"{tail}"
        )
```

### 5.5 两阶段压实 (Soft/Hard)

针对 messages 中的历史 `tool_result`，执行两阶段压实：

```python
class TwoPhaseCompactor:
    """两阶段消息压实。作用于 messages 中的历史 tool_result。"""

    def __init__(self, config: "CompactionConfig", token_counter: "TokenCounter"):
        self._config = config
        self._counter = token_counter

    def compact(self, messages: list[dict], available_tokens: int,
                protected_indices: set[int]) -> list[dict]:
        """两阶段压实。"""
        messages = [msg.copy() for msg in messages]
        ratio = self._usage_ratio(messages, available_tokens)

        # Phase 1: Soft Compact (ratio > 30%)
        if ratio > self._config.soft_compact_ratio:
            messages = self._soft_compact(messages, protected_indices)

        ratio = self._usage_ratio(messages, available_tokens)

        # Phase 2: Hard Compact (ratio > 50%)
        if ratio > self._config.hard_compact_ratio:
            messages = self._hard_compact(messages, protected_indices, available_tokens)

        return messages

    def _soft_compact(self, messages, protected):
        """Soft: 压缩大型 tool_result, 保留头尾。"""
        cfg = self._config
        for i, msg in enumerate(messages):
            if i in protected or msg.get("role") != "tool":
                continue
            content = msg.get("content", "")
            if not isinstance(content, str) or len(content) <= cfg.soft_min_chars:
                continue
            head = content[:cfg.soft_head_chars]
            tail = content[-cfg.soft_tail_chars:]
            trimmed = len(content) - cfg.soft_head_chars - cfg.soft_tail_chars
            messages[i] = {**msg, "content": (
                f"{head}\n... [{trimmed} chars soft-compacted] ...\n{tail}"
            )}
        return messages

    def _hard_compact(self, messages, protected, available_tokens):
        """Hard: 替换为占位符, 从最旧开始。"""
        for i, msg in enumerate(messages):
            if i in protected or msg.get("role") != "tool":
                continue
            content = msg.get("content", "")
            if not isinstance(content, str) or len(content) < 100:
                continue
            messages[i] = {**msg, "content": self._config.hard_placeholder}
            if self._usage_ratio(messages, available_tokens) < self._config.hard_compact_ratio:
                break
        return messages

    def _usage_ratio(self, messages, available_tokens):
        tokens = self._counter.count_messages(messages)
        return tokens / available_tokens if available_tokens > 0 else 1.0
```

---

## 六、策略三：摘要 (Summarization)

### 6.1 设计思想

摘要是唯一需要 LLM 调用的策略——用少量 Token 的高密度摘要替代大量 Token 的原始消息。

摘要在缓解注意力衰减方面有独特优势：**摘要消息被放置在上下文的开头部分（紧接 system_prompt 之后），处于 U 型注意力的高关注区。** 这意味着即使是很久以前的历史知识，经过摘要后也能获得比原始位置更高的注意力。

```text
核心思想: 信息蒸馏 + 注意力位置优化

  原始消息 (50 条, ~80K tokens):
    用户要求修复 auth bug → 读取 auth.py → 分析代码 → 读取测试 →
    发现 token 过期逻辑有问题 → 修改代码 → 运行测试 → 测试通过

  摘要 (~500 tokens):
    "用户要求修复 auth.py 中的登录 bug。经分析发现 verify_token()
     函数中的过期检查逻辑有误 (使用 > 而非 >=)。已修改第 42 行并通过
     test_auth.py 的全部 3 个测试用例。当前任务: 等待用户确认。"

  压缩比:   160:1, 关键知识全部保留
  注意力:   摘要置于开头 → 高关注区 (原信息在中间 → 低关注区)
  双重收益: Token 数↓ + 注意力↑
```

> **Phase 1 不实现此策略**, 当 Compaction 和 Eviction 不足时直接紧急截断。Phase 2 实现。

### 6.2 分阶段摘要

```python
# sahara_runtime/context/summarization.py  [Phase 2]

class Summarizer:
    """分阶段摘要生成器。"""

    async def summarize(self, messages: list[dict],
                        available_tokens: int) -> list[dict]:
        """将旧消息替换为摘要。

        算法:
        1. 确定分割点: 保留最近 N 条消息, 其余用于摘要
        2. 将待摘要消息分块 (每块 ~40% 上下文窗口)
        3. 对每块生成独立摘要
        4. 合并多个分块摘要为最终摘要
        5. 用摘要消息 + 保留的最近消息组成新列表
        """
        split = self._find_split_point(messages, available_tokens)
        old_messages = messages[:split]
        recent_messages = messages[split:]

        # 分块
        chunks = self._chunk_messages(old_messages, available_tokens)

        # 分块摘要
        chunk_summaries = []
        for chunk in chunks:
            summary = await self._summarize_chunk(chunk)
            chunk_summaries.append(summary)

        # 合并摘要
        final_summary = await self._merge_summaries(chunk_summaries)

        # 附加重要附加信息
        extra_info = self._extract_important_facts(old_messages)
        full_summary = f"{final_summary}\n\n{extra_info}" if extra_info else final_summary

        summary_msg = {"role": "user", "content": f"[对话历史摘要]\n{full_summary}"}
        return [summary_msg] + recent_messages

    def _extract_important_facts(self, messages: list[dict]) -> str:
        """从旧消息中提取不应被遗忘的关键事实。

        - 工具执行失败记录 (避免重复犯错)
        - 文件修改记录 (哪些文件被改过)
        - 用户的明确指示和偏好
        """
        facts = []
        tool_failures = []
        modified_files = set()

        for msg in messages:
            if msg.get("role") == "tool":
                content = str(msg.get("content", ""))
                # 记录失败
                if "error" in content.lower() or "failed" in content.lower():
                    tool_failures.append(content[:200])
                # 记录文件修改
                if msg.get("name") in ("write", "edit"):
                    # 从工具参数推断修改的文件
                    pass

        if tool_failures:
            facts.append("工具执行失败记录:\n" + "\n".join(f"- {f}" for f in tool_failures[:5]))
        if modified_files:
            facts.append("已修改的文件: " + ", ".join(sorted(modified_files)))

        return "\n".join(facts)
```

### 6.3 增量摘要

```text
增量摘要 vs 全量重建:

  全量重建 (每次重新摘要所有旧消息):
    ✗ 每次都需要 LLM 调用
    ✗ 摘要质量不稳定 (每次生成的摘要略有不同)
    ✗ 对话越长, 摘要开销越大

  增量摘要 (追加式):
    ✓ 只对新产生的旧消息生成追加摘要
    ✓ 前面的摘要保持稳定 → 利用 Prompt Cache
    ✓ 开销恒定 (只摘要增量部分)

  增量摘要结构:
    [摘要 v1] 第 1-10 轮: 用户要求修复 auth bug, 已定位到 verify_token()...
    [摘要 v2] 第 11-20 轮: 已修复 bug 并通过测试, 开始优化性能...
    [摘要 v3] 第 21-25 轮: 性能优化完成, 响应时间从 200ms 降至 50ms...
    [最近消息] 第 26-30 轮: (原始消息, 未摘要)
```

### 6.4 摘要 Prompt 设计

```text
系统提示词 (用于摘要生成):

  你是一个对话历史摘要助手。请将以下 Agent 工作对话压缩为简洁的摘要。

  保留优先级 (从高到低):
  1. 用户的原始目标和当前期望
  2. 已执行的关键操作及其结果 (成功/失败)
  3. 发现的问题和做出的决定
  4. 被修改的文件路径和关键代码变更
  5. 遇到的错误及解决方法

  忽略:
  - 工具的完整输出内容 (只保留关键结论)
  - 思考过程中的试探和废弃的方案
  - 格式化和排版相关的操作

  输出格式:
  用简洁的段落叙述, 用第三人称, 确保后续 LLM 能理解上下文。
  控制在 500 tokens 以内。
```

### 6.5 摘要质量保障

| 风险 | 缓解措施 |
| --- | --- |
| **关键信息丢失** | 强制保留: 工具失败记录、文件修改列表、用户明确指示 |
| **摘要不准确** | 使用与主任务相同的模型生成摘要，保持语义一致性 |
| **摘要过于简略** | 设定最小 Token 数 (200+), 确保摘要包含足够上下文 |
| **反复摘要导致信息退化** | 增量摘要而非全量重建，避免"摘要的摘要"链式退化 |

---

## 七、策略四：过滤 (Filtering)

### 7.1 设计思想

过滤是知识比特率优化中最精准的武器——不减少信息的体积，而是**只保留对当前决策最相关的信息**。

```text
核心思想: 注意力聚焦

  Agent 在第 15 轮, 当前任务是 "运行测试":

  高相关性 (保留):
  - 修改了哪些文件 (需要知道测什么)
  - 最近的代码变更 (需要知道改了什么)
  - 测试命令和配置 (需要知道怎么测)

  低相关性 (过滤):
  - 第 1 轮读取 README 的完整内容 (已过时)
  - 第 3 轮搜索代码的 grep 结果 (已找到目标)
  - 第 5 轮查看目录结构的 ls 输出 (已了解)
```

### 7.2 消息相关性评分

```python
# sahara_runtime/context/filtering.py

class MessageFilter:
    """基于规则的消息过滤器。Phase 1 实现。"""

    def filter(self, messages: list[dict],
               protected_indices: set[int]) -> list[dict]:
        """过滤低相关性消息。

        Phase 1 规则:
        1. 重复文件读取: 同一路径被 read 多次 → 只保留最后一次
        2. 探索性命令: ls, find, grep 等搜索类工具的旧结果 → 过滤
        3. 冗余确认: assistant 的"好的, 我来..." 类纯过渡文本 → 过滤
        """
        # 1. 检测重复文件读取
        file_reads = self._detect_duplicate_reads(messages)

        # 2. 检测探索性命令
        exploratory = self._detect_exploratory_commands(messages)

        # 3. 过滤
        result = []
        for i, msg in enumerate(messages):
            if i in protected_indices:
                result.append(msg)
                continue

            # 重复读取: 只保留最后一次
            if i in file_reads.get("duplicates", set()):
                continue

            # 旧的探索性命令结果: 过滤
            if i in exploratory:
                continue

            result.append(msg)

        return result

    def _detect_duplicate_reads(self, messages: list[dict]) -> dict:
        """检测对同一文件的重复读取。"""
        file_read_indices: dict[str, list[int]] = {}  # path → [indices]

        for i, msg in enumerate(messages):
            if msg.get("role") == "assistant":
                content = msg.get("content", "")
                # 检测 tool_use 中的 read 调用
                if isinstance(content, list):
                    for block in content:
                        if (isinstance(block, dict) and
                            block.get("type") == "tool_use" and
                            block.get("name") == "read"):
                            path = block.get("input", {}).get("path", "")
                            if path:
                                file_read_indices.setdefault(path, []).append(i)

        # 对每个路径, 标记除最后一次外的所有读取为重复
        duplicates = set()
        for path, indices in file_read_indices.items():
            if len(indices) > 1:
                for idx in indices[:-1]:
                    duplicates.add(idx)
                    # 同时标记对应的 tool_result
                    if idx + 1 < len(messages) and messages[idx + 1].get("role") == "tool":
                        duplicates.add(idx + 1)

        return {"duplicates": duplicates}

    def _detect_exploratory_commands(self, messages: list[dict]) -> set[int]:
        """检测旧的探索性命令。"""
        EXPLORATORY_PATTERNS = ["ls ", "find ", "grep ", "rg ", "tree ", "wc "]
        exploratory = set()

        for i, msg in enumerate(messages):
            if msg.get("role") != "tool":
                continue
            # 检查关联的 assistant 消息中的工具调用
            if i > 0 and messages[i-1].get("role") == "assistant":
                assistant_content = messages[i-1].get("content", "")
                if isinstance(assistant_content, list):
                    for block in assistant_content:
                        if (isinstance(block, dict) and
                            block.get("type") == "tool_use" and
                            block.get("name") == "exec"):
                            cmd = block.get("input", {}).get("command", "")
                            if any(cmd.strip().startswith(p) for p in EXPLORATORY_PATTERNS):
                                exploratory.add(i)

        return exploratory
```

### 7.3 上下文文件过滤

```python
class ContextFileFilter:
    """上下文文件过滤。根据 Agent 配置和任务状态决定加载哪些文件。"""

    async def filter_files(self, file_paths: list[str],
                           sandbox: "Sandbox",
                           current_task_hint: str = "") -> list[str]:
        """Phase 1: 基础过滤。

        规则:
        1. 文件必须存在于沙箱中
        2. 文件大小 > 0
        3. 文件格式在支持列表内 (.md, .txt, .json, .yaml, .toml, .py 等)
        """
        filtered = []
        for path in file_paths:
            try:
                content = await sandbox.read_file(path)
                if content and len(content.strip()) > 0:
                    filtered.append(path)
            except Exception:
                continue
        return filtered
```

### 7.4 工具结果过滤

```python
class ToolResultFilter:
    """工具结果过滤规则。[Phase 1]"""

    # 非核心信息的工具结果, 超过一定年龄后过滤
    FILTERABLE_TOOLS = {
        "glob": 3,       # glob 搜索结果, 3 轮后过滤
        "grep": 3,       # grep 搜索结果, 3 轮后过滤
        "web_search": 5,  # 网络搜索结果, 5 轮后过滤
        "web_fetch": 5,   # 网页内容, 5 轮后过滤
    }

    def should_filter(self, tool_name: str, age_iterations: int) -> bool:
        """判断是否应该过滤此工具结果。"""
        max_age = self.FILTERABLE_TOOLS.get(tool_name)
        if max_age is None:
            return False
        return age_iterations > max_age
```

---

## 八、策略编排引擎

### 8.1 ContextManager 核心接口

```python
# sahara_runtime/context/manager.py

class ContextManager:
    """上下文管理器 — 四种策略的编排引擎。

    双重目标:
    1. 在 available_tokens 约束下, 最大化知识比特率
    2. 缩短有效上下文长度, 缓解 LLM 注意力衰减 (Lost in the Middle)

    策略执行顺序 (成本递增):
    1. Filtering  — 先过滤掉明确无关的内容 (低成本, 高精度, 减少注意力干扰)
    2. Compaction — 压实剩余内容的物理体积 (低成本, 减少中间区域噪声)
    3. Eviction   — 将低频大消息卸载到文件 (零信息损失, 大幅缩短上下文)
    4. Summarize  — LLM 摘要, 置于开头高注意力区 (高成本, 注意力位置优化)
    5. Emergency  — 安全网: 紧急截断 (仅保留最近 4 条消息)
    """

    def __init__(self, config: "ContextConfig",
                 token_counter: "TokenCounter",
                 sandbox: "Sandbox | None" = None):
        self._config = config
        self._counter = token_counter
        self._filter = MessageFilter()
        self._compactor = TwoPhaseCompactor(config.compaction, token_counter)
        self._evictor = MessageEviction() if config.eviction.enabled else None
        self._summarizer = None  # Phase 2
        self._sandbox = sandbox

    async def fit(self, messages: list[dict], system_prompt: str,
                  model: "ModelConfig") -> list[dict]:
        """核心方法: 编排四种策略, 确保 messages 不超过预算。

        每次 LLM 调用前由 Agent Loop (§4 步骤 9a) 调用。
        """
        budget = model.max_context_tokens - model.max_tokens
        system_tokens = self._counter.count(system_prompt)
        available = budget - system_tokens

        # 计算保护区域
        protected = self._get_protected_indices(messages)

        # 记录初始状态 (可观测性)
        initial_tokens = self._counter.count_messages(messages)
        layers_triggered = []

        # ── Strategy 1: Filtering ──
        messages = self._filter.filter(messages, protected)
        if self._counter.count_messages(messages) < initial_tokens:
            layers_triggered.append("filtering")

        # ── Strategy 2: Compaction ──
        current = self._counter.count_messages(messages)
        if current > available:
            messages = self._compactor.compact(messages, available, protected)
            layers_triggered.append("compaction")

        # ── Strategy 3: Eviction ──
        current = self._counter.count_messages(messages)
        if current > available and self._evictor and self._sandbox:
            candidates = EvictionCandidateSelector().select(messages, protected)
            if candidates:
                messages, _ = await self._evictor.evict(
                    messages, self._sandbox, candidates
                )
                layers_triggered.append("eviction")

        # ── Strategy 4: Summarization [Phase 2] ──
        current = self._counter.count_messages(messages)
        if current > available and self._summarizer:
            messages = await self._summarizer.summarize(messages, available)
            layers_triggered.append("summarization")

        # ── Emergency: 安全网 ──
        current = self._counter.count_messages(messages)
        if current > available:
            messages = messages[-4:]  # 保留最近 4 条
            layers_triggered.append("emergency")

        # 记录结果
        final_tokens = self._counter.count_messages(messages)
        logger.info("context_fit_complete",
                     initial_tokens=initial_tokens,
                     final_tokens=final_tokens,
                     compression_ratio=round(initial_tokens / max(final_tokens, 1), 1),
                     saved_tokens=initial_tokens - final_tokens,
                     layers=layers_triggered,
                     utilization=round(final_tokens / available, 3),
                     # 有效上下文长度越短, 注意力质量越高
                     effective_context_length=final_tokens)

        return messages

    def _get_protected_indices(self, messages: list[dict]) -> set[int]:
        """确定受保护的消息索引。

        始终保护:
        1. 第一条 user 消息 (初始任务上下文)
        2. 最后一条 user 消息 (当前请求)
        3. 最近 N 轮 assistant 消息 + 关联的 tool_result
        """
        protected = set()
        # 首条 user
        for i, msg in enumerate(messages):
            if msg.get("role") == "user":
                protected.add(i)
                break
        # 末条 user
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].get("role") == "user":
                protected.add(i)
                break
        # 最近 N 轮
        keep = self._config.compaction.keep_last_assistants
        assistant_count = 0
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].get("role") == "assistant":
                assistant_count += 1
            if assistant_count <= keep:
                protected.add(i)
            else:
                break
        return protected
```

### 8.2 策略执行流水线

```text
messages 进入 fit()
       │
       ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 1: Filtering (过滤)                                        │
│                                                                  │
│  - 重复文件读取: 只保留最后一次                                   │
│  - 旧探索命令: ls/grep/find 结果超龄过滤                         │
│  - 冗余过渡文本: 纯确认性 assistant 消息过滤                     │
│                                                                  │
│  耗时: < 5ms    信息损失: 极低    Phase: 1                       │
└──────────┬───────────────────────────────────────────────────────┘
           │ if still > available
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 2: Compaction (压实)                                        │
│                                                                  │
│  Soft (ratio > 30%): 大型 tool_result → 保留头 1.5K + 尾 1.5K   │
│  Hard (ratio > 50%): 旧 tool_result → 占位符替代                 │
│                                                                  │
│  耗时: < 5ms    信息损失: 低-中    Phase: 1                      │
└──────────┬───────────────────────────────────────────────────────┘
           │ if still > available
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 3: Eviction (卸载)                                         │
│                                                                  │
│  选择候选: 旧的大型 tool_result (> 2K chars, 年龄 > 3 轮)       │
│  写入沙箱文件: /workspace/.context/evicted/msg_NNNN_*.md         │
│  替换为索引引用 (文件路径 + 摘要, ~50 tokens)                    │
│  LLM 需要时可 read 召回完整内容                                  │
│                                                                  │
│  耗时: < 10ms (文件 IO)    信息损失: 零    Phase: 1              │
└──────────┬───────────────────────────────────────────────────────┘
           │ if still > available
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 4: Summarization (摘要)                    [Phase 2]       │
│                                                                  │
│  分块摘要 → 合并 → 替换旧消息                                    │
│  保留: 工具失败记录、文件修改列表、用户指示                      │
│                                                                  │
│  耗时: 2-5s (LLM 调用)    信息损失: 中    Phase: 2               │
└──────────┬───────────────────────────────────────────────────────┘
           │ if still > available
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 5: Emergency (安全网)                                      │
│                                                                  │
│  保留最近 4 条消息 (约 2 轮对话)                                  │
│  丢失几乎所有历史上下文                                           │
│                                                                  │
│  耗时: < 1ms    信息损失: 极高    Phase: 1                       │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
     返回 optimized_messages → LLM 调用
```

### 8.3 上下文窗口守卫

```python
# sahara_runtime/context/guard.py

class ContextWindowGuard:
    """上下文窗口预检守卫。在 Agent Loop 开始前检查。[Phase 2]"""

    MIN_CONTEXT_WINDOW = 16_000
    WARN_CONTEXT_WINDOW = 32_000

    async def check(self, model: "ModelConfig") -> "GuardResult":
        context_window = model.max_context_tokens

        if context_window < self.MIN_CONTEXT_WINDOW:
            return GuardResult(status="BLOCK", context_window=context_window,
                message=f"模型上下文窗口过小 ({context_window} tokens)")

        if context_window < self.WARN_CONTEXT_WINDOW:
            return GuardResult(status="WARN", context_window=context_window,
                message=f"模型上下文窗口较小 ({context_window} tokens)")

        return GuardResult(status="PASS", context_window=context_window)
```

**LLM 溢出错误处理**：

```python
class OverflowHandler:
    """LLM 返回上下文溢出错误时的处理。"""

    OVERFLOW_PATTERNS = [
        "request_too_large", "context length exceeded",
        "maximum context length", "prompt is too long",
        "exceeds model context window", "context overflow",
    ]

    @classmethod
    def is_overflow_error(cls, error: Exception | str) -> bool:
        return any(p in str(error).lower() for p in cls.OVERFLOW_PATTERNS)
```

---

## 九、Token 计数器

```python
# sahara_runtime/context/token_counter.py

class TokenCounter:
    """Token 计数器。精确计数 + 快速估算双模式。"""

    MODEL_ENCODING_MAP = {
        "claude": "cl100k_base",
        "gpt-4o": "o200k_base",
        "gpt-4": "cl100k_base",
        "default": "cl100k_base",
    }
    MESSAGE_OVERHEAD_TOKENS = 4

    def __init__(self):
        self._encoders: dict[str, "tiktoken.Encoding"] = {}

    def count(self, text: str, model: str = "default") -> int:
        """精确计数: tiktoken。"""
        enc_name = self._resolve_encoding(model)
        if enc_name not in self._encoders:
            import tiktoken
            self._encoders[enc_name] = tiktoken.get_encoding(enc_name)
        return len(self._encoders[enc_name].encode(text))

    def count_messages(self, messages: list[dict], model: str = "default") -> int:
        """精确计数: 整个 messages 数组。"""
        total = 0
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                total += self.count(content, model)
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        total += self.count(json.dumps(block), model)
            total += self.MESSAGE_OVERHEAD_TOKENS
        return total

    @staticmethod
    def estimate(text: str) -> int:
        """快速估算: chars / 4。用于非关键路径。"""
        return len(text) // 4

    def _resolve_encoding(self, model: str) -> str:
        model_lower = model.lower()
        for prefix, enc in self.MODEL_ENCODING_MAP.items():
            if prefix in model_lower:
                return enc
        return self.MODEL_ENCODING_MAP["default"]
```

---

## 十、多模型上下文窗口适配

### 10.1 各模型窗口大小

| 模型 | 上下文窗口 | 输出上限 | 策略特点 |
| --- | --- | --- | --- |
| Claude Sonnet 4 | 200K | 8,192 | 窗口充裕, 压实/卸载频率低 |
| Claude Haiku 3.5 | 200K | 4,096 | 窗口充裕, 低成本 |
| GPT-4o | 128K | 4,096 | 中等窗口 |
| GPT-4o-mini | 128K | 16,384 | 输出预留大, 消息空间相应减少 |
| Deepseek-V3 | 64K | 8,192 | 窗口紧张, 需积极使用所有策略 |

### 10.2 动态策略调整

```python
def adjust_config_for_model(config: "ContextConfig", context_window: int) -> "ContextConfig":
    """根据模型窗口大小动态调整策略参数。"""

    if context_window >= 128_000:
        # 大窗口: 放宽压实阈值, 减少不必要操作
        config.compaction.soft_compact_ratio = 0.4
        config.compaction.hard_compact_ratio = 0.6
        config.compaction.keep_last_assistants = 5

    elif context_window < 64_000:
        # 小窗口: 收紧所有策略, 积极卸载和压实
        config.compaction.soft_compact_ratio = 0.2
        config.compaction.hard_compact_ratio = 0.35
        config.compaction.keep_last_assistants = 2
        config.eviction.min_message_chars = 1_000  # 更小的消息也卸载
        config.eviction.min_age_iterations = 2     # 更早开始卸载

    return config
```

---

## 十一、与其他子系统的协作

| 子系统 | 交互点 | 说明 |
| --- | --- | --- |
| **Agent Loop (§4)** | 步骤 9a 调用 `fit()` | 每次 LLM 调用前执行 |
| **Model Router (§6)** | 提供 `model.max_context_tokens` | 预算计算的基础数据 |
| **Tools (§7)** | §7.9 工具结果首次压实 | 工具执行后立即压实 (属于 Compaction 的第一道防线) |
| **Prompt Builder (§8)** | 段落 6 上下文文件注入 | `ContextFileCompactor` 压实后交给 Prompt Builder |
| **Sandbox (§9)** | 卸载消息写入/读取 | Eviction 策略依赖沙箱文件系统 |
| **Skills (§10)** | SKILL.md 作为 tool_result | 技能内容按 Compaction 策略处理 |
| **Agent Memory — Session Store (§12)** | 保存原始 messages | **持久化保存未处理的原始消息**, 裁剪仅在内存中 |
| **Agent Memory — Knowledge Store (§12)** | memory_search 结果作为 tool_result | 与普通 tool_result 同等处理, 但最近一次搜索结果受保护 [Phase 2] |
| **EventEmitter (§5)** | 发射策略触发事件 | 客户端可展示"上下文已优化"提示 |

**短期记忆持久化策略**：Session Store 保存原始 messages（未经策略处理）。理由：裁剪是针对当前 LLM 调用的优化；用户可能切换到更大窗口模型；审计需要完整记录。

---

## 十二、C 端成本控制

### 12.1 Token 成本量化

```text
以 Claude Sonnet 4 ($3/M input) 为例, 一个 20 轮工具调用任务:

  无上下文管理:
    messages 持续膨胀, 每轮 LLM 输入 ~100K tokens
    总输入成本: 20 × 100K × $3/M = $6.00

  仅 Compaction:
    工具结果压实, 平均每轮 ~50K tokens
    总输入成本: 20 × 50K × $3/M = $3.00 (节省 50%)

  Compaction + Eviction:
    旧消息卸载, 平均每轮 ~30K tokens
    总输入成本: 20 × 30K × $3/M = $1.80 (节省 70%)

  全策略 (+ Summarization + Filtering):
    平均每轮 ~20K tokens
    总输入成本: 20 × 20K × $3/M = $1.20 (节省 80%)

  叠加 Prompt Cache (§8.7):
    system_prompt ~3K tokens 缓存命中 → 实际计费更低
    最终成本: ~$0.90 (节省 85%)
```

---

## 十三、可观测性

### 13.1 核心指标

```python
# 策略触发计数
context_strategy_triggered = Counter(
    "sahara_context_strategy_triggered_total",
    "Times each strategy was triggered",
    ["strategy"],  # "filtering" / "compaction_soft" / "compaction_hard"
                   # "eviction" / "summarization" / "emergency"
)

# 知识比特率 (信息密度)
context_knowledge_bitrate = Histogram(
    "sahara_context_knowledge_bitrate",
    "Ratio of final_tokens / available_tokens after fit()",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# Token 节省量
context_tokens_saved = Counter(
    "sahara_context_tokens_saved_total",
    "Tokens saved by context management",
    ["strategy"],
)

# 卸载召回次数 (LLM 主动 read 已卸载文件)
context_eviction_recall = Counter(
    "sahara_context_eviction_recall_total",
    "Times LLM recalled an evicted message via read tool",
)

# 有效上下文长度 (越短注意力质量越高)
context_effective_length = Histogram(
    "sahara_context_effective_length_tokens",
    "Effective context length after fit() — lower is better for attention quality",
    buckets=[5000, 10000, 20000, 30000, 50000, 80000, 128000, 200000],
)

# 压缩率 (initial / final)
context_compression_ratio = Histogram(
    "sahara_context_compression_ratio",
    "Compression ratio of fit() (initial_tokens / final_tokens)",
    buckets=[1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0],
)

# fit() 延迟
context_fit_duration = Histogram(
    "sahara_context_fit_duration_seconds",
    "Duration of ContextManager.fit()",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)
```

### 13.2 关键日志

| 事件 | 级别 | 说明 |
| --- | --- | --- |
| `context_fit_complete` | INFO | 包含 initial/final tokens, 触发的策略列表 |
| `context_eviction_write` | INFO | 消息卸载到文件 |
| `context_eviction_recall` | INFO | LLM 召回已卸载消息 |
| `context_compaction_soft` | DEBUG | Soft 压实触发 |
| `context_compaction_hard` | WARN | Hard 压实触发 |
| `context_summarization` | WARN | LLM 摘要触发 [Phase 2] |
| `context_emergency` | ERROR | 紧急截断, 需关注 |

---

## 十四、配置管理

```python
# sahara_runtime/context/config.py

@dataclass
class CompactionConfig:
    soft_compact_ratio: float = 0.3
    hard_compact_ratio: float = 0.5
    keep_last_assistants: int = 3
    soft_min_chars: int = 4_000
    soft_head_chars: int = 1_500
    soft_tail_chars: int = 1_500
    hard_placeholder: str = "[Tool result compacted]"
    tool_result_max_chars: int = 8_000
    tool_result_head_chars: int = 4_000
    tool_result_tail_chars: int = 4_000
    tool_error_max_chars: int = 400

@dataclass
class EvictionConfig:
    enabled: bool = True
    eviction_dir: str = "/workspace/.context/evicted"
    min_message_chars: int = 2_000
    min_age_iterations: int = 3
    max_evicted_messages: int = 50

@dataclass
class SummarizationConfig:
    enabled: bool = False         # Phase 2 启用
    max_history_share: float = 0.5
    reserve_tokens: int = 20_000
    base_chunk_ratio: float = 0.4

@dataclass
class FilteringConfig:
    duplicate_read_filter: bool = True
    exploratory_command_filter: bool = True
    exploratory_max_age: int = 3

@dataclass
class ContextFileConfig:
    single_file_max_chars: int = 20_000
    total_max_chars: int = 50_000
    head_ratio: float = 0.7
    tail_ratio: float = 0.2

@dataclass
class ContextConfig:
    compaction: CompactionConfig = field(default_factory=CompactionConfig)
    eviction: EvictionConfig = field(default_factory=EvictionConfig)
    summarization: SummarizationConfig = field(default_factory=SummarizationConfig)
    filtering: FilteringConfig = field(default_factory=FilteringConfig)
    context_files: ContextFileConfig = field(default_factory=ContextFileConfig)

    # 窗口守卫
    min_context_window: int = 16_000
    warn_context_window: int = 32_000

    # 紧急截断
    emergency_keep_messages: int = 4

    # 动态阈值
    enable_dynamic_thresholds: bool = True
    large_window_threshold: int = 128_000
    small_window_threshold: int = 64_000
```

---

## 十五、Phase 规划

| 阶段 | 范围 |
| --- | --- |
| **Phase 1** | Filtering (规则: 重复读取/探索命令过滤) + Compaction (Soft/Hard 两阶段) + Eviction (消息卸载到沙箱文件) + Emergency (紧急截断) + TokenCounter + ContextFileCompactor + 基础指标 |
| **Phase 2** | Summarization (分阶段/增量 LLM 摘要) + ContextWindowGuard + 动态策略调整 + StructuredCompactor (JSON/堆栈/目录智能压实) + Eviction LLM 摘要 (卸载索引由 LLM 生成高质量摘要) |
| **Phase 3** | Filtering 语义评分 (基于 embedding 的消息相关性排序) + 跨会话上下文继承 + 知识比特率自动调优 (根据任务类型和历史数据动态选择最优策略组合) |

---

## 十六、包结构

```text
sahara_runtime/
  context/
  ├── __init__.py
  ├── manager.py              # ContextManager — 策略编排引擎
  ├── guard.py                # ContextWindowGuard — 窗口预检 [Phase 2]
  ├── filtering.py            # Filtering: MessageFilter, ToolResultFilter
  ├── compaction.py           # Compaction: TwoPhaseCompactor, ToolResultCompactor
  ├── eviction.py             # Eviction: MessageEviction, EvictionCandidateSelector
  ├── summarization.py        # Summarization: Summarizer [Phase 2]
  ├── overflow.py             # OverflowHandler (溢出错误处理)
  ├── context_files.py        # ContextFileCompactor, ContextFileLoader
  ├── token_counter.py        # TokenCounter
  ├── config.py               # ContextConfig 及子配置
  └── metrics.py              # Prometheus 指标
```
