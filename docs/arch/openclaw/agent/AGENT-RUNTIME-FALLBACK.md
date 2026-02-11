# Agent Runtime 模型降级与认证轮转

> 本文档详解 Agent Runtime 的多层容错体系：当 LLM 调用失败时，系统如何通过认证轮转、模型降级、上下文压缩和 Thinking 级别降级来自动恢复，最大化请求成功率。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、错误分类](#二错误分类)
- [三、重试循环](#三重试循环)
- [四、认证轮转 (Auth Profile Rotation)](#四认证轮转-auth-profile-rotation)
- [五、模型降级 (Model Fallback)](#五模型降级-model-fallback)
- [六、上下文溢出恢复](#六上下文溢出恢复)
- [七、Thinking 级别降级](#七thinking-级别降级)
- [八、模型解析与守卫](#八模型解析与守卫)
- [九、配置参考](#九配置参考)
- [十、关键源文件索引](#十关键源文件索引)

---

## 一、全局视角

### 1.1 为什么需要多层容错

LLM API 调用面临多种失败场景：

| 失败类型 | 频率 | 影响 | 恢复策略 |
| ---- | ---- | ---- | ---- |
| 速率限制 (429) | 高 | 暂时性 | 换 API Key → 换模型 |
| 认证失败 (401/403) | 中 | Key 级别 | 换 API Key |
| 计费耗尽 (402) | 低 | 账号级别 | 长时间冷却 → 换模型 |
| 上下文溢出 | 中 | 会话级别 | 压缩历史 → 重试 |
| Thinking 不支持 | 低 | 模型级别 | 降级 thinking → 重试 |
| 超时 | 中 | 网络级别 | 换 Key/换模型 |
| 格式错误 | 低 | 协议级别 | 换模型 |

### 1.2 四层防线架构

```text
LLM 调用失败
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  第一层: 重试循环内恢复 (run.ts while loop)                      │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ 上下文   │  │ 认证     │  │ Thinking │  │ 助手错误  │       │
│  │ 溢出     │  │ 失败     │  │ 不支持    │  │ (流中)   │       │
│  │          │  │          │  │          │  │          │       │
│  │ 压缩历史 │  │ 切换     │  │ 降级     │  │ 标记失败  │       │
│  │ → 重试   │  │ API Key  │  │ thinking │  │ → 切换   │       │
│  │          │  │ → 重试   │  │ → 重试   │  │  API Key │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
│                                                                 │
│  所有 Key 耗尽 → 抛出 FailoverError                             │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  第二层: 模型降级 (runWithModelFallback)                         │
│                                                                 │
│  主模型 ──失败──→ 备用模型 1 ──失败──→ 备用模型 2 ──失败──→ 报错 │
│  claude-opus      gpt-4o              claude-sonnet              │
│                                                                 │
│  每次切换模型时，第一层的 Key 轮转会重新从头开始                  │
└─────────────────────────────────────────────────────────────────┘
```

**关键设计**: 第一层在**同一模型**内尝试所有恢复手段（换 Key、压缩、降级 Thinking）。只有当所有手段都失败后，才触发第二层切换到完全不同的模型。

---

## 二、错误分类

### 2.1 FailoverError

> 源文件: `src/agents/failover-error.ts`

`FailoverError` 是触发降级的**唯一信号**。只有被识别为 `FailoverError` 的错误才会触发模型降级；其他错误直接抛出。

```typescript
class FailoverError extends Error {
  readonly reason: FailoverReason;   // 分类原因
  readonly provider?: string;        // 提供商
  readonly model?: string;           // 模型
  readonly profileId?: string;       // 认证配置 ID
  readonly status?: number;          // HTTP 状态码
  readonly code?: string;            // 错误代码
}
```

### 2.2 错误分类规则

> 源文件: `src/agents/pi-embedded-helpers/errors.ts` — `classifyFailoverReason()`

| 分类 | 触发模式（部分） | HTTP 状态 |
| ---- | ---- | ---- |
| `rate_limit` | "rate limit", "too many requests", "quota exceeded", "resource exhausted" | 429 |
| `billing` | "payment required", "insufficient credits", "402" | 402 |
| `auth` | "invalid api key", "unauthorized", "forbidden", "expired", "access denied" | 401, 403 |
| `timeout` | "timeout", "timed out", "deadline exceeded" | 408 |
| `format` | "tool_use.id", "invalid request format" | — |

**分类优先级**: 先检查 HTTP 状态码和错误代码（`ETIMEDOUT` / `ECONNRESET` 等），再检查错误消息文本的模式匹配。

### 2.3 分类如何驱动行为

```text
错误分类
    │
    ├── rate_limit → 标记 Profile 冷却 + 切换 Key + 重试
    ├── auth       → 标记 Profile 冷却 + 切换 Key + 重试
    ├── billing    → 标记 Profile 长时间禁用 + 切换 Key + 重试
    ├── timeout    → 标记 Profile 冷却 + 切换 Key + 重试
    ├── format     → 直接模型降级（Key 轮转无用）
    └── (非 FailoverError) → 直接抛出，不触发任何降级
```

---

## 三、重试循环

### 3.1 循环结构

> 源文件: `src/agents/pi-embedded-runner/run.ts`

```text
while (true) {
    │
    ├── attemptedThinking.add(thinkLevel)
    │
    ├── runEmbeddedAttempt({...})  ← 执行一次 LLM 调用
    │       │
    │       └── 返回 { promptError, aborted, timedOut, lastAssistant }
    │
    ├── promptError 存在?
    │   ├── 上下文溢出? → §六 压缩恢复
    │   ├── 认证/速率失败? → §四 advanceAuthProfile()
    │   ├── Thinking 不支持? → §七 pickFallbackThinkingLevel()
    │   └── 全部失败 → throw FailoverError → §五 模型降级
    │
    ├── 助手错误 (lastAssistant.errorMessage)?
    │   ├── 标记 Profile 失败
    │   ├── advanceAuthProfile()
    │   ├── Thinking 降级?
    │   └── 全部失败 → throw FailoverError → §五 模型降级
    │
    └── 成功 → buildPayloads() → return
}
```

### 3.2 两类错误入口

| 错误入口 | 触发时机 | 来源 |
| ---- | ---- | ---- |
| `promptError` | LLM 请求发送阶段失败 | API 拒绝请求（401/429/上下文溢出等） |
| `lastAssistant.errorMessage` | LLM 流式响应中途失败 | 流式输出时提供商返回错误 |

两类错误都会经过相同的恢复路径（认证轮转 → Thinking 降级 → 模型降级），但**助手错误**会额外标记当前 Profile 为失败（因为请求已经开始消耗配额）。

---

## 四、认证轮转 (Auth Profile Rotation)

### 4.1 Profile 排序与选择

> 源文件: `src/agents/auth-profiles/order.ts` — `resolveAuthProfileOrder()`

Runtime 启动时会构建一个**候选 Profile 列表**，按优先级排序：

```text
构建候选列表
    │
    ├── 来源 1: 存储的显式顺序 (store.order[provider])
    ├── 来源 2: 配置的显式顺序 (cfg.auth.order[provider])
    ├── 来源 3: 配置的 Profiles (cfg.auth.profiles)
    └── 来源 4: 自动发现的 Profiles
    │
    ▼
排序规则:
    ├── 显式顺序模式:
    │   1. 可用的 Profile 在前
    │   2. 冷却中的 Profile 在后（按冷却到期时间排序，最早的在前）
    │   3. preferredProfile 提升到最前
    │
    └── 轮询模式 (无显式顺序):
        1. 按凭证类型: oauth > token > api_key
        2. 按上次使用时间: 最久未用的在前
        3. 冷却中的 Profile 追加到末尾
```

### 4.2 advanceAuthProfile — 核心轮转函数

> 源文件: `src/agents/pi-embedded-runner/run.ts`

```typescript
const advanceAuthProfile = async (): Promise<boolean> => {
  if (lockedProfileId) return false;  // 锁定的不可轮转

  let nextIndex = profileIndex + 1;
  while (nextIndex < profileCandidates.length) {
    const candidate = profileCandidates[nextIndex];

    // 跳过冷却中的 Profile
    if (candidate && isProfileInCooldown(authStore, candidate)) {
      nextIndex += 1;
      continue;
    }

    try {
      await applyApiKeyInfo(candidate);   // 应用新 Key
      profileIndex = nextIndex;
      thinkLevel = initialThinkLevel;     // 重置 Thinking 级别
      attemptedThinking.clear();          // 清除已尝试的级别
      return true;
    } catch (err) {
      nextIndex += 1;
    }
  }
  return false;  // 无可用 Profile
};
```

**要点**:

- 切换 Key 时**重置 Thinking 级别**——因为新 Key 可能对应不同的模型配额/能力
- 跳过冷却中的 Profile（避免重复触发 429）
- 返回 `false` 表示所有 Key 耗尽，触发上层 `FailoverError`

### 4.3 冷却机制

> 源文件: `src/agents/auth-profiles/usage.ts`

#### 普通失败（rate_limit / auth / timeout）

指数退避公式：`cooldown = min(1 hour, 60s × 5^(min(errorCount - 1, 3)))`

| 连续失败次数 | 冷却时间 |
| ---- | ---- |
| 1 | 1 分钟 |
| 2 | 5 分钟 |
| 3 | 25 分钟 |
| 4+ | 60 分钟（上限） |

#### 计费失败 (billing)

更长的退避：`disabled = min(maxHours, baseHours × 2^(min(billingCount - 1, 10)))`

| 连续 billing 失败 | 禁用时间 | 默认配置 |
| ---- | ---- | ---- |
| 1 | 5 小时 | `billingBackoffHours = 5` |
| 2 | 10 小时 | |
| 3 | 20 小时 | |
| 4+ | 24 小时（上限） | `billingMaxHours = 24` |

#### 失败窗口

- 默认窗口: 24 小时（`failureWindowHours`）
- 如果上次失败超过窗口时间，错误计数**重置为 0**
- 冷却到期后 Profile 自动恢复可用

```text
Profile 失败
    │
    ├── 距离上次失败 > 24h?
    │   → 重置计数为 0 → 按首次失败计算冷却
    │
    └── 距离上次失败 ≤ 24h?
        → 累加计数 → 按累计次数计算冷却
        │
        ├── billing? → disabledUntil (小时级)
        └── 其他?    → cooldownUntil (分钟级)
```

---

## 五、模型降级 (Model Fallback)

### 5.1 降级链配置

> 源文件: `src/agents/model-fallback.ts`

```yaml
# 配置示例
agents:
  defaults:
    model:
      primary: "anthropic/claude-sonnet-4-20250514"
      fallbacks:
        - "openai/gpt-4o"
        - "anthropic/claude-haiku"
```

### 5.2 runWithModelFallback 流程

```text
候选模型列表: [claude-sonnet, gpt-4o, claude-haiku]
    │
    ▼
┌── 尝试 claude-sonnet ──────────────────────────────────────┐
│   第一层重试循环:                                           │
│   Key1 → 失败 → Key2 → 失败 → Key3 → 失败                │
│   → 所有 Key 耗尽 → throw FailoverError                   │
└────────────────────────────────────┬───────────────────────┘
                                     │ FailoverError
                                     ▼
┌── 尝试 gpt-4o ─────────────────────────────────────────────┐
│   检查: 该提供商的所有 Profile 是否都在冷却中?              │
│   ├── 是 → 跳过，尝试下一个                                │
│   └── 否 → 第一层重试循环 (新的 Key 候选列表)             │
│            Key1 → 失败 → throw FailoverError              │
└────────────────────────────────────┬───────────────────────┘
                                     │ FailoverError
                                     ▼
┌── 尝试 claude-haiku ───────────────────────────────────────┐
│   第一层重试循环...                                         │
│   → 成功! 返回结果                                         │
└────────────────────────────────────────────────────────────┘
```

**关键行为**:

- 只有 `FailoverError` 触发降级（普通错误直接抛出）
- 每次切换模型时，Auth Profile 候选列表重新构建
- 在尝试新模型前会检查其 Profile 冷却状态——如果所有 Key 都在冷却中，直接跳过
- `AbortError`（中止）立即终止，不触发降级（除非是超时导致的中止）

### 5.3 图像模型降级

图像/视觉模型有独立的降级链：

```yaml
agents:
  defaults:
    imageModel:
      primary: "openai/gpt-4o"
      fallbacks:
        - "anthropic/claude-sonnet-4-20250514"
```

图像降级逻辑更简单——不检查 Auth Profile 冷却，不做 `FailoverError` 规范化。

---

## 六、上下文溢出恢复

### 6.1 检测与压缩

```text
runEmbeddedAttempt() 返回 promptError
    │
    ▼
isContextOverflowError(errorText)?
    │
    ├── 否 → 其他错误恢复路径
    │
    └── 是 → 已经尝试过压缩?
             │
             ├── 是 → 返回错误给用户 "Context overflow..."
             │
             └── 否 → compactEmbeddedPiSessionDirect()
                      │
                      ├── 压缩成功? → continue (重试)
                      │
                      └── 压缩失败? → 返回错误给用户
```

**限制**: 每次运行**最多压缩一次**（`overflowCompactionAttempted` 标志）。如果压缩后仍然溢出，说明单条消息或系统提示本身就超出了上下文窗口限制。

### 6.2 与 Session 压缩的关系

上下文溢出压缩调用的是 `compactEmbeddedPiSessionDirect()`——与 SESSION.md 中描述的 `session.compact()` 相同。它使用 LLM 生成旧消息的摘要，替换掉详细的历史记录。

> 详见: [AGENT-RUNTIME-SESSION.md §四.4](./AGENT-RUNTIME-SESSION.md)、[AGENT-RUNTIME-SYSTEM-PROMPT.md §六](./AGENT-RUNTIME-SYSTEM-PROMPT.md)

---

## 七、Thinking 级别降级

### 7.1 触发条件

某些模型或 API Key 配额不支持特定的 Thinking 级别。当 LLM 返回类似 "supported values are: low, medium" 的错误时，Runtime 自动降级。

> 源文件: `src/agents/pi-embedded-helpers/thinking.ts` — `pickFallbackThinkingLevel()`

### 7.2 降级逻辑

```text
错误消息: "thinking level 'high' is not supported, supported values are: low, medium"
    │
    ▼
pickFallbackThinkingLevel({ message, attempted: Set(["high"]) })
    │
    ├── 正则提取: "supported values are: low, medium"
    ├── 解析为: ["low", "medium"]
    ├── 排除已尝试的: attempted = {"high"}
    ├── 返回第一个可用: "low"
    │
    ▼
thinkLevel = "low"
continue  ← 使用降级后的级别重试
```

**防循环**: `attemptedThinking` Set 记录所有已尝试的级别，每个级别最多尝试一次。切换 Auth Profile 时会**重置**这个 Set（新 Key 可能支持不同的级别）。

---

## 八、模型解析与守卫

### 8.1 模型解析

> 源文件: `src/agents/pi-embedded-runner/model.ts` — `resolveModel()`

```text
resolveModel(provider, modelId, agentDir, config)
    │
    ├── 从 ModelRegistry 查找 (基于 models.json)
    │   → 匹配 provider + modelId
    │
    ├── 未找到 → 检查 config 中的 inline models
    │   → cfg.models.providers[provider].models[*]
    │
    ├── 仍未找到 → 创建 fallback model
    │   → 使用 provider 的 API 类型和默认参数
    │
    └── 返回 { model, authStorage, modelRegistry }
```

### 8.2 上下文窗口守卫

> 源文件: `src/agents/context-window-guard.ts`

在 LLM 调用之前，Runtime 会检查模型的上下文窗口大小：

| 窗口大小 | 行为 |
| ---- | ---- |
| < 16,000 tokens | **阻止**: 抛出 `FailoverError`（触发模型降级） |
| 16,000 - 32,000 tokens | **警告**: 记录日志，继续执行 |
| > 32,000 tokens | 正常执行 |

**上下文窗口解析优先级**:

1. `cfg.models.providers[provider].models[].contextWindow`（配置中的显式值）
2. 模型自身的 `contextWindow` 属性
3. `cfg.agents.defaults.contextTokens`（全局上限）
4. 默认值

### 8.3 models.json

`models.json` 是模型注册表的磁盘缓存，由 `ensureOpenClawModelsJson()` 在每次运行前生成/更新：

```text
ensureOpenClawModelsJson()
    │
    ├── 合并隐式提供商 (环境变量中的 API Key → 自动发现)
    ├── 合并配置中的显式提供商 (cfg.models.providers)
    ├── 合并 GitHub Copilot / AWS Bedrock (如配置)
    │
    └── 写入 ~/.openclaw/agents/<agentId>/models.json
        → ModelRegistry 从此文件加载模型列表
```

---

## 九、配置参考

### 模型与降级

```yaml
agents:
  defaults:
    model:
      primary: "anthropic/claude-sonnet-4-20250514"    # 主模型
      fallbacks:                                        # 降级链
        - "openai/gpt-4o"
        - "anthropic/claude-haiku"

    imageModel:
      primary: "openai/gpt-4o"                         # 主图像模型
      fallbacks:
        - "anthropic/claude-sonnet-4-20250514"

    contextTokens: 200000                               # 全局上下文窗口上限

    models:                                             # 模型允许列表
      "anthropic/claude-sonnet-4-20250514":
        alias: "sonnet"
        params:
          temperature: 0.7
      "openai/gpt-4o":
        alias: "gpt4o"
```

### 认证冷却

```yaml
auth:
  cooldowns:
    failureWindowHours: 24          # 失败计数窗口（超出则重置）
    billingBackoffHours: 5          # billing 首次退避（小时）
    billingMaxHours: 24             # billing 最大退避（小时）
    billingBackoffHoursByProvider:   # 按提供商覆盖
      openai: 12
      anthropic: 8

  order:                            # Profile 显式排序
    anthropic: ["profile-a", "profile-b"]

  profiles:                         # Profile 定义
    profile-a:
      provider: anthropic
      apiKey: "sk-..."
    profile-b:
      provider: anthropic
      apiKey: "sk-..."
```

### 自定义模型提供商

```yaml
models:
  providers:
    my-provider:
      api: "openai"                 # API 兼容类型
      baseUrl: "https://my-llm.example.com/v1"
      models:
        - id: "my-model"
          contextWindow: 128000
          maxTokens: 4096
```

---

## 十、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/agents/model-fallback.ts` | `runWithModelFallback` / `runWithImageModelFallback`：模型降级链 |
| `src/agents/failover-error.ts` | `FailoverError` 类：降级信号 + 错误分类 |
| `src/agents/pi-embedded-helpers/errors.ts` | `classifyFailoverReason`：错误模式匹配与分类 |
| `src/agents/auth-profiles/usage.ts` | `markAuthProfileFailure` / `isProfileInCooldown`：冷却管理 |
| `src/agents/auth-profiles/order.ts` | `resolveAuthProfileOrder`：Profile 排序与选择 |
| `src/agents/pi-embedded-runner/run.ts` | 重试循环：`advanceAuthProfile` + 所有恢复策略 |
| `src/agents/pi-embedded-runner/model.ts` | `resolveModel`：模型解析 |
| `src/agents/pi-embedded-runner/compact.ts` | `compactEmbeddedPiSessionDirect`：上下文溢出压缩 |
| `src/agents/pi-embedded-helpers/thinking.ts` | `pickFallbackThinkingLevel`：Thinking 级别降级 |
| `src/agents/context-window-guard.ts` | `evaluateContextWindowGuard`：上下文窗口守卫 |
| `src/agents/models-config.ts` | `ensureOpenClawModelsJson`：models.json 生成 |
