# Agent Runtime 插件工具集成

> 本文档详解外部插件如何向 Agent Runtime 注入工具、技能和渠道能力：从插件发现、加载、注册，到工具解析、名称冲突检测、allowlist 过滤，以及渠道插件和 Runtime 扩展的机制。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、插件发现与加载](#二插件发现与加载)
- [三、插件 API 能力](#三插件-api-能力)
- [四、工具注入管线](#四工具注入管线)
- [五、Allowlist 与可选工具](#五allowlist-与可选工具)
- [六、名称冲突检测](#六名称冲突检测)
- [七、插件技能](#七插件技能)
- [八、渠道插件](#八渠道插件)
- [九、Runtime 扩展](#九runtime-扩展)
- [十、插件生命周期](#十插件生命周期)
- [十一、配置参考](#十一配置参考)
- [十二、关键源文件索引](#十二关键源文件索引)

---

## 一、全局视角

### 1.1 插件系统的角色

插件系统是 OpenClaw 的**可扩展骨架**——让第三方开发者在不修改核心代码的情况下，向 Agent Runtime 添加新的工具、技能、消息渠道和行为钩子。

### 1.2 插件能注入什么

```text
┌──────────────────────────────────────────────────────────────────┐
│  插件可注入的资源                                                 │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │  工具     │  │  技能     │  │  钩子     │  │  渠道            │ │
│  │          │  │          │  │          │  │                  │ │
│  │ 注册自定 │  │ 提供     │  │ 注入行为 │  │ 新的消息渠道     │ │
│  │ 义工具给 │  │ SKILL.md │  │ 到 Agent │  │ (Teams/Matrix/   │ │
│  │ LLM 使用 │  │ 给 Agent │  │ 流程节点 │  │  Zalo/Voice...)  │ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘ │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ HTTP路由  │  │ Gateway  │  │ CLI命令   │  │  服务/提供商      │ │
│  │          │  │ 方法     │  │          │  │                  │ │
│  │ 自定义   │  │ 扩展     │  │ 扩展 CLI │  │ 自定义 LLM      │ │
│  │ API 端点 │  │ RPC 方法 │  │ 子命令   │  │ 提供商/服务      │ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### 1.3 工具注入在 Runtime 中的位置

```text
createOpenClawCodingTools()
    │
    ├── ① 核心编码工具 (exec, read, write, edit, process)
    ├── ② 渠道 Agent 工具 (listChannelAgentTools)
    ├── ③ OpenClaw 工具 (browser, web_search, message, ...)
    │       │
    │       └── ④ 插件工具 (resolvePluginTools)  ← 本文档重点
    │
    ▼
工具策略过滤 (profile → provider → agent → group → sandbox)
    │
    ▼
最终工具集 → 注册到 AgentSession → LLM 可调用
```

---

## 二、插件发现与加载

### 2.1 插件存放位置

| 位置 | 路径 | 说明 |
| ---- | ---- | ---- |
| 工作区 | `<workspace>/.openclaw/extensions/` | 项目级插件 |
| 全局 | `~/.openclaw/extensions/` | 用户级插件 |
| 仓库内 | `extensions/*` | 捆绑插件（默认禁用） |
| 配置指定 | `plugins.load.paths` | 自定义路径 |

### 2.2 插件目录结构

```text
extensions/my-plugin/
  ├── package.json           ← npm 包定义
  ├── openclaw.plugin.json   ← 插件清单 (必需)
  ├── index.ts               ← 入口 (register/activate 函数)
  ├── hooks/                 ← 钩子目录 (可选)
  │   └── my-hook/
  │       ├── HOOK.md
  │       └── handler.ts
  └── skills/                ← 技能目录 (可选)
      └── my-skill/
          └── SKILL.md
```

### 2.3 插件清单 (openclaw.plugin.json)

```json
{
  "id": "my-plugin",
  "name": "My Plugin",
  "version": "1.0.0",
  "description": "A custom plugin",
  "configSchema": {
    "type": "object",
    "properties": {
      "apiKey": { "type": "string" }
    }
  },
  "skills": ["skills/my-skill"]
}
```

### 2.4 加载流程

```text
Gateway 启动
    │
    ▼
loadOpenClawPlugins(options)
    │
    ├── ① 规范化配置 (normalizePluginsConfig)
    │
    ├── ② 检查缓存 (按 workspace + config 键)
    │      ├── 命中 → 返回缓存的 registry
    │      └── 未命中 → 继续加载
    │
    ├── ③ 发现插件 (discoverOpenClawPlugins)
    │      → 扫描 4 个位置，收集候选列表
    │
    ├── ④ 加载清单 (loadPluginManifestRegistry)
    │      → 解析每个候选的 openclaw.plugin.json
    │      → 校验 configSchema
    │
    ├── ⑤ 创建 jiti 实例 (TypeScript 运行时加载)
    │      → 设置别名: openclaw/plugin-sdk → SDK 路径
    │
    └── ⑥ 逐个激活:
         ├── 检查启用状态 (resolveEnableState)
         ├── 校验配置 (validateJsonSchemaValue)
         ├── 加载模块 (jiti require)
         ├── 创建 API (createApi)
         └── 调用 register(api) 或 activate(api)
```

**依赖安装**: 插件目录运行 `npm install --omit=dev --silent`，只安装 `dependencies`（不装 `devDependencies`）。`openclaw` 本身应放在 `peerDependencies` 或 `devDependencies`，因为运行时通过 jiti 别名解析。

---

## 三、插件 API 能力

### 3.1 PluginApi 完整接口

插件的 `register(api)` / `activate(api)` 函数接收的 API 对象：

| 方法 | 说明 |
| ---- | ---- |
| `api.registerTool(tool, opts?)` | 注册自定义工具给 LLM |
| `api.on(hookName, handler, opts?)` | 注册类型安全的钩子 |
| `api.registerHook(events, handler)` | 注册内部钩子（传统方式） |
| `api.registerChannel(registration)` | 注册消息渠道插件 |
| `api.registerHttpHandler(handler)` | 注册 HTTP 中间件 |
| `api.registerHttpRoute(params)` | 注册 HTTP 路由 |
| `api.registerGatewayMethod(method, handler)` | 注册 Gateway RPC 方法 |
| `api.registerCli(registrar)` | 注册 CLI 子命令 |
| `api.registerService(service)` | 注册后台服务 |
| `api.registerProvider(provider)` | 注册 LLM 提供商 |
| `api.registerCommand(command)` | 注册用户命令 |

### 3.2 Runtime 辅助对象

`api.runtime` 提供丰富的运行时辅助：

| 分类 | 能力 |
| ---- | ---- |
| `runtime.config` | 读写 OpenClaw 配置 |
| `runtime.system` | 系统事件、命令执行、原生依赖 |
| `runtime.media` | 媒体加载、MIME 检测、图像处理、音频 |
| `runtime.tts` | 电话 TTS |
| `runtime.tools` | 记忆工具、记忆 CLI |
| `runtime.channel.*` | 渠道辅助（文本、回复、路由、配对、会话、提及、反应、群组、防抖、命令） |
| `runtime.logging` | 日志创建 |
| `runtime.state` | 状态目录解析 |

---

## 四、工具注入管线

### 4.1 resolvePluginTools — 核心函数

> 源文件: `src/plugins/tools.ts`

```text
resolvePluginTools({ context, existingToolNames, toolAllowlist })
    │
    ▼
遍历 registry 中所有工具注册:
    │
    ├── ① 检查插件是否被阻止
    ├── ② 检查插件 ID 与核心工具名不冲突
    ├── ③ 调用工厂函数: entry.factory(context)
    │      → 返回 AnyAgentTool | AnyAgentTool[]
    ├── ④ 可选工具 → 检查 allowlist
    │      ├── 在 allowlist 中 → 保留
    │      └── 不在 allowlist 中 → 过滤掉
    ├── ⑤ 工具名冲突检测
    │      ├── 与已有工具冲突 → 跳过并报错
    │      └── 插件间冲突 → 跳过并报错
    └── ⑥ 附加元数据 (pluginId, optional) 到 WeakMap
    │
    ▼
返回 AnyAgentTool[]  → 合并到 Agent 工具集
```

### 4.2 工具注册方式

```typescript
// 在插件的 register() 函数中
api.registerTool({
  name: "my_tool",
  description: "My custom tool",
  parameters: {
    type: "object",
    properties: {
      query: { type: "string", description: "Search query" }
    },
    required: ["query"],
  },
  execute: async (toolCallId, params) => {
    const result = await doSomething(params.query);
    return { content: [{ type: "text", text: result }] };
  },
}, {
  optional: true,  // 需要 allowlist 才启用
});
```

### 4.3 工具上下文

工厂函数收到的上下文：

```typescript
type OpenClawPluginToolContext = {
  config?: OpenClawConfig;
  workspaceDir?: string;
  agentDir?: string;
  sessionKey?: string;
  agentAccountId?: string;
  messageProvider?: string;
  modelProvider?: string;
  modelId?: string;
  modelHasVision?: boolean;
  sandbox?: SandboxContext;
  abortSignal?: AbortSignal;
};
```

---

## 五、Allowlist 与可选工具

### 5.1 必需 vs 可选工具

| 类型 | 注册方式 | 行为 |
| ---- | ---- | ---- |
| 必需 (`optional: false`, 默认) | `api.registerTool(tool)` | 总是包含在工具集中 |
| 可选 (`optional: true`) | `api.registerTool(tool, { optional: true })` | 只有在 allowlist 中才包含 |

### 5.2 Allowlist 匹配规则

可选工具通过以下任一匹配方式进入工具集：

```text
isOptionalToolAllowed({ toolName, pluginId, allowlist })
    │
    ├── toolName 在 allowlist 中? → 允许
    ├── pluginId 在 allowlist 中? → 允许 (该插件所有可选工具)
    └── "group:plugins" 在 allowlist 中? → 允许 (所有插件的可选工具)
```

### 5.3 策略配置中的 allowlist

```yaml
# 在工具策略中允许可选的插件工具
tools:
  allow:
    - "my_tool"           # 按工具名
    - "my-plugin"         # 按插件 ID (该插件所有可选工具)
    - "group:plugins"     # 所有插件的可选工具
```

### 5.4 策略展开

工具策略系统会展开插件组：

```text
tools.allow: ["group:plugins"]
    │
    ▼  expandPolicyWithPluginGroups()
    │
展开为: ["my_tool", "other_tool", ...]  (所有插件注册的工具名)
```

---

## 六、名称冲突检测

### 6.1 检测规则

```text
名称冲突检测 (在 resolvePluginTools 中):
    │
    ├── 插件 ID === 核心工具名?
    │   → 错误: 插件被阻止 (整个插件的工具都不加载)
    │   → 例: 插件 id="exec" 会与核心 exec 工具冲突
    │
    ├── 插件工具名 === 核心工具名?
    │   → 错误: 该工具跳过
    │   → 例: 插件注册 name="read" 会与核心 read 工具冲突
    │
    └── 插件 A 工具名 === 插件 B 工具名?
        → 错误: 后注册的工具跳过
        → 先到先得
```

**名称规范化**: 使用 `normalizeToolName()` 进行大小写不敏感比较。

---

## 七、插件技能

### 7.1 技能声明

在 `openclaw.plugin.json` 中声明技能目录：

```json
{
  "skills": ["skills/my-skill", "skills/another-skill"]
}
```

### 7.2 技能加载

> 源文件: `src/agents/skills/plugin-skills.ts`

```text
resolvePluginSkillDirs({ workspaceDir, config })
    │
    ├── 加载清单注册表
    ├── 遍历启用的插件
    │   ├── 检查 manifest.skills 数组
    │   └── 解析路径 (相对插件根目录)
    └── 返回去重的技能目录列表
    │
    ▼
与工作区/捆绑技能合并 → 统一加载 SKILL.md → 注入系统提示
```

### 7.3 技能格式

插件技能与普通技能格式完全相同（Markdown 文件 + SKILL.md 元数据），Agent 无法区分技能来源。

---

## 八、渠道插件

### 8.1 渠道插件接口

> 源文件: `src/channels/plugins/types.plugin.ts`

渠道插件是最复杂的插件类型，需要实现完整的消息收发能力：

```text
ChannelPlugin {
  id: ChannelId              ← 唯一标识 (如 "msteams", "matrix")
  meta: ChannelMeta          ← 名称、图标、排序
  capabilities               ← 能力声明

  config                     ← 配置适配器
  configSchema?              ← JSON Schema

  // ── 适配器 (按需实现) ──────────────
  setup?                     ← 设置/配对流程
  auth?                      ← 登录/登出
  messaging?                 ← 消息收发 (核心)
  outbound?                  ← 出站消息投递
  gateway?                   ← Gateway 集成
  gatewayMethods?            ← 自定义 RPC 方法
  status?                    ← 健康检查
  actions?                   ← 消息操作 (反应等)
  agentTools?                ← 渠道专属 Agent 工具
}
```

### 8.2 渠道工具注入

渠道插件可以通过 `agentTools` 字段提供渠道专属工具：

```typescript
// 渠道插件中
agentTools: (ctx) => [
  {
    name: "teams_send_card",
    description: "Send an adaptive card in Teams",
    parameters: { ... },
    execute: async (id, params) => { ... },
  },
],
```

这些工具通过 `listChannelAgentTools({ cfg })` 收集，与核心工具和插件工具一起注册到 Agent。

### 8.3 已有的渠道插件

| 插件 | 位置 | 渠道 |
| ---- | ---- | ---- |
| msteams | `extensions/msteams/` | Microsoft Teams |
| matrix | `extensions/matrix/` | Matrix |
| zalo | `extensions/zalo/` | Zalo (OA) |
| zalouser | `extensions/zalouser/` | Zalo (用户) |
| voice-call | `extensions/voice-call/` | 语音通话 |

---

## 九、Runtime 扩展

### 9.1 什么是 Runtime 扩展

Runtime 扩展（Pi Extensions）是一种不同于插件的内部扩展机制，它们直接修改 Pi SDK 的行为，而不是通过 Plugin API 注册。

> 源文件: `src/agents/pi-embedded-runner/extensions.ts`

### 9.2 已有的 Runtime 扩展

#### 上下文裁剪 (Context Pruning)

```text
启用条件: agents.defaults.contextPruning.mode === "cache-ttl"

作用: 在内存中裁剪旧的工具结果，不持久化
机制: 基于 TTL 过期或工具匹配规则
效果: 减少发送给 LLM 的上下文大小
```

#### 压缩安全阀 (Compaction Safeguard)

```text
启用条件: agents.defaults.compaction.mode === "safeguard"

作用: 在自动压缩前生成旧消息摘要
机制: 控制历史占比、预留 token 空间
效果: 压缩后不会丢失关键上下文
```

### 9.3 扩展加载方式

```typescript
const extensionPaths = buildEmbeddedExtensionPaths({
  cfg, sessionManager, provider, modelId, model,
});
// extensionPaths → 传给 createAgentSession({ extensions: extensionPaths })
// Pi SDK 加载这些 TypeScript 文件作为运行时扩展
```

**与插件的区别**: Runtime 扩展直接操作 Pi SDK 内部（消息历史、压缩策略），而插件通过 Plugin API 间接注册资源。

---

## 十、插件生命周期

### 10.1 启用状态决策

```text
resolveEnableState(id, origin, config)
    │
    ├── plugins.enabled === false → 全部禁用
    ├── id 在 plugins.deny 中? → 禁用 (deny 优先于 allow)
    ├── plugins.allow 存在且 id 不在其中? → 禁用
    ├── plugins.entries[id].enabled === false → 禁用
    ├── 仓库内捆绑 + 不在默认启用列表? → 禁用
    ├── 记忆槽位冲突? → 仅保留选中的记忆插件
    └── 以上都通过 → 启用
```

### 10.2 激活与错误处理

```text
插件加载:
    │
    ├── 清单缺失 → 错误, 跳过
    ├── configSchema 校验失败 → 错误, 跳过
    ├── 模块加载失败 → 错误, 跳过
    ├── register()/activate() 抛异常 → 错误, 标记, 记录诊断
    ├── register() 返回 Promise → 警告 (应该是同步的)
    └── 成功 → 状态 = "loaded"
```

**所有错误都被记录到 `registry.diagnostics`**，可通过 `openclaw doctor` 查看。

### 10.3 记忆槽位独占

只能有一个 `kind: "memory"` 的插件处于活跃状态：

```yaml
plugins:
  slots:
    memory: "my-memory-plugin"  # 选中的记忆插件, "none" 禁用所有
```

---

## 十一、配置参考

### 插件管理

```yaml
plugins:
  enabled: true                    # 主开关 (默认 true)

  allow: ["my-plugin"]             # 允许列表 (仅列出的可加载)
  deny: ["bad-plugin"]             # 拒绝列表 (deny > allow)

  load:
    paths:                         # 额外插件路径
      - "/path/to/my-plugin"
      - "/path/to/plugin-dir/"

  slots:
    memory: "my-memory-plugin"     # 记忆槽位

  entries:
    my-plugin:
      enabled: true                # 单个插件开关
      config:                      # 插件专属配置
        apiKey: "sk-..."
        maxResults: 10
```

### 插件工具策略

```yaml
tools:
  allow:
    - "my_tool"                    # 按工具名允许
    - "my-plugin"                  # 按插件 ID 允许 (所有工具)
    - "group:plugins"              # 允许所有插件工具
```

---

## 十二、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/plugins/loader.ts` | 插件发现、加载、激活主流程 |
| `src/plugins/registry.ts` | 插件注册表：api.registerTool / registerChannel 等实现 |
| `src/plugins/types.ts` | PluginApi、Hook 类型、Plugin 类型定义 |
| `src/plugins/tools.ts` | `resolvePluginTools`：工具解析、allowlist、冲突检测 |
| `src/plugins/discovery.ts` | 插件目录扫描与候选收集 |
| `src/plugins/manifest-registry.ts` | `openclaw.plugin.json` 清单加载与缓存 |
| `src/plugins/config-state.ts` | 启用状态决策、记忆槽位、配置规范化 |
| `src/plugins/install.ts` | `npm install --omit=dev` 安装 |
| `src/agents/openclaw-tools.ts` | 插件工具合并到 Agent 工具集 |
| `src/agents/channel-tools.ts` | 渠道插件工具收集 |
| `src/agents/pi-tools.ts` | 完整工具管线（含插件组展开） |
| `src/agents/skills/plugin-skills.ts` | 插件技能目录解析 |
| `src/channels/plugins/types.plugin.ts` | 渠道插件接口定义 |
| `src/channels/plugins/load.ts` | 渠道插件加载 |
| `src/agents/pi-embedded-runner/extensions.ts` | Runtime 扩展（Context Pruning / Compaction Safeguard） |
