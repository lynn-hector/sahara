# Agent Runtime 插件系统详解

> 本文档深入剖析 OpenClaw 插件系统的完整调用流程、内部机制和实现细节，帮助开发者理解插件从加载到执行的完整生命周期。

---

## 目录

- [一、架构总览](#一架构总览)
- [二、完整调用流程](#二完整调用流程)
- [三、Tools、Skills 与 Plugins 的关系](#三tools-skills-与-plugins-的关系)
- [四、插件生命周期详解](#四插件生命周期详解)
- [五、工具注册与解析](#五工具注册与解析)
- [六、钩子系统执行机制](#六钩子系统执行机制)
- [七、配置与状态管理](#七配置与状态管理)
- [八、代码示例](#八代码示例)
- [九、调试与诊断](#九调试与诊断)

---

## 一、架构总览

### 1.1 系统组件图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Agent Runtime 插件系统                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Discovery │───▶│    Loader   │───▶│   Registry  │───▶│    Hooks    │  │
│  │   插件发现   │    │   插件加载   │    │   注册表     │    │   钩子执行   │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│         │                  │                  │                  │         │
│         ▼                  ▼                  ▼                  ▼         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Plugin Runtime API                          │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │   │
│  │  │register │ │register │ │register │ │register │ │register │  ...   │   │
│  │  │  Tool   │ │  Hook   │ │ Channel │ │ Gateway │ │   CLI   │       │   │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Agent 工具集成流程                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   createOpenClawTools()                                                     │
│        │                                                                    │
│        ├── 1. 创建核心工具 (browser, read, write, edit...)                  │
│        ├── 2. 创建 OpenClaw 工具 (web_search, message, cron...)             │
│        │                                                                    │
│        ├── 3. resolvePluginTools() ◄────────── 插件工具注入点               │
│        │      │                                                             │
│        │      ├── 加载插件注册表                                            │
│        │      ├── 执行工具工厂函数                                          │
│        │      ├── 检查 allowlist                                            │
│        │      └── 名称冲突检测                                              │
│        │                                                                    │
│        └── 4. 合并所有工具                                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 关键文件职责

| 文件路径 | 职责 | 核心函数/类 |
|---------|------|-----------|
| `src/plugins/loader.ts` | 插件加载 orchestration | `loadOpenClawPlugins()` |
| `src/plugins/registry.ts` | 注册表管理 | `createPluginRegistry()` |
| `src/plugins/tools.ts` | 工具解析 | `resolvePluginTools()` |
| `src/plugins/hooks.ts` | 钩子执行引擎 | `createHookRunner()` |
| `src/plugins/discovery.ts` | 插件发现 | `discoverOpenClawPlugins()` |
| `src/plugins/config-state.ts` | 配置状态管理 | `resolveEnableState()` |
| `src/plugins/runtime/index.ts` | Runtime API 实现 | `createPluginRuntime()` |
| `src/agents/openclaw-tools.ts` | Agent 工具集成 | `createOpenClawTools()` |
| `src/plugins/types.ts` | 类型定义 | Plugin API 类型 |

---

## 二、完整调用流程

### 2.1 网关启动时的插件加载流程

```
Gateway 启动
    │
    ▼
loadOpenClawPlugins({ config, workspaceDir })
    │
    ├── 1. 配置规范化 (normalizePluginsConfig)
    │      ├── 解析 plugins.enabled
    │      ├── 解析 plugins.allow/deny 列表
    │      ├── 解析 plugins.load.paths
    │      └── 解析 plugins.slots.memory
    │
    ├── 2. 缓存检查
    │      ├── 命中缓存 → 返回缓存的 registry
    │      └── 未命中 → 继续加载
    │
    ├── 3. 状态清理
    │      └── clearPluginCommands() 清除旧命令
    │
    ├── 4. 创建 Runtime 和 Registry
    │      ├── createPluginRuntime() → PluginRuntime 对象
    │      └── createPluginRegistry() → { registry, createApi }
    │
    ├── 5. 插件发现 (discoverOpenClawPlugins)
    │      ├── 扫描 plugins.load.paths (origin: "config")
    │      ├── 扫描 .openclaw/extensions/ (origin: "workspace")
    │      ├── 扫描 ~/.openclaw/extensions/ (origin: "global")
    │      └── 扫描 extensions/* (origin: "bundled")
    │
    ├── 6. 加载清单 (loadPluginManifestRegistry)
    │      ├── 读取每个候选的 package.json
    │      ├── 查找 openclaw.plugin.json
    │      └── 解析 configSchema
    │
    ├── 7. 创建 JITI 实例
    │      ├── 设置 TS/JS 扩展名支持
    │      └── 配置 alias: openclaw/plugin-sdk → SDK 路径
    │
    └── 8. 逐个激活插件 (循环)
           │
           ├── 8.1 检查重复 ID
           ├── 8.2 解析启用状态 (resolveEnableState)
           ├── 8.3 检查 configSchema 存在
           ├── 8.4 JITI 加载模块
           ├── 8.5 解析 register/activate 导出
           ├── 8.6 处理 memory slot 独占逻辑
           ├── 8.7 验证配置 (validateJsonSchemaValue)
           ├── 8.8 调用 register(api) 或 activate(api)
           └── 8.9 记录插件状态到 registry
    │
    ▼
initializeGlobalHookRunner(registry) 初始化全局钩子运行器
    │
    ▼
返回 PluginRegistry
```

### 2.2 Agent 会话创建时的工具解析流程

```
AgentSession 创建
    │
    ▼
createOpenClawTools(options)
    │
    ├── 1. 创建核心工具集
    │      ├── browser, canvas, nodes, cron
    │      ├── message, gateway, tts
    │      ├── sessions 系列工具
    │      └── web_search, web_fetch, image
    │
    ├── 2. 收集已有工具名称
    │      └── existingToolNames = Set(tools.map(t => t.name))
    │
    └── 3. resolvePluginTools({ context, existingToolNames, toolAllowlist })
           │
           ├── 3.1 调用 loadOpenClawPlugins() 获取 registry
           │
           ├── 3.2 初始化追踪集合
           │      ├── existing: 已有工具名 (核心工具)
           │      ├── allowlist: 允许的可选工具
           │      └── blockedPlugins: 被阻止的插件
           │
           └── 3.3 遍历 registry.tools (循环每个工具注册)
                  │
                  ├── a. 检查插件是否被阻止
                  │
                  ├── b. 检查插件 ID 与核心工具名冲突
                  │      └── 冲突 → 阻止整个插件
                  │
                  ├── c. 执行工具工厂函数
                  │      factory(context) → AnyAgentTool | AnyAgentTool[]
                  │
                  ├── d. 可选工具过滤 (如果 optional: true)
                  │      └── 检查 toolName/pluginId/group:plugins 是否在 allowlist
                  │
                  ├── e. 工具名冲突检测
                  │      ├── 与已有工具冲突 → 跳过并报错
                  │      └── 插件间冲突 → 跳过并报错
                  │
                  └── f. 附加元数据到 WeakMap
                         └── { pluginId, optional }
           │
           ▼
           返回 AnyAgentTool[]
    │
    ▼
返回合并后的工具集 (核心工具 + 插件工具)
```

---

## 三、Tools、Skills 与 Plugins 的关系

### 3.1 核心概念对比

| 维度 | **Tool** | **Skill** | **Plugin** |
|------|---------|-----------|------------|
| **本质** | 可执行函数 | 行为指导文档 | 扩展容器/包 |
| **格式** | TypeScript 对象/函数 | Markdown (SKILL.md) | TypeScript 模块 + JSON 清单 |
| **作用** | LLM 可调用的能力 | 注入 System Prompt 的指令 | 注册 Tools/Skills/Hooks/Channels 等 |
| **来源** | Core / OpenClaw / Plugin / Channel | Bundled / Managed / Workspace / Plugin | extensions/ 目录 |
| **运行时** | 被 LLM 调用执行 | 被 LLM 阅读遵循 | 在 Gateway 启动时加载注册 |

### 3.2 架构层级关系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Plugin (扩展包)                                  │
│  ╔═══════════════════════════════════════════════════════════════════════╗ │
│  ║  容器角色：Plugin 是 Tools 和 Skills 的「载体」和「组织单元」            ║ │
│  ╚═══════════════════════════════════════════════════════════════════════╝ │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  openclaw.plugin.json (清单)                                         │   │
│  │  ├── id, name, version                                              │   │
│  │  ├── configSchema                                                   │   │
│  │  └── skills: ["skills/my-skill"]  ───────┐                         │   │
│  └──────────────────────────────────────────┼─────────────────────────┘   │
│                                             │                               │
│  ┌──────────────────────────────────────────┼─────────────────────────┐   │
│  │  index.ts (入口)                        │                         │   │
│  │                                          │                         │   │
│  │  register(api) {                         │                         │   │
│  │    api.registerTool(tool/factory) ◄─────┼── 注册「工具」           │   │
│  │    api.on("hook_name", handler)         │                         │   │
│  │    api.registerChannel(channel)         │                         │   │
│  │    ...                                  │                         │   │
│  │  }                                       │                         │   │
│  └──────────────────────────────────────────┘                         │   │
│                                                                         │   │
│  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  skills/                                                        │   │   │
│  │   └── my-skill/SKILL.md ◄──────────────┐                       │   │   │
│  │       ├── YAML frontmatter             │                       │   │   │
│  │       └── Markdown instructions ◄──────┼── 声明「技能」         │   │   │
│  └────────────────────────────────────────┘                       │   │   │
│                                                                   │   │   │
└───────────────────────────────────────────────────────────────────┼───┼───┘
                                                                    │   │
                    ┌───────────────────────────────────────────────┘   │
                    │                                                   │
                    ▼                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Agent Runtime 运行时层                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Tools 注入流程                    Skills 注入流程                          │
│   ───────────────                   ───────────────                          │
│                                                                             │
│   createOpenClawCodingTools()       buildSystemPrompt()                     │
│        │                                 │                                  │
│        ├── ① Core Tools (read/write)    ├── ① Core Identity                │
│        ├── ② Bash Tools (exec)          ├── ② Skills Section               │
│        ├── ③ OpenClaw Tools             │      (Plugin Skills 合并于此)      │
│        ├── ④ Channel Tools              ├── ③ Memory Section               │
│        │                                ├── ④ ...                          │
│        └── ⑤ Plugin Tools ◄─────────────┘                                  │
│              (resolvePluginTools)                                           │
│                                                                             │
│   ┌─────────────────┐                   ┌─────────────────┐                │
│   │   Tools 注册表   │                   │  System Prompt  │                │
│   │  (给 LLM 使用)   │                   │ (指导 LLM 行为)  │                │
│   └────────┬────────┘                   └─────────────────┘                │
│            │                                                                │
│            │    ┌─────────────────────────────────────────────────────┐    │
│            └───▶│  LLM 的决策流程                                      │    │
│                 │                                                     │    │
│                 │  1. 阅读 System Prompt 中的 Skills                  │    │
│                 │     → 了解「何时/如何使用工具」                     │    │
│                 │                                                     │    │
│                 │  2. 根据用户请求，选择合适的 Tool                     │    │
│                 │     → 执行「具体操作」                              │    │
│                 │                                                     │    │
│                 └─────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Plugin 与 Tools/Skills 的关系详解

**Plugin 作为「上层组织单元」:**

1. **Plugin ⊃ Tools** (插件包含工具)
   - 插件通过 `api.registerTool()` 注册工具
   - 这些工具在 Agent 会话创建时被收集到工具注册表
   - 工具可以被 LLM 直接调用

2. **Plugin ⊃ Skills** (插件包含技能)
   - 插件通过 `openclaw.plugin.json` 中的 `skills` 字段声明技能目录
   - 技能文件 (SKILL.md) 被读取并注入 System Prompt
   - 技能指导 LLM 如何使用工具

3. **Plugin ⊃ Hooks/Channels/Services** (插件包含其他扩展)
   - 插件还可以注册生命周期钩子、消息渠道、后台服务等
   - 这些是 Tools 和 Skills 之外的能力扩展

**重要区别：Plugin 并非唯一来源**

```
Tools 来源分布:                    Skills 来源分布:
┌──────────────────────┐          ┌──────────────────────┐
│ Core Tools           │          │ Bundled Skills       │
│ (read, write, edit)  │          │ (仓库内置)           │
├──────────────────────┤          ├──────────────────────┤
│ OpenClaw Tools       │          │ Managed Skills       │
│ (browser, message)   │          │ (~/.openclaw/skills) │
├──────────────────────┤          ├──────────────────────┤
│ Channel Tools        │          │ Workspace Skills     │
│ (渠道专属)            │          │ (<workspace>/skills) │
├──────────────────────┤          ├──────────────────────┤
│ ████████████████     │          │ ████████████████     │
│ ██ Plugin Tools ██   │          │ ██ Plugin Skills ██  │
│ ████████████████     │          │ ████████████████     │
└──────────────────────┘          └──────────────────────┘

        ↓                                   ↓
   通过 api.registerTool()           通过 manifest.skills
   在运行时动态注册                   声明目录路径
```

### 3.4 为什么需要这种分层设计

| 设计目标 | 解释 |
|---------|------|
| **解耦能力与指导** | Tools 提供「能力」，Skills 提供「使用指导」。同一套 Tools 可以有不同的 Skills 来描述不同场景下的使用方式 |
| **独立演进** | Core Tools 和 Bundled Skills 可以独立更新，不依赖 Plugin 系统 |
| **灵活组合** | Plugin 可以同时提供 Tools 和对应的 Skills，也可以只提供其中一种 |
| **清晰职责** | Plugin 负责「组织和管理」扩展，Tools 负责「执行」，Skills 负责「指导」 |

### 3.5 协作流程示例

以 **Lobster 工作流插件**为例：

```
用户输入: "创建一个带审批的工作流"

    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ System Prompt 构建                                          │
│ ───────────────                                             │
│ ...                                                         │
│ ## Skills (mandatory)                                       │
│ ...                                                         │
│ ### lobsters                                                │  ◄── Plugin Skill
│ How to interact with the Lobster workflow system...         │     (来自 extensions/
│ ...                                                         │      lobster/skills/)
│ ## Tooling                                                  │
│ - lobster_create: Create a new workflow                     │  ◄── Plugin Tool
│ - lobster_add_step: Add a step to workflow                  │     (来自 api.registerTool)
│ - lobster_start: Start the workflow                         │
│ ...                                                         │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
LLM 思考过程:
"用户要创建工作流 → 查看 Skills → lobsters skill 适用 →
 阅读 SKILL.md → 了解应该使用 lobster_create 工具"

    │
    ▼
LLM 调用 Tool: lobster_create
{
  "name": "Project Approval",
  "description": "Approval workflow for project proposals"
}

    │
    ▼
Plugin Tool 执行 (在 Plugin 的 register() 中定义)

    │
    ▼
返回结果，LLM 继续对话
```

### 3.6 总结：三者关系公式

```
Plugin = {
  metadata: { id, name, version },
  tools: [Tool, Tool, ...],           // 通过 registerTool()
  skills: [Skill, Skill, ...],        // 通过 manifest.skills
  hooks: [Hook, Hook, ...],           // 通过 on()/registerHook()
  channels: [Channel, ...],           // 通过 registerChannel()
  ...
}

Skill = {
  name: string,
  description: string,
  instructions: Markdown,   // 指导 LLM 如何使用 Tools
}

Tool = {
  name: string,
  description: string,
  parameters: JSONSchema,
  execute: Function          // 被 LLM 调用执行
}

关系总结:
- Plugin 是「容器」，可以装 Tools 和 Skills
- Tools 是「执行器」，提供具体功能
- Skills 是「说明书」，指导如何使用 Tools
- 三者协同：Skill 指导 → LLM 决策 → Tool 执行
```

---

## 四、插件生命周期详解

### 4.1 插件记录 (PluginRecord) 结构

```typescript
// src/plugins/registry.ts:97-122
type PluginRecord = {
  id: string;                          // 插件唯一标识
  name: string;                        // 显示名称
  version?: string;                    // 版本
  description?: string;                // 描述
  kind?: PluginKind;                   // 类型: "memory" | undefined
  source: string;                      // 入口文件路径
  origin: PluginOrigin;                // 来源: bundled/global/workspace/config
  workspaceDir?: string;               // 工作区目录
  enabled: boolean;                    // 是否启用
  status: "loaded" | "disabled" | "error";  // 状态
  error?: string;                      // 错误信息

  // 注册的能力追踪
  toolNames: string[];                 // 注册的工具名列表
  hookNames: string[];                 // 钩子名列表
  channelIds: string[];                // 渠道 ID 列表
  providerIds: string[];               // 提供商 ID 列表
  gatewayMethods: string[];            // Gateway 方法列表
  cliCommands: string[];               // CLI 命令列表
  services: string[];                  // 服务 ID 列表
  commands: string[];                  // 命令列表
  httpHandlers: number;                // HTTP 处理器数量
  hookCount: number;                   // 钩子处理器数量

  // 配置相关
  configSchema: boolean;               // 是否有配置 schema
  configUiHints?: Record<string, PluginConfigUiHint>;
  configJsonSchema?: Record<string, unknown>;
};
```

### 4.2 启用状态决策流程

```
resolveEnableState(pluginId, origin, normalizedConfig)
    │
    ├── 1. plugins.enabled === false?
    │      └── 是 → 返回 { enabled: false, reason: "plugins disabled" }
    │
    ├── 2. pluginId 在 plugins.deny 中?
    │      └── 是 → 返回 { enabled: false, reason: "in deny list" }
    │      (deny 优先于 allow)
    │
    ├── 3. plugins.allow 存在且 pluginId 不在其中?
    │      └── 是 → 返回 { enabled: false, reason: "not in allow list" }
    │
    ├── 4. plugins.entries[pluginId].enabled === false?
    │      └── 是 → 返回 { enabled: false, reason: "disabled in config" }
    │
    ├── 5. origin === "bundled" 且不在默认启用列表?
    │      └── 是 → 返回 { enabled: false, reason: "bundled not default" }
    │
    └── 6. 返回 { enabled: true }
```

### 4.3 Memory Slot 独占机制

```
resolveMemorySlotDecision({ id, kind, slot, selectedId })
    │
    ├── 插件 kind !== "memory"?
    │      └── 返回 { enabled: true } (非记忆插件不受影响)
    │
    ├── slot === "none"?
    │      └── 返回 { enabled: false, reason: "memory slot is none" }
    │
    ├── slot === id?
    │      └── 返回 { enabled: true, selected: true }
    │
    ├── selectedId 已存在 (已有选中插件)?
    │      └── 返回 { enabled: false, reason: "another memory plugin selected" }
    │
    └── slot 未设置或自动选择?
          └── 返回 { enabled: true, selected: true }

配置示例:
plugins:
  slots:
    memory: "my-memory-plugin"  # 只能有一个记忆插件活跃
```

---

## 五、工具注册与解析

### 5.1 工具注册 API

```typescript
// 插件中的工具注册
api.registerTool(tool, options);

// 支持两种形式:
// 1. 直接注册工具对象
api.registerTool({
  name: "my_tool",
  description: "...",
  parameters: { ... },
  execute: async (toolCallId, params) => { ... }
});

// 2. 注册工厂函数 (推荐，支持运行时上下文)
api.registerTool((ctx) => {
  // ctx: OpenClawPluginToolContext
  return {
    name: "my_tool",
    description: "...",
    parameters: { ... },
    execute: async (toolCallId, params) => {
      // 可以访问 ctx.config, ctx.workspaceDir 等
    }
  };
}, {
  name: "my_tool",      // 可选，工厂函数时用于追踪
  optional: true        // 可选工具，需要 allowlist
});
```

### 5.2 工具上下文

```typescript
// src/plugins/types.ts:56-65
type OpenClawPluginToolContext = {
  config?: OpenClawConfig;          // OpenClaw 配置
  workspaceDir?: string;            // 工作区目录
  agentDir?: string;                // Agent 目录
  agentId?: string;                 // Agent ID
  sessionKey?: string;              // 会话密钥
  messageChannel?: string;          // 消息渠道
  agentAccountId?: string;          // Agent 账户 ID
  sandboxed?: boolean;              // 是否在沙箱中
};
```

### 5.3 工具解析详细流程

```typescript
// src/plugins/tools.ts:43-129
export function resolvePluginTools(params) {
  // 1. 获取注册表
  const registry = loadOpenClawPlugins({ config, workspaceDir, logger });

  const tools = [];
  const existing = params.existingToolNames ?? new Set();
  const existingNormalized = new Set(Array.from(existing, normalizeToolName));
  const allowlist = normalizeAllowlist(params.toolAllowlist);
  const blockedPlugins = new Set();

  // 2. 遍历所有工具注册
  for (const entry of registry.tools) {
    // 2.1 跳过被阻止的插件
    if (blockedPlugins.has(entry.pluginId)) continue;

    // 2.2 检查插件 ID 与核心工具冲突
    const pluginIdKey = normalizeToolName(entry.pluginId);
    if (existingNormalized.has(pluginIdKey)) {
      // 记录错误，阻止整个插件
      blockedPlugins.add(entry.pluginId);
      continue;
    }

    // 2.3 执行工厂函数
    let resolved = null;
    try {
      resolved = entry.factory(params.context);
    } catch (err) {
      log.error(`plugin tool failed (${entry.pluginId}): ${String(err)}`);
      continue;
    }
    if (!resolved) continue;

    // 2.4 处理可选工具过滤
    const listRaw = Array.isArray(resolved) ? resolved : [resolved];
    const list = entry.optional
      ? listRaw.filter((tool) =>
          isOptionalToolAllowed({
            toolName: tool.name,
            pluginId: entry.pluginId,
            allowlist,
          }),
        )
      : listRaw;

    // 2.5 工具名冲突检测
    const nameSet = new Set();
    for (const tool of list) {
      if (nameSet.has(tool.name) || existing.has(tool.name)) {
        // 记录冲突错误，跳过此工具
        continue;
      }
      nameSet.add(tool.name);
      existing.add(tool.name);

      // 2.6 附加元数据
      pluginToolMeta.set(tool, {
        pluginId: entry.pluginId,
        optional: entry.optional,
      });

      tools.push(tool);
    }
  }

  return tools;
}
```

### 5.4 可选工具 Allowlist 匹配规则

```
isOptionalToolAllowed({ toolName, pluginId, allowlist })
    │
    ├── allowlist 为空?
    │      └── 返回 false
    │
    ├── toolName (规范化后) 在 allowlist 中?
    │      └── 返回 true
    │
    ├── pluginId (规范化后) 在 allowlist 中?
    │      └── 返回 true (允许该插件的所有工具)
    │
    └── "group:plugins" 在 allowlist 中?
          └── 返回 true (允许所有插件的可选工具)

配置示例:
tools:
  allow:
    - "my_specific_tool"    # 按工具名
    - "my-plugin"           # 按插件 ID
    - "group:plugins"       # 所有插件工具
```

---

## 六、钩子系统执行机制

### 6.1 钩子类型定义

```typescript
// src/plugins/types.ts:287-301
type PluginHookName =
  | "before_agent_start"    // Agent 启动前，可注入系统提示
  | "agent_end"             // Agent 结束，分析对话
  | "before_compaction"     // 压缩前
  | "after_compaction"      // 压缩后
  | "message_received"      // 收到消息
  | "message_sending"       // 发送消息前，可修改/取消
  | "message_sent"          // 消息已发送
  | "before_tool_call"      // 工具调用前，可修改参数/阻止
  | "after_tool_call"       // 工具调用后
  | "tool_result_persist"   // 工具结果持久化前，同步执行
  | "session_start"         // 会话开始
  | "session_end"           // 会话结束
  | "gateway_start"         // Gateway 启动
  | "gateway_stop";         // Gateway 停止
```

### 6.2 钩子执行模式

| 钩子类型 | 执行模式 | 用途 |
|---------|---------|------|
| Void Hooks | 并行 (Promise.all) | fire-and-forget，不等待结果 |
| Modifying Hooks | 串行 (for...of) | 可修改数据，结果合并 |
| Sync Hooks | 同步执行 | 热路径，如 tool_result_persist |

### 6.3 钩子注册

```typescript
// 在插件 register() 函数中
api.on("before_agent_start", async (event, ctx) => {
  return {
    systemPrompt: "Additional instructions...",
    prependContext: "Context to prepend..."
  };
}, { priority: 10 });  // 优先级越高越先执行
```

### 6.4 钩子执行代码详解

```typescript
// src/plugins/hooks.ts:93-172

/**
 * Void Hooks - 并行执行
 */
async function runVoidHook(hookName, event, ctx) {
  const hooks = getHooksForName(registry, hookName);
  if (hooks.length === 0) return;

  // 并行执行所有处理器
  const promises = hooks.map(async (hook) => {
    try {
      await hook.handler(event, ctx);
    } catch (err) {
      // 错误处理: 记录或抛出
      if (catchErrors) {
        logger?.error(`[hooks] ${hookName} handler from ${hook.pluginId} failed`);
      } else {
        throw new Error(...);
      }
    }
  });

  await Promise.all(promises);
}

/**
 * Modifying Hooks - 串行执行，结果合并
 */
async function runModifyingHook(hookName, event, ctx, mergeResults) {
  const hooks = getHooksForName(registry, hookName);
  if (hooks.length === 0) return undefined;

  let result = undefined;

  // 串行执行，按优先级排序
  for (const hook of hooks) {
    try {
      const handlerResult = await hook.handler(event, ctx);

      if (handlerResult !== undefined && handlerResult !== null) {
        if (mergeResults && result !== undefined) {
          // 合并结果
          result = mergeResults(result, handlerResult);
        } else {
          result = handlerResult;
        }
      }
    } catch (err) {
      // 错误处理
    }
  }

  return result;
}
```

### 6.5 before_agent_start 钩子示例

```typescript
// src/plugins/hooks.ts:183-199
async function runBeforeAgentStart(event, ctx) {
  return runModifyingHook(
    "before_agent_start",
    event,
    ctx,
    (acc, next) => ({
      // 后面的 systemPrompt 覆盖前面的
      systemPrompt: next.systemPrompt ?? acc?.systemPrompt,
      // prependContext 合并（换行分隔）
      prependContext: acc?.prependContext && next.prependContext
        ? `${acc.prependContext}\n\n${next.prependContext}`
        : (next.prependContext ?? acc?.prependContext),
    }),
  );
}
```

### 6.6 钩子执行时机

```
Gateway 启动
    │
    ├── gateway_start hook ─────────────────────────┐
    │                                                │
    ▼                                                │
AgentSession 创建                                   │
    │                                                │
    ├── session_start hook ────────────────────────┤
    │                                                │
    ├── before_agent_start hook ◄──┐               │
    │      │                        │               │
    │      └── 注入 systemPrompt    │               │
    │                               │               │
    ▼                               │               │
Agent 处理消息 ◄───────────────────┘               │
    │                                                │
    ├── message_received hook ─────────────────────┤
    │                                                │
    ├── before_tool_call hook ◄──┐                 │
    │      │                      │                 │
    │      └── 可修改/阻止调用    │                 │
    │                             │                 │
    ▼                             │                 │
执行工具调用 ◄────────────────────┘                 │
    │                                                │
    ├── after_tool_call hook ──────────────────────┤
    │                                                │
    ├── tool_result_persist hook ◄──┐              │
    │      │                         │              │
    │      └── 同步修改持久化消息    │              │
    │                                │              │
    ▼                                │              │
持久化结果 ◄─────────────────────────┘              │
    │                                                │
    ├── message_sending hook ◄──┐                  │
    │      │                     │                  │
    │      └── 可修改/取消消息   │                  │
    │                            │                  │
    ▼                            │                  │
发送消息 ◄───────────────────────┘                  │
    │                                                │
    ├── message_sent hook ─────────────────────────┤
    │                                                │
    ├── before_compaction hook ────────────────────┤
    ├── after_compaction hook ─────────────────────┤
    │                                                │
    ├── agent_end hook ────────────────────────────┤
    │                                                │
    └── session_end hook ──────────────────────────┘
```

---

## 七、配置与状态管理

### 7.1 插件配置结构

```yaml
# ~/.openclaw/config.yaml
plugins:
  enabled: true                    # 主开关

  allow: ["plugin-a", "plugin-b"]  # 允许列表（空=允许所有）
  deny: ["bad-plugin"]             # 拒绝列表（优先级高于 allow）

  load:
    paths:                         # 额外加载路径
      - "/path/to/custom-plugin"
      - "/another/plugin-dir/"

  slots:
    memory: "my-memory-plugin"     # 记忆插件槽位

  entries:                         # 插件专属配置
    my-plugin:
      enabled: true
      config:
        apiKey: "sk-..."
        maxResults: 10
```

### 7.2 配置 Schema 定义

```typescript
// openclaw.plugin.json
{
  "id": "my-plugin",
  "name": "My Plugin",
  "version": "1.0.0",
  "configSchema": {
    "type": "object",
    "properties": {
      "apiKey": {
        "type": "string",
        "description": "API Key for service"
      },
      "maxResults": {
        "type": "number",
        "default": 10
      }
    },
    "required": ["apiKey"]
  },
  "configUiHints": {
    "apiKey": {
      "label": "API Key",
      "help": "Get your API key from...",
      "sensitive": true,       // 敏感信息，输入时隐藏
      "advanced": false        // 基础配置项
    }
  }
}
```

### 7.3 规范化配置对象

```typescript
// normalizePluginsConfig 输出结构
{
  enabled: boolean;
  allow?: string[];           // undefined = 允许所有
  deny: string[];
  loadPaths: string[];
  entries: {
    [pluginId: string]: {
      enabled?: boolean;
      config?: Record<string, unknown>;
    }
  };
  slots: {
    memory?: string | "none";
  };
}
```

---

## 八、代码示例

### 8.1 完整插件示例

```typescript
// extensions/my-plugin/index.ts
import type { OpenClawPluginDefinition } from "openclaw/plugin-sdk";

const plugin: OpenClawPluginDefinition = {
  id: "my-plugin",
  name: "My Custom Plugin",
  version: "1.0.0",
  description: "A sample plugin demonstrating all features",
  kind: undefined,  // 或 "memory"

  register(api) {
    // 访问插件专属配置
    const config = api.pluginConfig as { apiKey?: string };

    // 1. 注册工具
    api.registerTool({
      name: "search_database",
      description: "Search the custom database",
      parameters: {
        type: "object",
        properties: {
          query: { type: "string" },
          limit: { type: "number", default: 10 }
        },
        required: ["query"]
      },
      execute: async (toolCallId, params) => {
        const results = await searchDb(params.query, params.limit);
        return {
          content: [{ type: "text", text: JSON.stringify(results) }]
        };
      }
    }, {
      optional: true  // 需要 allowlist 才启用
    });

    // 2. 注册带上下文的工具（工厂函数）
    api.registerTool((ctx) => {
      // 可以访问 runtime 上下文
      const workspaceDir = ctx.workspaceDir;

      return {
        name: "workspace_info",
        description: "Get workspace information",
        parameters: { type: "object", properties: {} },
        execute: async () => ({
          content: [{ type: "text", text: `Workspace: ${workspaceDir}` }]
        })
      };
    });

    // 3. 注册钩子
    api.on("before_agent_start", async (event, ctx) => {
      return {
        systemPrompt: "You have access to a custom database. Use it when relevant.",
        prependContext: "Current workspace context..."
      };
    }, { priority: 5 });

    api.on("after_tool_call", async (event, ctx) => {
      console.log(`Tool ${event.toolName} executed in ${event.durationMs}ms`);
    });

    // 4. 注册命令
    api.registerCommand({
      name: "status",
      description: "Check plugin status",
      acceptsArgs: false,
      requireAuth: true,
      handler: async (ctx) => {
        return {
          text: "Plugin is running!",
          blocks: [{
            type: "section",
            text: { type: "mrkdwn", text: "*Status:* Online" }
          }]
        };
      }
    });

    // 5. 使用 Runtime API
    const logger = api.logger;
    logger.info(`[${api.id}] Plugin registered successfully`);

    // 6. 访问系统功能
    const runtime = api.runtime;
    // runtime.config.loadConfig()
    // runtime.system.runCommandWithTimeout()
    // runtime.media.detectMime()
    // runtime.channel.text.send()
  }
};

export default plugin;
```

### 8.2 渠道插件示例

```typescript
// extensions/my-channel/index.ts
import type { ChannelPlugin } from "openclaw/plugin-sdk";

const channelPlugin: ChannelPlugin = {
  id: "my-channel",
  meta: {
    name: "My Channel",
    icon: "📱",
    sortOrder: 100
  },
  capabilities: {
    supportsThreading: false,
    supportsReactions: true,
    supportsTyping: true
  },
  config: {
    fields: [
      { name: "apiKey", type: "string", required: true },
      { name: "webhookUrl", type: "string", required: true }
    ]
  },

  // 消息收发适配器
  messaging: {
    async startInbound(dock) {
      // 启动 WebSocket 或轮询
      const ws = new WebSocket(config.webhookUrl);
      ws.on("message", (data) => {
        dock.receive({
          id: generateId(),
          from: data.sender,
          content: data.text,
          timestamp: Date.now()
        });
      });
    },

    async send(message) {
      // 发送消息
      await fetch(config.webhookUrl, {
        method: "POST",
        headers: { "Authorization": `Bearer ${config.apiKey}` },
        body: JSON.stringify({ text: message.content })
      });
    }
  },

  // Agent 专属工具
  agentTools: (ctx) => [
    {
      name: "mychannel_send_card",
      description: "Send a rich card message",
      parameters: {
        type: "object",
        properties: {
          title: { type: "string" },
          content: { type: "string" }
        }
      },
      execute: async (id, params) => {
        // 实现...
      }
    }
  ]
};

export default channelPlugin;
```

---

## 九、调试与诊断

### 9.1 诊断信息查看

```bash
# 查看插件加载状态
openclaw doctor

# 查看详细日志
DEBUG=openclaw:plugins openclaw gateway run
```

### 9.2 注册表诊断结构

```typescript
type PluginDiagnostic = {
  level: "warn" | "error";
  message: string;
  pluginId?: string;
  source?: string;
};

// 常见诊断信息
[
  { level: "error", pluginId: "my-plugin", message: "plugin id conflicts with core tool name" },
  { level: "error", pluginId: "my-plugin", message: "gateway method already registered: customMethod" },
  { level: "warn", pluginId: "my-plugin", message: "plugin register returned a promise; async registration is ignored" },
  { level: "error", pluginId: "my-plugin", message: "invalid config: apiKey is required" }
]
```

### 9.3 运行时检查

```typescript
// 获取当前活跃的注册表
import { getActivePluginRegistry } from "openclaw/plugin-sdk";

const registry = getActivePluginRegistry();

// 检查已加载的插件
console.log(registry.plugins.map(p => ({
  id: p.id,
  status: p.status,
  toolNames: p.toolNames,
  hookCount: p.hookCount
})));

// 检查钩子
console.log(registry.typedHooks.map(h => ({
  pluginId: h.pluginId,
  hookName: h.hookName,
  priority: h.priority
})));
```

### 9.4 工具元数据访问

```typescript
import { getPluginToolMeta } from "./plugins/tools.js";

// 获取工具来源信息
const tool = /* ... */;
const meta = getPluginToolMeta(tool);
// meta = { pluginId: "my-plugin", optional: true }
```

---

## 十、关键设计决策

### 10.1 为什么选择 JITI

- **TypeScript 原生支持**: 无需预编译，开发体验好
- **ESM/CJS 兼容**: 支持多种模块格式
- **别名解析**: 支持 `openclaw/plugin-sdk` 别名指向实际 SDK 路径

### 10.2 为什么工具使用工厂函数

- **延迟初始化**: 只在 Agent 会话创建时执行
- **上下文访问**: 可以获取 sessionKey, workspaceDir 等运行时信息
- **配置隔离**: 每个会话可以有独立的配置

### 10.3 为什么钩子分三种执行模式

| 模式 | 理由 |
|-----|------|
| 并行 (Void) | 性能优先，不阻塞主流程 |
| 串行 (Modifying) | 需要结果合并，避免竞态 |
| 同步 (tool_result_persist) | 热路径性能，避免 async 开销 |

### 10.4 为什么 Memory Slot 独占

- **一致性**: 避免多个记忆系统冲突
- **确定性**: 明确的启用/禁用逻辑
- **配置简单**: 用户只需选择一个

---

## 十一、性能考量

### 11.1 缓存机制

```
registryCache: Map<cacheKey, PluginRegistry>

cacheKey = `${workspaceDir}::${JSON.stringify(pluginsConfig)}`
```

- 插件只在配置变化时重新加载
- Agent 会话复用已加载的注册表

### 11.2 工具解析优化

- 可选工具在 allowlist 检查后才执行工厂函数
- 名称冲突检测使用规范化后的名称
- 元数据使用 WeakMap 避免内存泄漏

### 11.3 钩子执行优化

- 无钩子时直接返回，零开销
- Void hooks 并行执行，不等待
- 优先级排序只执行一次（注册时）

---

*文档版本: 1.0 | 最后更新: 2026-02-13*
