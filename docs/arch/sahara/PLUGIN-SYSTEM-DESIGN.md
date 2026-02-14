# Plugin 系统设计（Phase 3 规划）

> **状态**：Phase 3 规划。Phase 1-2 仅需在 Tools / Skills / Hooks 中预留扩展接口（§10），Phase 3 实现完整 Plugin 生命周期。
>
> Plugin 是 Tools + Skills + Hooks 的「原子发行单元」，解决能力包的打包、分发、配置、启停的统一管理问题。
> 本文档是 Plugin 系统的完整设计蓝图，与 Runtime 各模块的集成点在对应模块文档中以"预留接口"形式标注。
>
> 关联文档：
> - [Runtime 架构设计](./RUNTIME-ARCHITECTURE-DESIGN.md) — Tools (§7) / Skills (§10) / Hooks (§13) 中的 Plugin 预留接口
> - [OpenClaw 插件系统详解](../openclaw/agent/AGENT-RUNTIME-PLUGIN-DETAILED.md) — 核心设计灵感来源
> - [工程计划](./ENGINEERING-PLAN-C-END.md) — P3-14 Plugin 系统 / P3-15 Plugin 管理后台

---

## 目录

1. [设计动机与核心概念](#一设计动机与核心概念)
2. [Plugin 与 Tools/Skills/Hooks 的关系](#二plugin-与-toolsskillshooks-的关系)
3. [Plugin 生命周期](#三plugin-生命周期)
4. [核心抽象](#四核心抽象)
5. [Plugin Runtime API](#五plugin-runtime-api)
6. [Plugin Registry](#六plugin-registry)
7. [Slot 独占机制](#七slot-独占机制)
8. [安全与隔离](#八安全与隔离)
9. [包结构](#九包结构)
10. [Phase 1-2 预留接口](#十phase-1-2-预留接口)
11. [与工程计划的映射](#十一与工程计划的映射)

---

## 一、设计动机与核心概念

当 Sahara 平台接入的能力超过 10 种（Jira、GitHub、Slack、支付、CRM...），**Tools / Skills / Hooks 的独立管理变得不可维护**——每种能力涉及 N 个 Tool + 1 个 Skill + M 个 Hook，启停需要分别操作，配置散落在多处。

**Plugin 是 Tools + Skills + Hooks 的「原子发行单元」**：

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                           Plugin (原子能力包)                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  manifest.json                                                          │
│  ├── id: "jira"                                                         │
│  ├── name: "Jira 项目管理"                                              │
│  ├── version: "1.0.0"                                                   │
│  ├── config_schema: { url: str, token: str(secret), project: str }     │
│  └── slot: null                                                         │
│                                                                         │
│  register(api: PluginAPI):                                              │
│    ├── api.register_tool(JiraCreateIssueTool)    # Tool ① (Tier PLUGIN)│
│    ├── api.register_tool(JiraListIssuesTool)     # Tool ②              │
│    ├── api.register_tool(JiraTransitionTool)     # Tool ③              │
│    ├── api.register_skill("skills/jira")         # Skill (SKILL.md)    │
│    ├── api.on("after_tool_execute", audit_log)   # Hook ①              │
│    └── api.on("before_prompt_build", inject_ctx) # Hook ②              │
│                                                                         │
│  ✅ 一键安装 / 卸载 / 启停 / 版本升级                                  │
│  ✅ 配置集中管理 (JSON Schema 校验)                                     │
│  ✅ 能力追踪 (Registry 记录每个 Plugin 注册了什么)                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**一句话定义**：Plugin 不是能力本身，而是能力的「容器」和「发行包」。它把相关联的 Tools、Skills、Hooks 打包为一个可管理的实体。

**Plugin 不是唯一来源**：Core / Enhanced / Extended 级别的 Tools、Builtin / Configured / Managed 级别的 Skills 依然通过现有机制加载。Plugin 只是第四种来源（最外层扩展）。

---

## 二、Plugin 与 Tools/Skills/Hooks 的关系

### 2.1 能力来源分层

```text
Tools 来源                      Skills 来源                    Hooks 来源
┌─────────────────────┐         ┌─────────────────────┐        ┌─────────────────────┐
│ Tier 0: CORE        │         │ BUILTIN             │        │ source: "internal"  │
│ (exec/read/write)   │         │ (随 Runtime 发布)    │        │ (审计/安全/日志)     │
├─────────────────────┤         ├─────────────────────┤        ├─────────────────────┤
│ Tier 1: ENHANCED    │         │ CONFIGURED          │        │ source: "extension" │
│ (edit/glob/grep)    │         │ (业务配置)           │        │ (配置加载)           │
├─────────────────────┤         ├─────────────────────┤        ├─────────────────────┤
│ Tier 2: EXTENDED    │         │ MANAGED             │        │                     │
│ (web_search/image)  │         │ (平台市场安装)       │        │                     │
├─────────────────────┤         ├─────────────────────┤        ├─────────────────────┤
│ ████████████████████│         │ ████████████████████│        │ █████████████████████│
│ █ Tier 3: PLUGIN  █ │         │ █ USER (Plugin)    █│        │ █ source: "plugin:  █│
│ █ (Plugin 注册)   █ │         │ █ (Plugin 声明)    █│        │ █  {plugin_id}"     █│
│ ████████████████████│         │ ████████████████████│        │ █████████████████████│
└─────────────────────┘         └─────────────────────┘        └─────────────────────┘
        │                               │                              │
        └───────────────────────────────┴──────────────────────────────┘
                                        │
                                        ▼
                            ┌──────────────────────┐
                            │  Plugin Registry     │
                            │  统一追踪: Plugin →   │
                            │  {tools, skills,     │
                            │   hooks, config}     │
                            └──────────────────────┘
```

### 2.2 协作流程

```text
用户: "帮我在 Jira 上创建一个 bug"

  1. System Prompt Builder
     └── 包含 Plugin Skill "jira" → LLM 了解 Jira 工具的使用规范

  2. LLM 决策
     └── 阅读 Skill → 选择 jira_create_issue Tool

  3. Hook: before_tool_execute
     └── Plugin Hook 检查 Jira 连接是否有效

  4. Tool 执行
     └── jira_create_issue.execute() → 调用 Jira REST API

  5. Hook: after_tool_execute
     └── Plugin Hook 记录审计日志

  6. 结果返回给用户
```

---

## 三、Plugin 生命周期

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  Plugin 生命周期                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ① 发现 (Discovery)                                                    │
│     ├── 数据来源: 配置中心 / Agent 配置 DB / Plugin 目录扫描            │
│     └── 产出: List[PluginCandidate] — 候选清单                         │
│                                                                         │
│  ② 加载清单 (Load Manifest)                                            │
│     ├── 读取 manifest.json                                              │
│     ├── 解析 config_schema (JSON Schema)                                │
│     └── 产出: List[PluginManifest]                                     │
│                                                                         │
│  ③ 准入控制 (Access Control)                                           │
│     ├── 全局 allow/deny 列表                                           │
│     ├── Agent 级别 plugin 启用列表                                     │
│     ├── Slot 独占检查 (如 memory slot)                                 │
│     └── 产出: List[PluginManifest] (过滤后)                            │
│                                                                         │
│  ④ 配置校验 (Config Validation)                                        │
│     ├── 用 config_schema 校验用户提供的配置                             │
│     └── 失败 → 记录 diagnostic, 跳过该 Plugin                         │
│                                                                         │
│  ⑤ 激活 (Activation)                                                   │
│     ├── import plugin module (Python importlib)                         │
│     ├── 调用 register(api) — Plugin 通过 API 注册 tools/skills/hooks   │
│     └── 产出: PluginRecord (记录注册了什么)                             │
│                                                                         │
│  ⑥ 运行时 (Runtime)                                                    │
│     ├── 注册的 Tools 参与 ToolRegistry.create_tools()                  │
│     ├── 注册的 Skills 参与 SkillLoader.load_all()                      │
│     ├── 注册的 Hooks 参与 HookRunner.run()                             │
│     └── 状态: "loaded" | "disabled" | "error"                          │
│                                                                         │
│  ⑦ 停用 (Deactivation)                                                │
│     ├── 移除 Plugin 注册的所有 tools/skills/hooks                      │
│     ├── 调用 deactivate(api) (可选清理回调)                            │
│     └── 从 Registry 标记为 disabled                                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**与 OpenClaw 的关键区别**：

| 维度 | OpenClaw | Sahara |
| --- | --- | --- |
| 发现方式 | 本地目录扫描 (extensions/) | 配置中心 / Agent 配置 DB (C 端场景) |
| 加载时机 | Gateway 启动时 (TypeScript JITI) | Runtime Worker 启动时 (Python importlib) |
| 模块格式 | TypeScript ESM/CJS | Python Package (wheel/sdist) 或 DB 存储的模块 |
| 安装者 | 开发者 (手动放入目录) | 平台运营 (管理后台安装) 或 Agent 创建者 (配置) |
| 运行环境 | Gateway 进程 (Node.js) | Runtime Worker 进程 (Python asyncio) |

---

## 四、核心抽象

```python
# sahara_runtime/plugins/types.py

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable

# ── Plugin 清单 ──────────────────────────────────────────────

@dataclass
class PluginManifest:
    """Plugin 的静态描述（从 manifest.json 解析）"""
    id: str                                 # 全局唯一标识，如 "jira", "github"
    name: str                               # 显示名称
    version: str                            # 语义版本
    description: str = ""
    author: str = ""
    slot: str | None = None                 # 独占槽位: "memory" | None
    config_schema: dict[str, Any] | None = None  # JSON Schema
    skills: list[str] = field(default_factory=list)  # 技能目录相对路径
    entry_point: str = "index"              # Python 模块入口

# ── Plugin 注册函数签名 ────────────────────────────────────

PluginRegisterFn = Callable[["PluginAPI"], None | Awaitable[None]]

# ── Plugin 定义（运行时加载后的完整对象）───────────────────

@dataclass
class PluginDefinition:
    """Plugin 的运行时定义（模块加载后）"""
    manifest: PluginManifest
    register: PluginRegisterFn
    deactivate: Callable[["PluginAPI"], None] | None = None

# ── Plugin 记录（Registry 中的追踪记录）───────────────────

class PluginStatus(str, Enum):
    LOADED = "loaded"
    DISABLED = "disabled"
    ERROR = "error"

@dataclass
class PluginRecord:
    """Plugin 在 Registry 中的完整状态"""
    id: str
    name: str
    version: str
    status: PluginStatus
    source: str                             # "config" | "agent:{agent_id}" | "directory"
    error: str | None = None

    # 注册的能力追踪
    tool_names: list[str] = field(default_factory=list)
    skill_names: list[str] = field(default_factory=list)
    hook_names: list[str] = field(default_factory=list)

    # 配置
    has_config_schema: bool = False
    config: dict[str, Any] | None = None

# ── Plugin 诊断 ───────────────────────────────────────────

@dataclass
class PluginDiagnostic:
    """Plugin 加载过程中的诊断信息"""
    level: str                              # "warn" | "error"
    plugin_id: str
    message: str
```

---

## 五、Plugin Runtime API

Plugin 通过 `PluginAPI` 与 Runtime 交互，这是 Plugin 唯一可用的注册接口：

```python
# sahara_runtime/plugins/api.py

from sahara_runtime.tools.base import Tool
from sahara_runtime.hooks.types import HookName, HookHandler

class PluginAPI:
    """
    Plugin 注册 API — Plugin 的 register() 函数接收此对象。
    所有注册操作通过此 API 完成，Plugin 不直接访问 Runtime 内部。
    """

    def __init__(self, plugin_id: str, plugin_config: dict | None,
                 registry: "PluginRegistry"):
        self.id = plugin_id
        self.plugin_config = plugin_config or {}
        self._registry = registry

    # ── Tool 注册 ─────────────────────────────────────────

    def register_tool(
        self,
        tool_or_factory: Tool | Callable[["ToolContext"], Tool],
        *,
        optional: bool = False,
        name: str | None = None,  # 工厂函数时用于追踪
    ) -> None:
        """
        注册工具。支持两种形式：
        1. 直接传 Tool 实例
        2. 传工厂函数 (推荐) — 延迟到 Agent 会话创建时执行，可访问运行时上下文
        """
        self._registry.add_tool(
            plugin_id=self.id,
            tool_or_factory=tool_or_factory,
            optional=optional,
            name=name,
        )

    # ── Skill 注册 ────────────────────────────────────────

    def register_skill(self, skill_dir: str) -> None:
        """
        注册技能目录。skill_dir 为相对于 Plugin 根目录的路径。
        运行时会从该目录加载 SKILL.md 文件。
        """
        self._registry.add_skill(plugin_id=self.id, skill_dir=skill_dir)

    # ── Hook 注册 ─────────────────────────────────────────

    def on(
        self,
        hook_name: HookName | str,
        handler: HookHandler,
        *,
        priority: int = 50,  # 默认中等优先级
    ) -> None:
        """
        注册生命周期钩子。
        handler 签名: async def handler(event: HookEvent, ctx: HookContext) -> Any
        """
        self._registry.add_hook(
            plugin_id=self.id,
            hook_name=hook_name,
            handler=handler,
            priority=priority,
        )

    # ── 日志 ──────────────────────────────────────────────

    @property
    def logger(self):
        """Plugin 专属 logger (自动附加 plugin_id)"""
        import structlog
        return structlog.get_logger("plugin", plugin_id=self.id)
```

**Tool Factory 上下文**（Agent 会话创建时注入）：

```python
@dataclass
class ToolContext:
    """工厂函数执行时的运行时上下文"""
    plugin_config: dict[str, Any]           # Plugin 配置
    agent_id: str                           # Agent 标识
    session_key: str                        # 会话标识
    sandbox: "Sandbox"                      # 沙箱实例
    working_dir: str | None = None          # 工作目录
```

---

## 六、Plugin Registry

```python
# sahara_runtime/plugins/registry.py

class PluginRegistry:
    """
    Plugin 注册表 — 追踪所有已加载 Plugin 及其注册的能力。
    整个 Runtime Worker 共享一个实例 (注入到 Dependencies)。
    """

    def __init__(self):
        self._plugins: dict[str, PluginRecord] = {}
        self._tool_entries: list[ToolEntry] = []
        self._skill_entries: list[SkillEntry] = []
        self._hook_entries: list[HookEntry] = []
        self._diagnostics: list[PluginDiagnostic] = []

    # ── 内部注册方法 (由 PluginAPI 调用) ──────────────────

    def add_tool(self, plugin_id, tool_or_factory, optional, name): ...
    def add_skill(self, plugin_id, skill_dir): ...
    def add_hook(self, plugin_id, hook_name, handler, priority): ...

    # ── 查询方法 (由各子系统调用) ─────────────────────────

    def get_plugin_tools(self, agent_id: str) -> list["ToolEntry"]:
        """ToolRegistry 调用: 获取所有 Plugin 注册的工具"""
        ...

    def get_plugin_skills(self) -> list["SkillEntry"]:
        """SkillLoader 调用: 获取所有 Plugin 声明的技能"""
        ...

    def get_plugin_hooks(self) -> list["HookEntry"]:
        """HookRunner 调用: 获取所有 Plugin 注册的钩子"""
        ...

    # ── 诊断 ──────────────────────────────────────────────

    def get_diagnostics(self) -> list[PluginDiagnostic]: ...
    def get_plugin_summary(self) -> list[dict]: ...
```

**与各子系统的集成点**：

```text
┌──────────────────────────────────────────────────────────────────────┐
│  Dependencies 容器                                                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  plugin_registry: PluginRegistry ◄─── Worker 启动时创建               │
│       │                                                                │
│       ├──→ ToolRegistry.create_tools()                                │
│       │      └── registry.get_plugin_tools(agent_id)                  │
│       │          → 调用工厂函数 → 得到 Tool 实例                      │
│       │          → 标记 tier=PLUGIN, source="plugin:{id}"             │
│       │                                                                │
│       ├──→ SkillLoader.load_all()                                     │
│       │      └── registry.get_plugin_skills()                         │
│       │          → 加载 SKILL.md → SkillEntry(source="plugin:{id}")   │
│       │                                                                │
│       ├──→ HookRunner (启动时注册)                                    │
│       │      └── registry.get_plugin_hooks()                          │
│       │          → 注册 HookRegistration(source="plugin:{id}")        │
│       │                                                                │
│       └──→ gRPC HealthCheck / 运维端点                                │
│              └── registry.get_plugin_summary()                        │
│              └── registry.get_diagnostics()                           │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 七、Slot 独占机制

某些能力只能有一个 Provider 处于活跃状态（如 Memory 后端）。借鉴 OpenClaw 的 Memory Slot 独占设计：

```python
# sahara_runtime/plugins/slots.py

@dataclass
class SlotConfig:
    """Slot 配置 — 由 Agent 配置或全局配置指定"""
    memory: str | None = None     # "pgvector" | "pinecone" | "qdrant" | None (自动选择)
    # 未来可扩展:
    # search: str | None = None
    # embedding: str | None = None

class SlotManager:
    """
    管理 Plugin 的独占 Slot。
    如果一个 Plugin 声明了 slot="memory"，且当前已有另一个 memory Plugin 被选中，
    则该 Plugin 被自动禁用。
    """

    def __init__(self, config: SlotConfig):
        self._config = config
        self._occupied: dict[str, str] = {}  # slot_name → plugin_id

    def check(self, plugin: PluginManifest) -> tuple[bool, str]:
        """
        返回 (enabled, reason)。
        如果 Plugin 不声明 slot，始终返回 (True, "")。
        """
        if plugin.slot is None:
            return True, ""

        slot_name = plugin.slot
        selected = getattr(self._config, slot_name, None)

        # 用户明确选择了某个 Plugin
        if selected is not None:
            if selected == plugin.id:
                self._occupied[slot_name] = plugin.id
                return True, ""
            return False, f"slot '{slot_name}' assigned to '{selected}'"

        # 自动选择：先到先得
        if slot_name in self._occupied:
            return False, f"slot '{slot_name}' already occupied by '{self._occupied[slot_name]}'"

        self._occupied[slot_name] = plugin.id
        return True, ""
```

---

## 八、安全与隔离

C 端场景下 Plugin 安全要求远高于 B 端开发者工具：

| 安全维度 | 措施 | 说明 |
| --- | --- | --- |
| **代码审核** | Plugin 必须经过平台审核后才能上架 | 不允许用户自行安装未审核的 Plugin |
| **API 限制** | PluginAPI 是 Plugin 唯一的注册接口，不暴露 Runtime 内部 | Plugin 无法直接访问 Redis / PG / 文件系统 |
| **工具沙箱** | Plugin 注册的 Tool 执行仍然在 Sandbox 内 | 与 Core Tool 共享同一套沙箱隔离 |
| **配置加密** | `config_schema` 中标记 `secret: true` 的字段加密存储 | API Token 等敏感配置不明文 |
| **资源限制** | Plugin 注册的 Hook 有执行超时限制 (默认 5s) | 防止 Hook 拖慢 Agent Loop |
| **错误隔离** | Plugin Hook 异常不影响 Agent Loop 主流程 | `catch_errors=True` (Runtime §13.7 已定义) |
| **准入控制** | 全局 allow/deny + Agent 级别启用列表 | 细粒度控制哪些 Agent 可以使用哪些 Plugin |

```python
# 准入控制
@dataclass
class PluginAccessControl:
    """全局 Plugin 准入配置"""
    enabled: bool = True                      # Plugin 系统主开关
    global_allow: list[str] | None = None     # 全局允许列表 (None=允许所有已审核)
    global_deny: list[str] = field(default_factory=list)  # 全局拒绝列表 (优先于 allow)

@dataclass
class AgentPluginConfig:
    """Agent 级别的 Plugin 配置"""
    plugins: list[str] = field(default_factory=list)  # 该 Agent 启用的 Plugin ID 列表
    plugin_configs: dict[str, dict] = field(default_factory=dict)  # 每个 Plugin 的配置
```

---

## 九、包结构

```text
sahara_runtime/
├── plugins/
│   ├── __init__.py           # 公共导出: PluginRegistry, PluginAPI, load_plugins
│   ├── types.py              # PluginManifest, PluginRecord, PluginDiagnostic, PluginStatus
│   ├── loader.py             # PluginLoader: 从配置中心/DB/目录发现和加载 Plugin
│   ├── registry.py           # PluginRegistry: 能力注册表 + 追踪 + 诊断
│   ├── api.py                # PluginAPI: Plugin 唯一可用的注册接口
│   ├── slots.py              # SlotManager: 独占 Slot 管理 (memory 等)
│   ├── access.py             # PluginAccessControl: allow/deny + Agent 级别准入
│   └── config.py             # Plugin 配置校验 (JSON Schema)
```

---

## 十、Phase 1-2 预留接口

**Plugin 系统在 Phase 3 实现，但 Phase 1-2 的代码需要为其预留「接缝」**：

| 子系统 | 预留接口 | Phase 1-2 行为 |
| --- | --- | --- |
| **Tool** | `Tool.source: str` 字段 | 默认 `"builtin"`, Agent 配置的为 `"agent:{id}"` |
| **Tool** | `ToolRegistry` 接受外部 `plugin_tools` 参数 | Phase 1-2 传空列表 |
| **Skill** | `SkillEntry.source: str` 字段 | 默认 `"bundled"` / `"configured"` / `"managed"` |
| **Skill** | `SkillLoader` 接受外部 `plugin_skills` 参数 | Phase 1-2 传空列表 |
| **Hook** | `HookRegistration.source: str` 字段 | Phase 1 仅 `"internal"`, Phase 2 加 `"extension:{name}"` |
| **Hook** | `HookRunner.register()` 支持 `source="plugin:*"` | Phase 1-2 不调用，但接口已就绪 |
| **Dependencies** | `plugin_registry: PluginRegistry \| None` 字段 | Phase 1-2 为 `None`；Phase 3 注入实例 |
| **Config** | `RuntimeConfig.plugins` 配置块 | Phase 1-2 为空 / 不解析 |

```python
# Phase 1 代码示例 — 预留但不实现

@dataclass
class Tool:
    name: str
    description: str
    tier: ToolTier
    source: str = "builtin"  # ★ 预留: Phase 3 会有 "plugin:{id}"
    ...

class ToolRegistry:
    def create_tools(
        self, agent_id: str, sandbox: "Sandbox", session_key: str,
        plugin_tools: list["Tool"] | None = None,  # ★ 预留: Phase 3 传入
    ) -> list["Tool"]:
        tools = self._create_builtin_tools(sandbox)
        tools += self._create_agent_tools(agent_id, sandbox)
        if plugin_tools:
            tools += plugin_tools  # Phase 3 启用
        return self._apply_policy(tools)
```

---

## 十一、与工程计划的映射

| 工程任务 | Phase | 预估 |
| --- | --- | --- |
| Tool/Skill/Hook 添加 `source` 字段 | Phase 1 | 0（初始设计时包含） |
| ToolRegistry / SkillLoader 预留 `plugin_*` 参数 | Phase 1 | 0（初始设计时包含） |
| Dependencies 预留 `plugin_registry` 字段 | Phase 1 | 0 |
| PluginManifest + PluginRecord 类型定义 | Phase 3 | 1d |
| PluginAPI 实现 (register_tool/register_skill/on) | Phase 3 | 2d |
| PluginRegistry 实现 (追踪 + 诊断) | Phase 3 | 2d |
| PluginLoader 实现 (从配置中心/DB 加载) | Phase 3 | 3d |
| SlotManager 实现 (memory slot 独占) | Phase 3 | 1d |
| PluginAccessControl 实现 (allow/deny) | Phase 3 | 1d |
| 集成测试 (端到端 Plugin 加载 + Tool 执行) | Phase 3 | 2d |
| 管理后台 Plugin 安装/配置 UI (API Service 侧) | Phase 3+ | 5d |
