# Agent Runtime 技能管理系统详解

> 本文档详细解析 Agent Runtime 中技能 (Skills) 的类型定义、加载流程、过滤机制、环境变量注入、提示词生成及沙箱同步。

---

## 目录

1. [概述](#一概述)
2. [技能类型定义](#二技能类型定义)
3. [技能加载流程](#三技能加载流程)
4. [技能过滤机制](#四技能过滤机制)
5. [技能环境变量注入](#五技能环境变量注入)
6. [技能提示词生成](#六技能提示词生成)
7. [技能同步到沙箱](#七技能同步到沙箱)
8. [技能命令规格](#八技能命令规格)
9. [技能调用流程](#九技能调用流程)
   - [LLM 自动调用流程](#91-llm-自动调用流程prompt-驱动)
   - [用户命令调用流程](#92-用户命令调用流程命令分发)
   - [SKILL.md 内容与产出处理](#93-skillmd-内容与产出处理)

---

## 一、概述

技能 (Skills) 是 OpenClaw 扩展 Agent 能力的核心机制，允许用户和系统添加专门的功能模块。每个技能本质上是一个包含 `SKILL.md` 文件的目录，其中定义了技能的描述、使用方法、元数据以及调用策略。

技能系统在 Agent Runtime 中的位置：

```text
┌──────────────────────────────────────────────────────────────────┐
│  runEmbeddedAttempt()                                            │
│       │                                                          │
│       ├── loadWorkspaceSkillEntries()     ← 加载技能             │
│       ├── applySkillEnvOverrides()        ← 注入环境变量         │
│       ├── resolveSkillsPromptForRun()     ← 生成技能提示词       │
│       ├── syncSkillsToWorkspace()         ← 同步到沙箱(如需)     │
│       │                                                          │
│       └── buildEmbeddedSystemPrompt({                            │
│             skillsPrompt: ...              ← 注入到系统提示词     │
│           })                                                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 二、技能类型定义

```typescript
// src/agents/skills/types.ts:1-88

// 技能安装规格
export type SkillInstallSpec = {
  id?: string;
  kind: "brew" | "node" | "go" | "uv" | "download";  // 安装方式
  label?: string;
  bins?: string[];          // 需要的二进制文件
  os?: string[];            // 支持的操作系统
  formula?: string;         // Homebrew formula
  package?: string;         // npm/pip 包名
  module?: string;          // Go 模块
  url?: string;             // 下载 URL
  archive?: string;         // 归档类型
  extract?: boolean;        // 是否解压
  stripComponents?: number; // 解压时跳过的目录层级
  targetDir?: string;       // 目标目录
};

// OpenClaw 技能元数据
export type OpenClawSkillMetadata = {
  always?: boolean;         // 始终启用
  skillKey?: string;        // 技能标识键
  primaryEnv?: string;      // 主要环境变量 (用于 API Key)
  emoji?: string;           // 技能图标
  homepage?: string;        // 主页链接
  os?: string[];            // 支持的操作系统列表
  requires?: {
    bins?: string[];        // 必需的所有二进制
    anyBins?: string[];     // 必需的任意一个二进制
    env?: string[];         // 必需的环境变量
    config?: string[];      // 必需的配置路径
  };
  install?: SkillInstallSpec[];  // 安装说明
};

// 技能调用策略
export type SkillInvocationPolicy = {
  userInvocable: boolean;          // 是否可被用户命令调用
  disableModelInvocation: boolean; // 是否禁止模型自动调用
};

// 技能条目 (加载后的完整信息)
export type SkillEntry = {
  skill: Skill;                          // 技能对象 (来自 pi-coding-agent)
  frontmatter: ParsedSkillFrontmatter;   // YAML frontmatter
  metadata?: OpenClawSkillMetadata;      // OpenClaw 元数据
  invocation?: SkillInvocationPolicy;    // 调用策略
};

// 技能快照 (用于缓存)
export type SkillSnapshot = {
  prompt: string;                        // 技能提示词
  skills: Array<{                        // 技能列表
    name: string;
    primaryEnv?: string;
  }>;
  resolvedSkills?: Skill[];              // 解析后的技能对象
  version?: number;                      // 快照版本
};
```

---

## 三、技能加载流程

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           技能加载流程                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│  loadSkillEntries(workspaceDir, opts)                                           │
│  src/agents/skills/workspace.ts:99-189                                          │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 1: 确定技能目录                                                           │
│  ───────────────────────────────────────────────────────────────────────────    │
│  const managedSkillsDir = ~/.openclaw/skills            // 托管技能             │
│  const workspaceSkillsDir = {workspaceDir}/skills       // 工作空间技能         │
│  const bundledSkillsDir = resolveBundledSkillsDir()     // 内置技能             │
│  const extraDirs = config.skills?.load?.extraDirs       // 额外目录             │
│  const pluginSkillDirs = resolvePluginSkillDirs()       // 插件技能             │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 2: 从各目录加载技能                                                       │
│  ───────────────────────────────────────────────────────────────────────────    │
│  const bundledSkills = loadSkillsFromDir({ dir: bundledSkillsDir })             │
│  const extraSkills = extraDirs.flatMap(dir => loadSkillsFromDir({ dir }))       │
│  const managedSkills = loadSkillsFromDir({ dir: managedSkillsDir })             │
│  const workspaceSkills = loadSkillsFromDir({ dir: workspaceSkillsDir })         │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  loadSkillsFromDir 来自 @mariozechner/pi-coding-agent                   │   │
│  │  扫描目录下的 SKILL.md 文件，解析技能定义                               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 3: 合并技能 (优先级: extra < bundled < managed < workspace)               │
│  ───────────────────────────────────────────────────────────────────────────    │
│  const merged = new Map<string, Skill>();                                       │
│                                                                                 │
│  for (const skill of extraSkills)     merged.set(skill.name, skill);            │
│  for (const skill of bundledSkills)   merged.set(skill.name, skill);  // 覆盖   │
│  for (const skill of managedSkills)   merged.set(skill.name, skill);  // 覆盖   │
│  for (const skill of workspaceSkills) merged.set(skill.name, skill);  // 覆盖   │
│                                                                                 │
│  // 同名技能会被后加载的覆盖，workspace 优先级最高                              │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Step 4: 解析 frontmatter 和元数据                                              │
│  ───────────────────────────────────────────────────────────────────────────    │
│  const skillEntries = Array.from(merged.values()).map((skill) => {              │
│    // 读取 SKILL.md 文件                                                        │
│    const raw = fs.readFileSync(skill.filePath, "utf-8");                        │
│                                                                                 │
│    // 解析 YAML frontmatter                                                     │
│    const frontmatter = parseFrontmatter(raw);                                   │
│                                                                                 │
│    return {                                                                     │
│      skill,                                                                     │
│      frontmatter,                                                               │
│      metadata: resolveOpenClawMetadata(frontmatter),    // 解析 openclaw 元数据 │
│      invocation: resolveSkillInvocationPolicy(frontmatter), // 解析调用策略     │
│    };                                                                           │
│  });                                                                            │
└─────────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
返回: SkillEntry[]
```

### 技能目录优先级

| 优先级 | 目录 | 说明 |
| ---- | ---- | ---- |
| 1 (最低) | `config.skills.load.extraDirs` | 用户配置的额外目录 |
| 2 | Plugin Skills | 插件提供的技能 |
| 3 | `{bundledSkillsDir}` | OpenClaw 内置技能 |
| 4 | `~/.openclaw/skills` | 托管技能 (clawhub 安装) |
| 5 (最高) | `{workspace}/skills` | 工作空间本地技能 |

---

## 四、技能过滤机制

```typescript
// src/agents/skills/config.ts:114-191
export function shouldIncludeSkill(params: {
  entry: SkillEntry;
  config?: OpenClawConfig;
  eligibility?: SkillEligibilityContext;
}): boolean {
  const { entry, config, eligibility } = params;
  const skillKey = resolveSkillKey(entry.skill, entry);
  const skillConfig = resolveSkillConfig(config, skillKey);

  // 检查 1: 显式禁用
  if (skillConfig?.enabled === false) {
    return false;  // 配置中明确禁用
  }

  // 检查 2: 内置技能白名单
  const allowBundled = config?.skills?.allowBundled;
  if (!isBundledSkillAllowed(entry, allowBundled)) {
    return false;  // 内置技能不在白名单中
  }

  // 检查 3: 操作系统兼容性
  const osList = entry.metadata?.os ?? [];
  const remotePlatforms = eligibility?.remote?.platforms ?? [];
  if (osList.length > 0) {
    const currentPlatform = process.platform;  // darwin/linux/win32
    if (!osList.includes(currentPlatform) &&
        !remotePlatforms.some(p => osList.includes(p))) {
      return false;  // 当前系统不支持
    }
  }

  // 检查 4: always 标记
  if (entry.metadata?.always === true) {
    return true;  // 始终启用，跳过后续检查
  }

  // 检查 5: 必需二进制文件
  const requiredBins = entry.metadata?.requires?.bins ?? [];
  for (const bin of requiredBins) {
    if (!hasBinary(bin) && !eligibility?.remote?.hasBin?.(bin)) {
      return false;  // 缺少必需的二进制
    }
  }

  // anyBins: 只需要其中一个存在
  const requiredAnyBins = entry.metadata?.requires?.anyBins ?? [];
  if (requiredAnyBins.length > 0) {
    const anyFound = requiredAnyBins.some(bin => hasBinary(bin)) ||
                     eligibility?.remote?.hasAnyBin?.(requiredAnyBins);
    if (!anyFound) {
      return false;  // 没有任何一个可选二进制
    }
  }

  // 检查 6: 必需环境变量
  const requiredEnv = entry.metadata?.requires?.env ?? [];
  for (const envName of requiredEnv) {
    // 检查: 环境变量 OR 技能配置 OR apiKey
    if (process.env[envName]) continue;
    if (skillConfig?.env?.[envName]) continue;
    if (skillConfig?.apiKey && entry.metadata?.primaryEnv === envName) continue;
    return false;  // 缺少必需的环境变量
  }

  // 检查 7: 必需配置路径
  const requiredConfig = entry.metadata?.requires?.config ?? [];
  for (const configPath of requiredConfig) {
    if (!isConfigPathTruthy(config, configPath)) {
      return false;  // 缺少必需的配置
    }
  }

  return true;
}
```

### 过滤检查流程图

```text
技能条目 (SkillEntry)
         │
         ▼
    ┌─────────────┐      是
    │ enabled=false? ├──────────► 排除
    └──────┬──────┘
           │ 否
           ▼
    ┌─────────────┐      是
    │ 内置技能 &&   ├──────────► 排除
    │ 不在白名单?  │
    └──────┬──────┘
           │ 否
           ▼
    ┌─────────────┐      是
    │ OS 不兼容?   ├──────────► 排除
    └──────┬──────┘
           │ 否
           ▼
    ┌─────────────┐      是
    │ always=true? ├──────────► 包含 ✓
    └──────┬──────┘
           │ 否
           ▼
    ┌─────────────┐      是
    │ 缺少必需      ├──────────► 排除
    │ 二进制文件?   │
    └──────┬──────┘
           │ 否
           ▼
    ┌─────────────┐      是
    │ 缺少必需      ├──────────► 排除
    │ 环境变量?     │
    └──────┬──────┘
           │ 否
           ▼
    ┌─────────────┐      是
    │ 缺少必需配置? ├──────────► 排除
    └──────┬──────┘
           │ 否
           ▼
       包含 ✓
```

---

## 五、技能环境变量注入

技能可以配置环境变量，在 Agent 运行时注入：

```typescript
// src/agents/skills/env-overrides.ts:6-43
export function applySkillEnvOverrides(params: {
  skills: SkillEntry[];
  config?: OpenClawConfig;
}) {
  const { skills, config } = params;
  const updates: Array<{ key: string; prev: string | undefined }> = [];

  for (const entry of skills) {
    const skillKey = resolveSkillKey(entry.skill, entry);
    const skillConfig = resolveSkillConfig(config, skillKey);
    if (!skillConfig) continue;

    // 注入 env 配置的环境变量
    if (skillConfig.env) {
      for (const [envKey, envValue] of Object.entries(skillConfig.env)) {
        if (!envValue || process.env[envKey]) continue;  // 不覆盖已存在的
        updates.push({ key: envKey, prev: process.env[envKey] });
        process.env[envKey] = envValue;
      }
    }

    // 注入 apiKey 到 primaryEnv
    const primaryEnv = entry.metadata?.primaryEnv;
    if (primaryEnv && skillConfig.apiKey && !process.env[primaryEnv]) {
      updates.push({ key: primaryEnv, prev: process.env[primaryEnv] });
      process.env[primaryEnv] = skillConfig.apiKey;
    }
  }

  // 返回清理函数 (恢复原始环境变量)
  return () => {
    for (const update of updates) {
      if (update.prev === undefined) {
        delete process.env[update.key];
      } else {
        process.env[update.key] = update.prev;
      }
    }
  };
}
```

### 配置示例

```yaml
# openclaw.yaml
skills:
  entries:
    peekaboo:
      enabled: true
      apiKey: "sk-xxx"      # → 注入到 primaryEnv (如 OPENAI_API_KEY)
      env:
        PEEKABOO_MODEL: "gpt-4o"
    mcporter:
      enabled: true
      env:
        MCPORTER_CONFIG: "/path/to/config.json"
```

### 环境变量注入时序

```text
runEmbeddedAttempt()
    │
    ├── loadWorkspaceSkillEntries()        ← 加载技能条目
    │
    ├── applySkillEnvOverrides(skills)     ← 注入环境变量到 process.env
    │       │
    │       ├── skill.env → process.env    (不覆盖已有)
    │       └── skill.apiKey → primaryEnv  (不覆盖已有)
    │
    ├── ... LLM 交互和工具执行 ...        ← 工具可通过 process.env 使用注入的变量
    │
    └── restoreSkillEnv()                  ← 恢复原始环境变量 (finally 块)
```

---

## 六、技能提示词生成

```typescript
// src/agents/skills/workspace.ts:228-254
export function buildWorkspaceSkillsPrompt(
  workspaceDir: string,
  opts?: {
    config?: OpenClawConfig;
    entries?: SkillEntry[];
    skillFilter?: string[];
    eligibility?: SkillEligibilityContext;
  },
): string {
  // 1. 加载技能条目
  const skillEntries = opts?.entries ?? loadSkillEntries(workspaceDir, opts);

  // 2. 过滤符合条件的技能
  const eligible = filterSkillEntries(skillEntries, opts?.config, opts?.skillFilter);

  // 3. 排除禁用模型调用的技能
  const promptEntries = eligible.filter(
    (entry) => entry.invocation?.disableModelInvocation !== true,
  );

  // 4. 格式化为提示词
  return formatSkillsForPrompt(promptEntries.map((entry) => entry.skill));
}
```

在 `runEmbeddedAttempt` 中调用:

```typescript
// src/agents/pi-embedded-runner/run/attempt.ts:167-186
const skillEntries = loadWorkspaceSkillEntries(effectiveWorkspace);

restoreSkillEnv = applySkillEnvOverrides({
  skills: skillEntries,
  config: params.config,
});

const skillsPrompt = resolveSkillsPromptForRun({
  skillsSnapshot: params.skillsSnapshot,
  entries: skillEntries,
  config: params.config,
  workspaceDir: effectiveWorkspace,
});
```

### 生成的提示词格式

```xml
<available_skills>
<skill name="peekaboo" location="/path/to/skills/peekaboo/SKILL.md">
  <description>Fast macOS screenshots with optional AI vision analysis.</description>
</skill>
<skill name="mcporter" location="/path/to/skills/mcporter/SKILL.md">
  <description>Use the mcporter CLI to list, configure, auth, and call MCP servers/tools directly.</description>
</skill>
</available_skills>
```

此提示词会被注入到系统提示词的 Skills 节段中（详见 [AGENT-RUNTIME-SYSTEM-PROMPT.md](./AGENT-RUNTIME-SYSTEM-PROMPT.md)），指导 LLM 在回复前扫描技能列表并按需使用。

---

## 七、技能同步到沙箱

当沙箱启用时，技能需要同步到沙箱工作目录，使 LLM 通过 `read` 工具能在沙箱内读取 SKILL.md 文件：

```typescript
// src/agents/skills/workspace.ts:287-325
export async function syncSkillsToWorkspace(params: {
  sourceWorkspaceDir: string;
  targetWorkspaceDir: string;
  config?: OpenClawConfig;
  managedSkillsDir?: string;
  bundledSkillsDir?: string;
}) {
  const sourceDir = resolveUserPath(params.sourceWorkspaceDir);
  const targetDir = resolveUserPath(params.targetWorkspaceDir);

  // 源和目标相同则跳过
  if (sourceDir === targetDir) {
    return;
  }

  // 序列化执行 (防止并发冲突)
  await serializeByKey(`syncSkills:${targetDir}`, async () => {
    const targetSkillsDir = path.join(targetDir, "skills");

    // 1. 加载源目录的技能
    const entries = loadSkillEntries(sourceDir, {
      config: params.config,
      managedSkillsDir: params.managedSkillsDir,
      bundledSkillsDir: params.bundledSkillsDir,
    });

    // 2. 清空目标目录
    await fsp.rm(targetSkillsDir, { recursive: true, force: true });
    await fsp.mkdir(targetSkillsDir, { recursive: true });

    // 3. 复制每个技能目录
    for (const entry of entries) {
      const dest = path.join(targetSkillsDir, entry.skill.name);
      try {
        await fsp.cp(entry.skill.baseDir, dest, {
          recursive: true,
          force: true,
        });
      } catch (error) {
        console.warn(`[skills] Failed to copy ${entry.skill.name} to sandbox`);
      }
    }
  });
}
```

### 同步流程

```text
主机工作目录
    │
    ├── skills/
    │   ├── skill-a/
    │   │   └── SKILL.md
    │   └── skill-b/
    │       └── SKILL.md
    │
    └── ~/.openclaw/skills/        (托管技能)
        └── skill-c/
            └── SKILL.md

        │
        │  syncSkillsToWorkspace()
        │
        ▼

沙箱工作目录 (/tmp/openclaw-sandbox-xxx/)
    │
    └── skills/                    (合并后的所有技能)
        ├── skill-a/
        │   └── SKILL.md
        ├── skill-b/
        │   └── SKILL.md
        └── skill-c/
            └── SKILL.md
```

**为什么要同步到沙箱？**

系统提示词中的 `<skill location="...">` 指向的文件路径需要对 LLM 的 `read` 工具可见。当沙箱启用且 `workspaceAccess` 不是 `rw` 时，`effectiveWorkspace` 会切换到沙箱目录，LLM 读取文件的操作实际发生在沙箱目录内。因此必须将技能文件预先同步到沙箱的 `skills/` 子目录。

---

## 八、技能命令规格

技能可以注册为用户命令 (如 `/peekaboo`)，使用户可以通过消息渠道直接触发技能：

```typescript
// src/agents/skills/workspace.ts:334-440
export function buildWorkspaceSkillCommandSpecs(
  workspaceDir: string,
  opts?: {
    config?: OpenClawConfig;
    entries?: SkillEntry[];
    reservedNames?: Set<string>;  // 保留的命令名
  },
): SkillCommandSpec[] {
  const skillEntries = opts?.entries ?? loadSkillEntries(workspaceDir, opts);
  const eligible = filterSkillEntries(skillEntries, opts?.config);

  // 只包含 userInvocable 的技能
  const userInvocable = eligible.filter(
    (entry) => entry.invocation?.userInvocable !== false
  );

  const specs: SkillCommandSpec[] = [];
  const used = new Set<string>(opts?.reservedNames ?? []);

  for (const entry of userInvocable) {
    // 1. 规范化命令名 (只允许 a-z0-9_)
    const base = sanitizeSkillCommandName(entry.skill.name);

    // 2. 解决命名冲突
    const unique = resolveUniqueSkillCommandName(base, used);
    used.add(unique.toLowerCase());

    // 3. 截断描述 (Discord 限制 100 字符)
    const description = truncateDescription(entry.skill.description);

    // 4. 解析 dispatch 配置 (tool 分发)
    const dispatch = resolveDispatch(entry.frontmatter);

    specs.push({
      name: unique,
      skillName: entry.skill.name,
      description,
      ...(dispatch ? { dispatch } : {}),
    });
  }

  return specs;
}
```

### 命令规格示例

```typescript
[
  {
    name: "peekaboo",
    skillName: "peekaboo",
    description: "Fast macOS screenshots with optional AI vision analysis.",
  },
  {
    name: "mcporter",
    skillName: "mcporter",
    description: "Use the mcporter CLI to list, configure, auth, and call MCP servers/tools directly.",
    dispatch: {
      kind: "tool",
      toolName: "exec",
      argMode: "raw",
    },
  },
]
```

### 调用策略控制

| 属性 | 说明 | 影响 |
| ---- | ---- | ---- |
| `userInvocable: true` | 可被用户通过 `/命令名` 调用 | 注册为命令 |
| `userInvocable: false` | 不可被用户直接调用 | 不注册为命令 |
| `disableModelInvocation: true` | 禁止 LLM 自动调用 | 不出现在系统提示词中 |
| `disableModelInvocation: false` | 允许 LLM 自动调用 | 出现在 `<available_skills>` 中 |

---

## 九、技能调用流程

技能在 OpenClaw 中有一个重要的设计特点：**技能不是工具 (Tool)**。LLM 不会像调用 `read`、`exec` 那样通过 tool_call 来"调用"一个技能。技能的调用本质上是一个 **Prompt 驱动的两阶段过程**。

### 9.1 LLM 自动调用流程（Prompt 驱动）

当用户发来一条消息后，LLM 在回复前会经历以下决策和执行流程：

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                   LLM 技能调用流程 (Prompt 驱动)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. 用户发送消息: "帮我查一下伦敦今天的天气"                                  │
│                                                                             │
│  2. LLM 收到系统提示词，其中包含:                                             │
│     ┌────────────────────────────────────────────────────────────────┐      │
│     │ ## Skills (mandatory)                                         │      │
│     │ Before replying: scan <available_skills> <description>.       │      │
│     │ - If exactly one skill clearly applies:                       │      │
│     │   read its SKILL.md at <location> with `read`, then follow.  │      │
│     │ - If multiple could apply: choose the most specific one.      │      │
│     │ - If none clearly apply: do not read any SKILL.md.            │      │
│     │                                                               │      │
│     │ <available_skills>                                            │      │
│     │ <skill name="weather"                                         │      │
│     │    location="/path/to/skills/weather/SKILL.md">               │      │
│     │   <description>Get current weather and forecasts</description>│      │
│     │ </skill>                                                      │      │
│     │ <skill name="peekaboo" ...>...</skill>                        │      │
│     │ </available_skills>                                           │      │
│     └────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  3. LLM 决策: "weather" 技能的描述匹配用户需求                               │
│                                                                             │
│  4. LLM 发出 tool_call: read 工具                                           │
│     ┌────────────────────────────────────────────────────────────────┐      │
│     │ { type: "toolCall",                                           │      │
│     │   name: "read",           ← 使用标准的 read 工具               │      │
│     │   arguments: {                                                │      │
│     │     path: "/path/to/skills/weather/SKILL.md"                  │      │
│     │   }                        ← 路径来自 <skill location="...">   │      │
│     │ }                                                             │      │
│     └────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  5. Runtime 执行 read 工具 → 返回 SKILL.md 完整内容                          │
│     ┌────────────────────────────────────────────────────────────────┐      │
│     │ ---                                                           │      │
│     │ name: weather                                                 │      │
│     │ description: Get current weather and forecasts                │      │
│     │ ---                                                           │      │
│     │                                                               │      │
│     │ # Weather                                                     │      │
│     │ Two free services, no API keys needed.                        │      │
│     │ ## wttr.in (primary)                                          │      │
│     │ ```bash                                                       │      │
│     │ curl -s "wttr.in/London?format=3"                             │      │
│     │ ```                                                           │      │
│     │ ...                                                           │      │
│     └────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  6. LLM 阅读 SKILL.md 内容，按其中的指示执行                                 │
│     ┌────────────────────────────────────────────────────────────────┐      │
│     │ { type: "toolCall",                                           │      │
│     │   name: "exec",           ← 按 SKILL.md 指示使用 exec 工具    │      │
│     │   arguments: {                                                │      │
│     │     command: "curl -s \"wttr.in/London?format=3\""             │      │
│     │   }                                                           │      │
│     │ }                                                             │      │
│     └────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  7. Runtime 执行 exec 工具 → 返回结果: "London: ⛅️ +8°C"                    │
│                                                                             │
│  8. LLM 基于结果生成最终回复                                                 │
│     → "伦敦今天的天气是多云，气温 8°C。"                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 关键要点

1. **技能 ≠ 工具**: 系统中没有名为 `weather` 或 `peekaboo` 的工具。技能是通过提示词告知 LLM 的一组"参考指南"。
2. **两阶段执行**: 第一阶段 LLM 用 `read` 工具读取 SKILL.md；第二阶段 LLM 根据文件内容使用其他工具（如 `exec`、`read`、`write` 等）完成任务。
3. **LLM 自主决策**: Runtime 不参与"是否使用技能"的决策，这完全由 LLM 基于系统提示词中的 `<available_skills>` 描述和用户输入自行判断。
4. **只读取一个**: 系统提示词明确要求 LLM"never read more than one skill up front"，避免不必要地消耗上下文窗口。

#### 提示词中的调用指令

```typescript
// src/agents/system-prompt.ts:27-36
return [
  "## Skills (mandatory)",
  "Before replying: scan <available_skills> <description> entries.",
  `- If exactly one skill clearly applies: read its SKILL.md at <location> with \`${readToolName}\`, then follow it.`,
  "- If multiple could apply: choose the most specific one, then read/follow it.",
  "- If none clearly apply: do not read any SKILL.md.",
  "Constraints: never read more than one skill up front; only read after selecting.",
  trimmed,  // ← <available_skills>...</available_skills> 内容
  "",
];
```

其中 `readToolName` 会根据实际可用的 read 工具名动态替换（通常是 `read` 或 `Read`）。

### 9.2 用户命令调用流程（命令分发）

用户也可以通过 `/命令名` 的方式主动触发技能，此时走的是另一套路径：

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                   用户命令调用流程                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  用户输入: "/weather London"                                                 │
│       │                                                                     │
│       ▼                                                                     │
│  resolveSkillCommandInvocation()                                             │
│  匹配命令名 → { command: "weather", args: "London" }                        │
│       │                                                                     │
│       ├─── dispatch 配置存在？                                               │
│       │     │                                                               │
│       │     ├── 是 (dispatch.kind === "tool")                                │
│       │     │   ┌────────────────────────────────────────────┐              │
│       │     │   │ 直接调用指定工具（跳过 LLM）                │              │
│       │     │   │                                            │              │
│       │     │   │ tool = tools.find(t => t.name === "exec")  │              │
│       │     │   │ result = tool.execute(toolCallId, {        │              │
│       │     │   │   command: "London",                       │              │
│       │     │   │   commandName: "weather",                  │              │
│       │     │   │   skillName: "weather",                    │              │
│       │     │   │ })                                         │              │
│       │     │   │                                            │              │
│       │     │   │ → 直接返回工具执行结果给用户                │              │
│       │     │   └────────────────────────────────────────────┘              │
│       │     │                                                               │
│       │     └── 否 (无 dispatch 配置，默认路径)                              │
│       │         ┌────────────────────────────────────────────┐              │
│       │         │ 改写消息体，引导 LLM 使用技能               │              │
│       │         │                                            │              │
│       │         │ rewrittenBody =                            │              │
│       │         │   'Use the "weather" skill for this        │              │
│       │         │    request.\n\nUser input:\nLondon'        │              │
│       │         │                                            │              │
│       │         │ ctx.Body = rewrittenBody                   │              │
│       │         │ → 继续正常的 LLM 交互流程                  │              │
│       │         │ → LLM 会读取 SKILL.md 并按指示操作          │              │
│       │         └────────────────────────────────────────────┘              │
│       │                                                                     │
│       └─── 命令未匹配 → 继续正常的消息处理流程                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**源文件**: `src/auto-reply/reply/get-reply-inline-actions.ts`, `src/auto-reply/skill-commands.ts`

#### 两种分发模式对比

| 模式 | 触发条件 | LLM 参与 | 执行方式 | 适用场景 |
| ---- | ---- | ---- | ---- | ---- |
| **默认分发** | 用户输入 `/skillname args` 且无 dispatch 配置 | 是 | 改写消息体 → LLM 读取 SKILL.md → 按指示执行 | 需要理解力的复杂技能 |
| **工具分发** | 用户输入 `/skillname args` 且有 `dispatch: { kind: "tool" }` | 否 | 直接调用指定工具，跳过 LLM | 简单的命令行包装 |

#### 默认分发的消息改写

```typescript
// src/auto-reply/reply/get-reply-inline-actions.ts:207-218
const promptParts = [
  `Use the "${skillInvocation.command.skillName}" skill for this request.`,
  skillInvocation.args ? `User input:\n${skillInvocation.args}` : null,
].filter((entry): entry is string => Boolean(entry));
const rewrittenBody = promptParts.join("\n\n");

// 替换所有消息体引用
ctx.Body = rewrittenBody;
ctx.BodyForAgent = rewrittenBody;
sessionCtx.Body = rewrittenBody;
sessionCtx.BodyForAgent = rewrittenBody;
sessionCtx.BodyStripped = rewrittenBody;
```

改写后，LLM 收到的消息变为：

```text
Use the "weather" skill for this request.

User input:
London
```

由于系统提示词中已有 Skills 调用指令，LLM 会识别到需要使用 "weather" 技能，自动读取对应的 SKILL.md 并执行。

### 9.3 SKILL.md 内容与产出处理

#### SKILL.md 是什么

每个技能的核心是一个 Markdown 文件，包含：

1. **YAML Frontmatter**: 元数据（名称、描述、依赖、平台要求等）
2. **Markdown Body**: 给 LLM 的详细使用说明

````markdown
---
name: weather
description: Get current weather and forecasts (no API key required).
homepage: https://wttr.in/:help
metadata: { "openclaw": { "emoji": "...", "requires": { "bins": ["curl"] } } }
---

# Weather

Two free services, no API keys needed.

## wttr.in (primary)

```bash
curl -s "wttr.in/London?format=3"
```

...
````

LLM 读取后，会将 Body 部分作为"操作手册"，按照其中的指示使用工具完成任务。

#### 产出的处理

技能的产出并非特殊的"技能输出"——它就是 LLM 在对话中正常产生的工具调用和文本回复。

```text
用户: "帮我查伦敦天气"
    │
    ▼
LLM: [tool_call: read → SKILL.md]                    ← 第一阶段：读取技能
    │
    ▼
LLM: [tool_call: exec → curl "wttr.in/London?f=3"]   ← 第二阶段：按技能指示执行
    │
    ▼
Runtime: 返回 "London: ⛅️ +8°C"                      ← 工具结果 (与普通工具执行相同)
    │
    ▼
LLM: "伦敦今天多云，气温 8°C。"                       ← 最终回复 (普通消息)
```

所有中间产出（工具调用、工具结果）都遵循标准的工具调用协议（详见 [AGENT-RUNTIME-TOOLS.md](./AGENT-RUNTIME-TOOLS.md)），经历相同的：

- **工具结果截断**: `TOOL_RESULT_MAX_CHARS = 8,000` 字符
- **事件通知**: `tool_execution_start` / `_update` / `_end`
- **上下文管理**: Soft Trim / Hard Clear / Auto Compaction（详见 [AGENT-RUNTIME-SYSTEM-PROMPT.md](./AGENT-RUNTIME-SYSTEM-PROMPT.md#六上下文管理与溢出处理)）
- **Session 持久化**: 写入 `.jsonl` session 文件

#### 与普通工具调用的区别

| 维度 | 普通工具调用 | 技能驱动的工具调用 |
| ---- | ---- | ---- |
| 决策来源 | LLM 直接决定使用哪个工具 | LLM 先读取 SKILL.md，再按指示选择工具 |
| 上下文消耗 | 工具定义在系统提示词中 | SKILL.md 内容会额外占用上下文窗口 |
| 执行路径 | 单步：LLM → 工具 → 结果 | 两步：LLM → read(SKILL.md) → LLM → 工具 → 结果 |
| 结果处理 | 标准工具结果流程 | 与标准工具结果流程完全相同 |
| 可追溯性 | tool_call 记录 | read(SKILL.md) + 后续 tool_call 记录均在 session 中 |

#### 调用策略控制的作用

Frontmatter 中的调用策略决定了技能在两个调用路径中的可见性：

```text
                        disableModelInvocation
                        ┌─── false (默认) ───────────────┐
                        │                                 │
                        │  出现在 <available_skills> 中    │
                        │  LLM 可自动识别并调用             │
                        │                                 │
                        └─────────────────────────────────┘
                        ┌─── true ────────────────────────┐
                        │                                 │
                        │  不出现在 <available_skills> 中   │
                        │  LLM 无法自主选择此技能           │
                        │  仅能通过用户 /命令 触发           │
                        │                                 │
                        └─────────────────────────────────┘

                        userInvocable
                        ┌─── true (默认) ────────────────┐
                        │                                 │
                        │  注册为 /命令                    │
                        │  用户可通过消息渠道直接触发       │
                        │                                 │
                        └─────────────────────────────────┘
                        ┌─── false ───────────────────────┐
                        │                                 │
                        │  不注册为 /命令                   │
                        │  仅由 LLM 自动调用               │
                        │                                 │
                        └─────────────────────────────────┘
```

---

## 技能系统架构总览

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Skill Management System                                                        │
│       │                                                                         │
│       ├── 1. 技能加载: loadSkillEntries()                                       │
│       │       │                                                                 │
│       │       ├── extraDirs (额外目录)                                          │
│       │       ├── pluginSkillDirs (插件技能)                                    │
│       │       ├── bundledSkillsDir (内置技能)                                   │
│       │       ├── managedSkillsDir (~/.openclaw/skills)                         │
│       │       └── workspaceSkillsDir (workspace/skills) ← 最高优先级            │
│       │                                                                         │
│       ├── 2. 技能过滤: shouldIncludeSkill()                                     │
│       │       │                                                                 │
│       │       ├── enabled 检查                                                  │
│       │       ├── allowBundled 白名单                                           │
│       │       ├── OS 兼容性                                                     │
│       │       ├── 必需二进制文件                                                │
│       │       ├── 必需环境变量                                                  │
│       │       └── 必需配置路径                                                  │
│       │                                                                         │
│       ├── 3. 环境变量注入: applySkillEnvOverrides()                             │
│       │       │                                                                 │
│       │       ├── skillConfig.env → process.env                                 │
│       │       └── skillConfig.apiKey → primaryEnv                               │
│       │                                                                         │
│       ├── 4. 提示词生成: buildWorkspaceSkillsPrompt()                           │
│       │       │                                                                 │
│       │       └── formatSkillsForPrompt() → <available_skills>...</>            │
│       │                                                                         │
│       ├── 5. 沙箱同步: syncSkillsToWorkspace()                                  │
│       │       │                                                                 │
│       │       └── 复制技能目录到沙箱 workspace/skills/                          │
│       │                                                                         │
│       └── 6. 命令注册: buildWorkspaceSkillCommandSpecs()                        │
│               │                                                                 │
│               └── 注册为用户可调用的 /命令                                      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 关键源文件

| 文件 | 行数 | 核心功能 |
| ---- | ---- | -------- |
| `src/agents/skills/workspace.ts` | ~441 | 技能加载、提示词生成、沙箱同步 |
| `src/agents/skills/config.ts` | ~192 | 技能过滤逻辑 |
| `src/agents/skills/env-overrides.ts` | ~90 | 技能环境变量注入 |
| `src/agents/skills/types.ts` | ~88 | 技能类型定义 |
| `src/agents/system-prompt.ts` | ~630 | Skills 节段注入系统提示词 |
| `src/auto-reply/reply/get-reply-inline-actions.ts` | ~380 | 用户命令调用分发逻辑 |
| `src/auto-reply/skill-commands.ts` | ~132 | 技能命令解析和匹配 |
| `src/auto-reply/commands-registry.ts` | ~169 | 技能命令注册为聊天命令 |
