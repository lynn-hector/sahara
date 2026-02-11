# 沙箱系统实现详解

> 本文档详细介绍 OpenClaw 沙箱系统的作用、架构和源码实现。

---

## 目录

1. [沙箱的作用](#一沙箱的作用)
2. [整体架构](#二整体架构)
3. [核心配置](#三核心配置)
4. [源码实现](#四源码实现)
5. [工具策略](#五工具策略)
6. [沙箱浏览器](#六沙箱浏览器)
7. [生命周期管理](#七生命周期管理)
8. [数据流动](#八数据流动)

---

## 一、沙箱的作用

### 1.1 核心目标

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           沙箱系统核心目标                                       │
└─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
  │      隔离       │     │      安全       │     │      可控       │
  │   Isolation     │     │    Security     │     │    Controlled   │
  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘
           │                       │                       │
           ▼                       ▼                       ▼
  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
  │ • 隔离不同会话  │     │ • 限制文件访问  │     │ • 工具策略控制  │
  │ • 隔离不同代理  │     │ • 限制网络访问  │     │ • 资源限制      │
  │ • 隔离主机系统  │     │ • 限制系统调用  │     │ • 可审计        │
  └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### 1.2 沙箱解决的问题

| 问题 | 沙箱解决方案 |
|------|-------------|
| **Agent 执行恶意命令** | 限制在容器内执行，无法影响主机 |
| **会话间数据泄露** | 每个会话独立工作目录 |
| **网络攻击** | 默认 `network: none`，无网络访问 |
| **文件系统破坏** | 只读根文件系统 + 工作目录隔离 |
| **资源耗尽** | CPU/内存/进程数限制 |
| **敏感工具滥用** | 工具策略 allow/deny |

### 1.3 何时启用沙箱

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           沙箱启用模式 (mode)                                    │
└─────────────────────────────────────────────────────────────────────────────────┘

  配置: agents.defaults.sandbox.mode

  ┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
  │    "off"    │     │   "non-main"    │     │    "all"    │
  │   (默认)    │     │    (推荐)       │     │   (最严格)  │
  └──────┬──────┘     └────────┬────────┘     └──────┬──────┘
         │                     │                     │
         ▼                     ▼                     ▼
  ┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
  │ 所有会话    │     │ 主会话不沙箱    │     │ 所有会话    │
  │ 都不沙箱    │     │ 非主会话沙箱    │     │ 都沙箱      │
  └─────────────┘     └─────────────────┘     └─────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ 适合场景:       │
                    │ • 多用户共享    │
                    │ • Telegram 群组 │
                    │ • Discord 服务器│
                    └─────────────────┘
```

---

## 二、整体架构

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           沙箱系统架构                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

                              用户请求
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        resolveSandboxContext()                                  │
│                      (src/agents/sandbox/context.ts)                            │
│                                                                                 │
│  1. 检查是否应该沙箱化 (resolveSandboxRuntimeStatus)                            │
│  2. 解析沙箱配置 (resolveSandboxConfigForAgent)                                 │
│  3. 清理过期沙箱 (maybePruneSandboxes)                                          │
│  4. 准备工作空间 (ensureSandboxWorkspace)                                       │
│  5. 确保 Docker 容器运行 (ensureSandboxContainer)                               │
│  6. 确保沙箱浏览器运行 (ensureSandboxBrowser)                                   │
└────────────────────────────────┬────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          SandboxContext 返回                                    │
│                                                                                 │
│  {                                                                              │
│    enabled: true,                                                               │
│    sessionKey: "main:telegram:123456",                                          │
│    workspaceDir: "/home/user/.openclaw/sandboxes/abc123/",                      │
│    agentWorkspaceDir: "/home/user/workspace/",                                  │
│    workspaceAccess: "ro",                                                       │
│    containerName: "openclaw-sbx-abc123",                                        │
│    containerWorkdir: "/workspace",                                              │
│    docker: { ... },                                                             │
│    tools: { allow: [...], deny: [...] },                                        │
│    browser: { bridgeUrl, noVncUrl, containerName },                             │
│  }                                                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Docker 容器 (运行中)                                   │
│                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │                    openclaw-sbx-{slug}                                    │ │
│  │                                                                           │ │
│  │  Image: openclaw-sandbox:bookworm-slim (基于 debian:bookworm-slim)        │ │
│  │                                                                           │ │
│  │  /workspace           ← 沙箱工作目录 (挂载)                               │ │
│  │  /agent               ← Agent 工作空间 (可选挂载)                         │ │
│  │  /tmp, /var/tmp, /run ← tmpfs (内存文件系统)                              │ │
│  │                                                                           │ │
│  │  安全限制:                                                                │ │
│  │  • --read-only (只读根文件系统)                                           │ │
│  │  • --network none (无网络)                                                │ │
│  │  • --cap-drop ALL (移除所有 capabilities)                                 │ │
│  │  • --security-opt no-new-privileges                                       │ │
│  │  • --pids-limit, --memory, --cpus (资源限制)                              │ │
│  │                                                                           │ │
│  │  进程: sleep infinity (保持容器运行)                                      │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │                 openclaw-sbx-browser-{slug} (可选)                        │ │
│  │                                                                           │ │
│  │  Image: openclaw-sandbox-browser:bookworm-slim                            │ │
│  │                                                                           │ │
│  │  • Chromium 浏览器                                                        │ │
│  │  • CDP 端口 (远程调试)                                                    │ │
│  │  • VNC/noVNC (可视化访问)                                                 │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 文件结构

```
src/agents/sandbox/
├── context.ts          # 入口: resolveSandboxContext()
├── config.ts           # 配置解析
├── types.ts            # 类型定义
├── constants.ts        # 常量定义
├── docker.ts           # Docker 容器管理
├── browser.ts          # 沙箱浏览器管理
├── workspace.ts        # 工作空间管理
├── runtime-status.ts   # 运行时状态判断
├── tool-policy.ts      # 工具策略
├── prune.ts            # 容器清理
├── registry.ts         # 容器注册表
├── shared.ts           # 共享工具函数
├── config-hash.ts      # 配置哈希 (检测变更)
└── browser-bridges.ts  # 浏览器桥接管理
```

---

## 三、核心配置

### 3.1 配置结构

```typescript
// src/agents/sandbox/types.ts:51-60
type SandboxConfig = {
  mode: "off" | "non-main" | "all";     // 启用模式
  scope: "session" | "agent" | "shared"; // 容器粒度
  workspaceAccess: "none" | "ro" | "rw"; // 工作空间访问
  workspaceRoot: string;                 // 沙箱工作空间根目录
  docker: SandboxDockerConfig;           // Docker 配置
  browser: SandboxBrowserConfig;         // 浏览器配置
  tools: SandboxToolPolicy;              // 工具策略
  prune: SandboxPruneConfig;             // 清理策略
};
```

### 3.2 配置选项详解

#### mode (启用模式)

| 值 | 描述 | 适用场景 |
|---|------|---------|
| `"off"` | 禁用沙箱 (默认) | 个人使用，完全信任 |
| `"non-main"` | 仅非主会话沙箱化 | 多用户共享，主用户有完整权限 |
| `"all"` | 所有会话沙箱化 | 最高安全性 |

#### scope (容器粒度)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           容器粒度 (scope)                                       │
└─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
  │    "session"    │     │    "agent"      │     │    "shared"     │
  │   (每会话一个)  │     │  (每代理一个)   │     │   (全局共享)    │
  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘
           │                       │                       │
           ▼                       ▼                       ▼
  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
  │ Session A       │     │ Agent "main"    │     │                 │
  │ └─ Container A  │     │ └─ Container A  │     │   Container     │
  │                 │     │                 │     │   (shared)      │
  │ Session B       │     │ Agent "work"    │     │                 │
  │ └─ Container B  │     │ └─ Container B  │     │   所有会话共用  │
  │                 │     │                 │     │                 │
  │ 完全隔离        │     │ 同代理共享      │     │   最少隔离      │
  │ 资源最多        │     │ 适中            │     │   资源最少      │
  └─────────────────┘     └─────────────────┘     └─────────────────┘
```

#### workspaceAccess (工作空间访问)

| 值 | 描述 | 挂载方式 |
|---|------|---------|
| `"none"` | 沙箱独立工作目录 | 只挂载沙箱目录 |
| `"ro"` | 只读访问主工作空间 | 主工作空间 `:ro` 挂载 |
| `"rw"` | 读写访问主工作空间 | 直接使用主工作空间 |

### 3.3 Docker 配置

```typescript
// src/agents/sandbox/types.docker.ts
type SandboxDockerConfig = {
  image: string;              // Docker 镜像
  containerPrefix: string;    // 容器名前缀
  workdir: string;            // 容器内工作目录
  readOnlyRoot: boolean;      // 只读根文件系统
  tmpfs: string[];            // tmpfs 挂载点
  network: string;            // 网络模式
  user?: string;              // 运行用户
  capDrop: string[];          // 移除的 capabilities
  env: Record<string, string>;// 环境变量
  setupCommand?: string;      // 初始化命令
  pidsLimit?: number;         // 进程数限制
  memory?: string;            // 内存限制
  memorySwap?: string;        // 交换内存限制
  cpus?: number;              // CPU 限制
  ulimits?: Record<string, ...>; // ulimit 限制
  seccompProfile?: string;    // seccomp 配置
  apparmorProfile?: string;   // apparmor 配置
  dns?: string[];             // DNS 服务器
  extraHosts?: string[];      // 额外 hosts
  binds?: string[];           // 额外挂载
};
```

### 3.4 默认值

```typescript
// src/agents/sandbox/constants.ts
export const DEFAULT_SANDBOX_IMAGE = "openclaw-sandbox:bookworm-slim";
export const DEFAULT_SANDBOX_CONTAINER_PREFIX = "openclaw-sbx-";
export const DEFAULT_SANDBOX_WORKDIR = "/workspace";
export const DEFAULT_SANDBOX_IDLE_HOURS = 24;      // 24小时空闲后清理
export const DEFAULT_SANDBOX_MAX_AGE_DAYS = 7;     // 7天后强制清理
export const DEFAULT_SANDBOX_WORKSPACE_ROOT = "~/.openclaw/sandboxes";
```

---

## 四、源码实现

### 4.1 入口函数 resolveSandboxContext

```typescript
// src/agents/sandbox/context.ts:17-99

export async function resolveSandboxContext(params: {
  config?: OpenClawConfig;
  sessionKey?: string;
  workspaceDir?: string;
}): Promise<SandboxContext | null> {
  
  // ═══════════════════════════════════════════════════════════════════════════
  // Step 1: 检查是否需要沙箱化
  // ═══════════════════════════════════════════════════════════════════════════
  const rawSessionKey = params.sessionKey?.trim();
  if (!rawSessionKey) {
    return null;  // 无会话键，不沙箱
  }

  const runtime = resolveSandboxRuntimeStatus({
    cfg: params.config,
    sessionKey: rawSessionKey,
  });
  if (!runtime.sandboxed) {
    return null;  // 不需要沙箱化
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Step 2: 解析沙箱配置
  // ═══════════════════════════════════════════════════════════════════════════
  const cfg = resolveSandboxConfigForAgent(params.config, runtime.agentId);

  // ═══════════════════════════════════════════════════════════════════════════
  // Step 3: 清理过期沙箱 (每5分钟最多执行一次)
  // ═══════════════════════════════════════════════════════════════════════════
  await maybePruneSandboxes(cfg);

  // ═══════════════════════════════════════════════════════════════════════════
  // Step 4: 确定工作目录
  // ═══════════════════════════════════════════════════════════════════════════
  const agentWorkspaceDir = resolveUserPath(
    params.workspaceDir?.trim() || DEFAULT_AGENT_WORKSPACE_DIR,
  );
  const workspaceRoot = resolveUserPath(cfg.workspaceRoot);
  const scopeKey = resolveSandboxScopeKey(cfg.scope, rawSessionKey);
  const sandboxWorkspaceDir =
    cfg.scope === "shared" 
      ? workspaceRoot 
      : resolveSandboxWorkspaceDir(workspaceRoot, scopeKey);
  
  // 根据 workspaceAccess 决定实际工作目录
  const workspaceDir = cfg.workspaceAccess === "rw" 
    ? agentWorkspaceDir      // 读写: 直接用主工作空间
    : sandboxWorkspaceDir;   // 其他: 用沙箱目录

  // ═══════════════════════════════════════════════════════════════════════════
  // Step 5: 准备沙箱工作空间
  // ═══════════════════════════════════════════════════════════════════════════
  if (workspaceDir === sandboxWorkspaceDir) {
    await ensureSandboxWorkspace(
      sandboxWorkspaceDir,
      agentWorkspaceDir,  // 从主工作空间复制配置文件
      params.config?.agents?.defaults?.skipBootstrap,
    );
    // 同步技能到沙箱
    if (cfg.workspaceAccess !== "rw") {
      await syncSkillsToWorkspace({
        sourceWorkspaceDir: agentWorkspaceDir,
        targetWorkspaceDir: sandboxWorkspaceDir,
        config: params.config,
      });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Step 6: 确保 Docker 容器运行
  // ═══════════════════════════════════════════════════════════════════════════
  const containerName = await ensureSandboxContainer({
    sessionKey: rawSessionKey,
    workspaceDir,
    agentWorkspaceDir,
    cfg,
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // Step 7: 确保沙箱浏览器运行 (如果启用)
  // ═══════════════════════════════════════════════════════════════════════════
  const browser = await ensureSandboxBrowser({
    scopeKey,
    workspaceDir,
    agentWorkspaceDir,
    cfg,
    evaluateEnabled,
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // Step 8: 返回沙箱上下文
  // ═══════════════════════════════════════════════════════════════════════════
  return {
    enabled: true,
    sessionKey: rawSessionKey,
    workspaceDir,
    agentWorkspaceDir,
    workspaceAccess: cfg.workspaceAccess,
    containerName,
    containerWorkdir: cfg.docker.workdir,
    docker: cfg.docker,
    tools: cfg.tools,
    browserAllowHostControl: cfg.browser.allowHostControl,
    browser: browser ?? undefined,
  };
}
```

### 4.2 Docker 容器创建

```typescript
// src/agents/sandbox/docker.ts:208-245

async function createSandboxContainer(params: {
  name: string;
  cfg: SandboxDockerConfig;
  workspaceDir: string;
  workspaceAccess: SandboxWorkspaceAccess;
  agentWorkspaceDir: string;
  scopeKey: string;
  configHash?: string;
}) {
  const { name, cfg, workspaceDir, scopeKey } = params;
  
  // 确保镜像存在
  await ensureDockerImage(cfg.image);

  // 构建 docker create 参数
  const args = buildSandboxCreateArgs({
    name,
    cfg,
    scopeKey,
    configHash: params.configHash,
  });
  
  // 设置工作目录
  args.push("--workdir", cfg.workdir);
  
  // 挂载工作空间
  const mainMountSuffix =
    params.workspaceAccess === "ro" && workspaceDir === params.agentWorkspaceDir 
      ? ":ro" 
      : "";
  args.push("-v", `${workspaceDir}:${cfg.workdir}${mainMountSuffix}`);
  
  // 可选: 挂载 Agent 工作空间到 /agent
  if (params.workspaceAccess !== "none" && workspaceDir !== params.agentWorkspaceDir) {
    const agentMountSuffix = params.workspaceAccess === "ro" ? ":ro" : "";
    args.push(
      "-v",
      `${params.agentWorkspaceDir}:${SANDBOX_AGENT_WORKSPACE_MOUNT}${agentMountSuffix}`,
    );
  }
  
  // 镜像和命令
  args.push(cfg.image, "sleep", "infinity");

  // 创建并启动容器
  await execDocker(args);
  await execDocker(["start", name]);

  // 运行初始化命令 (如果配置)
  if (cfg.setupCommand?.trim()) {
    await execDocker(["exec", "-i", name, "sh", "-lc", cfg.setupCommand]);
  }
}
```

### 4.3 Docker 创建参数构建

```typescript
// src/agents/sandbox/docker.ts:125-206

export function buildSandboxCreateArgs(params: {
  name: string;
  cfg: SandboxDockerConfig;
  scopeKey: string;
  createdAtMs?: number;
  labels?: Record<string, string>;
  configHash?: string;
}) {
  const args = ["create", "--name", params.name];
  
  // ─────────────────────────────────────────────────────────────────────────
  // 标签 (用于管理和清理)
  // ─────────────────────────────────────────────────────────────────────────
  args.push("--label", "openclaw.sandbox=1");
  args.push("--label", `openclaw.sessionKey=${params.scopeKey}`);
  args.push("--label", `openclaw.createdAtMs=${createdAtMs}`);
  if (params.configHash) {
    args.push("--label", `openclaw.configHash=${params.configHash}`);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 文件系统安全
  // ─────────────────────────────────────────────────────────────────────────
  if (params.cfg.readOnlyRoot) {
    args.push("--read-only");  // 只读根文件系统
  }
  for (const entry of params.cfg.tmpfs) {
    args.push("--tmpfs", entry);  // tmpfs 挂载 (可写的内存文件系统)
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 网络安全
  // ─────────────────────────────────────────────────────────────────────────
  if (params.cfg.network) {
    args.push("--network", params.cfg.network);  // 默认 "none"
  }
  for (const entry of params.cfg.dns ?? []) {
    args.push("--dns", entry);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 用户和权限
  // ─────────────────────────────────────────────────────────────────────────
  if (params.cfg.user) {
    args.push("--user", params.cfg.user);
  }
  for (const cap of params.cfg.capDrop) {
    args.push("--cap-drop", cap);  // 默认 ["ALL"]
  }
  args.push("--security-opt", "no-new-privileges");  // 禁止提权
  
  // seccomp 和 apparmor
  if (params.cfg.seccompProfile) {
    args.push("--security-opt", `seccomp=${params.cfg.seccompProfile}`);
  }
  if (params.cfg.apparmorProfile) {
    args.push("--security-opt", `apparmor=${params.cfg.apparmorProfile}`);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 资源限制
  // ─────────────────────────────────────────────────────────────────────────
  if (typeof params.cfg.pidsLimit === "number" && params.cfg.pidsLimit > 0) {
    args.push("--pids-limit", String(params.cfg.pidsLimit));
  }
  if (params.cfg.memory) {
    args.push("--memory", params.cfg.memory);
  }
  if (params.cfg.memorySwap) {
    args.push("--memory-swap", params.cfg.memorySwap);
  }
  if (typeof params.cfg.cpus === "number" && params.cfg.cpus > 0) {
    args.push("--cpus", String(params.cfg.cpus));
  }
  
  // ulimits
  for (const [name, value] of Object.entries(params.cfg.ulimits ?? {})) {
    const formatted = formatUlimitValue(name, value);
    if (formatted) {
      args.push("--ulimit", formatted);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 额外挂载
  // ─────────────────────────────────────────────────────────────────────────
  if (params.cfg.binds?.length) {
    for (const bind of params.cfg.binds) {
      args.push("-v", bind);
    }
  }

  return args;
}
```

### 4.4 运行时状态判断

```typescript
// src/agents/sandbox/runtime-status.ts:10-18

function shouldSandboxSession(cfg: SandboxConfig, sessionKey: string, mainSessionKey: string) {
  // mode = "off" → 永不沙箱
  if (cfg.mode === "off") {
    return false;
  }
  
  // mode = "all" → 全部沙箱
  if (cfg.mode === "all") {
    return true;
  }
  
  // mode = "non-main" → 只有非主会话沙箱
  return sessionKey.trim() !== mainSessionKey.trim();
}
```

---

## 五、工具策略

### 5.1 默认策略

```typescript
// src/agents/sandbox/constants.ts:14-37

// 沙箱内允许的工具
export const DEFAULT_TOOL_ALLOW = [
  "exec",           // 执行命令 (在沙箱内)
  "process",        // 进程管理
  "read",           // 读文件
  "write",          // 写文件
  "edit",           // 编辑文件
  "apply_patch",    // 应用补丁
  "image",          // 图像生成
  "sessions_list",  // 列出会话
  "sessions_history", // 会话历史
  "sessions_send",  // 发送到会话
  "sessions_spawn", // 创建子会话
  "session_status", // 会话状态
] as const;

// 沙箱内禁止的工具
export const DEFAULT_TOOL_DENY = [
  "browser",        // 浏览器控制 (除非启用沙箱浏览器)
  "canvas",         // 画布
  "nodes",          // 节点控制
  "cron",           // 定时任务
  "gateway",        // 网关调用
  ...CHANNEL_IDS,   // 所有消息渠道 (telegram, discord, slack, ...)
] as const;
```

### 5.2 策略检查流程

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          工具策略检查流程                                        │
└─────────────────────────────────────────────────────────────────────────────────┘

  Agent 调用工具
         │
         ▼
  ┌─────────────────┐
  │ 是否在沙箱中？  │
  └────────┬────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
   [是]        [否]
     │           │
     │           └──────────────┐
     ▼                          ▼
  ┌─────────────────┐     ┌─────────────────┐
  │ 检查 allow 列表 │     │ 允许执行        │
  └────────┬────────┘     └─────────────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
   [在]        [不在]
     │           │
     │           └──────────────┐
     ▼                          ▼
  ┌─────────────────┐     ┌─────────────────┐
  │ 检查 deny 列表  │     │ 阻止执行        │
  └────────┬────────┘     │ (返回错误)      │
           │              └─────────────────┘
     ┌─────┴─────┐
     ▼           ▼
   [不在]      [在]
     │           │
     ▼           └──────────────┐
  ┌─────────────────┐           ▼
  │ 允许执行        │     ┌─────────────────┐
  │ (在沙箱内)      │     │ 阻止执行        │
  └─────────────────┘     │ (返回错误)      │
                          └─────────────────┘
```

---

## 六、沙箱浏览器

### 6.1 架构

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          沙箱浏览器架构                                          │
└─────────────────────────────────────────────────────────────────────────────────┘

  Agent (工具调用: browser)
         │
         ▼
  ┌─────────────────┐
  │  Browser Tool   │
  │                 │
  │  使用 CDP 协议  │
  └────────┬────────┘
           │
           │ HTTP (CDP)
           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │                    Browser Bridge Server                        │
  │                    (127.0.0.1:随机端口)                         │
  └────────────────────────────┬────────────────────────────────────┘
                               │
                               │ HTTP (CDP)
                               ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │            Docker: openclaw-sbx-browser-{slug}                  │
  │                                                                 │
  │  ┌───────────────────────────────────────────────────────────┐ │
  │  │                     Chromium                              │ │
  │  │                                                           │ │
  │  │  CDP 端口: 9222 (映射到主机随机端口)                      │ │
  │  │  VNC 端口: 5900                                           │ │
  │  │  noVNC 端口: 6080 (Web 可视化)                            │ │
  │  └───────────────────────────────────────────────────────────┘ │
  │                                                                 │
  │  工作空间挂载: /workspace                                      │
  │  Agent 目录: /agent (可选)                                     │
  └─────────────────────────────────────────────────────────────────┘
```

### 6.2 配置选项

```typescript
// src/agents/sandbox/types.ts:30-42
type SandboxBrowserConfig = {
  enabled: boolean;           // 是否启用
  image: string;              // 浏览器镜像
  containerPrefix: string;    // 容器名前缀
  cdpPort: number;            // CDP 端口 (默认 9222)
  vncPort: number;            // VNC 端口 (默认 5900)
  noVncPort: number;          // noVNC 端口 (默认 6080)
  headless: boolean;          // 无头模式
  enableNoVnc: boolean;       // 启用 noVNC
  allowHostControl: boolean;  // 允许控制主机浏览器
  autoStart: boolean;         // 自动启动
  autoStartTimeoutMs: number; // 启动超时
};
```

---

## 七、生命周期管理

### 7.1 容器生命周期

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          容器生命周期                                            │
└─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐
  │   首次请求      │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  ensureSandboxContainer()                                       │
  │                                                                 │
  │  1. 检查容器是否存在                                            │
  │  2. 检查配置哈希是否匹配                                        │
  │     - 匹配: 复用容器                                            │
  │     - 不匹配 + 空闲: 删除并重建                                 │
  │     - 不匹配 + 活跃: 提示手动重建                               │
  │  3. 如果不存在: 创建新容器                                      │
  │  4. 如果存在但停止: 启动容器                                    │
  │  5. 更新注册表 (lastUsedAtMs)                                   │
  └────────────────────────────┬────────────────────────────────────┘
                               │
                               ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │                      容器运行中                                 │
  │                                                                 │
  │  • 执行工具命令 (docker exec)                                   │
  │  • 每次使用更新 lastUsedAtMs                                    │
  └────────────────────────────┬────────────────────────────────────┘
                               │
                               ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │  maybePruneSandboxes() (每5分钟检查)                            │
  │                                                                 │
  │  清理条件 (满足任一):                                           │
  │  • 空闲超过 idleHours (默认 24 小时)                            │
  │  • 存在超过 maxAgeDays (默认 7 天)                              │
  │                                                                 │
  │  清理动作:                                                      │
  │  • docker rm -f {containerName}                                 │
  │  • 从注册表删除条目                                             │
  └─────────────────────────────────────────────────────────────────┘
```

### 7.2 注册表管理

```typescript
// ~/.openclaw/sandbox/containers.json
{
  "entries": [
    {
      "containerName": "openclaw-sbx-abc123",
      "sessionKey": "main:telegram:123456",
      "createdAtMs": 1704067200000,
      "lastUsedAtMs": 1704153600000,
      "image": "openclaw-sandbox:bookworm-slim",
      "configHash": "sha256:abc..."
    },
    // ...
  ]
}
```

---

## 八、数据流动

### 8.1 什么文件会放进沙箱

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           放入沙箱的文件                                         │
└─────────────────────────────────────────────────────────────────────────────────┘

主工作空间 (~/.openclaw/workspace/)          沙箱工作空间 (~/.openclaw/sandboxes/{slug}/)
┌─────────────────────────────────────┐     ┌─────────────────────────────────────┐
│                                     │     │                                     │
│  📄 AGENTS.md      ─────────────────┼─────▶  📄 AGENTS.md                        │
│  📄 SOUL.md        ─────────────────┼─────▶  📄 SOUL.md                          │
│  📄 TOOLS.md       ─────────────────┼─────▶  📄 TOOLS.md                         │
│  📄 IDENTITY.md    ─────────────────┼─────▶  📄 IDENTITY.md                      │
│  📄 USER.md        ─────────────────┼─────▶  📄 USER.md                          │
│  📄 BOOTSTRAP.md   ─────────────────┼─────▶  📄 BOOTSTRAP.md                     │
│  📄 HEARTBEAT.md   ─────────────────┼─────▶  📄 HEARTBEAT.md                     │
│                                     │     │                                     │
│  📁 skills/        ─────────────────┼─────▶  📁 skills/  (完整复制)              │
│    └── skill-a/                     │     │    └── skill-a/                     │
│    └── skill-b/                     │     │    └── skill-b/                     │
│                                     │     │                                     │
│  📁 media/inbound/ ─────────────────┼─────▶  📁 media/inbound/                   │
│    └── image.png                    │     │    └── image.png                    │
│                                     │     │                                     │
└─────────────────────────────────────┘     └─────────────────────────────────────┘

复制的文件 (首次初始化时):
• AGENTS.md, SOUL.md, TOOLS.md, IDENTITY.md, USER.md, BOOTSTRAP.md, HEARTBEAT.md

同步的目录 (每次运行时):
• skills/ - 完全同步 (先删除再复制)
```

### 8.1.1 为什么需要复制这些文件？

这是 OpenClaw 的**深度隔离设计**：当沙箱启用时，Agent Runtime **整体切换**到沙箱目录工作。

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           深度隔离设计                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

  非沙箱模式                                    沙箱模式 (workspaceAccess = none/ro)
  
  effectiveWorkspace                            effectiveWorkspace
  = ~/.openclaw/workspace/                      = ~/.openclaw/sandboxes/{slug}/
         │                                               │
         ▼                                               ▼
  ┌─────────────────────────────────────┐     ┌─────────────────────────────────────┐
  │  Agent Runtime 从这里读取:           │     │  Agent Runtime 从这里读取:           │
  │                                     │     │                                     │
  │  • loadWorkspaceSkillEntries()      │     │  • loadWorkspaceSkillEntries()      │
  │    → skills/                        │     │    → skills/ (必须存在!)             │
  │                                     │     │                                     │
  │  • resolveBootstrapContextForRun()  │     │  • resolveBootstrapContextForRun()  │
  │    → AGENTS.md, SOUL.md, ...        │     │    → AGENTS.md, SOUL.md, ... (必须!) │
  │                                     │     │                                     │
  │  • buildEmbeddedSystemPrompt()      │     │  • buildEmbeddedSystemPrompt()      │
  │    → 构建系统提示                   │     │    → 构建系统提示                   │
  └─────────────────────────────────────┘     └─────────────────────────────────────┘
  
                                               如果不复制，这些文件就不存在，
                                               Agent 就无法正常工作！
```

**核心源码**:

```typescript
// src/agents/pi-embedded-runner/run/attempt.ts:157-170
const effectiveWorkspace = sandbox?.enabled
  ? sandbox.workspaceAccess === "rw"
    ? resolvedWorkspace
    : sandbox.workspaceDir  // ← 当沙箱启用时，使用沙箱目录！
  : resolvedWorkspace;

process.chdir(effectiveWorkspace);  // ★ 切换工作目录到沙箱！

const skillEntries = shouldLoadSkillEntries
  ? loadWorkspaceSkillEntries(effectiveWorkspace)  // ★ 从沙箱目录加载技能
  : [];
```

**安全优势**:

| 特性 | 说明 |
|------|------|
| **主动隔离** | Agent 完全在沙箱环境中运行，无法访问主工作空间的其他文件 |
| **选择性复制** | 只复制已知的配置文件，敏感文件 (.env, secrets) 不会被复制 |
| **路径限制** | 即使 Agent 尝试 `read("../../secrets.txt")`，也会被 `assertSandboxPath` 阻止 |

**源码位置**:

```typescript
// src/agents/sandbox/workspace.ts:15-51
export async function ensureSandboxWorkspace(
  workspaceDir: string,
  seedFrom?: string,
  skipBootstrap?: boolean,
) {
  await fs.mkdir(workspaceDir, { recursive: true });
  if (seedFrom) {
    const seed = resolveUserPath(seedFrom);
    const files = [
      DEFAULT_AGENTS_FILENAME,    // AGENTS.md
      DEFAULT_SOUL_FILENAME,      // SOUL.md
      DEFAULT_TOOLS_FILENAME,     // TOOLS.md
      DEFAULT_IDENTITY_FILENAME,  // IDENTITY.md
      DEFAULT_USER_FILENAME,      // USER.md
      DEFAULT_BOOTSTRAP_FILENAME, // BOOTSTRAP.md
      DEFAULT_HEARTBEAT_FILENAME, // HEARTBEAT.md
    ];
    for (const name of files) {
      const src = path.join(seed, name);
      const dest = path.join(workspaceDir, name);
      try {
        await fs.access(dest);
      } catch {
        try {
          const content = await fs.readFile(src, "utf-8");
          await fs.writeFile(dest, content, { encoding: "utf-8", flag: "wx" });
        } catch {
          // ignore missing seed file
        }
      }
    }
  }
}
```

### 8.2 什么时候放进去

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           文件放入时机                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

用户发送消息
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  runEmbeddedAttempt()                                                           │
│                                                                                 │
│  1. resolveSandboxContext()                                                     │
│     │                                                                           │
│     ├── ensureSandboxWorkspace()  ← ★ 复制配置文件 (首次或不存在时)             │
│     │   │                                                                       │
│     │   └── 复制: AGENTS.md, SOUL.md, TOOLS.md, IDENTITY.md,                    │
│     │             USER.md, BOOTSTRAP.md, HEARTBEAT.md                           │
│     │                                                                           │
│     └── syncSkillsToWorkspace()   ← ★ 同步技能 (每次都执行)                     │
│         │                                                                       │
│         └── 1. 删除目标 skills/                                                 │
│             2. 创建空的 skills/                                                 │
│             3. 复制每个技能目录                                                 │
│                                                                                 │
│  2. ensureSandboxContainer()       ← 确保 Docker 容器运行                       │
│     │                                                                           │
│     └── 挂载: 沙箱工作目录 → /workspace                                         │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**源码位置**:

```typescript
// src/agents/sandbox/context.ts:47-64
if (workspaceDir === sandboxWorkspaceDir) {
  // ★ Step 1: 复制配置文件
  await ensureSandboxWorkspace(
    sandboxWorkspaceDir,
    agentWorkspaceDir,  // seedFrom: 从主工作空间复制
    params.config?.agents?.defaults?.skipBootstrap,
  );
  
  // ★ Step 2: 同步技能
  if (cfg.workspaceAccess !== "rw") {
    await syncSkillsToWorkspace({
      sourceWorkspaceDir: agentWorkspaceDir,
      targetWorkspaceDir: sandboxWorkspaceDir,
      config: params.config,
    });
  }
}
```

### 8.3 沙箱里执行什么操作

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           沙箱内执行的操作                                       │
└─────────────────────────────────────────────────────────────────────────────────┘

LLM 调用工具
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  工具执行方式                                                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  exec 工具 (命令执行)                                                   │   │
│  │                                                                         │   │
│  │  主机执行:                                                              │   │
│  │    child_process.spawn("sh", ["-c", command])                           │   │
│  │                                                                         │   │
│  │  沙箱执行:                                                              │   │
│  │    docker exec -i -w /workspace                                         │   │
│  │           -e PATH=... -e HOME=/workspace                                │   │
│  │           openclaw-sbx-{slug}                                           │   │
│  │           sh -lc "command"                                              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  read/write/edit 工具 (文件操作)                                        │   │
│  │                                                                         │   │
│  │  直接操作挂载的文件:                                                    │   │
│  │  • 主机路径: ~/.openclaw/sandboxes/{slug}/file.txt                      │   │
│  │  • 容器路径: /workspace/file.txt                                        │   │
│  │                                                                         │   │
│  │  文件读写通过主机 Node.js fs 模块，不需要 docker exec                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**exec 工具源码**:

```typescript
// src/agents/bash-tools.shared.ts:51-82
export function buildDockerExecArgs(params: {
  containerName: string;
  command: string;
  workdir?: string;
  env: Record<string, string>;
  tty: boolean;
}) {
  const args = ["exec", "-i"];
  if (params.tty) {
    args.push("-t");
  }
  if (params.workdir) {
    args.push("-w", params.workdir);  // 工作目录
  }
  for (const [key, value] of Object.entries(params.env)) {
    args.push("-e", `${key}=${value}`);  // 环境变量
  }
  // 在容器内执行命令
  args.push(params.containerName, "sh", "-lc", `${pathExport}${params.command}`);
  return args;
}
```

### 8.3.1 工具执行位置详解

**关键区别**: 不同工具在不同位置执行！

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     LLM 工具的执行位置                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

                           主机 (Agent Runtime 运行的地方)
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│  沙箱目录: ~/.openclaw/sandboxes/{slug}/                                        │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │  file.txt                                                       │           │
│  │  output.json                                                    │           │
│  │  script.py                                                      │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│                           ▲                           │                        │
│                           │                           │                        │
│  ┌────────────────────────┴─────────────┐             │ (挂载 -v)              │
│  │  read/write/edit 工具                │             ▼                        │
│  │                                      │  ┌───────────────────────────────┐   │
│  │  Node.js fs.readFile()               │  │  Docker 容器                  │   │
│  │  Node.js fs.writeFile()              │  │  /workspace/                  │   │
│  │                                      │  │    file.txt                   │   │
│  │  ★ 直接在主机上读写文件              │  │    output.json                │   │
│  │  ★ 不通过 Docker                     │  │    script.py                  │   │
│  └──────────────────────────────────────┘  └───────────────────────────────┘   │
│                                                        ▲                        │
│                                                        │                        │
│                                            ┌───────────┴───────────┐            │
│                                            │  exec 工具            │            │
│                                            │                       │            │
│                                            │  docker exec          │            │
│                                            │  sh -lc "python ..."  │            │
│                                            │                       │            │
│                                            │  ★ 在容器内执行命令   │            │
│                                            └───────────────────────┘            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

| 工具 | 执行位置 | 实现机制 | 原因 |
|------|---------|----------|------|
| **read** | 主机 | `fs.readFile()` | 性能优先，无需容器开销 |
| **write** | 主机 | `fs.writeFile()` | 性能优先，无需容器开销 |
| **edit** | 主机 | `fs` 模块 | 性能优先，无需容器开销 |
| **exec** | Docker 容器 | `docker exec` | **安全隔离**，执行任意命令最危险 |

**源码确认**:

```typescript
// src/agents/pi-tools.ts:225-244
const sandboxRoot = sandbox?.workspaceDir;  // 主机上的沙箱目录路径

if (sandboxRoot) {
  return [createSandboxedReadTool(sandboxRoot)];  // 传入主机路径
}

// src/agents/pi-tools.read.ts:271-274
export function createSandboxedReadTool(root: string) {
  const base = createReadTool(root) as unknown as AnyAgentTool;  // 用主机路径创建
  return wrapSandboxPathGuard(createOpenClawReadTool(base), root);
}
```

`createReadTool(root)` 来自 `@mariozechner/pi-coding-agent`，使用 **Node.js fs 模块**在主机上读取文件。

**这样设计的原因**:

1. **性能**: 文件操作在主机上执行更快，不需要 `docker exec` 的开销
2. **一致性**: 通过挂载卷，主机和容器看到的是**同一份文件**
3. **安全分层**: `exec` 是最危险的工具（可执行任意命令），所以放在隔离容器内；文件读写相对安全，且有路径检查 (`assertSandboxPath`)

### 8.4 结果文件如何传递出来

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           结果传递机制                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│  方式 1: 挂载卷 (Volume Mount) - 最主要的方式                                   │
│                                                                                 │
│  Docker 容器                              主机                                  │
│  ┌─────────────────────┐                 ┌─────────────────────┐               │
│  │  /workspace/        │ ◄──── 挂载 ────▶ │ ~/.openclaw/        │               │
│  │    └── output.txt   │                 │   sandboxes/{slug}/ │               │
│  │    └── result.json  │                 │    └── output.txt   │               │
│  │    └── image.png    │                 │    └── result.json  │               │
│  └─────────────────────┘                 │    └── image.png    │               │
│                                          └─────────────────────┘               │
│                                                     │                          │
│  沙箱内创建的文件                          主机上直接可见                       │
│  会立即出现在主机目录                      Node.js 可以直接读取                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│  方式 2: 命令输出 (stdout/stderr)                                               │
│                                                                                 │
│  docker exec 返回:                                                              │
│  • stdout: 命令输出                                                             │
│  • stderr: 错误输出                                                             │
│  • exitCode: 退出码                                                             │
│                                                                                 │
│  exec 工具收集这些输出，返回给 LLM                                              │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│  方式 3: read 工具主动读取                                                      │
│                                                                                 │
│  LLM: "读取 output.txt"                                                         │
│       │                                                                         │
│       ▼                                                                         │
│  read 工具:                                                                     │
│    fs.readFile("~/.openclaw/sandboxes/{slug}/output.txt")                       │
│       │                                                                         │
│       ▼                                                                         │
│  返回文件内容给 LLM                                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**核心原理**: 沙箱工作目录是主机目录通过 `-v` 参数**挂载**到容器内，文件在主机和容器间**双向透明**！

```typescript
// src/agents/sandbox/docker.ts:230-236
// 挂载工作空间
const mainMountSuffix =
  params.workspaceAccess === "ro" && workspaceDir === params.agentWorkspaceDir 
    ? ":ro" 
    : "";
args.push("-v", `${workspaceDir}:${cfg.workdir}${mainMountSuffix}`);
```

### 8.5 传递给谁

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           结果传递目标                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

沙箱内产生的数据
        │
        ├───────────────────────────────────────────────────────────────┐
        │                                                               │
        ▼                                                               ▼
┌───────────────────────┐                                 ┌───────────────────────┐
│  目标 1: LLM          │                                 │  目标 2: 用户         │
│                       │                                 │                       │
│  • 命令输出 (exec)    │                                 │  • 最终回复文本       │
│  • 文件内容 (read)    │                                 │  • 媒体文件 (图片等)  │
│  • 工具结果           │                                 │                       │
│                       │                                 │  通过:                │
│  用于:                │                                 │  • Telegram/Discord   │
│  • 理解执行结果       │                                 │  • Web UI             │
│  • 决定下一步操作     │                                 │  • CLI                │
└───────────────────────┘                                 └───────────────────────┘
        │
        ▼
┌───────────────────────┐
│  目标 3: 会话历史     │
│                       │
│  保存到:              │
│  ~/.openclaw/agents/  │
│    pi/sessions/       │
│    {sessionId}.jsonl  │
│                       │
│  包含:                │
│  • 用户消息           │
│  • 助手回复           │
│  • 工具调用及结果     │
└───────────────────────┘
```

### 8.6 完整数据流图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           完整数据流                                             │
└─────────────────────────────────────────────────────────────────────────────────┘

  用户消息 (Telegram/Discord/CLI/Web)
         │
         ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                              Gateway                                         │
  └───────────────────────────────────┬─────────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                          Agent Runtime                                       │
  │                                                                              │
  │  1. 准备沙箱                                                                 │
  │     • 创建沙箱工作目录                                                       │
  │     • 复制配置文件 (AGENTS.md, SOUL.md, ...)                                 │
  │     • 同步技能目录                                                           │
  │                                                                              │
  │  2. 启动 Docker 容器                                                         │
  │     • 挂载: 沙箱目录 → /workspace                                            │
  │     • 安全限制: --read-only, --network none, --cap-drop ALL                  │
  │                                                                              │
  │  3. LLM 对话循环                                                             │
  │     ┌────────────────────────────────────────────────────────────────────┐  │
  │     │  用户: "创建一个 Python 脚本"                                       │  │
  │     │                                                                    │  │
  │     │  LLM: 调用 write 工具                                              │  │
  │     │       │                                                            │  │
  │     │       ▼                                                            │  │
  │     │  write → fs.writeFile("~/.openclaw/sandboxes/.../script.py")       │  │
  │     │       │                                                            │  │
  │     │       │  (文件立即出现在容器 /workspace/script.py)                 │  │
  │     │       ▼                                                            │  │
  │     │  工具结果 → LLM: "文件已创建"                                      │  │
  │     │                                                                    │  │
  │     │  LLM: 调用 exec 工具                                               │  │
  │     │       │                                                            │  │
  │     │       ▼                                                            │  │
  │     │  exec → docker exec openclaw-sbx-xxx                               │  │
  │     │         sh -lc "python script.py"                                  │  │
  │     │       │                                                            │  │
  │     │       │  (在隔离容器内执行)                                        │  │
  │     │       ▼                                                            │  │
  │     │  stdout/stderr → 工具结果 → LLM: "执行结果..."                     │  │
  │     │                                                                    │  │
  │     │  LLM: 调用 read 工具读取输出文件                                   │  │
  │     │       │                                                            │  │
  │     │       ▼                                                            │  │
  │     │  read → fs.readFile("~/.openclaw/sandboxes/.../output.txt")        │  │
  │     │       │                                                            │  │
  │     │       ▼                                                            │  │
  │     │  文件内容 → 工具结果 → LLM                                         │  │
  │     └────────────────────────────────────────────────────────────────────┘  │
  │                                                                              │
  │  4. 构建回复                                                                 │
  │     • 收集 LLM 输出文本                                                      │
  │     • 收集媒体文件 URL                                                       │
  │                                                                              │
  └───────────────────────────────────┬─────────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                              输出                                            │
  │                                                                              │
  │  • 用户: 收到回复消息 + 媒体文件                                             │
  │  • 会话: 保存到 .jsonl 文件                                                  │
  │  • 沙箱: 文件保留在沙箱目录 (可被后续消息访问)                               │
  └─────────────────────────────────────────────────────────────────────────────┘
```

### 8.7 数据流动总结

| 问题 | 答案 |
|------|------|
| **什么文件放入沙箱？** | 配置文件 (AGENTS.md 等) + 技能目录 (skills/) |
| **什么时候放入？** | 每次 Agent 运行前，在 `resolveSandboxContext()` 中 |
| **沙箱内执行什么？** | `docker exec` 执行命令，文件读写通过挂载卷 |
| **结果如何传出？** | 挂载卷 (文件直接可见) + 命令输出 (stdout) |
| **传递给谁？** | LLM (工具结果) → 用户 (最终回复) → 会话历史 (.jsonl) |

---

## 总结

### 核心流程

```
请求到达
    │
    ▼
resolveSandboxRuntimeStatus()  ← 判断是否需要沙箱
    │
    ├── mode = "off" → 不沙箱
    ├── mode = "all" → 沙箱
    └── mode = "non-main" + 非主会话 → 沙箱
            │
            ▼
resolveSandboxConfigForAgent()  ← 解析配置
    │
    ▼
ensureSandboxWorkspace()  ← 准备工作目录
    │
    ▼
ensureSandboxContainer()  ← 确保容器运行
    │
    ▼
SandboxContext  ← 返回上下文
```

### 安全层级

| 层级 | 机制 | 作用 |
|------|------|------|
| **文件系统** | 只读根 + tmpfs | 防止持久化修改 |
| **网络** | network: none | 阻止网络访问 |
| **权限** | cap-drop ALL + no-new-privileges | 最小权限 |
| **资源** | pids/memory/cpu limits | 防止资源耗尽 |
| **工具** | allow/deny 策略 | 控制可用工具 |
| **隔离** | scope (session/agent/shared) | 会话/代理隔离 |
| **工作目录** | effectiveWorkspace 切换 | Agent Runtime 整体隔离 |
| **路径检查** | assertSandboxPath | 防止路径遍历攻击 |

### 工具执行位置

| 工具类型 | 执行位置 | 说明 |
|---------|---------|------|
| **read/write/edit** | 主机 (Node.js fs) | 直接操作沙箱目录文件，通过挂载卷与容器共享 |
| **exec** | Docker 容器 | 在隔离环境内执行命令，最高安全级别 |

> **设计原则**: 文件操作在主机执行（性能优先），命令执行在容器内（安全优先）。两者通过挂载卷共享同一份文件。

### 关键文件

| 文件 | 核心功能 |
|------|----------|
| `context.ts` | 入口函数，协调整体流程 |
| `config.ts` | 配置解析与合并 |
| `docker.ts` | Docker 容器管理 |
| `runtime-status.ts` | 判断是否沙箱化 |
| `tool-policy.ts` | 工具策略检查 |
| `browser.ts` | 沙箱浏览器管理 |
| `prune.ts` | 容器清理 |
