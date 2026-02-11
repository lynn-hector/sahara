# Sandbox 管理架构与演进规划

> Runtime Worker 内的沙箱容器池管理：预创建、分配、回收、安全限制与从 Docker → gVisor → Firecracker 的演进路径。
> 本文档是 Python 团队实现沙箱模块的详细设计，同时为后续隔离升级提供接口抽象。
>
> 关联文档：
> - [Runtime 架构设计 §11](./RUNTIME-ARCHITECTURE-DESIGN.md) — Sandbox Manager 接口
> - [技术方案 §六](./TECH-PROPOSAL-C-END-REFACTOR.md) — 六种方案对比与分阶段推荐
> - [OpenClaw AGENT-RUNTIME-SANDBOX.md](./openclaw/agent/AGENT-RUNTIME-SANDBOX.md) — 原始沙箱实现参考

---

## 目录

1. [概述](#一概述)
2. [架构全景](#二架构全景)
3. [容器池 (Pool)](#三容器池-pool)
4. [容器配置与安全限制](#四容器配置与安全限制)
5. [Workspace 管理](#五workspace-管理)
6. [Sandbox 操作接口](#六sandbox-操作接口)
7. [容器生命周期](#七容器生命周期)
8. [健康检查与故障恢复](#八健康检查与故障恢复)
9. [C 端安全加固](#九c-端安全加固)
10. [资源控制与计量](#十资源控制与计量)
11. [可观测性](#十一可观测性)
12. [演进路径：Docker → gVisor → Firecracker](#十二演进路径docker--gvisor--firecracker)
13. [配置管理](#十三配置管理)
14. [Phase 1 最小实现](#十四phase-1-最小实现)

---

## 一、概述

### 1.1 沙箱的角色

沙箱为 Agent 的代码执行（`exec`）和文件操作（`read`/`write`）提供**隔离环境**。C 端用户不可信，沙箱必须防止：

- **主机逃逸**：容器内代码不能影响宿主机
- **会话间泄露**：不同用户的文件互不可见
- **资源耗尽**：单个任务不能吃光 CPU/内存
- **网络攻击**：默认无外网访问

### 1.2 与 OpenClaw 的关键差异

| 维度 | OpenClaw | Sahara C 端 |
| --- | --- | --- |
| 生命周期 | 长驻（空闲 24h 清理） | **短生命周期**（任务完成即回收） |
| 并发 | 个位数 | **数百到数千** |
| 启动 | 冷启动 1-3s | **容器池预创建 <200ms** |
| 隔离 | Docker 默认 | **Phase 1 Docker + Phase 2 gVisor** |
| 容器粒度 | session/agent/shared | **per-task**（最强隔离） |
| 文件操作 | Volume Mount 主机读写 | **全部在容器内**（无 Volume 穿越） |

---

## 二、架构全景

### 2.1 沙箱在 Runtime 中的位置

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Runtime Worker                                                     │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐ │
│  │  Agent Loop                                                    │ │
│  │                                                                │ │
│  │  exec("ls -la")  ──────▶ sandbox.exec("ls -la")               │ │
│  │  write("a.py")   ──────▶ sandbox.write_file("a.py", "...")    │ │
│  │  read("a.py")    ──────▶ sandbox.read_file("a.py")            │ │
│  │                                                                │ │
│  └──────────────────────────────┬────────────────────────────────┘ │
│                                 │                                   │
│  ┌──────────────────────────────▼────────────────────────────────┐ │
│  │  Sandbox Manager                                               │ │
│  │                                                                │ │
│  │  ┌─────────────────────────────────────────────────────────┐  │ │
│  │  │  Container Pool                                          │  │ │
│  │  │                                                          │  │ │
│  │  │  idle:    [ C1, C2, C3, C4, C5 ]    ← 预热待命          │  │ │
│  │  │  in_use:  { task_01: C6, task_02: C7 }  ← 使用中       │  │ │
│  │  │  total:   7 / max 20                                     │  │ │
│  │  │                                                          │  │ │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                 │  │ │
│  │  │  │  C1     │  │  C2     │  │  C3     │  ...            │  │ │
│  │  │  │ (idle)  │  │ (idle)  │  │ (idle)  │                 │  │ │
│  │  │  │ Alpine  │  │ Alpine  │  │ Alpine  │                 │  │ │
│  │  │  │ <20MB   │  │ <20MB   │  │ <20MB   │                 │  │ │
│  │  │  └─────────┘  └─────────┘  └─────────┘                 │  │ │
│  │  └─────────────────────────────────────────────────────────┘  │ │
│  │                                                                │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  Docker Engine                                                      │
│  └── Docker Socket (/var/run/docker.sock)                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 一个请求的沙箱流程

```text
SubmitTask 到达
  │
  ▼
sandbox_manager.acquire(task_id)
  │
  ├── 1. 从 idle 池取一个容器 (~50ms)
  ├── 2. 为该 task 创建 workspace 目录
  ├── 3. 将容器标记为 in_use
  └── 返回 Sandbox 对象
  │
  ▼
Agent Loop 执行
  ├── sandbox.exec("python sort.py")     → docker exec -i <container> sh -c "..."
  ├── sandbox.write_file("sort.py", ...) → docker cp / volume write
  ├── sandbox.read_file("sort.py")       → docker cp / volume read
  └── ...
  │
  ▼
任务完成
  │
  ▼
sandbox_manager.release(sandbox)
  │
  ├── 1. 杀死容器内所有用户进程
  ├── 2. 清空 workspace 目录
  ├── 3. 重置容器环境
  └── 4. 放回 idle 池 (或销毁 + 创建新容器)
```

---

## 三、容器池 (Pool)

### 3.1 设计目标

将 Docker 容器冷启动时间从 **1-3 秒** 降低到 **<200ms**（从池中取出的热容器）。

### 3.2 池化状态机

```text
                     Worker 启动
                         │
                         ▼
              ┌──────────────────────┐
              │  预创建 N 个容器      │  (N = pool_min_idle, 默认 5)
              │  docker create + start│
              └──────────┬───────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
    ┌─────────┐    ┌─────────┐    ┌─────────┐
    │  IDLE   │    │  IDLE   │    │  IDLE   │
    │  C1     │    │  C2     │    │  C3     │
    └────┬────┘    └─────────┘    └─────────┘
         │
         │ acquire() 分配给 task
         ▼
    ┌─────────┐
    │ IN_USE  │
    │  C1     │ ← sandbox.exec() / read() / write()
    └────┬────┘
         │
         │ release() 任务完成
         ▼
    ┌─────────┐
    │ CLEANUP │ ← kill 进程 + 清空 workspace
    └────┬────┘
         │
         ├── 容器健康 → 放回 IDLE
         └── 容器异常 → 销毁 + 创建新容器补充
```

### 3.3 核心实现

```python
# sahara_runtime/sandbox/pool.py

class ContainerPool:
    def __init__(self, config: SandboxConfig, docker_client: DockerClient):
        self.config = config
        self.docker = docker_client
        self.idle: asyncio.Queue[Container] = asyncio.Queue()
        self.in_use: dict[str, Container] = {}  # task_id → container
        self._total = 0
        self._lock = asyncio.Lock()

    async def initialize(self):
        """Worker 启动时预创建容器"""
        tasks = []
        for _ in range(self.config.pool_min_idle):
            tasks.append(self._create_container())
        containers = await asyncio.gather(*tasks)
        for c in containers:
            await self.idle.put(c)
        logger.info("pool initialized", count=len(containers))

    async def checkout(self, task_id: str) -> Container:
        """从池中取出一个容器"""
        try:
            container = self.idle.get_nowait()
        except asyncio.QueueEmpty:
            # 池空了 → 动态创建 (会慢一些)
            if self._total >= self.config.pool_max_total:
                raise SandboxPoolExhaustedError("pool at capacity")
            container = await self._create_container()
            logger.warning("pool empty, created on-demand", total=self._total)

        self.in_use[task_id] = container

        # 后台补充池水位
        asyncio.create_task(self._replenish())

        return container

    async def checkin(self, task_id: str):
        """归还容器到池"""
        container = self.in_use.pop(task_id, None)
        if container is None:
            return

        try:
            await self._reset_container(container)
            await self.idle.put(container)
        except Exception:
            # 重置失败 → 销毁 + 补充
            await self._destroy_container(container)
            asyncio.create_task(self._replenish())

    async def _create_container(self) -> Container:
        """创建一个新的沙箱容器"""
        name = f"sahara-sbx-{ulid.new().str[:10]}"

        container = await asyncio.to_thread(
            self.docker.containers.create,
            image=self.config.image,
            name=name,
            command="sleep infinity",
            detach=True,
            **self._container_options(),
        )
        await asyncio.to_thread(container.start)
        self._total += 1

        return Container(
            id=container.id,
            name=name,
            docker_container=container,
            created_at=time.time(),
        )

    async def _reset_container(self, container: Container):
        """重置容器到干净状态"""
        # 1. 杀死容器内所有用户进程 (保留 sleep infinity)
        await container.exec("pkill -9 -u sandbox || true", timeout=5)

        # 2. 清空 workspace
        await container.exec("rm -rf /workspace/* /workspace/.* 2>/dev/null || true", timeout=5)

        # 3. 重置 /tmp
        await container.exec("rm -rf /tmp/* /tmp/.* 2>/dev/null || true", timeout=5)

        container.use_count += 1

    async def _replenish(self):
        """后台补充池水位"""
        async with self._lock:
            while self.idle.qsize() < self.config.pool_min_idle:
                if self._total >= self.config.pool_max_total:
                    break
                container = await self._create_container()
                await self.idle.put(container)

    async def _destroy_container(self, container: Container):
        """销毁容器"""
        try:
            await asyncio.to_thread(container.docker_container.remove, force=True)
        except Exception:
            pass
        self._total -= 1

    def _container_options(self) -> dict:
        """Docker 容器安全配置"""
        return {
            "network_mode": "none",
            "read_only": True,
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges"],
            "pids_limit": self.config.pids_limit,          # 100
            "mem_limit": self.config.memory_limit,          # 256m
            "memswap_limit": self.config.memory_limit,      # 禁用 swap
            "cpu_period": 100000,
            "cpu_quota": self.config.cpu_quota,              # 100000 = 1 core
            "tmpfs": {
                "/tmp": "rw,exec,nosuid,size=128m",
                "/var/tmp": "rw,nosuid,size=64m",
                "/run": "rw,nosuid,size=16m",
            },
            "volumes": {},  # 无宿主机挂载 (C 端安全)
            "user": "1000:1000",  # 非 root 用户
            "working_dir": "/workspace",
        }

    async def shutdown(self):
        """Worker 关闭时销毁所有容器"""
        # 先销毁 in_use 的 (强制)
        for task_id, container in list(self.in_use.items()):
            await self._destroy_container(container)
        # 再销毁 idle 的
        while not self.idle.empty():
            container = self.idle.get_nowait()
            await self._destroy_container(container)
        logger.info("pool shutdown complete")
```

### 3.4 池参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `pool_min_idle` | 5 | 最小空闲容器数（低于此值自动补充） |
| `pool_max_total` | 20 | 最大容器总数（idle + in_use） |
| `container_max_use_count` | 50 | 单容器最大使用次数（超过后销毁重建） |
| `container_max_age` | 1h | 单容器最大存活时间（超过后销毁重建） |

---

## 四、容器配置与安全限制

### 4.1 镜像

```dockerfile
# docker/sandbox/Dockerfile

FROM alpine:3.19

# 安装常用工具
RUN apk add --no-cache \
    bash coreutils \
    python3 py3-pip \
    nodejs npm \
    git curl wget \
    && rm -rf /var/cache/apk/*

# 创建非 root 用户
RUN adduser -D -u 1000 sandbox
USER sandbox
WORKDIR /workspace
```

**镜像大小目标**：<50MB（Alpine 精简）。预装 Python + Node.js 覆盖大部分 Agent 代码执行需求。

### 4.2 安全限制清单

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Docker 容器安全限制                                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  文件系统                                                           │
│  ├── --read-only                只读根文件系统                       │
│  ├── tmpfs /tmp (128m)          可写临时目录 (内存)                  │
│  ├── tmpfs /var/tmp (64m)       可写临时目录                         │
│  └── /workspace                 任务工作目录 (容器内 tmpfs 或 volume)│
│                                                                     │
│  网络                                                               │
│  └── --network none             ★ 完全无网络                        │
│                                                                     │
│  权限                                                               │
│  ├── --cap-drop ALL             移除所有 Linux capabilities          │
│  ├── --security-opt no-new-privileges  禁止提权                     │
│  └── --user 1000:1000           非 root 运行                        │
│                                                                     │
│  资源                                                               │
│  ├── --memory 256m              内存上限 256MB                       │
│  ├── --memory-swap 256m         禁用 swap                           │
│  ├── --cpus 1                   CPU 上限 1 核                        │
│  ├── --pids-limit 100           最大进程数 100                       │
│  └── --ulimit nofile=1024:1024  文件描述符限制                       │
│                                                                     │
│  其他                                                               │
│  ├── 无宿主机 Volume 挂载       ★ C 端关键: 不暴露主机文件系统      │
│  └── 无 Docker Socket 挂载      禁止容器内操作 Docker                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.3 与 OpenClaw 的安全差异

| 配置 | OpenClaw | Sahara C 端 | 原因 |
| --- | --- | --- | --- |
| Volume Mount | 主机 workspace 挂载到容器 | **无宿主机挂载** | C 端用户不可信 |
| 文件操作方式 | read/write 直接操作主机路径 | **docker cp 或 container exec** | 不穿越容器边界 |
| 网络 | `--network none` | `--network none` | 不变 |
| 用户 | root | **非 root (uid 1000)** | 减少攻击面 |
| 生命周期 | 长驻 24h | **任务级回收** | 防止状态累积 |

---

## 五、Workspace 管理

### 5.1 C 端 Workspace 策略

OpenClaw 通过 Volume Mount 让容器直接访问主机工作目录。Sahara C 端**不做主机挂载**——所有文件操作在容器内完成，任务结束后 workspace 随容器回收清空。

```text
任务开始:
  /workspace/              ← 空目录 (容器内 tmpfs)

Agent 执行中:
  /workspace/
  ├── sort.py              ← Agent 写入
  ├── data.csv             ← Agent 写入
  └── output.txt           ← Agent 写入

任务结束:
  /workspace/ 清空 (rm -rf)
  容器放回 idle 池
```

### 5.2 文件传入传出

如果用户上传了附件，或 Agent 需要导出文件：

```text
文件传入 (用户附件):
  1. 用户上传文件到临时存储 (HTTP API → 对象存储)
  2. Agent Loop 启动前, 通过 docker cp 将文件复制到容器 /workspace/
     docker cp /tmp/upload-xxx.csv container:/workspace/data.csv

文件传出 (Agent 产出):
  1. Agent 执行完成后, 通过 docker cp 取出需要的文件
     docker cp container:/workspace/result.txt /tmp/output-xxx.txt
  2. 上传到对象存储, 返回 URL 给用户
```

### 5.3 文件路径安全

```python
# sahara_runtime/sandbox/container.py

def _validate_path(self, path: str) -> str:
    """防止路径穿越攻击"""
    # 1. 规范化路径
    resolved = os.path.normpath(os.path.join("/workspace", path))

    # 2. 必须在 /workspace 内
    if not resolved.startswith("/workspace/") and resolved != "/workspace":
        raise SandboxPathError(f"path escape: {path}")

    # 3. 不允许符号链接穿越 (在容器内验证)
    return resolved
```

---

## 六、Sandbox 操作接口

### 6.1 抽象接口

```python
# sahara_runtime/sandbox/manager.py

class Sandbox(ABC):
    """沙箱操作接口 — 被 Agent Loop 和 Tools 调用"""

    @property
    @abstractmethod
    def enabled(self) -> bool: ...

    @abstractmethod
    async def exec(self, command: str, timeout: int = 30,
                   workdir: str = "/workspace") -> ExecResult: ...

    @abstractmethod
    async def read_file(self, path: str) -> str: ...

    @abstractmethod
    async def write_file(self, path: str, content: str) -> None: ...

    @abstractmethod
    async def list_files(self, path: str = ".") -> list[FileInfo]: ...

    @abstractmethod
    async def cleanup(self) -> None: ...


@dataclass
class ExecResult:
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    duration_ms: int
```

### 6.2 Docker 实现

```python
# sahara_runtime/sandbox/container.py

class DockerSandbox(Sandbox):
    def __init__(self, container: Container):
        self._container = container
        self._enabled = True

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def exec(self, command: str, timeout: int = 30,
                   workdir: str = "/workspace") -> ExecResult:
        """在容器内执行命令"""
        self._validate_path(workdir)

        start = time.monotonic()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    self._container.docker_container.exec_run,
                    cmd=["sh", "-c", command],
                    workdir=workdir,
                    user="sandbox",
                    demux=True,          # 分离 stdout/stderr
                    environment={
                        "HOME": "/workspace",
                        "PATH": "/usr/local/bin:/usr/bin:/bin",
                        "LANG": "C.UTF-8",
                    },
                ),
                timeout=timeout,
            )
            duration_ms = int((time.monotonic() - start) * 1000)

            stdout = (result.output[0] or b"").decode("utf-8", errors="replace")
            stderr = (result.output[1] or b"").decode("utf-8", errors="replace")

            return ExecResult(
                exit_code=result.exit_code,
                stdout=stdout,
                stderr=stderr,
                timed_out=False,
                duration_ms=duration_ms,
            )

        except asyncio.TimeoutError:
            # 超时 → kill 该进程 (不 kill 整个容器)
            await self._kill_user_processes()
            duration_ms = int((time.monotonic() - start) * 1000)
            return ExecResult(
                exit_code=-1,
                stdout="",
                stderr=f"Command timed out after {timeout}s",
                timed_out=True,
                duration_ms=duration_ms,
            )

    async def read_file(self, path: str) -> str:
        """读取容器内文件"""
        resolved = self._validate_path(path)
        result = await self.exec(f"cat '{resolved}'", timeout=5)
        if result.exit_code != 0:
            raise FileNotFoundError(f"file not found: {path}")
        return result.stdout

    async def write_file(self, path: str, content: str) -> None:
        """写入文件到容器内"""
        resolved = self._validate_path(path)
        # 使用 base64 传输避免 shell 转义问题
        b64 = base64.b64encode(content.encode()).decode()
        result = await self.exec(
            f"mkdir -p $(dirname '{resolved}') && echo '{b64}' | base64 -d > '{resolved}'",
            timeout=10,
        )
        if result.exit_code != 0:
            raise SandboxIOError(f"write failed: {result.stderr}")

    async def list_files(self, path: str = ".") -> list[FileInfo]:
        """列出目录内容"""
        resolved = self._validate_path(path)
        result = await self.exec(f"ls -la '{resolved}'", timeout=5)
        return _parse_ls_output(result.stdout)

    async def cleanup(self) -> None:
        """清理容器状态 (由 Pool.checkin 调用)"""
        await self._kill_user_processes()

    async def _kill_user_processes(self):
        """杀死容器内所有用户进程"""
        try:
            await asyncio.to_thread(
                self._container.docker_container.exec_run,
                cmd=["sh", "-c", "pkill -9 -u sandbox 2>/dev/null || true"],
                user="root",
            )
        except Exception:
            pass
```

### 6.3 NoopSandbox（开发/测试用）

```python
class NoopSandbox(Sandbox):
    """无沙箱模式 — 直接在主机执行 (仅用于开发环境)"""

    @property
    def enabled(self) -> bool:
        return False

    async def exec(self, command, timeout=30, workdir="/workspace"):
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=workdir,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        return ExecResult(
            exit_code=proc.returncode,
            stdout=stdout.decode(),
            stderr=stderr.decode(),
            timed_out=False,
            duration_ms=0,
        )
    # ... read_file / write_file 用普通文件 IO
```

---

## 七、容器生命周期

### 7.1 状态流转

```text
┌──────────┐  docker create+start  ┌──────────┐
│ CREATING │ ─────────────────────▶│  IDLE    │◀─────────┐
└──────────┘                       └────┬─────┘          │
                                        │ checkout()     │ reset 成功
                                        ▼                │
                                   ┌──────────┐     ┌────┴─────┐
                                   │  IN_USE  │────▶│ RESETTING│
                                   └──────────┘     └────┬─────┘
                                     checkin()           │ reset 失败
                                                         ▼
                                                    ┌──────────┐
                                                    │ DESTROYED│
                                                    └──────────┘
                                                    (+ 补充新容器)
```

### 7.2 容器淘汰策略

即使容器正常使用，也需要定期淘汰以防止状态累积：

| 条件 | 动作 |
| --- | --- |
| `use_count >= 50` | 下次 checkin 时销毁，创建新容器 |
| `age >= 1h` | 定时检查，从 idle 池中移除销毁 |
| `reset 失败` | 立即销毁，创建新容器 |
| `exec 超时 3 次` | 标记为异常，下次 checkin 销毁 |

### 7.3 Worker 关闭时的容器处理

```python
async def shutdown(self):
    """优雅关闭：等待 in_use 容器的任务完成"""
    # 1. 停止补充新容器
    self._shutting_down = True

    # 2. 等待 in_use 容器释放 (最长 60s, 由 Worker Drain 控制)
    deadline = time.time() + 60
    while self.in_use and time.time() < deadline:
        await asyncio.sleep(1)

    # 3. 销毁所有容器
    for container in list(self.in_use.values()):
        await self._destroy_container(container)
    while not self.idle.empty():
        await self._destroy_container(self.idle.get_nowait())
```

---

## 八、健康检查与故障恢复

### 8.1 容器健康检查

```python
# sahara_runtime/sandbox/pool.py

async def _health_check_loop(self):
    """定期检查 idle 容器的健康状态"""
    while not self._shutting_down:
        await asyncio.sleep(30)  # 每 30s 检查一次

        checked = []
        while not self.idle.empty():
            container = self.idle.get_nowait()
            checked.append(container)

        for container in checked:
            healthy = await self._check_container(container)
            if healthy and not self._should_retire(container):
                await self.idle.put(container)
            else:
                await self._destroy_container(container)
                asyncio.create_task(self._replenish())

async def _check_container(self, container: Container) -> bool:
    """检查容器是否健康"""
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(
                container.docker_container.exec_run,
                cmd=["echo", "ok"],
                timeout=5,
            ),
            timeout=10,
        )
        return result.exit_code == 0
    except Exception:
        return False
```

### 8.2 Docker Engine 故障

```text
Docker Engine 不可用:
  ├── 创建容器失败 → SandboxUnavailableError
  ├── exec 失败 → 重试 1 次; 仍失败 → 返回错误给 LLM
  └── 连续 3 次失败 → 将 Worker 标记为 UNHEALTHY (GetStatus 上报)
      → Gateway 停止调度到该 Worker
```

---

## 九、C 端安全加固

### 9.1 威胁模型

| 威胁 | 攻击方式 | 防御 |
| --- | --- | --- |
| **容器逃逸** | 内核漏洞利用 | Phase 2: gVisor (系统调用拦截) |
| **文件系统穿越** | 符号链接/路径穿越 | `_validate_path()` + 容器内无宿主机挂载 |
| **网络攻击** | 扫描/DDoS/外联 C2 | `--network none` |
| **资源耗尽** | fork 炸弹/内存泄漏 | `--pids-limit 100` + `--memory 256m` |
| **数据泄露** | 读取其他用户文件 | per-task workspace + 任务后清空 |
| **提权** | setuid/capabilities | `--cap-drop ALL` + `no-new-privileges` + 非 root |
| **持久化后门** | 写入 crontab/startup | `--read-only` + tmpfs 只在 /tmp |

### 9.2 命令黑名单 (Runtime 侧)

在将命令传递给 `sandbox.exec()` 之前，做快速正则检查：

```python
BLOCKED_PATTERNS = [
    r"docker\s",           # 禁止操作 Docker
    r"mount\s",            # 禁止挂载
    r"nsenter\s",          # 禁止进入其他 namespace
    r"chroot\s",           # 禁止 chroot
    r"/proc/\d+/",         # 禁止访问其他进程
    r"/dev/sd[a-z]",       # 禁止访问块设备
]

def validate_command(command: str) -> bool:
    for pattern in BLOCKED_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            return False
    return True
```

> 这是**第一层**防御（快速过滤）。即使绕过了正则，容器安全配置仍然生效。

---

## 十、资源控制与计量

### 10.1 资源限制

| 资源 | 限制 | 对 1000 并发的影响 |
| --- | --- | --- |
| 内存 | 256MB/容器 | 1000 × 256MB = 256GB (需要分布到多 Worker) |
| CPU | 1 core/容器 | 20 容器/Worker × 1 core = 20 cores |
| PID | 100/容器 | — |
| 文件描述符 | 1024/容器 | — |
| /tmp 磁盘 | 128MB (tmpfs) | 20 × 128MB = 2.56GB RAM |
| 执行超时 | 30s (默认) | — |

### 10.2 计量 (Phase 2)

```python
# 每次 exec 记录资源消耗
@dataclass
class ExecMetrics:
    duration_ms: int
    cpu_time_ms: int
    memory_peak_bytes: int
    io_read_bytes: int
    io_write_bytes: int

# 通过 docker stats API 采集
async def _collect_metrics(self, container: Container) -> ExecMetrics:
    stats = await asyncio.to_thread(container.docker_container.stats, stream=False)
    return ExecMetrics(
        cpu_time_ms=_parse_cpu(stats),
        memory_peak_bytes=stats["memory_stats"].get("max_usage", 0),
        io_read_bytes=_parse_io(stats, "read"),
        io_write_bytes=_parse_io(stats, "write"),
    )
```

---

## 十一、可观测性

### 11.1 Prometheus 指标

```python
# Pool
sandbox_pool_idle       Gauge    空闲容器数
sandbox_pool_in_use     Gauge    使用中容器数
sandbox_pool_total      Gauge    总容器数

# 分配
sandbox_checkout_duration_ms   Histogram  分配延迟
sandbox_checkout_total         Counter    分配次数 { source: idle|created }

# 执行
sandbox_exec_duration_ms       Histogram  命令执行延迟 { tool }
sandbox_exec_timeout_total     Counter    超时次数
sandbox_exec_errors_total      Counter    错误次数 { error_type }

# 生命周期
sandbox_created_total          Counter    创建次数
sandbox_destroyed_total        Counter    销毁次数 { reason: retired|unhealthy|shutdown }
sandbox_reset_duration_ms      Histogram  重置延迟
sandbox_reset_errors_total     Counter    重置失败次数
```

### 11.2 关键告警

| 告警 | 条件 | 严重级别 |
| --- | --- | --- |
| 池耗尽 | `sandbox_pool_idle == 0` 持续 1min | Critical |
| 分配延迟高 | `sandbox_checkout_duration_ms P99 > 1s` | Warning |
| 执行超时率高 | `sandbox_exec_timeout_total > 10/min` | Warning |
| Docker 不可用 | `sandbox_created_total` 连续失败 | Critical |

---

## 十二、演进路径：Docker → gVisor → Firecracker

### 12.1 三阶段

```text
Phase 1: Docker (默认 runtime)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  隔离级别: 中 (共享内核, namespace 隔离)
  启动时间: ~200ms (池化)
  内存:     ~15-20MB/容器 (Alpine)
  开发成本: 低 (docker-py 已有)
  适用:     MVP, 内测

Phase 2: Docker + gVisor (runsc)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  隔离级别: 高 (系统调用级拦截)
  启动时间: ~400ms (池化可降到 ~250ms)
  内存:     ~20-40MB/容器
  开发成本: 极低 (一行配置)
  适用:     公测, 生产

  启用方式:
    # /etc/docker/daemon.json
    {
      "runtimes": {
        "runsc": { "path": "/usr/local/bin/runsc" }
      }
    }

    # 容器创建时指定
    docker create --runtime=runsc ...

Phase 3: Firecracker microVM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  隔离级别: 最高 (VM 级, 独立内核)
  启动时间: ~125ms
  内存:     ~5-20MB/VM
  开发成本: 中-高 (需要 KVM, 自定义内核)
  适用:     大规模, 最高安全要求

  前提: 宿主机支持 KVM (裸金属/嵌套虚拟化)
```

### 12.2 接口抽象保证平滑迁移

所有阶段共用 `Sandbox` 抽象接口。切换隔离方案只需替换实现：

```python
# 工厂方法
def create_sandbox_manager(config: SandboxConfig) -> SandboxManager:
    match config.runtime:
        case "docker":
            return DockerSandboxManager(config)
        case "gvisor":
            return GVisorSandboxManager(config)   # 实际仍是 Docker, 指定 --runtime=runsc
        case "firecracker":
            return FirecrackerSandboxManager(config)
        case "noop":
            return NoopSandboxManager()           # 开发环境
```

`GVisorSandboxManager` 继承 `DockerSandboxManager`，只覆盖 `_container_options()` 添加 `--runtime=runsc`。

### 12.3 迁移检查清单

**Docker → gVisor：**

- [ ] 安装 gVisor (`runsc`) 到宿主机
- [ ] 配置 Docker daemon 添加 `runsc` runtime
- [ ] 修改配置：`sandbox.runtime = "gvisor"`
- [ ] 验证：所有工具在 gVisor 下正常工作
- [ ] 注意：gVisor 不支持某些系统调用（如 `ptrace`）

**gVisor → Firecracker：**

- [ ] 宿主机支持 KVM (`/dev/kvm`)
- [ ] 构建 Firecracker rootfs 和内核
- [ ] 实现 `FirecrackerSandboxManager`（替代 docker-py，使用 Firecracker API）
- [ ] 容器池改为 microVM 池
- [ ] 文件传入/传出改为 virtio-vsock 或 SSH

---

## 十三、配置管理

```python
# sahara_runtime/config.py (沙箱相关)

class SandboxConfig(BaseModel):
    # 基础
    enabled: bool = True
    runtime: str = "docker"           # docker / gvisor / firecracker / noop
    image: str = "sahara-sandbox:latest"

    # 容器池
    pool_min_idle: int = 5
    pool_max_total: int = 20

    # 容器资源限制
    memory_limit: str = "256m"
    cpu_quota: int = 100000           # 100000 = 1 core
    pids_limit: int = 100
    tmp_size: str = "128m"

    # 执行限制
    default_timeout: int = 30         # 秒
    max_timeout: int = 300            # 秒

    # 生命周期
    container_max_use_count: int = 50
    container_max_age_seconds: int = 3600  # 1h
    health_check_interval: int = 30        # 秒

    # 安全
    network_mode: str = "none"        # none / 白名单 (Phase 3)
    user: str = "1000:1000"
    read_only_root: bool = True
```

---

## 十四、Phase 1 最小实现

| 模块 | Phase 1 范围 | 可推迟 |
| --- | --- | --- |
| **ContainerPool** | ★ 预创建 + checkout + checkin + replenish | 健康检查循环, 淘汰策略 |
| **DockerSandbox** | ★ exec + read_file + write_file | list_files, 资源计量 |
| **安全限制** | ★ read-only + no-network + cap-drop + 非 root | gVisor, 命令黑名单 |
| **路径校验** | ★ _validate_path (防穿越) | — |
| **NoopSandbox** | ★ 开发环境用 | — |
| **镜像** | ★ Alpine + Python + Node.js (<50MB) | 按需定制 |
| **配置** | ★ 环境变量 + pydantic | 配置中心热更新 |
| **指标** | ★ pool_idle / pool_in_use / exec_duration | 详细计量 |
| **shutdown** | ★ 销毁所有容器 | 优雅等待 in_use |

### Phase 1 开发顺序

```text
Week 7 (与 Agent Loop 集成):
  1. 镜像构建 (Dockerfile)                    0.5d
  2. ContainerPool 核心 (create/checkout/checkin) 2d
  3. DockerSandbox (exec/read/write)           2d
  4. NoopSandbox (开发环境)                    0.5d

Week 8 (集成测试):
  5. 与 ExecTool / ReadTool / WriteTool 集成   1d
  6. 安全限制验证 (手动测试)                    0.5d
  7. Prometheus 基础指标                        0.5d
```

---

## 附录

### 附录 A. 沙箱镜像预装工具

| 工具 | 版本 | 用途 |
| --- | --- | --- |
| bash | 5.x | Shell 执行 |
| python3 | 3.12+ | Python 脚本 |
| pip | latest | Python 包安装 |
| node | 22.x LTS | JavaScript 执行 |
| npm | latest | Node 包安装 |
| git | latest | 版本控制 |
| curl / wget | latest | HTTP 工具 (网络禁用时无法用) |
| coreutils | — | 基础文件操作 |
| jq | latest | JSON 处理 |

### 附录 B. 与工程计划的任务映射

| 本文档章节 | 工程计划任务 | Phase |
| --- | --- | --- |
| §3 容器池 | P1-12 Docker 沙箱容器池 | Phase 1 |
| §4 安全限制 | P1-12 | Phase 1 |
| §6 Sandbox 接口 | P1-13 基础工具实现 | Phase 1 |
| §9 C 端安全加固 | P1-15 gVisor 沙箱加固 | Phase 1 |
| §12 gVisor 演进 | P1-15 | Phase 1 |
| §12 Firecracker | P3-7 Firecracker 评估 | Phase 3 |
