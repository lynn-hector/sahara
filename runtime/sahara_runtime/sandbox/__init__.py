"""沙箱管理 — 支持 noop / Docker / E2B 三种 provider。

通过 SAHARA_SANDBOX_PROVIDER 环境变量切换:
    noop   — 本地直接执行, 不隔离 (开发默认)
    docker — Docker 容器池 (需要 sahara-sandbox 镜像)
    e2b    — E2B 云端 VM (需要 E2B_API_KEY, pip install e2b)
"""
