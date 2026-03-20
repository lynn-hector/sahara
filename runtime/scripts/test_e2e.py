#!/usr/bin/env python3
"""端到端测试脚本 — 通过 gRPC 提交任务，监听 Redis 事件流，实时打印 LLM 响应。

用法:
    # 确保 Redis 和 Runtime Server 已启动
    uv run python scripts/test_e2e.py
    uv run python scripts/test_e2e.py --message "写一首关于AI的诗"
    uv run python scripts/test_e2e.py --message "hello" --session my-test

运行前提:
    1. Redis: docker compose -f deploy/docker-compose.yml up -d redis
    2. Runtime: uv run python -m sahara_runtime.server
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_gen = os.path.join(_root, "gen")
if _gen not in sys.path:
    sys.path.insert(0, _gen)

import grpc
from redis.asyncio import Redis
from sahara.agent.v1 import agent_pb2, agent_pb2_grpc
from sahara.event.v1 import event_pb2

EVENT_NAMES = {
    event_pb2.EVENT_TYPE_RUN_START: "run_start",
    event_pb2.EVENT_TYPE_DELTA: "delta",
    event_pb2.EVENT_TYPE_THINKING: "thinking",
    event_pb2.EVENT_TYPE_TOOL_START: "tool_start",
    event_pb2.EVENT_TYPE_TOOL_RESULT: "tool_result",
    event_pb2.EVENT_TYPE_RUN_COMPLETE: "run_complete",
    event_pb2.EVENT_TYPE_RUN_ERROR: "run_error",
    event_pb2.EVENT_TYPE_RUN_ABORT: "run_abort",
    event_pb2.EVENT_TYPE_USAGE: "usage",
    event_pb2.EVENT_TYPE_INPUT_REQUIRED: "input_required",
    event_pb2.EVENT_TYPE_TOOL_CONFIRM_REQUIRED: "tool_confirm",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sahara Runtime E2E 测试")
    p.add_argument("--message", "-m", default="帮我把两日游的计划写到文件中吧")
    p.add_argument("--session", "-s", default="e2e-test:user-1:cli:local")
    p.add_argument("--agent", default="e2e-test")
    p.add_argument("--grpc-addr", default="localhost:50051")
    p.add_argument("--redis-url", default="redis://localhost:6379")
    p.add_argument("--timeout", type=int, default=60)
    return p.parse_args()


async def submit_task(
    addr: str, session_key: str, agent_id: str, message: str,
) -> agent_pb2.SubmitTaskResponse:
    channel = grpc.aio.insecure_channel(addr)
    stub = agent_pb2_grpc.AgentServiceStub(channel)
    request = agent_pb2.SubmitTaskRequest(
        session_key=session_key,
        agent_id=agent_id,
        user_message=agent_pb2.UserMessage(text=message),
    )
    print(f"\n{'='*60}")
    print(f"  发送消息: {message}")
    print(f"  Session:  {session_key}")
    print(f"  gRPC:     {addr}")
    print(f"{'='*60}\n")
    resp = await stub.SubmitTask(request)
    print(f"[ok] 任务已提交  run_id={resp.run_id}  worker={resp.worker_id}\n")
    await channel.close()
    return resp


async def listen_events(
    redis_url: str, session_key: str, timeout: int,
) -> None:
    redis = Redis.from_url(redis_url, decode_responses=False)
    stream_key = f"events:{session_key}"
    last_id = "0-0"
    deadline = time.time() + timeout
    done = False

    print(f"[listen] 监听事件流: {stream_key}")
    print(f"{'─'*60}")

    while not done and time.time() < deadline:
        results = await redis.xread(
            {stream_key: last_id}, count=10, block=1000,
        )
        if not results:
            continue

        for _stream, entries in results:
            for entry_id, fields in entries:
                last_id = entry_id
                raw = fields.get(b"data")
                if not raw:
                    continue

                event = event_pb2.AgentEvent()
                event.ParseFromString(raw)
                done = _print_event(event)

    print(f"\n{'─'*60}")
    if not done:
        print("[warn] 超时未收到 run_complete 事件")
    await redis.close()


def _print_event(event: event_pb2.AgentEvent) -> bool:
    """打印单个事件，返回 True 表示流结束。"""
    etype = EVENT_NAMES.get(event.type, f"unknown({event.type})")

    if event.type == event_pb2.EVENT_TYPE_RUN_START:
        rs = event.run_start
        print(f"[{etype}] agent={rs.agent_id}  model={rs.model}")
        return False

    if event.type == event_pb2.EVENT_TYPE_DELTA:
        sys.stdout.write(event.delta.text)
        sys.stdout.flush()
        return False

    if event.type == event_pb2.EVENT_TYPE_THINKING:
        sys.stdout.write(f"\033[2m{event.thinking.text}\033[0m")
        sys.stdout.flush()
        return False

    if event.type == event_pb2.EVENT_TYPE_TOOL_START:
        ts = event.tool_start
        print(f"\n[{etype}] {ts.tool_name}({ts.input_json[:80]})")
        return False

    if event.type == event_pb2.EVENT_TYPE_TOOL_RESULT:
        tr = event.tool_result
        status = "ok" if tr.success else "ERR"
        print(f"[{etype}] [{status}] {tr.output[:120]}  ({tr.duration_ms}ms)")
        return False

    if event.type == event_pb2.EVENT_TYPE_USAGE:
        u = event.usage
        print(f"\n[{etype}] model={u.model}  "
              f"in={u.input_tokens} out={u.output_tokens}  "
              f"iter={u.iteration}")
        return False

    if event.type == event_pb2.EVENT_TYPE_RUN_COMPLETE:
        rc = event.run_complete
        print(f"\n[{etype}] iterations={rc.iterations}  "
              f"duration={rc.duration_ms}ms")
        print(f"\n--- 完整响应 ---\n{rc.final_text}")
        return True

    if event.type == event_pb2.EVENT_TYPE_RUN_ERROR:
        re = event.run_error
        print(f"\n[{etype}] {re.error_code}: {re.error_message}")
        return True

    if event.type == event_pb2.EVENT_TYPE_RUN_ABORT:
        ra = event.run_abort
        print(f"\n[{etype}] {ra.reason} (by {ra.aborted_by})")
        return True

    print(f"[{etype}] seq={event.seq}")
    return False


async def main() -> None:
    args = parse_args()
    t0 = time.time()

    listen_task = asyncio.create_task(
        listen_events(args.redis_url, args.session, args.timeout),
    )

    await asyncio.sleep(0.3)

    try:
        await submit_task(args.grpc_addr, args.session, args.agent, args.message)
    except grpc.aio.AioRpcError as e:
        print(f"\n[error] gRPC 调用失败: {e.code()} — {e.details()}")
        listen_task.cancel()
        sys.exit(1)

    await listen_task
    print(f"\n总耗时: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
