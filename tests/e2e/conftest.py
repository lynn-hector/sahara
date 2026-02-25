"""E2E test fixtures.

Requires: Redis running on localhost:6379, Runtime on :50051, Gateway on :8080
"""

import asyncio
import json

import pytest
import websockets


@pytest.fixture
def gateway_url():
    return "ws://localhost:8080/ws"


@pytest.fixture
def unique_session():
    """Generate a unique session key for test isolation."""
    import time

    return f"test-agent:user1:ws:test_{int(time.time() * 1000)}"


async def ws_submit_and_collect(
    url: str,
    session_key: str,
    text: str,
    timeout: float = 15.0,
) -> tuple[dict, list[dict]]:
    """Connect to WS, submit a task, and collect all events until completion."""
    async with websockets.connect(url) as ws:
        req = {
            "type": "req",
            "id": "test-req",
            "method": "agent.submit",
            "params": {
                "sessionKey": session_key,
                "agentId": "test-agent",
                "text": text,
            },
        }
        await ws.send(json.dumps(req))

        response = None
        events = []

        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            remaining = deadline - asyncio.get_event_loop().time()
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5.0))
            except asyncio.TimeoutError:
                break

            data = json.loads(msg)
            if data.get("type") == "res":
                response = data
            elif data.get("type") == "event":
                events.append(data)
                if data.get("event") in ("agent.run_complete", "agent.run_error", "agent.run_abort"):
                    break

        return response, events
