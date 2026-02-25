"""End-to-end integration tests for Phase 1.

Prerequisites:
  1. Redis running on localhost:6379
  2. Runtime: cd runtime && PYTHONPATH=gen:. uv run python -m sahara_runtime.server
  3. Gateway: cd gateway && go run ./cmd/sahara-gw/
"""

import asyncio
import json

import pytest
import websockets

from conftest import ws_submit_and_collect


@pytest.mark.asyncio
async def test_submit_and_receive_events(gateway_url, unique_session):
    """Normal conversation: submit → run_start → delta(s) → usage → run_complete."""
    response, events = await ws_submit_and_collect(
        gateway_url,
        unique_session,
        "Hello from E2E test!",
    )

    assert response is not None
    assert response["status"] == "accepted"
    assert "runId" in response.get("payload", {})
    assert "taskId" in response.get("payload", {})

    event_types = [e["event"] for e in events]
    assert "agent.run_start" in event_types
    assert "agent.delta" in event_types
    assert "agent.run_complete" in event_types

    # Verify seq numbers are monotonically increasing
    seqs = [e["seq"] for e in events]
    assert seqs == sorted(seqs)

    # Verify delta text assembles to a complete response
    delta_texts = [
        e.get("payload", {}).get("text", "")
        for e in events
        if e["event"] == "agent.delta"
    ]
    full_text = "".join(delta_texts)
    assert "Hello from E2E test!" in full_text


@pytest.mark.asyncio
async def test_invalid_frame(gateway_url):
    """Sending invalid JSON should return error response."""
    async with websockets.connect(gateway_url) as ws:
        await ws.send("not json at all")
        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
        data = json.loads(msg)
        assert data["type"] == "res"
        assert data["code"] == 400
        assert data["status"] == "error"


@pytest.mark.asyncio
async def test_missing_method(gateway_url):
    """Sending a request with empty method should error."""
    async with websockets.connect(gateway_url) as ws:
        await ws.send(json.dumps({"type": "req", "id": "x", "method": ""}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
        data = json.loads(msg)
        assert data["code"] == 400


@pytest.mark.asyncio
async def test_unknown_method(gateway_url):
    """Sending a request with unknown method should return 404."""
    async with websockets.connect(gateway_url) as ws:
        await ws.send(json.dumps({"type": "req", "id": "x", "method": "foo.bar", "params": {}}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
        data = json.loads(msg)
        assert data["code"] == 404
        assert data["error"]["reason"] == "METHOD_NOT_FOUND"


@pytest.mark.asyncio
async def test_submit_missing_params(gateway_url):
    """Submit with missing required params should error."""
    async with websockets.connect(gateway_url) as ws:
        await ws.send(json.dumps({
            "type": "req",
            "id": "x",
            "method": "agent.submit",
            "params": {"sessionKey": "", "text": ""},
        }))
        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
        data = json.loads(msg)
        assert data["code"] == 400


@pytest.mark.asyncio
async def test_multiple_sessions(gateway_url):
    """Two concurrent sessions should each receive their own events."""
    import time

    sess1 = f"test:user1:ws:multi_{int(time.time() * 1000)}_1"
    sess2 = f"test:user2:ws:multi_{int(time.time() * 1000)}_2"

    results = await asyncio.gather(
        ws_submit_and_collect(gateway_url, sess1, "Session 1 message"),
        ws_submit_and_collect(gateway_url, sess2, "Session 2 message"),
    )

    for resp, events in results:
        assert resp["status"] == "accepted"
        types = [e["event"] for e in events]
        assert "agent.run_start" in types
        assert "agent.run_complete" in types
