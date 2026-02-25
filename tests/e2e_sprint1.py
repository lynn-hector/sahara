#!/usr/bin/env python3
"""Sprint 1 E2E test: WS → Gateway → Runtime → Mock LLM → Redis Streams → WS"""

import asyncio
import json
import sys

try:
    import websockets
except ImportError:
    print("Installing websockets...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets


async def main():
    uri = "ws://localhost:8080/ws"
    print(f"Connecting to {uri}...")

    async with websockets.connect(uri) as ws:
        print("Connected!")

        # Send agent.submit
        submit_req = {
            "type": "req",
            "id": "test-001",
            "method": "agent.submit",
            "params": {
                "sessionKey": "test-agent:user1:ws:peer1",
                "agentId": "test-agent",
                "text": "Hello Sahara!",
            },
        }

        print(f"\n>>> Sending: {json.dumps(submit_req, indent=2)}")
        await ws.send(json.dumps(submit_req))

        # Collect responses and events
        events = []
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
                data = json.loads(msg)
                events.append(data)

                if data.get("type") == "res":
                    print(f"\n<<< Response: {json.dumps(data, indent=2)}")
                elif data.get("type") == "event":
                    event_name = data.get("event", "unknown")
                    payload = data.get("payload", {})
                    print(f"<<< Event [{data.get('seq', '?')}]: {event_name} -> {json.dumps(payload)}")

                    if event_name in ("agent.run_complete", "agent.run_error"):
                        break
        except asyncio.TimeoutError:
            print("\nTimeout waiting for events")

    # Verify results
    print("\n" + "=" * 60)
    print("E2E VERIFICATION")
    print("=" * 60)

    res_frames = [e for e in events if e.get("type") == "res"]
    event_frames = [e for e in events if e.get("type") == "event"]
    event_types = [e.get("event") for e in event_frames]

    checks = [
        ("Got submit response", len(res_frames) >= 1),
        ("Response status is accepted", res_frames[0].get("status") == "accepted" if res_frames else False),
        ("Got RUN_START event", "agent.run_start" in event_types),
        ("Got DELTA events", "agent.delta" in event_types),
        ("Got USAGE event", "agent.usage" in event_types),
        ("Got RUN_COMPLETE event", "agent.run_complete" in event_types),
    ]

    all_passed = True
    for label, passed in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {label}")
        if not passed:
            all_passed = False

    delta_texts = [
        e.get("payload", {}).get("text", "")
        for e in event_frames
        if e.get("event") == "agent.delta"
    ]
    full_text = "".join(delta_texts)
    print(f"\n  Full response text: {full_text!r}")
    print(f"  Total events: {len(event_frames)}")

    if all_passed:
        print("\n  ALL CHECKS PASSED - Sprint 1 E2E verified!")
        return 0
    else:
        print("\n  SOME CHECKS FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
