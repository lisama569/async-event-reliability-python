"""Monotonic state, late/duplicate messages, query-does-not-block-consume."""
from __future__ import annotations

import threading
import time

from app.models import EventState
from app.processor import EventProcessor
from app.sideeffects import SideEffectGateway
from app.storage import EventStore
from app.worker import Worker

from .conftest import make_settings


def test_completed_result_is_immutable_to_late_messages(worker_env):
    """Late/duplicated completion attempts never overwrite a done result."""
    store, proc, worker = worker_env
    event = store.submit("t-m", "monotonic", "order.created", {},
                         "h", time.time()).event
    e = store.claim_due("w-test", time.time(), 60)
    assert store.complete(event.id, e.attempt, "w-test",
                          {"answer": 42}, time.time()) is True

    # Late duplicate completion from the same worker at the same attempt.
    assert store.complete(event.id, e.attempt, "w-test",
                          {"answer": 999}, time.time()) is False
    # Late requeue / dead-letter from a different (imposter) worker.
    assert store.requeue(event.id, e.attempt, "w-other",
                         "recoverable:x", "late", time.time() + 100,
                         time.time()) is False
    assert store.dead_letter(event.id, e.attempt, "w-other",
                             "fatal:x", "late", time.time()) is False

    final = store.get(event.id)
    assert final.state is EventState.COMPLETED
    assert final.result == {"answer": 42}


def test_state_moves_are_monotonic_and_queryable(worker_env):
    store, proc, worker = worker_env
    event = store.submit("t-m2", "flow", "order.created", {}, "h",
                         time.time()).event
    proc.set_failures({event.id: {"mode": "recoverable", "times": 1}})

    e1 = store.claim_due("w-test", time.time(), 60)
    worker.process_claim(e1)
    time.sleep(worker.backoff_delay(1) + 0.05)
    e2 = store.claim_due("w-test", time.time(), 60)
    worker.process_claim(e2)

    transitions = store.list_transitions(event.id)
    pairs = [(t.from_state, t.to_state) for t in transitions]
    assert pairs == [
        (None, "PENDING"),
        ("PENDING", "PROCESSING"),
        ("PROCESSING", "PENDING"),
        ("PENDING", "PROCESSING"),
        ("PROCESSING", "COMPLETED"),
    ]
    assert store.get(event.id).state is EventState.COMPLETED
    # attempt counts are recorded on every processing transition
    processing = [t.attempt for t in transitions
                  if t.to_state == "PROCESSING"]
    assert processing == [1, 2]


def test_duplicate_inbound_message_does_not_roll_back_completion(client):
    body = {"tenant_id": "t-d", "business_key": "dup",
            "event_type": "order.created", "payload": {"x": 1}}
    first = client.post("/events", json=body)
    eid = first.json()["event"]["id"]

    from .conftest import wait_for_state
    done = wait_for_state(client, eid, {"COMPLETED"})
    assert done["state"] == "COMPLETED"

    # Repeated identical submissions after completion return the same result.
    again = client.post("/events", json=body)
    assert again.status_code == 200
    assert again.json()["duplicate"] is True
    assert again.json()["event"]["state"] == "COMPLETED"
    assert again.json()["event"]["id"] == eid
    # The finished result is untouched and no new side effect appeared.
    effects = client.get(f"/events/{eid}/side-effects").json()["side_effects"]
    assert len(effects) == 1


def test_status_reads_do_not_block_consumption(tmp_path):
    """Heavy read traffic must not stall the write-locked consumer loop."""
    settings = make_settings(tmp_path, tenant_quota=100,
                             worker_poll_interval=0.005)
    store = EventStore(settings.db_path, settings.tenant_quota)
    proc = EventProcessor(SideEffectGateway(store))
    worker = Worker(store, proc, settings, worker_id="w-read")

    import asyncio

    async def scenario():
        task = asyncio.create_task(worker.run())
        ids = []
        for i in range(20):
            ids.append(store.submit(
                "t-read", f"k{i}", "order.created", {}, f"h{i}",
                time.time()).event.id)

        # Hammer read-only status endpoints while the worker drains events.
        def reader():
            end = time.time() + 3
            while time.time() < end:
                for eid in ids:
                    store.get(eid)
                    store.list_transitions(eid)
        rt = threading.Thread(target=reader, daemon=True)
        rt.start()

        deadline = time.time() + 8
        while time.time() < deadline:
            if all(store.get(eid).state is EventState.COMPLETED
                   for eid in ids):
                break
            await asyncio.sleep(0.01)
        worker.stop()
        await task
        return ids

    ids = asyncio.run(scenario())
    assert all(store.get(eid).state is EventState.COMPLETED for eid in ids)
