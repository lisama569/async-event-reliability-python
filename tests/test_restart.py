"""Durability across 'restarts': state survives new connections/processes."""
from __future__ import annotations

import time

from app.models import EventState
from app.processor import EventProcessor
from app.sideeffects import SideEffectGateway
from app.storage import EventStore
from app.worker import Worker

from .conftest import make_settings


def test_pending_event_consumed_after_restart(tmp_path):
    settings = make_settings(tmp_path)
    store1 = EventStore(settings.db_path, settings.tenant_quota)
    event = store1.submit("t-r", "after-restart", "order.created", {},
                          "h", time.time()).event
    assert store1.get(event.id).state is EventState.PENDING

    # Simulate full process restart: brand-new store/processor/worker objects
    # against the same durable file.
    store2 = EventStore(settings.db_path, settings.tenant_quota)
    proc2 = EventProcessor(SideEffectGateway(store2))
    worker2 = Worker(store2, proc2, settings, worker_id="w-after-restart")
    claimed = store2.claim_due(worker2.worker_id, time.time(),
                               settings.lease_seconds)
    assert claimed is not None and claimed.id == event.id
    worker2.process_claim(claimed)

    final = store2.get(event.id)
    assert final.state is EventState.COMPLETED
    assert final.result is not None
    assert len(store2.list_side_effects(event.id)) == 1


def test_all_states_and_history_survive_restart(tmp_path):
    settings = make_settings(tmp_path)
    s1 = EventStore(settings.db_path, settings.tenant_quota)
    proc = EventProcessor(SideEffectGateway(s1))
    w = Worker(s1, proc, settings, worker_id="w1")

    completed = s1.submit("t-h", "done", "order.created", {}, "h1",
                          time.time()).event
    e = s1.claim_due("w1", time.time(), 60)
    w.process_claim(e)

    failing = s1.submit("t-h", "dead", "order.created", {}, "h2",
                        time.time()).event
    proc.set_failures({failing.id: {"mode": "fatal", "times": 1,
                                    "message": "boom"}})
    e = s1.claim_due("w1", time.time(), 60)
    w.process_claim(e)

    waiting = s1.submit("t-h", "waiting", "order.created", {}, "h3",
                        time.time()).event

    # Restart: inspect with fresh connections.
    s2 = EventStore(settings.db_path, settings.tenant_quota)
    assert s2.get(completed.id).state is EventState.COMPLETED
    assert s2.get(failing.id).state is EventState.DEAD_LETTER
    assert s2.get(waiting.id).state is EventState.PENDING
    assert {x.id for x in s2.list_dlq("t-h")} == {failing.id}
    moves = [t.to_state for t in s2.list_transitions(completed.id)]
    assert moves == ["PENDING", "PROCESSING", "COMPLETED"]
    assert len(s2.list_side_effects(completed.id)) == 1


def test_replay_after_restart_then_drain(tmp_path):
    settings = make_settings(tmp_path)
    s1 = EventStore(settings.db_path, settings.tenant_quota)
    proc = EventProcessor(SideEffectGateway(s1))
    w = Worker(s1, proc, settings, worker_id="w1")
    event = s1.submit("t-r2", "dlq-restart", "order.created", {}, "h",
                      time.time()).event
    proc.set_failures({event.id: {"mode": "fatal", "times": 1}})
    w.process_claim(s1.claim_due("w1", time.time(), 60))
    assert s1.get(event.id).state is EventState.DEAD_LETTER

    # Restart, operator replays through a fresh connection.
    s2 = EventStore(settings.db_path, settings.tenant_quota)
    proc2 = EventProcessor(SideEffectGateway(s2))
    proc2.set_failures({event.id: {"mode": "success"}})
    w2 = Worker(s2, proc2, settings, worker_id="w2")
    s2.replay_dead_letter(event.id, time.time())
    w2.process_claim(s2.claim_due("w2", time.time(), 60))
    assert s2.get(event.id).state is EventState.COMPLETED
    assert s2.get(event.id).replay_count == 1


def test_submit_duplicate_after_restart_still_idempotent(tmp_path):
    settings = make_settings(tmp_path)
    s1 = EventStore(settings.db_path, settings.tenant_quota)
    r1 = s1.submit("t-r3", "same-key", "order.created", {"a": 1},
                   "hash1", time.time())
    assert r1.duplicate is False

    s2 = EventStore(settings.db_path, settings.tenant_quota)
    r2 = s2.submit("t-r3", "same-key", "order.created", {"a": 1},
                   "hash1", time.time())
    assert r2.duplicate is True
    assert r2.event.id == r1.event.id

    from app.errors import KeyConflictError
    try:
        s2.submit("t-r3", "same-key", "order.created", {"a": 2},
                  "hash2", time.time())
        raise AssertionError("expected conflict after restart")
    except KeyConflictError:
        pass
