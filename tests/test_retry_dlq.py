"""Backoff retry, retry exhaustion, fatal DLQ and idempotent manual replay."""
from __future__ import annotations

import time

from app.models import EventState
from app.sideeffects import SideEffectGateway
from app.storage import EventStore
from app.worker import Worker


def _submit(store, tenant, key):
    return store.submit(tenant, key, "order.created", {},
                        "hash", time.time()).event


def _drive(store, worker, max_rounds=20):
    """Manually run claim->process rounds until nothing is due."""
    for _ in range(max_rounds):
        event = store.claim_due(worker.worker_id, time.time(),
                                worker.settings.lease_seconds)
        if event is None:
            return
        worker.process_claim(event)


def test_recoverable_then_success_retries_and_completes(worker_env, settings):
    store, proc, worker = worker_env
    event = _submit(store, "t1", "recover-then-ok")
    proc.set_failures({event.id: {"mode": "recoverable", "times": 2,
                                  "message": "temporary glitch"}})

    e1 = store.claim_due("w-test", time.time(), settings.lease_seconds)
    assert e1.attempt == 1
    worker.process_claim(e1)
    assert store.get(event.id).state is EventState.PENDING
    assert store.get(event.id).last_error_code == "recoverable:recoverable"

    # Not due before backoff elapses.
    assert store.claim_due("w-test", time.time(),
                           settings.lease_seconds) is None
    delay1 = worker.backoff_delay(1)
    time.sleep(delay1 + 0.05)

    e2 = store.claim_due("w-test", time.time(), settings.lease_seconds)
    assert e2.attempt == 2
    worker.process_claim(e2)
    time.sleep(worker.backoff_delay(2) + 0.05)

    e3 = store.claim_due("w-test", time.time(), settings.lease_seconds)
    assert e3.attempt == 3
    worker.process_claim(e3)  # attempts 3 > times 2 -> succeeds

    done = store.get(event.id)
    assert done.state is EventState.COMPLETED
    assert done.attempt == 3
    # Exactly one side effect despite two failed + one successful attempt.
    effects = store.list_side_effects(event.id)
    assert len(effects) == 1


def test_retry_exhaustion_enters_dead_letter(worker_env, settings):
    store, proc, worker = worker_env
    event = _submit(store, "t1", "always-fail")
    proc.set_failures({event.id: {"mode": "recoverable_always",
                                  "message": "persistent outage"}})

    for expected_attempt in range(1, settings.max_attempts + 1):
        e = store.claim_due("w-test", time.time(), settings.lease_seconds)
        assert e is not None and e.attempt == expected_attempt
        worker.process_claim(e)
        cur = store.get(event.id)
        if expected_attempt < settings.max_attempts:
            assert cur.state is EventState.PENDING
            time.sleep(worker.backoff_delay(expected_attempt) + 0.05)
        else:
            assert cur.state is EventState.DEAD_LETTER
            assert cur.last_error_code == "recoverable:recoverable"

    transitions = [(t.from_state, t.to_state)
                   for t in store.list_transitions(event.id)]
    assert transitions[-1][1] == EventState.DEAD_LETTER.value
    assert len(store.list_dlq("t1")) == 1
    # No side effect ever happened.
    assert store.list_side_effects(event.id) == []


def test_fatal_error_goes_straight_to_dlq(worker_env):
    store, proc, worker = worker_env
    event = _submit(store, "t1", "fatal-one")
    proc.set_failures({event.id: {"mode": "fatal", "times": 1,
                                  "message": "validation impossible"}})
    e = store.claim_due("w-test", time.time(), 60)
    worker.process_claim(e)
    cur = store.get(event.id)
    assert cur.state is EventState.DEAD_LETTER
    assert cur.attempt == 1
    assert cur.last_error_code == "fatal:fatal"


def test_dead_letter_replay_succeeds_without_duplicate_effect(
        worker_env, settings):
    store, proc, worker = worker_env
    event = _submit(store, "t1", "replay-me")

    # First life: the external effect was delivered once (e.g. a partial
    # success), but responses kept being lost, so retries exhausted into DLQ.
    assert store.insert_side_effect(
        event.id, f"order.created:t1:replay-me", "partial delivery",
        time.time()) is True
    proc.set_failures({event.id: {"mode": "recoverable_always",
                                  "message": "lost acks"}})
    for attempt in range(1, settings.max_attempts + 1):
        e = store.claim_due("w-test", time.time(), settings.lease_seconds)
        assert e is not None and e.attempt == attempt
        worker.process_claim(e)
        cur = store.get(event.id)
        if cur.state is EventState.PENDING:
            time.sleep(worker.backoff_delay(attempt) + 0.05)
    assert store.get(event.id).state is EventState.DEAD_LETTER
    assert len(store.list_side_effects(event.id)) == 1

    # Operator fixes the downstream and replays; processor now succeeds.
    proc.set_failures({event.id: {"mode": "success"}})
    replayed = store.replay_dead_letter(event.id, time.time())
    assert replayed.state is EventState.PENDING
    assert replayed.replay_count == 1
    assert replayed.attempt == 0

    _drive(store, worker)
    done = store.get(event.id)
    assert done.state is EventState.COMPLETED
    assert done.replay_count == 1
    # The replay re-ran the handler but the ledger suppressed the duplicate.
    effects = store.list_side_effects(event.id)
    assert len(effects) == 1
    assert done.result["effect_performed"] is False


def test_replay_rejects_non_dead_letter_event(worker_env):
    store, proc, worker = worker_env
    event = _submit(store, "t1", "still-pending")
    from app.errors import InvalidStateError
    try:
        store.replay_dead_letter(event.id, time.time())
        raise AssertionError("expected InvalidStateError")
    except InvalidStateError:
        pass


def test_backoff_grows_exponentially_and_is_capped(worker_env, settings):
    _, _, worker = worker_env
    d1 = worker.backoff_delay(1)
    d2 = worker.backoff_delay(2)
    d3 = worker.backoff_delay(3)
    assert d1 == settings.retry_base_delay
    assert d2 > d1 and d3 > d2
    assert worker.backoff_delay(99) <= settings.retry_max_delay
