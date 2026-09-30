"""Concurrent consumption: one event is never processed twice.

Covers both multiple worker threads on one store and multiple store
connections sharing the same SQLite file (as separate processes would).
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.api import create_app
from app.models import EventState
from app.processor import EventProcessor
from app.sideeffects import SideEffectGateway
from app.storage import EventStore
from app.worker import Worker

from .conftest import make_settings


def test_many_workers_consume_each_event_once(tmp_path):
    settings = make_settings(tmp_path, tenant_quota=100)
    n_events = 30
    n_workers = 6
    stop = threading.Event()
    claimed_ids: list[str] = []
    lock = threading.Lock()

    def run_worker(wid: str) -> None:
        store = EventStore(settings.db_path, settings.tenant_quota)
        proc = EventProcessor(SideEffectGateway(store))
        worker = Worker(store, proc, settings, worker_id=wid)
        while not stop.is_set():
            event = store.claim_due(wid, time.time(), settings.lease_seconds)
            if event is None:
                time.sleep(0.005)
                continue
            with lock:
                claimed_ids.append(event.id)
            worker.process_claim(event)

    producer = EventStore(settings.db_path, settings.tenant_quota)
    event_ids = [
        producer.submit("t-c", f"k{i}", "order.created", {},
                        f"h{i}", time.time()).event.id
        for i in range(n_events)]

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(run_worker, f"w{i}") for i in range(n_workers)]
        deadline = time.time() + 10
        while time.time() < deadline:
            done = sum(1 for eid in event_ids
                       if producer.get(eid).state is EventState.COMPLETED)
            if done == n_events:
                break
            time.sleep(0.02)
        stop.set()
        for f in futures:
            f.result(timeout=5)

    assert len(claimed_ids) == n_events  # exactly one claim per event
    assert sorted(claimed_ids) == sorted(event_ids)
    for eid in event_ids:
        event = producer.get(eid)
        assert event.state is EventState.COMPLETED
        assert event.attempt == 1
        effects = producer.list_side_effects(eid)
        assert len(effects) == 1  # no duplicated side effects


def test_expired_lease_is_redelivered_but_late_finish_loses(settings):
    """A crashed worker's event is reclaimed; its late write is fenced out."""
    store = EventStore(settings.db_path, settings.tenant_quota)
    event = store.submit("t-z", "crashed", "order.created", {},
                         "h", time.time()).event

    # Zombie claims with a short lease and dies mid-processing.
    zombie = store.claim_due("w-zombie", time.time(), lease_seconds=0.05)
    assert zombie.state is EventState.PROCESSING and zombie.owner == "w-zombie"

    time.sleep(0.08)  # lease expires
    takeover = EventStore(settings.db_path, settings.tenant_quota)
    redelivered = takeover.claim_due("w-recovery", time.time(), 30)
    assert redelivered is not None and redelivered.id == event.id
    assert redelivered.attempt == 2 and redelivered.owner == "w-recovery"

    # The zombie wakes up and tries to finish its stale attempt (1): rejected.
    late_ok = store.complete(event.id, attempt=1, worker_id="w-zombie",
                             result={"stale": True}, now=time.time())
    assert late_ok is False
    late_requeue = store.requeue(
        event.id, 1, "w-zombie", "recoverable:late", "late",
        time.time() + 10, time.time())
    assert late_requeue is False

    # The recovery worker completes at attempt 2; result sticks.
    assert takeover.complete(event.id, 2, "w-recovery",
                             {"real": True}, time.time()) is True
    final = store.get(event.id)
    assert final.state is EventState.COMPLETED
    assert final.result == {"real": True}
    assert final.attempt == 2


def test_parallel_workers_via_api(tmp_path):
    """In-process worker + manually driven extra worker: no double processing."""
    from app.processor import EventProcessor as Proc
    from app.sideeffects import SideEffectGateway as GW

    settings = make_settings(tmp_path, tenant_quota=100)
    app = create_app(settings, start_worker=True)
    with TestClient(app) as client:
        ids = []
        for i in range(10):
            r = client.post("/events", json={
                "tenant_id": "t-api", "business_key": f"k{i}",
                "event_type": "payment.captured", "payload": {"n": i}})
            assert r.status_code == 201
            ids.append(r.json()["event"]["id"])

        extra_store = EventStore(settings.db_path, settings.tenant_quota)
        extra = Worker(extra_store, Proc(GW(extra_store)),
                       settings, worker_id="w-extra")
        deadline = time.time() + 10
        while time.time() < deadline:
            event = extra_store.claim_due(
                "w-extra", time.time(), settings.lease_seconds)
            if event is not None:
                extra.process_claim(event)
            states = [client.get(f"/events/{eid}").json()["state"]
                      for eid in ids]
            if all(s == "COMPLETED" for s in states):
                break
            time.sleep(0.01)

        for eid in ids:
            detail = client.get(f"/events/{eid}").json()
            assert detail["state"] == "COMPLETED"
            effects = client.get(f"/events/{eid}/side-effects").json()[
                "side_effects"]
            assert len(effects) == 1
