"""Shared pytest fixtures: isolated DB, fast backoff, API client."""
from __future__ import annotations

import logging
import time

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings

# Unit-test logs must show idempotency keys, attempt counts and state moves.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def make_settings(tmp_path, **overrides) -> Settings:
    defaults = dict(
        db_path=str(tmp_path / "events.db"),
        worker_poll_interval=0.01,
        lease_seconds=30.0,          # long lease: crash reclaim tested separately
        retry_base_delay=0.02,
        retry_max_delay=0.2,
        retry_backoff_factor=2.0,
        max_attempts=3,
        tenant_quota=5,
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture()
def settings(tmp_path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture()
def store(settings):
    from app.storage import EventStore
    return EventStore(settings.db_path, tenant_quota=settings.tenant_quota)


@pytest.fixture()
def worker_env(settings):
    """Store + processor + gateway + a manually driven worker (no loop)."""
    from app.processor import EventProcessor
    from app.sideeffects import SideEffectGateway
    from app.storage import EventStore
    from app.worker import Worker

    s = EventStore(settings.db_path, tenant_quota=settings.tenant_quota)
    proc = EventProcessor(SideEffectGateway(s))
    w = Worker(s, proc, settings, worker_id="w-test")
    return s, proc, w


@pytest.fixture()
def client(settings):
    app = create_app(settings, start_worker=True)
    with TestClient(app) as c:
        yield c
    app.state.worker.stop()


def wait_for_state(client, event_id: str, states: set[str],
                   timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = client.get(f"/events/{event_id}").json()
        if last["state"] in states:
            return last
        time.sleep(0.02)
    raise AssertionError(f"event {event_id} never reached {states}; last={last}")
