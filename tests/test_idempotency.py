"""Concurrent idempotent submission and distinguishable rejection codes."""
from __future__ import annotations

import threading

import pytest


def _submit(client, body, results, idx):
    results[idx] = client.post("/events", json=body)


def test_concurrent_duplicate_submission_creates_one_event(client):
    """N concurrent identical requests -> exactly one effective event."""
    body = {"tenant_id": "t-a", "business_key": "bk-1",
            "event_type": "order.created", "payload": {"amount": 100}}
    n = 12
    results: list = [None] * n
    threads = [threading.Thread(target=_submit,
                                args=(client, body, results, i))
               for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    statuses = sorted(r.status_code for r in results)
    assert statuses[0] in (200, 201)
    created = [r for r in results if r.status_code == 201]
    duplicates = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(duplicates) == n - 1
    event_ids = {r.json()["event"]["id"] for r in results}
    assert len(event_ids) == 1

    listing = client.get("/tenants/t-a/events").json()["events"]
    assert len(listing) == 1  # duplicates consumed no quota / no new row


def test_key_conflict_is_distinguishable_from_duplicate(client):
    body_v1 = {"tenant_id": "t-b", "business_key": "bk",
               "event_type": "order.created", "payload": {"v": 1}}
    body_v2 = dict(body_v1, payload={"v": 2})
    assert client.post("/events", json=body_v1).status_code == 201

    # Same body -> benign duplicate (200, duplicate=true)
    same = client.post("/events", json=body_v1)
    assert same.status_code == 200 and same.json()["duplicate"] is True

    # Same key, different body -> 409 key_conflict
    conflict = client.post("/events", json=body_v2)
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "key_conflict"


@pytest.mark.parametrize("bad_body,expected_code", [
    ({"tenant_id": "", "business_key": "k", "event_type": "order.created",
      "payload": {}}, "invalid_event"),                      # missing tenant
    ({"tenant_id": "t", "business_key": "k",
      "event_type": "unknown.type", "payload": {}},
     "invalid_event"),                                       # bad event type
    ({"tenant_id": "t", "business_key": "",
      "event_type": "order.created", "payload": {}},
     "invalid_event"),                                       # empty key
])
def test_invalid_events_rejected_with_code(client, bad_body, expected_code):
    resp = client.post("/events", json=bad_body)
    assert resp.status_code == 400
    assert resp.json()["error"] == expected_code


def test_quota_exceeded_rejected(client, settings):
    for i in range(settings.tenant_quota):
        resp = client.post("/events", json={
            "tenant_id": "t-q", "business_key": f"k{i}",
            "event_type": "user.registered", "payload": {}})
        assert resp.status_code == 201
    over = client.post("/events", json={
        "tenant_id": "t-q", "business_key": "overflow",
        "event_type": "user.registered", "payload": {}})
    assert over.status_code == 429
    assert over.json()["error"] == "quota_exceeded"

    # A duplicate of an existing key still succeeds: repeats don't cost quota.
    dup = client.post("/events", json={
        "tenant_id": "t-q", "business_key": "k0",
        "event_type": "user.registered", "payload": {}})
    assert dup.status_code == 200 and dup.json()["duplicate"] is True


def test_unknown_event_returns_not_found(client):
    resp = client.get("/events/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["error"] == "not_found"
