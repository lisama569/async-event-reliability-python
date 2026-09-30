"""死信恢复：人工重放不能产生重复副作用，且只有死信可重放。"""
from __future__ import annotations

import pytest

from app.errors import RejectError
from app.models import ErrorKind, EventStatus, EventSubmit, RejectReason
from app.worker import WorkerGroup

from tests.conftest import wait_for_status


async def _drive_to_dead(service, key, payload):
    g = WorkerGroup(service, concurrency=2)
    await g.start()
    try:
        r = await service.submit(EventSubmit(
            tenant_id="t", business_key=key, event_type="test.echo",
            payload=payload))
        await wait_for_status(service, r.event_id, EventStatus.DEAD_LETTER)
        return r.event_id
    finally:
        await g.stop()


@pytest.mark.asyncio
async def test_replay_dead_letter_succeeds_without_duplicate_effects(service):
    storage = service._storage  # noqa: SLF001
    event_id = await _drive_to_dead(
        service, "dl-1",
        {"mode": "transient", "fail_until_replay": True,
         "effects": ["coupon"]})

    # 进死信前副作用已经执行过一次
    assert storage.record_effect("t:dl-1", "coupon", "probe") is False

    # 人工重放：用 fail_until_replay 让重放后成功
    replay_resp = await service.replay(event_id)
    assert replay_resp.status is EventStatus.ACCEPTED

    g = WorkerGroup(service, concurrency=2)
    await g.start()
    try:
        view = await wait_for_status(service, event_id,
                                    EventStatus.SUCCEEDED)
    finally:
        await g.stop()
    assert view.replayed is True
    assert view.error_kind is None
    # 重放后副作用仍然只登记过一次（probe 返回 False 说明已存在）
    assert storage.record_effect("t:dl-1", "coupon", "probe2") is False


@pytest.mark.asyncio
async def test_replay_permanent_error_still_suppresses_effects(service):
    storage = service._storage  # noqa: SLF001
    event_id = await _drive_to_dead(
        service, "dl-2",
        {"mode": "permanent", "effects": ["grant"]})
    await service.replay(event_id)
    g = WorkerGroup(service, concurrency=2)
    await g.start()
    try:
        view = await wait_for_status(service, event_id,
                                    EventStatus.DEAD_LETTER)
    finally:
        await g.stop()
    assert view.replayed is True
    assert view.error_kind is ErrorKind.PERMANENT
    assert storage.record_effect("t:dl-2", "grant", "probe") is False


@pytest.mark.asyncio
async def test_only_dead_letter_can_be_replayed(service):
    r = await service.submit(EventSubmit(
        tenant_id="t", business_key="alive", event_type="test.echo"))
    with pytest.raises(RejectError) as ei:
        await service.replay(r.event_id)
    assert ei.value.reason is RejectReason.INVALID_EVENT


@pytest.mark.asyncio
async def test_replay_unknown_event_raises(service):
    with pytest.raises(LookupError):
        await service.replay(99999)
