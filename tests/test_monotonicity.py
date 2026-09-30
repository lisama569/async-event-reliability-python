"""状态单调：迟到/重复消息不得回滚或覆盖已完成结果。"""
from __future__ import annotations

import pytest

from app.models import ErrorKind, EventStatus, EventSubmit
from app.worker import WorkerGroup

from tests.conftest import wait_for_status


@pytest.mark.asyncio
async def test_late_retry_cannot_overwrite_succeeded(service, storage):
    g = WorkerGroup(service, concurrency=2)
    await g.start()
    try:
        r = await service.submit(EventSubmit(
            tenant_id="t", business_key="mono", event_type="test.echo",
            payload={"mode": "ok"}))
        view = await wait_for_status(service, r.event_id,
                                    EventStatus.SUCCEEDED)
        assert view.attempts == 1
    finally:
        await g.stop()

    # 模拟一条迟到消息：尝试以持锁身份把已成功事件改回 RETRYING/DEAD_LETTER
    now = "2026-01-01T00:00:00+00:00"
    # 无有效租约 -> CAS 必须 0 行受影响
    assert storage.mark_retry(r.event_id, "late-worker", now, 9,
                              "late", now) is False
    assert storage.mark_dead(r.event_id, "late-worker", now, 9,
                             ErrorKind.PERMANENT.value, "late") is False
    view = await service.get(r.event_id)
    assert view.status is EventStatus.SUCCEEDED
    assert view.attempts == 1
    assert view.error_kind is None


@pytest.mark.asyncio
async def test_replay_does_not_affect_succeeded(service, storage):
    g = WorkerGroup(service, concurrency=1)
    await g.start()
    try:
        r = await service.submit(EventSubmit(
            tenant_id="t", business_key="done", event_type="test.echo"))
        await wait_for_status(service, r.event_id, EventStatus.SUCCEEDED)
    finally:
        await g.stop()

    # 对已成功事件执行重放 -> 被拒绝（仅死信可重放），状态不变
    from app.errors import RejectError
    with pytest.raises(RejectError):
        await service.replay(r.event_id)
    view = await service.get(r.event_id)
    assert view.status is EventStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_status_only_moves_forward_during_retry(service, storage):
    """重试点的可见时间被改到过去后，重复投递也不会让 attempts 倒退。"""
    g = WorkerGroup(service, concurrency=1)
    await g.start()
    try:
        r = await service.submit(EventSubmit(
            tenant_id="t", business_key="back", event_type="test.echo",
            payload={"mode": "transient", "fail_times": 2}))
        await wait_for_status(service, r.event_id, EventStatus.SUCCEEDED)
    finally:
        await g.stop()

    view = await service.get(r.event_id)
    assert view.attempts == 3
    assert view.status is EventStatus.SUCCEEDED
