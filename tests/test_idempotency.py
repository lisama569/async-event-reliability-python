"""并发幂等提交：同一租户+业务键并发提交只能成功一次。"""
from __future__ import annotations

import asyncio

import pytest

from app.errors import RejectError
from app.models import EventStatus, EventSubmit, RejectReason


@pytest.mark.asyncio
async def test_concurrent_duplicate_submit_single_success(service):
    event = EventSubmit(tenant_id="t1", business_key="order-1",
                        event_type="order.created", payload={"x": 1})
    results = await asyncio.gather(
        *[service.submit(event.model_copy(deep=True)) for _ in range(20)],
        return_exceptions=True,
    )
    accepted = [r for r in results if not isinstance(r, Exception)]
    rejected = [r for r in results if isinstance(r, RejectError)]
    assert len(accepted) == 1
    assert len(rejected) == 19
    assert all(r.reason is RejectReason.DUPLICATE_KEY for r in rejected)
    # 重复提交返回的是同一条事件
    view = await service.get(accepted[0].event_id)
    assert view.status in {EventStatus.ACCEPTED, EventStatus.PROCESSING,
                           EventStatus.SUCCEEDED}


@pytest.mark.asyncio
async def test_sequential_duplicate_rejected_with_reason(service):
    event = EventSubmit(tenant_id="t1", business_key="b",
                        event_type="test.echo")
    first = await service.submit(event)
    with pytest.raises(RejectError) as ei:
        await service.submit(event)
    assert ei.value.reason is RejectReason.DUPLICATE_KEY
    assert first.duplicate is False


@pytest.mark.asyncio
async def test_different_business_keys_both_accepted(service):
    r1 = await service.submit(EventSubmit(
        tenant_id="t1", business_key="a", event_type="test.echo"))
    r2 = await service.submit(EventSubmit(
        tenant_id="t1", business_key="b", event_type="test.echo"))
    assert r1.event_id != r2.event_id
