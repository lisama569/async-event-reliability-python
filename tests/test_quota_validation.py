"""配额超额与非法事件拒绝；三类拒绝原因必须可区分。"""
from __future__ import annotations

import pytest

from app.errors import RejectError
from app.models import EventSubmit, RejectReason


async def _expect_reason(service, event: EventSubmit,
                         reason: RejectReason) -> RejectError:
    with pytest.raises(RejectError) as ei:
        await service.submit(event)
    assert ei.value.reason is reason
    return ei.value


@pytest.mark.asyncio
async def test_quota_exceeded_distinguishable(service):
    for i in range(5):
        await service.submit(EventSubmit(
            tenant_id="q", business_key=f"k{i}", event_type="test.echo"))
    await _expect_reason(
        service,
        EventSubmit(tenant_id="q", business_key="k-overflow",
                    event_type="test.echo"),
        RejectReason.QUOTA_EXCEEDED)


@pytest.mark.asyncio
async def test_unsupported_event_type_rejected(service):
    await _expect_reason(
        service,
        EventSubmit(tenant_id="t", business_key="bad-type",
                    event_type="evil.happened"),
        RejectReason.INVALID_EVENT)


@pytest.mark.asyncio
async def test_three_reasons_are_distinct(service):
    # 占满 m 租户配额
    for i in range(5):
        await service.submit(EventSubmit(
            tenant_id="m", business_key=f"k{i}", event_type="test.echo"))

    dup = await _expect_reason(
        service,
        EventSubmit(tenant_id="m", business_key="k0",
                    event_type="test.echo"),
        RejectReason.DUPLICATE_KEY)

    quota = await _expect_reason(
        service,
        EventSubmit(tenant_id="m", business_key="brand-new",
                    event_type="test.echo"),
        RejectReason.QUOTA_EXCEEDED)

    invalid = await _expect_reason(
        service,
        EventSubmit(tenant_id="m", business_key="bad",
                    event_type="nope"),
        RejectReason.INVALID_EVENT)

    # 原因彼此不同，且消息可解释
    assert dup.reason != quota.reason != invalid.reason
    assert "already accepted" in str(dup)
    assert "quota" in str(quota)
    assert "event_type" in str(invalid)
