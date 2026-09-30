"""退避重试、重试耗尽与不可恢复错误。"""
from __future__ import annotations

import asyncio

import pytest

from app.models import ErrorKind, EventStatus, EventSubmit
from app.worker import WorkerGroup

from tests.conftest import wait_for_status


@pytest.mark.asyncio
async def test_transient_retries_then_succeeds(service):
    g = WorkerGroup(service, concurrency=2)
    await g.start()
    try:
        r = await service.submit(EventSubmit(
            tenant_id="t", business_key="retry-ok", event_type="test.echo",
            payload={"mode": "transient", "fail_times": 2,
                     "effects": ["send-mail"]}))
        view = await wait_for_status(service, r.event_id,
                                    EventStatus.SUCCEEDED)
        assert view.attempts == 3
        # 副作用在第一次尝试已登记，后续尝试被抑制 -> 只执行一次
        assert service._storage.record_effect(  # noqa: SLF001
            "t:retry-ok", "send-mail", "later") is False
    finally:
        await g.stop()


@pytest.mark.asyncio
async def test_retry_exhausted_enters_dead_letter(service):
    g = WorkerGroup(service, concurrency=2)
    await g.start()
    try:
        r = await service.submit(EventSubmit(
            tenant_id="t", business_key="always-fail", event_type="test.echo",
            payload={"mode": "transient", "fail_times": 99}))
        view = await wait_for_status(service, r.event_id,
                                    EventStatus.DEAD_LETTER)
        assert view.attempts == 3
        assert view.error_kind is ErrorKind.RETRY_EXHAUSTED
        assert "transient" in view.error_detail
    finally:
        await g.stop()


@pytest.mark.asyncio
async def test_permanent_error_dead_letter_immediately(service):
    g = WorkerGroup(service, concurrency=1)
    await g.start()
    try:
        r = await service.submit(EventSubmit(
            tenant_id="t", business_key="perm", event_type="test.echo",
            payload={"mode": "permanent"}))
        view = await wait_for_status(service, r.event_id,
                                    EventStatus.DEAD_LETTER)
        assert view.attempts == 1
        assert view.error_kind is ErrorKind.PERMANENT
    finally:
        await g.stop()


def test_backoff_delay_grows_and_caps():
    from app.models import RetryPolicy
    p = RetryPolicy(base_delay=0.1, max_delay=1.0, multiplier=2.0)
    assert p.delay_for(1) == 0.1
    assert p.delay_for(2) == 0.2
    assert p.delay_for(3) == 0.4
    assert p.delay_for(10) == 1.0
