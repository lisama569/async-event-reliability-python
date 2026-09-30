"""重启恢复：数据/状态持久化，处理进程崩溃后租约可被接管。"""
from __future__ import annotations

import asyncio

import pytest

from app.models import EventStatus, EventSubmit, RetryPolicy
from app.service import EventService
from app.sim_handler import SimulatedHandler
from app.storage import Storage
from app.worker import WorkerGroup

from tests.conftest import wait_for_status


def _make_service(db_path: str, *, lease_seconds: float = 5.0,
                  handler=None) -> EventService:
    policy = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0)
    return EventService(Storage(db_path), handler or SimulatedHandler(),
                        retry_policy=policy, quota_limit=100,
                        lease_seconds=lease_seconds)


@pytest.mark.asyncio
async def test_succeeded_state_survives_restart(tmp_db):
    svc = _make_service(tmp_db)
    g = WorkerGroup(svc, concurrency=2)
    await g.start()
    try:
        r = await svc.submit(EventSubmit(
            tenant_id="t", business_key="persist", event_type="test.echo",
            payload={"mode": "ok"}))
        await wait_for_status(svc, r.event_id, EventStatus.SUCCEEDED)
    finally:
        await g.stop()
    svc._storage.close()  # noqa: SLF001

    # 模拟服务重启：重新打开同一数据库文件
    svc2 = _make_service(tmp_db)
    view = await svc2.get(r.event_id)
    assert view.status is EventStatus.SUCCEEDED
    assert view.attempts == 1
    # 重启后重复提交同一幂等键仍然被拒绝
    from app.errors import RejectError
    from app.models import RejectReason
    with pytest.raises(RejectError) as ei:
        await svc2.submit(EventSubmit(
            tenant_id="t", business_key="persist", event_type="test.echo"))
    assert ei.value.reason is RejectReason.DUPLICATE_KEY
    svc2._storage.close()  # noqa: SLF001


class SlowHandler:
    """第一次被领取时挂起，模拟处理进程在 PROCESSING 中崩溃。"""

    def __init__(self):
        self.entered = asyncio.Event()

    async def handle(self, record, ctx):
        self.entered.set()
        await asyncio.sleep(10)  # 永远不返回（由测试取消）


@pytest.mark.asyncio
async def test_expired_lease_claimed_by_another_worker(tmp_db):
    slow = SlowHandler()
    # 极短租约：0.05s 后即可被接管
    svc = _make_service(tmp_db, lease_seconds=0.05, handler=slow)
    r = await svc.submit(EventSubmit(
        tenant_id="t", business_key="leased", event_type="test.echo",
        payload={"mode": "ok"}))

    owner = "dying-worker"
    task = asyncio.create_task(svc.process_one_claim(owner))
    await asyncio.wait_for(slow.entered.wait(), timeout=2.0)
    task.cancel()  # 处理进程崩溃，没提交任何结果
    with pytest.raises(asyncio.CancelledError):
        await task

    # 用新存储连接（=新进程）接管：租约过期后可重新领取并成功
    svc._storage.close()  # noqa: SLF001
    svc2 = _make_service(tmp_db, lease_seconds=5.0)
    g = WorkerGroup(svc2, concurrency=2)
    await g.start()
    try:
        view = await wait_for_status(svc2, r.event_id,
                                    EventStatus.SUCCEEDED, timeout=3.0)
    finally:
        await g.stop()
    assert view.attempts >= 2  # 第一次是崩溃前的领取
    svc2._storage.close()  # noqa: SLF001


@pytest.mark.asyncio
async def test_retrying_event_processed_after_restart(tmp_db):
    # 第一次运行：事件进入 RETRYING
    svc = _make_service(tmp_db)
    g = WorkerGroup(svc, concurrency=1)
    await g.start()
    try:
        r = await svc.submit(EventSubmit(
            tenant_id="t", business_key="later", event_type="test.echo",
            payload={"mode": "transient", "fail_until_replay": False,
                     "fail_times": 1}))
        # 等待至少一次重试进入 RETRYING/SUCCEEDED
        for _ in range(200):
            v = await svc.get(r.event_id)
            if v.attempts >= 1:
                break
            await asyncio.sleep(0.01)
    finally:
        await g.stop()
    svc._storage.close()  # noqa: SLF001

    # 重启新进程继续消费，最终成功
    svc2 = _make_service(tmp_db)
    g2 = WorkerGroup(svc2, concurrency=2)
    await g2.start()
    try:
        view = await wait_for_status(svc2, r.event_id,
                                    EventStatus.SUCCEEDED)
    finally:
        await g2.stop()
    assert view.attempts == 2
    svc2._storage.close()  # noqa: SLF001
