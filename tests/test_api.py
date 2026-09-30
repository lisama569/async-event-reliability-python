"""HTTP 层：提交/拒绝原因/查询/重放的端到端 HTTP 行为。"""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import create_app
from app.models import EventStatus, EventSubmit, RetryPolicy
from app.service import EventService
from app.sim_handler import SimulatedHandler
from app.storage import Storage
from app.worker import WorkerGroup

from tests.conftest import wait_for_status


def _build(tmp_db, *, concurrency=2, quota=5):
    storage = Storage(tmp_db)
    policy = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0)
    svc = EventService(storage, SimulatedHandler(), retry_policy=policy,
                       quota_limit=quota, lease_seconds=5.0)
    group = WorkerGroup(svc, concurrency=concurrency)
    return storage, svc, group, create_app(svc, worker_group=group)


@pytest.mark.asyncio
async def test_submit_and_query_lifecycle(tmp_db):
    storage, svc, group, app = _build(tmp_db)
    await group.start()
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            resp = await c.post("/events", json={
                "tenant_id": "t1", "business_key": "http-1",
                "event_type": "test.echo",
                "payload": {"mode": "transient", "fail_times": 1}})
            assert resp.status_code == 202
            body = resp.json()
            assert body["status"] == "ACCEPTED"
            event_id = body["event_id"]

            view = await wait_for_status(svc, event_id,
                                         EventStatus.SUCCEEDED)
            resp = await c.get(f"/events/{event_id}")
            assert resp.status_code == 200
            assert resp.json()["status"] == "SUCCEEDED"
            assert resp.json()["attempts"] == view.attempts == 2
    finally:
        await group.stop()
        storage.close()


@pytest.mark.asyncio
async def test_rejection_status_codes_and_reasons(tmp_db):
    storage, svc, group, app = _build(tmp_db, quota=2)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        # 正常两条
        for i in range(2):
            assert (await c.post("/events", json={
                "tenant_id": "t", "business_key": f"k{i}",
                "event_type": "test.echo"})).status_code == 202
        # 重复
        r = await c.post("/events", json={
            "tenant_id": "t", "business_key": "k0",
            "event_type": "test.echo"})
        assert r.status_code == 409 and r.json()["reason"] == "DUPLICATE_KEY"
        # 配额
        r = await c.post("/events", json={
            "tenant_id": "t", "business_key": "new",
            "event_type": "test.echo"})
        assert r.status_code == 429 and r.json()["reason"] == "QUOTA_EXCEEDED"
        # 非法类型
        r = await c.post("/events", json={
            "tenant_id": "t", "business_key": "bad",
            "event_type": "nope"})
        assert r.status_code == 422 and r.json()["reason"] == "INVALID_EVENT"
        # 字段缺失（Pydantic 校验失败 -> 422）
        r = await c.post("/events", json={"tenant_id": "t"})
        assert r.status_code == 422
        # 不存在
        assert (await c.get("/events/99999")).status_code == 404
    storage.close()


@pytest.mark.asyncio
async def test_dead_letter_replay_endpoint(tmp_db):
    storage, svc, group, app = _build(tmp_db)
    await group.start()
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post("/events", json={
                "tenant_id": "t", "business_key": "dl",
                "event_type": "test.echo",
                "payload": {"mode": "permanent"}})
            event_id = r.json()["event_id"]
            await wait_for_status(svc, event_id, EventStatus.DEAD_LETTER)
            # 终态事件再次重放前先确认状态
            r = await c.get(f"/events/{event_id}")
            assert r.json()["status"] == "DEAD_LETTER"
            # 重放后仍是不可恢复错误 -> 回到死信
            assert (await c.post(f"/events/{event_id}/replay")).status_code == 202
            view = await wait_for_status(svc, event_id,
                                         EventStatus.DEAD_LETTER)
            assert view.replayed is True
            # 再次重放仍允许（当前为死信），不会报 404
            r = await c.post(f"/events/{event_id}/replay")
            assert r.status_code == 202
    finally:
        await group.stop()
        storage.close()


@pytest.mark.asyncio
async def test_status_query_does_not_block_consumption(tmp_db):
    """处理重试期间持续高频查询，消费与查询都能推进且状态单调。"""
    storage, svc, group, app = _build(tmp_db, concurrency=2)
    # 非零退避，制造明确的处理窗口
    svc._retry = RetryPolicy(max_attempts=3, base_delay=0.02, max_delay=0.05)
    svc._processor._retry = svc._retry
    await group.start()
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post("/events", json={
                "tenant_id": "t", "business_key": "slowish",
                "event_type": "test.echo",
                "payload": {"mode": "transient", "fail_times": 1}})
            event_id = r.json()["event_id"]
            queries = 0
            seen: list[str] = []
            deadline = asyncio.get_running_loop().time() + 3.0
            while asyncio.get_running_loop().time() < deadline:
                resp = await c.get(f"/events/{event_id}")
                assert resp.status_code == 200
                status = resp.json()["status"]
                if not seen or seen[-1] != status:
                    seen.append(status)
                queries += 1
                if status == "SUCCEEDED":
                    break
                await asyncio.sleep(0.002)
            assert queries > 10
            assert seen[-1] == "SUCCEEDED"
            # 状态迁移单调：一旦成功不再回退
            assert (await c.get(f"/events/{event_id}")).json()["status"] \
                == "SUCCEEDED"
    finally:
        await group.stop()
        storage.close()
