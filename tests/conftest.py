"""pytest 公共夹具：独立临时数据库 + 快速重试策略 + 可选 worker 组。"""
from __future__ import annotations

import asyncio
import os

import pytest
import pytest_asyncio

from app.logging_conf import configure_logging
from app.models import EventStatus, RetryPolicy
from app.service import EventService
from app.sim_handler import SimulatedHandler
from app.storage import Storage
from app.worker import WorkerGroup

configure_logging()


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_events.db")


@pytest_asyncio.fixture
async def storage(tmp_db):
    s = Storage(tmp_db)
    try:
        yield s
    finally:
        s.close()


@pytest_asyncio.fixture
async def service(storage):
    # base_delay=0：测试中退避立即到期，保持确定性与速度
    policy = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0)
    svc = EventService(storage, SimulatedHandler(), retry_policy=policy,
                       quota_limit=5, quota_window_seconds=60.0,
                       lease_seconds=5.0)
    yield svc


async def wait_for_status(svc, event_id: int, expected: EventStatus,
                          timeout: float = 3.0) -> dict:
    """轮询状态直到达到期望（终态）或超时。"""
    deadline = asyncio.get_running_loop().time() + timeout
    view = None
    while asyncio.get_running_loop().time() < deadline:
        view = await svc.get(event_id)
        if view is not None and view.status is expected:
            return view
        await asyncio.sleep(0.005)
    raise AssertionError(
        f"event {event_id} did not reach {expected}, last={view}")
