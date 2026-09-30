"""FastAPI 路由与应用装配。

拒绝原因映射（HTTP 状态 + body.reason）：
    409 DUPLICATE_KEY    幂等键重复（同一 tenant + business_key 已受理）
    429 QUOTA_EXCEEDED   超过租户滑动窗口配额
    422 INVALID_EVENT    事件非法（字段校验失败 / 类型不支持 / 重放状态不对）
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.errors import RejectError
from app.logging_conf import configure_logging, get_logger
from app.models import EventSubmit, RetryPolicy
from app.service import EventService
from app.sim_handler import SimulatedHandler
from app.storage import Storage
from app.worker import WorkerGroup

log = get_logger("api")


def create_app(service: EventService,
               worker_group: WorkerGroup | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if worker_group is not None:
            await worker_group.start()
        try:
            yield
        finally:
            if worker_group is not None:
                await worker_group.stop()

    app = FastAPI(title="async-event-reliability", lifespan=lifespan)

    @app.exception_handler(RejectError)
    async def reject_handler(request, exc: RejectError):
        status_code = {
            "DUPLICATE_KEY": 409,
            "QUOTA_EXCEEDED": 429,
            "INVALID_EVENT": 422,
        }[exc.reason.value]
        return JSONResponse(
            status_code=status_code,
            content={"error": "rejected", "reason": exc.reason.value,
                     "message": str(exc)},
        )

    @app.get("/")
    async def root():
        return {"service": "async-event-reliability", "status": "ok"}

    @app.post("/events", status_code=202)
    async def submit_event(event: EventSubmit):
        result = await service.submit(event)
        return result.model_dump(mode="json")

    @app.get("/events/{event_id}")
    async def get_event(event_id: int):
        view = await service.get(event_id)
        if view is None:
            return JSONResponse(status_code=404,
                                content={"error": "not_found"})
        return view.model_dump(mode="json")

    @app.post("/events/{event_id}/replay", status_code=202)
    async def replay_event(event_id: int):
        try:
            result = await service.replay(event_id)
        except LookupError:
            return JSONResponse(status_code=404,
                                content={"error": "not_found"})
        return result.model_dump(mode="json")

    return app


def build_default_app() -> FastAPI:
    """从环境变量装配一个本地可运行应用（main.py 使用）。"""
    configure_logging()
    db_path = os.environ.get("EVENT_DB_PATH", "./data/events.db")
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
    quota = int(os.environ.get("TENANT_QUOTA", "100"))
    max_attempts = int(os.environ.get("MAX_ATTEMPTS", "3"))

    storage = Storage(db_path)
    handler = SimulatedHandler()
    service = EventService(
        storage, handler,
        retry_policy=RetryPolicy(max_attempts=max_attempts,
                                 base_delay=0.05, max_delay=1.0),
        quota_limit=quota)
    group = WorkerGroup(service, concurrency=concurrency)
    return create_app(service, worker_group=group)


app: FastAPI = build_default_app()
