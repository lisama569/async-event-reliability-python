"""FastAPI inbound API: submit / status / list / DLQ / replay.

The API process also hosts an in-process consumer task by default, so the
project runs locally with a single command. For multi-process consumption,
start extra `python main.py worker` processes against the same EVENT_DB_PATH.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import Settings, load_settings
from .errors import RejectedError
from .models import Event
from .processor import EventProcessor
from .sideeffects import SideEffectGateway
from .storage import EventStore
from .worker import Worker

logger = logging.getLogger("event.api")

ALLOWED_TYPES = frozenset(
    {"order.created", "payment.captured", "user.registered"})
MAX_KEY_LEN = 200
MAX_TENANT_LEN = 100


class SubmitRequest(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=MAX_TENANT_LEN)
    business_key: str = Field(min_length=1, max_length=MAX_KEY_LEN)
    event_type: str = Field(min_length=1, max_length=100)
    payload: dict = Field(default_factory=dict)


class _RejectedResponse(JSONResponse):
    pass


def _request_hash(event_type: str, payload: dict) -> str:
    """Stable hash of the semantic request body (excludes the routing key)."""
    body = json.dumps(
        {"event_type": event_type, "payload": payload},
        separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _event_to_json(event: Event) -> dict:
    return {
        "id": event.id,
        "tenant_id": event.tenant_id,
        "business_key": event.business_key,
        "event_type": event.event_type,
        "payload": event.payload,
        "state": event.state.value,
        "attempt": event.attempt,
        "replay_count": event.replay_count,
        "result": event.result,
        "last_error_code": event.last_error_code,
        "last_error_message": event.last_error_message,
        "visible_at": event.visible_at,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
    }


def _validate(req: SubmitRequest, raw_size: int, max_bytes: int) -> None:
    """Validate inbound request; raise InvalidEventError with a precise code."""
    from .errors import InvalidEventError

    if raw_size > max_bytes:
        raise InvalidEventError(f"request body exceeds {max_bytes} bytes")
    if req.tenant_id != req.tenant_id.strip():
        raise InvalidEventError("tenant_id must not contain surrounding spaces")
    if req.business_key != req.business_key.strip():
        raise InvalidEventError(
            "business_key must not contain surrounding spaces")
    if req.event_type not in ALLOWED_TYPES:
        raise InvalidEventError(
            f"event_type '{req.event_type}' is not supported; allowed: "
            f"{sorted(ALLOWED_TYPES)}")
    if not isinstance(req.payload, dict):
        raise InvalidEventError("payload must be a JSON object")


def create_app(settings: Settings | None = None,
               start_worker: bool = True) -> FastAPI:
    settings = settings or load_settings()
    app = FastAPI(title="Async Event Reliability", version="0.1.0")

    store = EventStore(settings.db_path, tenant_quota=settings.tenant_quota)
    processor = EventProcessor(SideEffectGateway(store))
    worker = Worker(store, processor, settings)

    app.state.settings = settings
    app.state.store = store
    app.state.processor = processor
    app.state.worker = worker
    worker_task: asyncio.Task | None = None

    @app.exception_handler(RejectedError)
    async def rejected_handler(request: Request, exc: RejectedError):
        logger.info("rejected code=%s reason=%s", exc.code, exc)
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.code, "message": str(exc)},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request,
                                 exc: RequestValidationError):
        # Malformed requests are invalid events (400), not generic 422s.
        logger.info("rejected code=invalid_event reason=schema %s", exc.errors())
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_event",
                     "message": "request schema validation failed",
                     "details": exc.errors()},
        )

    @app.post("/events")
    async def submit_event(request: Request, req: SubmitRequest):
        raw = await request.body()
        _validate(req, len(raw), settings.request_max_bytes)
        rhash = _request_hash(req.event_type, req.payload)
        result = await asyncio.to_thread(
            store.submit, req.tenant_id, req.business_key, req.event_type,
            req.payload, rhash, time.time())
        code = 200 if result.duplicate else 201
        return JSONResponse(
            status_code=code,
            content={
                "duplicate": result.duplicate,
                "event": _event_to_json(result.event),
            },
        )

    @app.get("/events/{event_id}")
    async def get_event(event_id: str):
        # Plain indexed read; never takes the write lock, so status queries do
        # not block or slow down consumers.
        event = store.get_required(event_id)
        return _event_to_json(event)

    @app.get("/tenants/{tenant_id}/events")
    async def list_tenant_events(tenant_id: str, limit: int = 100):
        limit = max(1, min(limit, 500))
        events = await asyncio.to_thread(
            store.list_events, tenant_id, limit)
        return {"events": [_event_to_json(e) for e in events]}

    @app.get("/events/{event_id}/transitions")
    async def get_transitions(event_id: str):
        store.get_required(event_id)
        transitions = await asyncio.to_thread(
            store.list_transitions, event_id)
        return {"transitions": [
            {"from_state": t.from_state, "to_state": t.to_state,
             "attempt": t.attempt, "reason": t.reason,
             "worker_id": t.worker_id, "at": t.at}
            for t in transitions]}

    @app.get("/events/{event_id}/side-effects")
    async def get_side_effects(event_id: str):
        store.get_required(event_id)
        effects = await asyncio.to_thread(store.list_side_effects, event_id)
        return {"side_effects": effects}

    @app.get("/dead-letters")
    async def list_dead_letters(tenant_id: str | None = None):
        events = await asyncio.to_thread(store.list_dlq, tenant_id)
        return {"events": [_event_to_json(e) for e in events]}

    @app.post("/events/{event_id}/replay", status_code=200)
    async def replay_event(event_id: str):
        event = await asyncio.to_thread(
            store.replay_dead_letter, event_id, time.time())
        return {"replayed": True, "event": _event_to_json(event)}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    if start_worker:
        @app.on_event("startup")
        async def _start_worker():
            nonlocal worker_task
            worker_task = asyncio.create_task(worker.run())

        @app.on_event("shutdown")
        async def _stop_worker():
            worker.stop()
            if worker_task is not None:
                try:
                    await asyncio.wait_for(worker_task, timeout=5)
                except (asyncio.TimeoutError, TimeoutError):
                    worker_task.cancel()

    return app


app = create_app()
