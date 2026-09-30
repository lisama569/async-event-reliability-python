"""领域服务：幂等提交、配额、状态查询、死信重放。

提交拒绝原因（RejectReason，HTTP 409/422 可区分）：
    DUPLICATE_KEY    同一 tenant_id + business_key 的幂等键已存在
    QUOTA_EXCEEDED   滑动窗口内事件数达到配额上限
    INVALID_EVENT    事件类型非法 / 字段不合法
"""
from __future__ import annotations

import json
import sqlite3
from datetime import timedelta

from app.errors import RejectError
from app.logging_conf import get_logger
from app.models import (
    EventStatus,
    EventStatusView,
    EventSubmit,
    EventSubmitResult,
    ErrorKind,
    RejectReason,
    ReplayResult,
    RetryPolicy,
    utcnow,
)
from app.processor import Processor
from app.storage import DuplicateKeyError, Storage

log = get_logger("service")

ALLOWED_EVENT_TYPES = frozenset({
    "order.created", "payment.captured", "user.signed_up", "test.echo",
})


class EventService:
    def __init__(self, storage: Storage, handler,
                 retry_policy: RetryPolicy | None = None,
                 quota_limit: int = 100,
                 quota_window_seconds: float = 60.0,
                 lease_seconds: float = 30.0):
        self._storage = storage
        self._retry = retry_policy or RetryPolicy()
        self._quota_limit = quota_limit
        self._quota_window = quota_window_seconds
        self._processor = Processor(storage, handler, self._retry,
                                    lease_seconds=lease_seconds)

    # ---- 提交 ---------------------------------------------------------

    async def submit(self, event: EventSubmit) -> EventSubmitResult:
        if event.event_type not in ALLOWED_EVENT_TYPES:
            log.info("reject idem=%s reason=INVALID_EVENT type=%s",
                     self._idem(event), event.event_type)
            raise RejectError(
                RejectReason.INVALID_EVENT,
                f"unsupported event_type: {event.event_type}")

        idem = self._idem(event)
        payload_json = json.dumps(event.payload, ensure_ascii=False,
                                  sort_keys=True)
        now = utcnow()
        window_start = (now - timedelta(seconds=self._quota_window)).isoformat()

        try:
            record = self._storage.insert_accepted(
                tenant_id=event.tenant_id,
                business_key=event.business_key,
                idempotency_key=idem,
                event_type=event.event_type,
                payload_json=payload_json,
                now_iso=now.isoformat(),
                quota_limit=self._quota_limit,
                window_start_iso=window_start,
            )
        except (sqlite3.IntegrityError, DuplicateKeyError):
            existing = self._storage.get_by_key(idem)
            log.info("[idem=%s] reject reason=DUPLICATE_KEY existing_id=%s",
                     idem, existing.id if existing else "?")
            raise RejectError(
                RejectReason.DUPLICATE_KEY,
                f"event already accepted: {idem}")

        if record is None:
            log.info("[idem=%s] reject reason=QUOTA_EXCEEDED", idem)
            raise RejectError(
                RejectReason.QUOTA_EXCEEDED,
                f"tenant quota exceeded: {self._quota_limit}"
                f" events per {self._quota_window}s")

        log.info("[idem=%s] accepted event_id=%d status=ACCEPTED",
                 idem, record.id)
        return EventSubmitResult(event_id=record.id, idempotency_key=idem,
                                 status=EventStatus.ACCEPTED, duplicate=False)

    # ---- 查询 ---------------------------------------------------------

    async def get(self, event_id: int) -> EventStatusView | None:
        record = self._storage.get_by_id(event_id)
        return self._to_view(record) if record else None

    async def replay(self, event_id: int) -> ReplayResult:
        record = self._storage.get_by_id(event_id)
        if record is None:
            raise LookupError(f"event not found: {event_id}")
        if record.status != EventStatus.DEAD_LETTER.value:
            raise RejectError(
                RejectReason.INVALID_EVENT,
                f"only DEAD_LETTER events can be replayed, got {record.status}")
        now = utcnow().isoformat()
        ok = self._storage.reset_for_replay(event_id, now)
        if not ok:
            raise RejectError(RejectReason.INVALID_EVENT,
                              "replay lost the CAS race")
        log.info("[idem=%s] event_id=%d DEAD_LETTER->ACCEPTED (manual replay)",
                 record.idempotency_key, event_id)
        return ReplayResult(event_id=event_id, status=EventStatus.ACCEPTED)

    # ---- 消费（由 worker 驱动） ---------------------------------------

    async def process_one_claim(self, owner: str) -> bool:
        return await self._processor.process_once(owner)

    # ---- 内部 ---------------------------------------------------------

    @staticmethod
    def _idem(event: EventSubmit) -> str:
        return f"{event.tenant_id}:{event.business_key}"

    @staticmethod
    def _to_view(record) -> EventStatusView:
        return EventStatusView(
            event_id=record.id,
            tenant_id=record.tenant_id,
            business_key=record.business_key,
            idempotency_key=record.idempotency_key,
            status=EventStatus(record.status),
            attempts=record.attempts,
            error_kind=ErrorKind(record.error_kind) if record.error_kind else None,
            error_detail=record.error_detail,
            created_at=record.created_at,
            updated_at=record.updated_at,
            replayed=bool(record.replayed),
        )
