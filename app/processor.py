"""处理状态机：领取 -> 执行处理器 -> 成功 / 重试 / 死信。

日志固定打印幂等键、尝试次数与状态迁移，例如：
    [idem=t1:k1] attempt=1 ACCEPTED->PROCESSING
    [idem=t1:k1] attempt=2 PROCESSING->RETRYING reason=TRANSIENT detail=...
    [idem=t1:k1] attempt=3 PROCESSING->DEAD_LETTER reason=RETRY_EXHAUSTED
"""
from __future__ import annotations

from datetime import timedelta

from app.errors import PermanentError, ProcessingError, TransientError
from app.logging_conf import get_logger
from app.models import ErrorKind, RetryPolicy, StoreRecord, utcnow
from app.storage import Storage

log = get_logger("processor")


class ProcessingContext:
    """处理器副作用登记上下文；副作用以 (幂等键, effect_key) 去重。"""

    def __init__(self, storage: Storage, record: StoreRecord):
        self._storage = storage
        self._record = record

    def record_effect(self, effect_key: str) -> bool:
        now = utcnow().isoformat()
        return self._storage.record_effect(
            self._record.idempotency_key, effect_key, now)


class Processor:
    def __init__(self, storage: Storage, handler, retry_policy: RetryPolicy,
                 lease_seconds: float = 30.0):
        self._storage = storage
        self._handler = handler
        self._retry = retry_policy
        self._lease = lease_seconds

    async def process_once(self, owner: str) -> bool:
        """尝试领取并完整处理一条到期事件；返回是否有事件被处理。"""
        now = utcnow()
        lease_expires = (now + timedelta(seconds=self._lease)).isoformat()
        record = self._storage.claim_due(owner, now.isoformat(), lease_expires)
        if record is None:
            return False

        attempts = record.attempts + 1
        idem = record.idempotency_key
        log.info("[idem=%s] attempt=%d %s->PROCESSING owner=%s",
                 idem, attempts, record.status, owner)

        ctx = ProcessingContext(self._storage, record)
        try:
            await self._handler.handle(record, ctx)
        except ProcessingError as exc:
            self._settle(record.id, owner, attempts, exc.kind, str(exc),
                         record.replayed == 1)
        except Exception as exc:  # 未声明错误按可恢复处理，但会被显式标注
            log.warning("[idem=%s] attempt=%d undeclared error: %r",
                        idem, attempts, exc)
            self._settle(record.id, owner, attempts, ErrorKind.TRANSIENT,
                         f"undeclared:{type(exc).__name__}:{exc}",
                         record.replayed == 1)
        else:
            now_iso = utcnow().isoformat()
            self._storage.mark_succeeded(record.id, owner, now_iso, attempts)
            log.info("[idem=%s] attempt=%d PROCESSING->SUCCEEDED",
                     idem, attempts)
        return True

    def _settle(self, event_id: int, owner: str, attempts: int,
                kind: ErrorKind, detail: str, replayed: bool) -> None:
        now = utcnow()
        idem_view = self._storage.get_by_id(event_id)
        idem = idem_view.idempotency_key if idem_view else event_id

        if kind is ErrorKind.PERMANENT:
            self._storage.mark_dead(event_id, owner, now.isoformat(),
                                    attempts, ErrorKind.PERMANENT.value, detail)
            log.info("[idem=%s] attempt=%d PROCESSING->DEAD_LETTER"
                     " reason=PERMANENT detail=%s", idem, attempts, detail)
            return

        exhausted = attempts >= self._retry.max_attempts
        if exhausted:
            self._storage.mark_dead(event_id, owner, now.isoformat(),
                                    attempts, ErrorKind.RETRY_EXHAUSTED.value,
                                    detail)
            log.info("[idem=%s] attempt=%d PROCESSING->DEAD_LETTER"
                     " reason=RETRY_EXHAUSTED detail=%s",
                     idem, attempts, detail)
            return

        visible = (now + timedelta(
            seconds=self._retry.delay_for(attempts))).isoformat()
        self._storage.mark_retry(event_id, owner, now.isoformat(), attempts,
                                 detail, visible)
        log.info("[idem=%s] attempt=%d PROCESSING->RETRYING reason=TRANSIENT"
                 " detail=%s visible_at=%s", idem, attempts, detail, visible)
