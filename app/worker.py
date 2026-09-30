"""Background consumer loop with claim / backoff / DLQ semantics."""
from __future__ import annotations

import asyncio
import logging
import time
import uuid

from .config import Settings
from .errors import FatalError, RecoverableError
from .processor import EventProcessor
from .storage import EventStore

logger = logging.getLogger("event.worker")


class Worker:
    """Polls due events, claims a lease, processes, retries or dead-letters.

    Multiple workers (threads or separate OS processes) share the same SQLite
    file; claim_due() is a single atomic write transaction, so any one event
    is processed by at most one worker at a time. A dead worker's leased event
    becomes due again after the lease expires and is redelivered exactly once.
    """

    def __init__(self, store: EventStore, processor: EventProcessor,
                 settings: Settings, worker_id: str | None = None,
         sleep=asyncio.sleep) -> None:
        self.store = store
        self.processor = processor
        self.settings = settings
        self.worker_id = worker_id or f"w-{uuid.uuid4().hex[:8]}"
        self._stop = asyncio.Event()
        self._sleep = sleep

    def stop(self) -> None:
        self._stop.set()

    def backoff_delay(self, attempt: int) -> float:
        s = self.settings
        delay = s.retry_base_delay * (s.retry_backoff_factor ** (attempt - 1))
        return min(delay, s.retry_max_delay)

    async def run(self) -> None:
        """Consume loop until stop() is called."""
        logger.info("worker-start worker_id=%s", self.worker_id)
        while not self._stop.is_set():
            now = time.time()
            event = self.store.claim_due(
                self.worker_id, now, self.settings.lease_seconds)
            if event is None:
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.settings.worker_poll_interval)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
                continue
            # Run the blocking DB/handler work off the event loop thread.
            await asyncio.to_thread(self.process_claim, event)
        logger.info("worker-stop worker_id=%s", self.worker_id)

    def process_claim(self, event) -> None:
        """One attempt; blocking, designed to run in a worker thread."""
        attempt = event.attempt
        try:
            result = self.processor.process(event, attempt)
            self.store.complete(event.id, attempt, self.worker_id,
                                result, time.time())
            return
        except RecoverableError as exc:
            # The except-bound name is deleted after the except block in
            # Python 3, so capture the message while it is still in scope.
            error_code = f"recoverable:{getattr(exc, 'code', 'error')}"
            error_message = str(exc)
            logger.info(
                "attempt-failed-retryable event_id=%s attempt=%s/%s reason=%s",
                event.id, attempt, self.settings.max_attempts, exc)
        except FatalError as exc:
            logger.warning(
                "attempt-failed-fatal event_id=%s attempt=%s reason=%s "
                "-> DEAD_LETTER", event.id, attempt, exc)
            self.store.dead_letter(event.id, attempt, self.worker_id,
                                   f"fatal:{getattr(exc, 'code', 'fatal')}",
                                   str(exc), time.time())
            return
        except Exception as exc:  # defensive: unknown errors are fatal
            logger.exception(
                "attempt-failed-unexpected event_id=%s attempt=%s -> DLQ",
                event.id, attempt)
            self.store.dead_letter(event.id, attempt, self.worker_id,
                                   "fatal:unexpected", repr(exc), time.time())
            return

        if attempt >= self.settings.max_attempts:
            logger.warning(
                "retries-exhausted event_id=%s attempts=%s -> DEAD_LETTER",
                event.id, attempt)
            self.store.dead_letter(event.id, attempt, self.worker_id,
                                   error_code, "retry budget exhausted",
                                   time.time())
        else:
            visible_at = time.time() + self.backoff_delay(attempt)
            self.store.requeue(event.id, attempt, self.worker_id,
                               error_code, error_message, visible_at,
                               time.time())
