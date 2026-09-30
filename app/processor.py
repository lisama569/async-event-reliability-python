"""Business handler with injectable failure modes for local demos/tests.

Failure specs map an event_id to:
    {"mode": "recoverable", "times": n, "message": "..."}
        raise RecoverableError on the first `times` attempts, then succeed.
    {"mode": "fatal", "times": n, "message": "..."}
        raise FatalError for `times` attempts (usually 1: unrecoverable).
    {"mode": "recoverable_always"}
        fail every attempt until max_attempts is exhausted.
Specs may be injected in-process via set_failures() (tests) or read from a
JSON file pointed at by FAIL_SPEC_PATH (separate worker processes).
"""
from __future__ import annotations

import json
import logging
import os
import time

from .errors import FatalError, RecoverableError
from .sideeffects import SideEffectGateway

FAIL_SPEC_PATH = os.environ.get("FAIL_SPEC_PATH", "data/fail_spec.json")

logger = logging.getLogger("event.processor")


class EventProcessor:
    """Processes one event; records a deduped side effect."""

    def __init__(self, gateway: SideEffectGateway,
                 fail_spec_path: str | None = None) -> None:
        self.gateway = gateway
        self.fail_spec_path = fail_spec_path or FAIL_SPEC_PATH
        # In-process overrides keyed by event_id (preferred over the file).
        self._overrides: dict[str, dict] = {}

    def set_failures(self, spec: dict[str, dict]) -> None:
        """Inject failure specs in-process (keyed by event_id), for tests."""
        self._overrides = dict(spec)

    def _lookup_spec(self, event_id: str) -> dict | None:
        if event_id in self._overrides:
            return self._overrides[event_id]
        path = self.fail_spec_path
        if path and os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    return json.load(fh).get(event_id)
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("cannot read fail spec %s: %s", path, exc)
        return None

    def process(self, event, attempt: int) -> dict:
        """Handle one attempt.

        Raises RecoverableError/FatalError according to the injected spec;
        otherwise records the single idempotent side effect and returns result.
        """
        spec = self._lookup_spec(event.id)
        mode = (spec or {}).get("mode", "success")
        times = int((spec or {}).get("times", 0))
        message = (spec or {}).get("message", "injected failure")

        logger.info(
            "processing event_id=%s idempotency_key=%s/%s attempt=%s mode=%s",
            event.id, event.tenant_id, event.business_key, attempt, mode)

        if mode == "fatal" and attempt <= times:
            logger.info(
                "fatal-failure event_id=%s attempt=%s error_code=fatal:%s",
                event.id, attempt, message)
            raise FatalError(message)
        if mode == "recoverable" and attempt <= times:
            logger.info(
                "recoverable-failure event_id=%s attempt=%s/%s "
                "error_code=recoverable:%s",
                event.id, attempt, times, message)
            raise RecoverableError(message)
        if mode == "recoverable_always":
            logger.info(
                "recoverable-failure event_id=%s attempt=%s "
                "error_code=recoverable:always", event.id, attempt)
            raise RecoverableError(message)

        # The real (simulated) side effect: deduplicated per (event, key),
        # so retries/replays never emit it twice.
        effect_key = f"{event.event_type}:{event.tenant_id}:{event.business_key}"
        performed = self.gateway.perform_once(
            event.id, effect_key,
            f"processed {event.event_type} for {event.tenant_id}")
        result = {
            "handled_event_type": event.event_type,
            "effect_key": effect_key,
            "effect_performed": performed,
            "attempt": attempt,
            "replay_count": event.replay_count,
            "processed_at": time.time(),
        }
        logger.info(
            "processed-ok event_id=%s attempt=%s effect_performed=%s",
            event.id, attempt, performed)
        return result
