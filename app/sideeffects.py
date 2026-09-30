"""Idempotent side-effect ledger: replay must never duplicate an effect."""
from __future__ import annotations

import logging
import time

logger = logging.getLogger("event.sideeffect")


class SideEffectGateway:
    """Records intended side effects once and only once.

    Backed by the durable `side_effects` table with a unique constraint on
    (event_id, effect_key). A retried or manually replayed event re-runs its
    handler, but any effect already recorded is suppressed, so at-least-once
    delivery is observed externally as exactly-once side effects.
    """

    def __init__(self, store) -> None:  # EventStore, avoids import cycle
        self.store = store

    def perform_once(self, event_id: str, effect_key: str,
                     description: str) -> bool:
        """Attempt to record a side effect.

        Returns True only for the first call of (event_id, effect_key); a
        repeated call returns False and performs nothing else.
        """
        performed = self.store.insert_side_effect(
            event_id, effect_key, description, time.time())
        if performed:
            logger.info(
                "side-effect-performed idempotency effect_id=%s/%s",
                event_id, effect_key)
        else:
            logger.info(
                "side-effect-deduped idempotency effect_id=%s/%s "
                "(replay/retry produced no duplicate)",
                event_id, effect_key)
        return performed
