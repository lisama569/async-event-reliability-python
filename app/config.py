"""Runtime settings loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Local file simulating both the message broker (due queue + leases)
    # and durable processing state.
    db_path: str = "data/events.db"
    # How long a consumer sleeps when no event is due.
    worker_poll_interval: float = 0.1
    # Lease: an event claimed by a dead worker becomes due again after this.
    lease_seconds: float = 5.0
    # Exponential backoff: min(base * factor**(attempt-1), max_delay).
    retry_base_delay: float = 0.5
    retry_max_delay: float = 30.0
    retry_backoff_factor: float = 2.0
    # Total attempts before the event enters the dead-letter queue.
    max_attempts: int = 5
    # Hard cap of accepted (effective) events per tenant.
    tenant_quota: int = 100
    request_max_bytes: int = 65536


def _env(name: str, default, cast):
    raw = os.environ.get(name)
    return default if raw is None or raw == "" else cast(raw)


def load_settings() -> Settings:
    """Build Settings from environment variables."""
    return Settings(
        db_path=_env("EVENT_DB_PATH", "data/events.db", str),
        worker_poll_interval=_env("WORKER_POLL_INTERVAL", 0.1, float),
        lease_seconds=_env("LEASE_SECONDS", 5.0, float),
        retry_base_delay=_env("RETRY_BASE_DELAY", 0.5, float),
        retry_max_delay=_env("RETRY_MAX_DELAY", 30.0, float),
        retry_backoff_factor=_env("RETRY_BACKOFF_FACTOR", 2.0, float),
        max_attempts=_env("MAX_ATTEMPTS", 5, int),
        tenant_quota=_env("TENANT_QUOTA", 100, int),
        request_max_bytes=_env("REQUEST_MAX_BYTES", 65536, int),
    )
