"""Domain types: event states, events, status views, transitions."""
from __future__ import annotations

import enum
from dataclasses import dataclass


class EventState(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    DEAD_LETTER = "DEAD_LETTER"


TERMINAL_STATES = frozenset({EventState.COMPLETED})


@dataclass(frozen=True)
class EventIn:
    tenant_id: str
    business_key: str
    event_type: str
    payload: dict


@dataclass(frozen=True)
class Event:
    id: str
    tenant_id: str
    business_key: str
    event_type: str
    payload: dict
    state: EventState
    attempt: int
    replay_count: int
    result: dict | None
    last_error_code: str | None
    last_error_message: str | None
    visible_at: float
    lease_expires_at: float | None
    owner: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class Transition:
    event_id: str
    from_state: str | None
    to_state: str
    attempt: int
    reason: str | None
    worker_id: str | None
    at: float


@dataclass(frozen=True)
class SubmitResult:
    event: Event
    duplicate: bool
