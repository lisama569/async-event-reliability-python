"""Domain error hierarchy with distinguishable rejection/failure codes."""
from __future__ import annotations


class AppError(Exception):
    """Base error."""


class RejectedError(AppError):
    """Inbound request rejected before becoming an effective event."""

    code: str = "rejected"
    http_status: int = 422

    def __init__(self, message: str = "") -> None:
        super().__init__(message or self.code)


class InvalidEventError(RejectedError):
    code = "invalid_event"
    http_status = 400


class KeyConflictError(RejectedError):
    code = "key_conflict"
    http_status = 409


class QuotaExceededError(RejectedError):
    code = "quota_exceeded"
    http_status = 429


class NotFoundError(RejectedError):
    code = "not_found"
    http_status = 404


class InvalidStateError(RejectedError):
    code = "invalid_state"
    http_status = 409


class RecoverableError(AppError):
    """Temporary processing failure; the event must be retried with backoff."""

    code = "recoverable"


class FatalError(AppError):
    """Unrecoverable failure; the event goes to the dead-letter queue."""

    code = "fatal"
