"""领域模型与枚举定义。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class EventStatus(str, Enum):
    """事件处理状态（单调推进）。"""
    ACCEPTED = "ACCEPTED"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"


class RejectReason(str, Enum):
    """提交被拒绝的可区分原因。"""
    DUPLICATE_KEY = "DUPLICATE_KEY"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    INVALID_EVENT = "INVALID_EVENT"


class ErrorKind(str, Enum):
    """处理器错误分类。"""
    TRANSIENT = "TRANSIENT"          # 可恢复，按退避重试
    PERMANENT = "PERMANENT"          # 不可恢复，直接进死信
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"


class EventSubmit(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64)
    business_key: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=64)
    payload: dict = Field(default_factory=dict)


class EventSubmitResult(BaseModel):
    event_id: int
    idempotency_key: str
    status: EventStatus
    duplicate: bool


class EventStatusView(BaseModel):
    event_id: int
    tenant_id: str
    business_key: str
    idempotency_key: str
    status: EventStatus
    attempts: int
    error_kind: ErrorKind | None
    error_detail: str | None
    created_at: str
    updated_at: str
    replayed: bool


class ReplayResult(BaseModel):
    event_id: int
    status: EventStatus


@dataclass(frozen=True)
class RetryPolicy:
    """退避重试策略。"""
    max_attempts: int = 3
    base_delay: float = 0.05
    max_delay: float = 1.0
    multiplier: float = 2.0

    def delay_for(self, attempt: int) -> float:
        """第 attempt 次尝试失败后的退避秒数：base * multiplier**(attempt-1)，封顶 max_delay。"""
        if attempt < 1:
            return 0.0
        return min(self.base_delay * (self.multiplier ** (attempt - 1)),
                   self.max_delay)


@dataclass
class StoreRecord:
    """存储层一行事件记录（仅内部使用）。"""
    id: int
    tenant_id: str
    business_key: str
    idempotency_key: str
    event_type: str
    payload_json: str
    status: str
    attempts: int
    error_kind: str | None
    error_detail: str | None
    visible_at: str | None
    lease_owner: str | None
    lease_expires_at: str | None
    replayed: int
    created_at: str
    updated_at: str


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
