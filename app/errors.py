"""可区分的领域错误。"""
from __future__ import annotations

from app.models import ErrorKind, RejectReason


class RejectError(Exception):
    """事件提交被拒绝（携带可区分原因）。"""

    def __init__(self, reason: RejectReason, message: str):
        self.reason = reason
        super().__init__(message)


class ProcessingError(Exception):
    """处理阶段错误基类。"""

    def __init__(self, kind: ErrorKind, message: str):
        self.kind = kind
        super().__init__(message)


class TransientError(ProcessingError):
    def __init__(self, message: str = "transient failure"):
        super().__init__(ErrorKind.TRANSIENT, message)


class PermanentError(ProcessingError):
    def __init__(self, message: str = "permanent failure"):
        super().__init__(ErrorKind.PERMANENT, message)
