"""副作用处理器抽象与本地模拟实现。"""
from __future__ import annotations

from typing import Protocol

from app.models import StoreRecord


class ProcessingContext(Protocol):
    def record_effect(self, effect_key: str) -> bool:
        """登记副作用；返回 True 表示首次登记，False 表示此前已执行过。"""
        ...


class EventHandler(Protocol):
    async def handle(self, record: StoreRecord, ctx: ProcessingContext) -> None:
        ...
