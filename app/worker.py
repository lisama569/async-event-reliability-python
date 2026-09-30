"""异步消费进程组。

每个 Worker 是一个独立 asyncio 任务（"处理进程"的本地模拟），
各自带 worker_id 作为租约持有者；同一数据库文件也可以被多个
独立 Python 进程同时打开，SQLite 租约同样保证不重复处理。
"""
from __future__ import annotations

import asyncio

from app.logging_conf import get_logger

log = get_logger("worker")


class Worker:
    def __init__(self, service, worker_id: str,
                 poll_interval: float = 0.02):
        self._service = service
        self.worker_id = worker_id
        self._poll = poll_interval
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self.processed = 0

    async def run_forever(self) -> None:
        log.info("worker %s started", self.worker_id)
        try:
            while not self._stopping.is_set():
                handled = await self._service.process_one_claim(
                    self.worker_id)
                if handled:
                    self.processed += 1
                else:
                    try:
                        await asyncio.wait_for(self._stopping.wait(),
                                               timeout=self._poll)
                    except TimeoutError:
                        pass
        finally:
            log.info("worker %s stopped (processed=%d)",
                     self.worker_id, self.processed)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run_forever())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None


class WorkerGroup:
    def __init__(self, service, concurrency: int = 2,
                 poll_interval: float = 0.02):
        self._concurrency = concurrency
        self._workers = [
            Worker(service, f"worker-{i}", poll_interval=poll_interval)
            for i in range(concurrency)
        ]

    async def start(self) -> None:
        for w in self._workers:
            w.start()
        # 让出控制权，让 worker 任务真正跑起来
        await asyncio.sleep(0)

    async def stop(self) -> None:
        for w in self._workers:
            await w.stop()

    @property
    def workers(self) -> list[Worker]:
        return self._workers
