"""日志配置：统一打印时间、级别、组件与消息。

业务日志消息由调用方拼装，固定包含幂等键、尝试次数与状态迁移，
例如：[idem=tenant:key] attempt=2 RETRYING->DEAD_LETTER reason=RETRY_EXHAUSTED
"""
from __future__ import annotations

import logging

_CONFIGURED = False

_FORMAT = "%(asctime)s %(levelname)-7s [%(name)s] %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_FORMAT))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(level)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)
