"""本地模拟处理器。

payload 指令（仅用于本地演示/测试）：
    {
      "mode": "ok" | "transient" | "permanent",   # 默认 ok
      "fail_times": 2,        # mode=transient 时，前 N 次尝试失败，之后成功
      "fail_until_replay": true,  # 重放前一直可恢复失败，重放后成功
      "effects": ["send-mail", "issue-coupon"]   # 需要去重的外部副作用
    }

"外部副作用"只在 ctx.record_effect 首次登记成功时才执行（打印日志）；
重放或租约恢复导致的重复执行不会产生第二次副作用。
"""
from __future__ import annotations

import json

from app.errors import PermanentError, TransientError
from app.logging_conf import get_logger
from app.models import StoreRecord

log = get_logger("sim-handler")


class SimulatedHandler:
    async def handle(self, record: StoreRecord, ctx) -> None:
        try:
            payload = json.loads(record.payload_json or "{}")
        except json.JSONDecodeError:
            payload = {}

        idem = record.idempotency_key
        attempt_no = record.attempts + 1

        # 幂等副作用：先登记，只有首次登记真正"执行"。
        for effect_key in payload.get("effects", []):
            first = ctx.record_effect(str(effect_key))
            if first:
                log.info("[idem=%s] attempt=%d side-effect performed: %s",
                         idem, attempt_no, effect_key)
            else:
                log.info("[idem=%s] attempt=%d side-effect suppressed"
                         " (already performed): %s",
                         idem, attempt_no, effect_key)

        mode = payload.get("mode", "ok")
        fail_times = int(payload.get("fail_times", 0))
        fail_until_replay = bool(payload.get("fail_until_replay", False))

        if mode == "permanent":
            log.info("[idem=%s] attempt=%d raising PERMANENT error",
                     idem, attempt_no)
            raise PermanentError("payload instructed permanent failure")

        will_fail = (
            mode == "transient" and attempt_no <= fail_times
        ) or (fail_until_replay and record.replayed == 0)
        if will_fail:
            log.info("[idem=%s] attempt=%d raising TRANSIENT error",
                     idem, attempt_no)
            raise TransientError(
                f"payload instructed transient failure (attempt {attempt_no})")

        log.info("[idem=%s] attempt=%d handler business logic completed",
                 idem, attempt_no)
