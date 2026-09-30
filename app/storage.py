"""基于本地 SQLite 的持久化存储：同时承担"消息代理"与"处理状态库"。

设计要点
--------
* WAL + busy_timeout：多个处理进程/任务可同时打开同一数据库文件。
* 所有状态推进都使用带条件的单条 UPDATE（CAS），保证状态单调、
  迟到/重复消息无法覆盖 SUCCEEDED / DEAD_LETTER 等终态。
* claim_due 在一个 IMMEDIATE 事务内完成"挑一条到期消息 + 上锁"，
  保证多个并发消费者不会重复处理同一条事件。
* PROCESSING 租约超时后可被其他消费者重新领取（崩溃/重启恢复）。
"""
from __future__ import annotations

import dataclasses
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from app.models import StoreRecord

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id        TEXT    NOT NULL,
    business_key     TEXT    NOT NULL,
    idempotency_key  TEXT    NOT NULL UNIQUE,
    event_type       TEXT    NOT NULL,
    payload_json     TEXT    NOT NULL,
    status           TEXT    NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    error_kind       TEXT,
    error_detail     TEXT,
    visible_at       TEXT    NOT NULL,
    lease_owner      TEXT,
    lease_expires_at TEXT,
    replayed         INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_dispatch
    ON events(status, visible_at);
CREATE INDEX IF NOT EXISTS idx_events_tenant_created
    ON events(tenant_id, created_at);

CREATE TABLE IF NOT EXISTS side_effects (
    idempotency_key TEXT NOT NULL,
    effect_key      TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (idempotency_key, effect_key)
) WITHOUT ROWID;
"""


class DuplicateKeyError(Exception):
    """幂等键重复。"""


def row_to_record(row: sqlite3.Row) -> StoreRecord:
    return StoreRecord(
        id=row["id"],
        tenant_id=row["tenant_id"],
        business_key=row["business_key"],
        idempotency_key=row["idempotency_key"],
        event_type=row["event_type"],
        payload_json=row["payload_json"],
        status=row["status"],
        attempts=row["attempts"],
        error_kind=row["error_kind"],
        error_detail=row["error_detail"],
        visible_at=row["visible_at"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        replayed=row["replayed"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self.init_schema()

    def close(self) -> None:
        self._conn.close()

    def init_schema(self) -> None:
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @contextmanager
    def immediate(self) -> Iterator[sqlite3.Connection]:
        """开启写事务；并发写入方会在锁上等待而不是立即失败。"""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # ---- 写入 / 查询 -------------------------------------------------

    def insert_event(self, *, tenant_id: str, business_key: str,
                     idempotency_key: str, event_type: str, payload_json: str,
                     now_iso: str) -> StoreRecord:
        with self.immediate() as conn:
            cur = conn.execute(
                "INSERT INTO events (tenant_id, business_key, idempotency_key,"
                " event_type, payload_json, status, attempts, visible_at,"
                " created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, 'ACCEPTED', 0, ?, ?, ?)",
                (tenant_id, business_key, idempotency_key, event_type,
                 payload_json, now_iso, now_iso, now_iso),
            )
            event_id = cur.lastrowid
            row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        return row_to_record(row)

    def insert_accepted(self, *, tenant_id: str, business_key: str,
                        idempotency_key: str, event_type: str,
                        payload_json: str, now_iso: str,
                        quota_limit: int, window_start_iso: str
                        ) -> StoreRecord | None:
        """幂等插入 + 配额检查，在同一个 IMMEDIATE 事务内完成。

        顺序（语义上重复键是确定性事实，优先于窗口性配额）：
          1) 幂等键已存在 -> DuplicateKeyError（调用方映射 DUPLICATE_KEY）；
          2) 否则若窗口内计数已达配额 -> 返回 None（QUOTA_EXCEEDED）；
          3) 否则插入新事件。
        """
        with self.immediate() as conn:
            dup = conn.execute(
                "SELECT 1 FROM events WHERE idempotency_key=? LIMIT 1",
                (idempotency_key,)).fetchone()
            if dup is not None:
                raise DuplicateKeyError(idempotency_key)
            count_row = conn.execute(
                "SELECT COUNT(*) AS c FROM events"
                " WHERE tenant_id=? AND created_at>=?",
                (tenant_id, window_start_iso)).fetchone()
            if int(count_row["c"]) >= quota_limit:
                return None
            cur = conn.execute(
                "INSERT INTO events (tenant_id, business_key, idempotency_key,"
                " event_type, payload_json, status, attempts, visible_at,"
                " created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, 'ACCEPTED', 0, ?, ?, ?)",
                (tenant_id, business_key, idempotency_key, event_type,
                 payload_json, now_iso, now_iso, now_iso),
            )
            event_id = cur.lastrowid
            row = conn.execute("SELECT * FROM events WHERE id=?",
                               (event_id,)).fetchone()
        return row_to_record(row)

    def get_by_id(self, event_id: int) -> StoreRecord | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        return row_to_record(row) if row else None

    def get_by_key(self, idempotency_key: str) -> StoreRecord | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE idempotency_key=?",
            (idempotency_key,)).fetchone()
        return row_to_record(row) if row else None

    def count_tenant_since(self, tenant_id: str, since_iso: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS c FROM events"
            " WHERE tenant_id=? AND created_at>=?",
            (tenant_id, since_iso)).fetchone()
        return int(row["c"])

    def claim_due(self, owner: str, now_iso: str,
                  lease_expires_iso: str) -> StoreRecord | None:
        """原子领取一条可消费事件。

        可领取条件（满足其一）：
          * 状态为 ACCEPTED / RETRYING 且 visible_at 到期且无有效租约；
          * 状态为 PROCESSING 但租约已过期（前一个消费者崩溃/重启）。
        """
        with self.immediate() as conn:
            row = conn.execute(
                "SELECT * FROM events"
                " WHERE (status IN ('ACCEPTED','RETRYING')"
                "        AND visible_at<=?"
                "        AND (lease_expires_at IS NULL OR lease_expires_at<?))"
                "    OR (status='PROCESSING' AND lease_expires_at<?)"
                " ORDER BY id LIMIT 1",
                (now_iso, now_iso, now_iso)).fetchone()
            if row is None:
                return None
            prev = row_to_record(row)  # 保留领取前状态用于日志
            # 接管过期 PROCESSING 时，崩溃前的那次尝试计入 attempts
            extra_attempt = 1 if row["status"] == "PROCESSING" else 0
            conn.execute(
                "UPDATE events SET status='PROCESSING', lease_owner=?,"
                " lease_expires_at=?, attempts=attempts+?,"
                " updated_at=? WHERE id=?",
                (owner, lease_expires_iso, extra_attempt, now_iso, row["id"]))
            row = conn.execute("SELECT * FROM events WHERE id=?",
                               (row["id"],)).fetchone()
            current = row_to_record(row)
        # 状态保留领取前的值（用于日志展示状态迁移），attempts 取最新值
        return dataclasses.replace(current, status=prev.status)

    # ---- 状态推进（全部带 CAS 条件，保证单调） ------------------------

    def _cas_finish(self, event_id: int, owner: str, now_iso: str,
                    status: str, attempts: int, error_kind: str | None,
                    error_detail: str | None, visible_at: str | None) -> bool:
        with self.immediate() as conn:
            cur = conn.execute(
                "UPDATE events SET status=?, attempts=?, error_kind=?,"
                " error_detail=?, lease_owner=NULL, lease_expires_at=NULL,"
                " visible_at=COALESCE(?, visible_at), updated_at=?"
                " WHERE id=? AND status='PROCESSING' AND lease_owner=?",
                (status, attempts, error_kind, error_detail, visible_at,
                 now_iso, event_id, owner))
            return cur.rowcount == 1

    def mark_succeeded(self, event_id: int, owner: str, now_iso: str,
                       attempts: int) -> bool:
        return self._cas_finish(event_id, owner, now_iso, "SUCCEEDED",
                                attempts, None, None, now_iso)

    def mark_retry(self, event_id: int, owner: str, now_iso: str,
                   attempts: int, error_detail: str,
                   visible_at_iso: str) -> bool:
        return self._cas_finish(event_id, owner, now_iso, "RETRYING",
                                attempts, "TRANSIENT", error_detail,
                                visible_at_iso)

    def mark_dead(self, event_id: int, owner: str, now_iso: str,
                  attempts: int, error_kind: str,
                  error_detail: str) -> bool:
        return self._cas_finish(event_id, owner, now_iso, "DEAD_LETTER",
                                attempts, error_kind, error_detail, now_iso)

    def reset_for_replay(self, event_id: int, now_iso: str) -> bool:
        """死信人工重放：仅 DEAD_LETTER -> ACCEPTED，重置尝试预算。"""
        with self.immediate() as conn:
            cur = conn.execute(
                "UPDATE events SET status='ACCEPTED', attempts=0,"
                " error_kind=NULL, error_detail=NULL, visible_at=?,"
                " lease_owner=NULL, lease_expires_at=NULL, replayed=1,"
                " updated_at=? WHERE id=? AND status='DEAD_LETTER'",
                (now_iso, now_iso, event_id))
            return cur.rowcount == 1

    # ---- 副作用去重 ---------------------------------------------------

    def record_effect(self, idempotency_key: str, effect_key: str,
                      now_iso: str) -> bool:
        """登记副作用；首次返回 True，重复登记返回 False（不报错）。"""
        with self.immediate() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO side_effects"
                " (idempotency_key, effect_key, created_at)"
                " VALUES (?, ?, ?)",
                (idempotency_key, effect_key, now_iso))
            return cur.rowcount == 1
