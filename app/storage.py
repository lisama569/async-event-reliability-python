"""SQLite-backed durable store: events, transitions, side effects, quotas.

SQLite with WAL + BEGIN IMMEDIATE simulates the durable broker/queue and the
processing database. The same file is shared by the API process and any number
of worker processes, so state survives restarts and cross-process claims are
serialized by database write locks.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid

from .errors import (
    InvalidStateError,
    KeyConflictError,
    NotFoundError,
    QuotaExceededError,
)
from .models import Event, EventState, SubmitResult, Transition

logger = logging.getLogger("event.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    business_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    replay_count INTEGER NOT NULL DEFAULT 0,
    result_json TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    visible_at REAL NOT NULL,
    lease_expires_at REAL,
    owner TEXT,
    request_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(tenant_id, business_key)
);
CREATE INDEX IF NOT EXISTS idx_events_due ON events(state, visible_at);
CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id, created_at);

CREATE TABLE IF NOT EXISTS state_transitions (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    reason TEXT,
    worker_id TEXT,
    at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transitions_event ON state_transitions(event_id, seq);

CREATE TABLE IF NOT EXISTS side_effects (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    effect_key TEXT NOT NULL,
    description TEXT NOT NULL,
    at REAL NOT NULL,
    UNIQUE(event_id, effect_key)
);
"""


class EventStore:
    """Durable event repository and processing-state machine."""

    def __init__(self, db_path: str, tenant_quota: int = 100) -> None:
        self.db_path = db_path
        self.tenant_quota = tenant_quota
        self._local = threading.local()
        self.init_schema()

    # ---------- connection management ----------

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            directory = os.path.dirname(self.db_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            conn = sqlite3.connect(self.db_path, check_same_thread=False,
                                   isolation_level=None)
            conn.row_factory = sqlite3.Row
            # Autocommit mode: every transaction is explicit (BEGIN IMMEDIATE),
            # so single statements like INSERT OR IGNORE commit on their own
            # and never leave an implicit transaction open across calls.
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    def init_schema(self) -> None:
        conn = self._conn()
        conn.executescript(_SCHEMA)

    # ---------- row mapping ----------

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        return Event(
            id=row["id"],
            tenant_id=row["tenant_id"],
            business_key=row["business_key"],
            event_type=row["event_type"],
            payload=json.loads(row["payload"]),
            state=EventState(row["state"]),
            attempt=row["attempt"],
            replay_count=row["replay_count"],
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            last_error_code=row["last_error_code"],
            last_error_message=row["last_error_message"],
            visible_at=row["visible_at"],
            lease_expires_at=row["lease_expires_at"],
            owner=row["owner"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ---------- inbound ----------

    def submit(self, tenant_id: str, business_key: str, event_type: str,
               payload: dict, request_hash: str, now: float) -> SubmitResult:
        """Idempotent submit keyed by (tenant_id, business_key).

        Same key + same request body -> the existing event is returned and no
        new effect is produced. Same key + different body -> KeyConflictError.
        New key beyond the tenant quota -> QuotaExceededError.
        """
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM events WHERE tenant_id=? AND business_key=?",
                (tenant_id, business_key),
            ).fetchone()
            if row is not None:
                existing = self._row_to_event(row)
                if row["request_hash"] == request_hash:
                    conn.commit()
                    logger.info(
                        "idempotent-hit idempotency_key=%s/%s event_id=%s "
                        "state=%s (no new effect produced)",
                        tenant_id, business_key, existing.id, existing.state.value,
                    )
                    return SubmitResult(event=existing, duplicate=True)
                conn.rollback()
                logger.info(
                    "key-conflict idempotency_key=%s/%s existing_event_id=%s",
                    tenant_id, business_key, existing.id,
                )
                raise KeyConflictError(
                    f"business_key '{business_key}' already exists for tenant "
                    f"'{tenant_id}' with a different request body")

            used = conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE tenant_id=?",
                (tenant_id,),
            ).fetchone()["c"]
            if used >= self.tenant_quota:
                conn.rollback()
                logger.info(
                    "quota-exceeded tenant=%s used=%s quota=%s",
                    tenant_id, used, self.tenant_quota,
                )
                raise QuotaExceededError(
                    f"tenant '{tenant_id}' quota {self.tenant_quota} exhausted")

            event_id = uuid.uuid4().hex
            conn.execute(
                """INSERT INTO events(id, tenant_id, business_key, event_type,
                       payload, state, attempt, visible_at, request_hash,
                       created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (event_id, tenant_id, business_key, event_type,
                 json.dumps(payload, separators=(",", ":"), sort_keys=True),
                 EventState.PENDING.value, 0, now, request_hash, now, now),
            )
            self._insert_transition(conn, event_id, None,
                                    EventState.PENDING, 0, "accepted", None, now)
            conn.commit()
            event = self.get(event_id)
            logger.info(
                "idempotent-miss idempotency_key=%s/%s event_id=%s state=PENDING",
                tenant_id, business_key, event_id,
            )
            return SubmitResult(event=event, duplicate=False)
        except Exception:
            conn.rollback()
            raise

    # ---------- queries ----------

    def get(self, event_id: str) -> Event | None:
        row = self._conn().execute(
            "SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        return self._row_to_event(row) if row else None

    def get_required(self, event_id: str) -> Event:
        event = self.get(event_id)
        if event is None:
            raise NotFoundError(f"event '{event_id}' not found")
        return event

    def get_by_key(self, tenant_id: str, business_key: str) -> Event | None:
        row = self._conn().execute(
            "SELECT * FROM events WHERE tenant_id=? AND business_key=?",
            (tenant_id, business_key)).fetchone()
        return self._row_to_event(row) if row else None

    def list_events(self, tenant_id: str, limit: int = 100) -> list[Event]:
        rows = self._conn().execute(
            "SELECT * FROM events WHERE tenant_id=? ORDER BY created_at DESC "
            "LIMIT ?", (tenant_id, limit)).fetchall()
        return [self._row_to_event(r) for r in rows]

    def list_dlq(self, tenant_id: str | None = None) -> list[Event]:
        if tenant_id is None:
            rows = self._conn().execute(
                "SELECT * FROM events WHERE state=? ORDER BY updated_at",
                (EventState.DEAD_LETTER.value,)).fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM events WHERE state=? AND tenant_id=? "
                "ORDER BY updated_at",
                (EventState.DEAD_LETTER.value, tenant_id)).fetchall()
        return [self._row_to_event(r) for r in rows]

    def list_transitions(self, event_id: str) -> list[Transition]:
        rows = self._conn().execute(
            "SELECT * FROM state_transitions WHERE event_id=? ORDER BY seq",
            (event_id,)).fetchall()
        return [Transition(
            event_id=r["event_id"], from_state=r["from_state"],
            to_state=r["to_state"], attempt=r["attempt"],
            reason=r["reason"], worker_id=r["worker_id"], at=r["at"],
        ) for r in rows]

    # ---------- consumer state machine ----------

    def claim_due(self, worker_id: str, now: float,
                  lease_seconds: float) -> Event | None:
        """Atomically claim one due event.

        Due = PENDING and visible, or PROCESSING whose lease expired (worker
        crashed). visible_at is pushed to lease expiry so a crashed claim
        becomes reclaimable automatically. Cross-process safety is provided by
        BEGIN IMMEDIATE: only one worker wins the claim.
        """
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """SELECT * FROM events
                   WHERE state IN (?, ?) AND visible_at <= ?
                   ORDER BY visible_at LIMIT 1""",
                (EventState.PENDING.value, EventState.PROCESSING.value, now),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            event_id = row["id"]
            new_attempt = row["attempt"] + 1
            lease_until = now + lease_seconds
            prev_state = row["state"]
            conn.execute(
                """UPDATE events SET state=?, attempt=?, owner=?,
                       lease_expires_at=?, visible_at=?, updated_at=?
                   WHERE id=?""",
                (EventState.PROCESSING.value, new_attempt, worker_id,
                 lease_until, lease_until, now, event_id),
            )
            self._insert_transition(
                conn, event_id, prev_state, EventState.PROCESSING,
                new_attempt, f"claimed attempt={new_attempt}", worker_id, now)
            conn.commit()
            event = self.get(event_id)
            logger.info(
                "claim event_id=%s attempt=%s owner=%s prev_state=%s",
                event_id, new_attempt, worker_id, prev_state,
            )
            return event
        except Exception:
            conn.rollback()
            raise

    def complete(self, event_id: str, attempt: int, worker_id: str,
                 result: dict, now: float) -> bool:
        """Monotonic completion: only the live lease owner at this attempt wins.

        A late/duplicated completion (wrong attempt or a non-processing event)
        is rejected and never overwrites a finished result.
        """
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                conn.rollback()
                raise NotFoundError(f"event '{event_id}' not found")
            if (row["state"] != EventState.PROCESSING.value
                    or row["attempt"] != attempt or row["owner"] != worker_id):
                conn.rollback()
                logger.info(
                    "late-complete-rejected event_id=%s attempt=%s/%s "
                    "owner=%s state=%s",
                    event_id, attempt, row["attempt"], worker_id, row["state"])
                return False
            conn.execute(
                """UPDATE events SET state=?, result_json=?, owner=NULL,
                       lease_expires_at=NULL, last_error_code=NULL,
                       last_error_message=NULL, updated_at=?
                   WHERE id=?""",
                (EventState.COMPLETED.value,
                 json.dumps(result, separators=(",", ":"), sort_keys=True),
                 now, event_id),
            )
            self._insert_transition(
                conn, event_id, EventState.PROCESSING.value,
                EventState.COMPLETED, attempt,
                f"completed attempt={attempt}", worker_id, now)
            conn.commit()
            logger.info(
                "transition event_id=%s PROCESSING->COMPLETED attempt=%s owner=%s",
                event_id, attempt, worker_id)
            return True
        except Exception:
            conn.rollback()
            raise

    def requeue(self, event_id: str, attempt: int, worker_id: str,
                error_code: str, error_message: str,
                visible_at: float, now: float) -> bool:
        """Release back to PENDING with backoff; same fencing as complete."""
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                conn.rollback()
                raise NotFoundError(f"event '{event_id}' not found")
            if (row["state"] != EventState.PROCESSING.value
                    or row["attempt"] != attempt or row["owner"] != worker_id):
                conn.rollback()
                logger.info(
                    "late-requeue-rejected event_id=%s attempt=%s/%s owner=%s",
                    event_id, attempt, row["attempt"], worker_id)
                return False
            conn.execute(
                """UPDATE events SET state=?, owner=NULL, lease_expires_at=NULL,
                       visible_at=?, last_error_code=?, last_error_message=?,
                       updated_at=?
                   WHERE id=?""",
                (EventState.PENDING.value, visible_at, error_code,
                 error_message[:500], now, event_id),
            )
            self._insert_transition(
                conn, event_id, EventState.PROCESSING.value,
                EventState.PENDING, attempt,
                f"retry:{error_code} attempt={attempt}", worker_id, now)
            conn.commit()
            logger.info(
                "transition event_id=%s PROCESSING->PENDING attempt=%s "
                "error=%s visible_at=%.3f",
                event_id, attempt, error_code, visible_at)
            return True
        except Exception:
            conn.rollback()
            raise

    def dead_letter(self, event_id: str, attempt: int, worker_id: str,
                    error_code: str, error_message: str, now: float) -> bool:
        """Move to DEAD_LETTER; same fencing as complete."""
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                conn.rollback()
                raise NotFoundError(f"event '{event_id}' not found")
            if (row["state"] != EventState.PROCESSING.value
                    or row["attempt"] != attempt or row["owner"] != worker_id):
                conn.rollback()
                logger.info(
                    "late-deadletter-rejected event_id=%s attempt=%s/%s owner=%s",
                    event_id, attempt, row["attempt"], worker_id)
                return False
            conn.execute(
                """UPDATE events SET state=?, owner=NULL, lease_expires_at=NULL,
                       last_error_code=?, last_error_message=?, updated_at=?
                   WHERE id=?""",
                (EventState.DEAD_LETTER.value, error_code,
                 error_message[:500], now, event_id),
            )
            self._insert_transition(
                conn, event_id, EventState.PROCESSING.value,
                EventState.DEAD_LETTER, attempt,
                f"dlq:{error_code} attempt={attempt}", worker_id, now)
            conn.commit()
            logger.warning(
                "transition event_id=%s PROCESSING->DEAD_LETTER attempt=%s "
                "error=%s", event_id, attempt, error_code)
            return True
        except Exception:
            conn.rollback()
            raise

    def replay_dead_letter(self, event_id: str, now: float) -> Event:
        """Manual replay of a dead-lettered event.

        Only DEAD_LETTER events can be replayed. The attempt counter resets and
        replay_count grows; the side-effect ledger guarantees a replay cannot
        produce a duplicate effect.
        """
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                conn.rollback()
                raise NotFoundError(f"event '{event_id}' not found")
            if row["state"] != EventState.DEAD_LETTER.value:
                conn.rollback()
                raise InvalidStateError(
                    f"event '{event_id}' is {row['state']}, not DEAD_LETTER")
            new_replay_count = row["replay_count"] + 1
            conn.execute(
                """UPDATE events SET state=?, attempt=0, owner=NULL,
                       lease_expires_at=NULL, visible_at=?,
                       last_error_code=NULL, last_error_message=NULL,
                       replay_count=?, updated_at=?
                   WHERE id=?""",
                (EventState.PENDING.value, now, new_replay_count, now,
                 event_id),
            )
            self._insert_transition(
                conn, event_id, EventState.DEAD_LETTER.value,
                EventState.PENDING, 0,
                f"manual_replay replay_count={new_replay_count}", None, now)
            conn.commit()
            event = self.get_required(event_id)
            logger.info(
                "replay event_id=%s DEAD_LETTER->PENDING replay_count=%s",
                event_id, new_replay_count)
            return event
        except Exception:
            conn.rollback()
            raise

    # ---------- side-effect ledger ----------

    def insert_side_effect(self, event_id: str, effect_key: str,
                           description: str, now: float) -> bool:
        """Insert an intended effect once. Returns False on duplicate."""
        inserted = self._conn().execute(
            """INSERT OR IGNORE INTO side_effects
                   (event_id, effect_key, description, at)
               VALUES(?,?,?,?)""",
            (event_id, effect_key, description, now)).rowcount == 1
        if inserted:
            logger.info("side-effect event_id=%s key=%s performed",
                        event_id, effect_key)
        else:
            logger.info("side-effect event_id=%s key=%s suppressed-duplicate",
                        event_id, effect_key)
        return inserted

    def list_side_effects(self, event_id: str) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM side_effects WHERE event_id=? ORDER BY seq",
            (event_id,)).fetchall()
        return [{"effect_key": r["effect_key"], "description": r["description"],
                 "at": r["at"]} for r in rows]

    # ---------- transitions ----------

    def _insert_transition(self, conn: sqlite3.Connection, event_id: str,
                           from_state, to_state: EventState, attempt: int,
                           reason: str | None, worker_id: str | None,
                           now: float) -> None:
        conn.execute(
            """INSERT INTO state_transitions
                   (event_id, from_state, to_state, attempt, reason,
                    worker_id, at)
               VALUES(?,?,?,?,?,?,?)""",
            (event_id,
             from_state.value if isinstance(from_state, EventState)
             else from_state,
             to_state.value, attempt, reason, worker_id, now),
        )

    def record_transition(self, event_id: str, from_state: str | None,
                          to_state: EventState, attempt: int,
                          reason: str | None, worker_id: str | None,
                          now: float) -> None:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            self._insert_transition(conn, event_id, from_state, to_state,
                                    attempt, reason, worker_id, now)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
