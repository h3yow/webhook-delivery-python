"""SQLite 持久化层：事件与投递尝试的存取，保证重启后不丢失、不误判。"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from .models import DeliveryAttempt, DeliveryStatus, Event


class IdempotencyConflictError(Exception):
    """同一幂等标识已存在但请求内容不一致。"""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    endpoint TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_failure_kind TEXT,
    last_failure_detail TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    failure_kind TEXT,
    http_status INTEGER,
    duration_ms REAL NOT NULL DEFAULT 0,
    detail TEXT,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_status_due ON events(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id);
"""


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        event_id=row["event_id"],
        idempotency_key=row["idempotency_key"],
        endpoint=row["endpoint"],
        payload=json.loads(row["payload"]),
        status=DeliveryStatus(row["status"]),
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        next_attempt_at=row["next_attempt_at"],
        last_failure_kind=row["last_failure_kind"],
        last_failure_detail=row["last_failure_detail"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_attempt(row: sqlite3.Row) -> DeliveryAttempt:
    return DeliveryAttempt(
        event_id=row["event_id"],
        attempt_no=row["attempt_no"],
        outcome=row["outcome"],
        failure_kind=row["failure_kind"],
        http_status=row["http_status"],
        duration_ms=row["duration_ms"],
        detail=row["detail"],
        ts=row["ts"],
    )


class EventStore:
    """基于 SQLite 的事件存储，线程安全（单连接 + 可重入锁，事务保证原子性）。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --- 事件写入 ---
    def insert_event(self, event: Event) -> None:
        """插入新事件；幂等键冲突时抛 sqlite3.IntegrityError 由上层区分处理。"""
        with self._lock:
            self._conn.execute(
                """INSERT INTO events
                   (event_id, idempotency_key, endpoint, payload, status, attempts,
                    max_attempts, next_attempt_at, last_failure_kind,
                    last_failure_detail, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (event.event_id, event.idempotency_key, event.endpoint,
                 json.dumps(event.payload), event.status.value, event.attempts,
                 event.max_attempts, event.next_attempt_at, event.last_failure_kind,
                 event.last_failure_detail, event.created_at, event.updated_at),
            )
            self._conn.commit()

    def get_by_idempotency_key(self, key: str) -> Event | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE idempotency_key=?", (key,)
            ).fetchone()
        return _row_to_event(row) if row else None

    def get_event(self, event_id: str) -> Event | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
        return _row_to_event(row) if row else None

    # --- 调度 ---
    def claim_due_events(self, now: float, limit: int,
                         ordered_per_endpoint: bool = False) -> list[Event]:
        """原子地领取到期的 PENDING 事件并标记为 IN_FLIGHT（单事务内完成）。

        ordered_per_endpoint=True 时跳过已有在途事件的接收端，
        保证同一接收端的事件按提交顺序串行投递。
        """
        with self._lock:
            sql = """SELECT event_id FROM events
                     WHERE status=? AND next_attempt_at<=?"""
            params: list = [DeliveryStatus.PENDING.value, now]
            if ordered_per_endpoint:
                # 每个接收端只取最早的一个到期事件，且该接收端无在途事件，
                # 保证同一接收端串行、按提交顺序投递
                sql += """ AND endpoint NOT IN
                           (SELECT endpoint FROM events WHERE status=?)
                           AND NOT EXISTS (
                               SELECT 1 FROM events e2
                               WHERE e2.status=? AND e2.endpoint=events.endpoint
                                 AND e2.next_attempt_at<=?
                                 AND (e2.created_at < events.created_at
                                      OR (e2.created_at = events.created_at
                                          AND e2.event_id < events.event_id))
                           )"""
                params += [DeliveryStatus.IN_FLIGHT.value,
                           DeliveryStatus.PENDING.value, now]
            sql += " ORDER BY next_attempt_at, created_at LIMIT ?"
            params.append(limit)
            rows = self._conn.execute(sql, params).fetchall()
            ids = [r["event_id"] for r in rows]
            if not ids:
                return []
            ts = time.time()
            self._conn.executemany(
                "UPDATE events SET status=?, updated_at=? WHERE event_id=? AND status=?",
                [(DeliveryStatus.IN_FLIGHT.value, ts, eid, DeliveryStatus.PENDING.value)
                 for eid in ids],
            )
            self._conn.commit()
            out = self._conn.execute(
                f"SELECT * FROM events WHERE event_id IN ({','.join('?' * len(ids))})",
                ids,
            ).fetchall()
        return [_row_to_event(r) for r in out]

    def _finish_attempt(self, event_id: str, attempt: DeliveryAttempt,
                        status: DeliveryStatus, next_attempt_at: float,
                        failure_kind: str | None, failure_detail: str | None) -> None:
        with self._lock:
            ts = time.time()
            self._conn.execute(
                """INSERT INTO attempts
                   (event_id, attempt_no, outcome, failure_kind, http_status,
                    duration_ms, detail, ts)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (attempt.event_id, attempt.attempt_no, attempt.outcome,
                 attempt.failure_kind, attempt.http_status, attempt.duration_ms,
                 attempt.detail, attempt.ts),
            )
            self._conn.execute(
                """UPDATE events SET status=?, attempts=?, next_attempt_at=?,
                   last_failure_kind=?, last_failure_detail=?, updated_at=?
                   WHERE event_id=?""",
                (status.value, attempt.attempt_no, next_attempt_at,
                 failure_kind, failure_detail, ts, event_id),
            )
            self._conn.commit()

    def mark_delivered(self, event_id: str, attempt: DeliveryAttempt) -> None:
        self._finish_attempt(event_id, attempt, DeliveryStatus.DELIVERED, 0.0, None, None)

    def mark_retry(self, event_id: str, attempt: DeliveryAttempt, next_attempt_at: float) -> None:
        self._finish_attempt(event_id, attempt, DeliveryStatus.PENDING,
                             next_attempt_at, attempt.failure_kind, attempt.detail)

    def mark_dead(self, event_id: str, attempt: DeliveryAttempt) -> None:
        self._finish_attempt(event_id, attempt, DeliveryStatus.DEAD, 0.0,
                             attempt.failure_kind, attempt.detail)

    def recover_in_flight(self) -> int:
        """重启恢复：把残留的 IN_FLIGHT 事件重置为 PENDING（立即到期），返回恢复数量。"""
        with self._lock:
            cur = self._conn.execute(
                """UPDATE events SET status=?, next_attempt_at=0, updated_at=?
                   WHERE status=?""",
                (DeliveryStatus.PENDING.value, time.time(), DeliveryStatus.IN_FLIGHT.value),
            )
            self._conn.commit()
            return cur.rowcount

    # --- 查询 ---
    def list_events(self, endpoint: str | None = None, status: str | None = None) -> list[Event]:
        sql = "SELECT * FROM events"
        conds, params = [], []
        if endpoint is not None:
            conds.append("endpoint=?")
            params.append(endpoint)
        if status is not None:
            conds.append("status=?")
            params.append(status)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY created_at"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_event(r) for r in rows]

    def list_attempts(self, event_id: str) -> list[DeliveryAttempt]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM attempts WHERE event_id=? ORDER BY attempt_no", (event_id,)
            ).fetchall()
        return [_row_to_attempt(r) for r in rows]

    def stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM events GROUP BY status"
            ).fetchall()
        result = {s.value: 0 for s in DeliveryStatus}
        for r in rows:
            result[r["status"]] = r["n"]
        return result
