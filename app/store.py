"""SQLite 事件存储：持久化保证重启后不丢失、不误判。"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from .models import AttemptOut, EventRecord, EventStatus, FailureKind

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    receiver TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    last_failure_kind TEXT NOT NULL DEFAULT 'none',
    last_failure_detail TEXT NOT NULL DEFAULT '',
    next_retry_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    history TEXT NOT NULL DEFAULT '[]'
);
"""


class DuplicateEventError(Exception):
    """幂等标识冲突：同一 event_id 已存在。"""


def _row_to_record(row: sqlite3.Row) -> EventRecord:
    history = [AttemptOut(**h) for h in json.loads(row["history"])]
    return EventRecord(
        event_id=row["event_id"],
        event_type=row["event_type"],
        receiver=row["receiver"],
        payload=json.loads(row["payload"]),
        status=EventStatus(row["status"]),
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        last_failure_kind=FailureKind(row["last_failure_kind"]),
        last_failure_detail=row["last_failure_detail"],
        next_retry_at=row["next_retry_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        history=history,
    )


class EventStore:
    """基于 SQLite 的事件持久化存储，进程重启后状态可恢复。

    单连接 + 互斥锁序列化所有写操作；claim_due 使用 BEGIN IMMEDIATE
    事务保证“认领”原子性，避免并发下同一事件被重复投递。
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        # isolation_level=None：自动提交模式，事务由我们显式 BEGIN/COMMIT 控制
        self._conn = sqlite3.connect(db_path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def insert(self, record: EventRecord) -> None:
        now = time.time()
        record.created_at = record.created_at or now
        record.updated_at = now
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO events (event_id, event_type, receiver, payload,"
                    " status, attempts, max_attempts, next_retry_at, created_at,"
                    " updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (record.event_id, record.event_type, record.receiver,
                     json.dumps(record.payload), record.status.value,
                     record.attempts, record.max_attempts, record.next_retry_at,
                     record.created_at, record.updated_at),
                )
                self._conn.commit()
        except sqlite3.IntegrityError as exc:
            raise DuplicateEventError(record.event_id) from exc

    def get(self, event_id: str) -> EventRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return _row_to_record(row) if row else None

    def list_events(self) -> list[EventRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY created_at, event_id"
            ).fetchall()
        return [_row_to_record(r) for r in rows]

    def claim_due(self, now: float, limit: int) -> list[EventRecord]:
        """原子认领到期事件：pending 且到期 -> in_flight，返回被认领记录。"""
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                rows = self._conn.execute(
                    "SELECT * FROM events WHERE status = ?"
                    " AND (next_retry_at IS NULL OR next_retry_at <= ?)"
                    " ORDER BY created_at, event_id LIMIT ?",
                    (EventStatus.PENDING.value, now, limit),
                ).fetchall()
                ids = [r["event_id"] for r in rows]
                if ids:
                    marks = ",".join("?" for _ in ids)
                    self._conn.execute(
                        f"UPDATE events SET status = ?, updated_at = ?"
                        f" WHERE event_id IN ({marks})",
                        [EventStatus.IN_FLIGHT.value, now, *ids],
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        records = [_row_to_record(r) for r in rows]
        for rec in records:  # 行是 UPDATE 前的快照，返回时反映已认领状态
            rec.status = EventStatus.IN_FLIGHT
        return records

    def _finish_attempt(self, event_id: str, kind: FailureKind, detail: str,
                        attempt: int, at: float) -> list[dict]:
        """公共部分：累加尝试次数、记录失败类别、追加历史，返回新历史。"""
        row = self._conn.execute(
            "SELECT attempts, history FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        history = json.loads(row["history"])
        history.append(
            AttemptOut(attempt=attempt, kind=kind, detail=detail, at=at).model_dump()
        )
        return history

    def complete(self, event_id: str, status: EventStatus, kind: FailureKind,
                 detail: str, attempt: int, at: float) -> None:
        """投递结束（成功或不可重试失败）：写终态并追加历史。"""
        with self._lock:
            history = self._finish_attempt(event_id, kind, detail, attempt, at)
            self._conn.execute(
                "UPDATE events SET status=?, attempts=?, last_failure_kind=?,"
                " last_failure_detail=?, next_retry_at=NULL, updated_at=?,"
                " history=? WHERE event_id=?",
                (status.value, attempt, kind.value, detail, at,
                 json.dumps(history), event_id),
            )
            self._conn.commit()

    def reschedule(self, event_id: str, kind: FailureKind, detail: str,
                   attempt: int, next_retry_at: float, at: float) -> None:
        """可重试失败：回到 pending 并设置下次重试时间。"""
        with self._lock:
            history = self._finish_attempt(event_id, kind, detail, attempt, at)
            self._conn.execute(
                "UPDATE events SET status=?, attempts=?, last_failure_kind=?,"
                " last_failure_detail=?, next_retry_at=?, updated_at=?,"
                " history=? WHERE event_id=?",
                (EventStatus.PENDING.value, attempt, kind.value, detail,
                 next_retry_at, at, json.dumps(history), event_id),
            )
            self._conn.commit()

    def recover_in_flight(self) -> int:
        """启动恢复：in_flight -> pending。返回恢复条数。"""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE events SET status=?, updated_at=? WHERE status=?",
                (EventStatus.PENDING.value, time.time(),
                 EventStatus.IN_FLIGHT.value),
            )
            self._conn.commit()
            return cur.rowcount
