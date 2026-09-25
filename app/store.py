"""SQLite 事件存储（骨架：接口与空实现）。"""
from __future__ import annotations

import threading

from .models import EventRecord, EventStatus, FailureKind


class DuplicateEventError(Exception):
    """幂等标识冲突：同一 event_id 已存在。"""


class EventStore:
    """基于 SQLite 的事件持久化存储，进程重启后状态可恢复。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = None  # TODO: 建立连接并建表

    def close(self) -> None:
        """关闭连接。"""
        raise NotImplementedError

    def insert(self, record: EventRecord) -> None:
        """插入新事件；event_id 冲突时抛 DuplicateEventError。"""
        raise NotImplementedError

    def get(self, event_id: str) -> EventRecord | None:
        """按幂等标识查询事件。"""
        raise NotImplementedError

    def list_events(self) -> list[EventRecord]:
        """列出全部事件（按创建时间排序）。"""
        raise NotImplementedError

    def claim_due(self, now: float, limit: int) -> list[EventRecord]:
        """原子认领到期事件：pending 且 next_retry_at<=now -> in_flight。"""
        raise NotImplementedError

    def complete(self, event_id: str, status: EventStatus, kind: FailureKind,
                 detail: str, attempt: int, at: float) -> None:
        """投递结束：写终态（delivered/failed）并追加历史。"""
        raise NotImplementedError

    def reschedule(self, event_id: str, kind: FailureKind, detail: str,
                   attempt: int, next_retry_at: float, at: float) -> None:
        """投递失败但可重试：回到 pending 并设置下次重试时间。"""
        raise NotImplementedError

    def recover_in_flight(self) -> int:
        """启动恢复：in_flight -> pending（重启后未完成任务不得丢失/误判）。"""
        raise NotImplementedError
