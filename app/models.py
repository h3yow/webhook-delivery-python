"""领域模型与类型定义：投递状态、失败分类、事件与尝试记录。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


class DeliveryStatus(str, enum.Enum):
    """事件投递生命周期状态。"""

    PENDING = "pending"          # 已入队，等待投递
    IN_FLIGHT = "in_flight"      # 正在投递中
    DELIVERED = "delivered"      # 投递成功（终态）
    DEAD = "dead"                # 终态失败：不可重试失败或重试耗尽


class FailureKind(str, enum.Enum):
    """投递失败分类，决定重试策略。"""

    TIMEOUT = "timeout"              # 单次投递超时（可重试）
    CONNECTION = "connection"        # 连接错误（可重试）
    HTTP_5XX = "http_5xx"            # 接收端 5xx（可重试）
    HTTP_4XX = "http_4xx"            # 接收端 4xx（不可重试）


# 可重试的失败类别
RETRYABLE_KINDS: frozenset[FailureKind] = frozenset(
    {FailureKind.TIMEOUT, FailureKind.CONNECTION, FailureKind.HTTP_5XX}
)


@dataclass
class Event:
    """待投递事件。"""

    event_id: str
    idempotency_key: str
    endpoint: str
    payload: dict[str, Any]
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempts: int = 0
    max_attempts: int = 5
    next_attempt_at: float = 0.0
    last_failure_kind: str | None = None
    last_failure_detail: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex


@dataclass
class DeliveryAttempt:
    """单次投递尝试记录。"""

    event_id: str
    attempt_no: int
    outcome: str                 # "success" | "failure"
    failure_kind: str | None = None
    http_status: int | None = None
    duration_ms: float = 0.0
    detail: str | None = None
    ts: float = field(default_factory=time.time)


def compute_backoff(attempt_no: int, base: float = 0.05, cap: float = 5.0) -> float:
    """指数退避：base * 2^(attempt_no-1)，封顶 cap 秒。"""
    return min(base * (2 ** max(0, attempt_no - 1)), cap)
