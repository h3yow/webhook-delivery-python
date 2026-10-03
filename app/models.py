"""领域模型与状态定义（骨架：仅类型定义）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field


class EventStatus(str, enum.Enum):
    """事件生命周期状态。"""

    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    DELIVERED = "delivered"
    FAILED = "failed"  # 不可重试失败，终态


class FailureKind(str, enum.Enum):
    """投递失败分类。"""

    NONE = "none"
    TIMEOUT = "timeout"  # 可重试
    CONNECTION = "connection"  # 可重试
    SERVER_5XX = "server_5xx"  # 可重试
    RATE_LIMITED = "rate_limited"  # 可重试 (HTTP 429)
    CLIENT_4XX = "client_4xx"  # 不可重试
    RECEIVER_REJECTED = "receiver_rejected"  # 不可重试（接收端业务拒绝）


# 可重试的失败类别
RETRIABLE_KINDS: frozenset[FailureKind] = frozenset(
    {
        FailureKind.TIMEOUT,
        FailureKind.CONNECTION,
        FailureKind.SERVER_5XX,
        FailureKind.RATE_LIMITED,
    }
)


class EventIn(BaseModel):
    """事件提交请求体。"""

    event_id: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=128)
    receiver: str = Field(min_length=1, max_length=256)
    payload: dict[str, Any] = Field(default_factory=dict)


class TargetIn(BaseModel):
    """投递目标注册请求体。"""

    url: str = Field(min_length=1, max_length=2048)


class TargetUpdate(BaseModel):
    """投递目标更新请求体；目前仅允许停用/重新启用。"""

    enabled: bool


class TargetOut(BaseModel):
    """投递目标查询/登记响应。"""

    name: str
    url: str
    enabled: bool
    created_at: float
    updated_at: float
    duplicated: bool = False


class EventAccepted(BaseModel):
    """事件受理响应。"""

    event_id: str
    status: EventStatus
    duplicated: bool = False


class AttemptOut(BaseModel):
    """单次投递尝试记录。"""

    attempt: int
    kind: FailureKind
    detail: str
    at: float


class EventOut(BaseModel):
    """事件状态查询响应。"""

    event_id: str
    event_type: str
    receiver: str
    target: str | None = None
    status: EventStatus
    attempts: int
    max_attempts: int
    last_failure_kind: FailureKind
    last_failure_detail: str
    next_retry_at: float | None
    history: list[AttemptOut] = Field(default_factory=list)


@dataclass
class TargetRecord:
    """持久化的投递目标记录。名字即路由键，名字与地址一一对应、不可改址。"""

    name: str
    url: str
    enabled: bool = True
    created_at: float = 0.0
    updated_at: float = 0.0
    duplicated: bool = False


@dataclass
class EventRecord:
    """持久化的事件记录。"""

    event_id: str
    event_type: str
    receiver: str
    payload: dict[str, Any]
    status: EventStatus = EventStatus.PENDING
    attempts: int = 0
    max_attempts: int = 5
    last_failure_kind: FailureKind = FailureKind.NONE
    last_failure_detail: str = ""
    next_retry_at: float | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    history: list[AttemptOut] = field(default_factory=list)
