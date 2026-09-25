"""投递调度器（骨架：接口与空实现）。"""
from __future__ import annotations

import asyncio

from .models import FailureKind
from .store import EventStore


class RetryPolicy:
    """可解释的指数退避策略：delay = base * factor^(attempt-1)，封顶 max_delay。"""

    def __init__(self, base_delay: float = 0.5, factor: float = 2.0,
                 max_delay: float = 30.0, max_attempts: int = 5) -> None:
        self.base_delay = base_delay
        self.factor = factor
        self.max_delay = max_delay
        self.max_attempts = max_attempts

    def delay_for(self, attempt: int) -> float:
        """第 attempt 次失败后的等待秒数（attempt 从 1 开始）。"""
        raise NotImplementedError


class Dispatcher:
    """后台投递循环：按接收端串行、不同接收端并发，超时判定失败。"""

    def __init__(self, store: EventStore, deliver_url: str,
                 policy: RetryPolicy | None = None,
                 timeout: float = 2.0,
                 concurrency: int = 4,
                 poll_interval: float = 0.05) -> None:
        self._store = store
        self._deliver_url = deliver_url
        self._policy = policy or RetryPolicy()
        self._timeout = timeout
        self._concurrency = concurrency
        self._poll_interval = poll_interval
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event() if False else None  # 占位

    async def start(self) -> None:
        """启动后台调度循环（先做崩溃恢复）。"""
        raise NotImplementedError

    async def stop(self) -> None:
        """停止调度循环并等待退出。"""
        raise NotImplementedError

    def classify(self, exc: BaseException | None, status_code: int | None,
                 body: bytes | None) -> FailureKind:
        """把一次投递结果分类为失败类别（判定依据可解释）。"""
        raise NotImplementedError
