"""投递调度器：按接收端串行、跨接收端并发，可解释的分类与退避重试。"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict

import httpx

from .models import EventRecord, EventStatus, FailureKind, RETRIABLE_KINDS
from .store import EventStore

logger = logging.getLogger("dispatcher")


class RetryPolicy:
    """可解释的指数退避：delay = base_delay * factor^(attempt-1)，封顶 max_delay。

    attempt 从 1 开始（第 1 次失败后的等待）。达到 max_attempts 后不再重试。
    """

    def __init__(self, base_delay: float = 0.5, factor: float = 2.0,
                 max_delay: float = 30.0, max_attempts: int = 5) -> None:
        self.base_delay = base_delay
        self.factor = factor
        self.max_delay = max_delay
        self.max_attempts = max_attempts

    def delay_for(self, attempt: int) -> float:
        return min(self.max_delay, self.base_delay * (self.factor ** (attempt - 1)))


class Dispatcher:
    """后台投递循环。

    - 每次投递有独立超时，超时判定为失败，且单个慢接收端不阻塞其他接收端；
    - 同一接收端（目标）的事件严格按序投递（per-receiver 锁），不同目标并发；
    - 每个事件按 receiver 名字路由到已登记目标的地址；未登记的名字回落到
      deliver_url（兼容固定地址模式；为 None 时未登记名字在提交期已被拒绝）；
    - 全局信号量限制并发投递数，避免资源无界增长。
    """

    def __init__(self, store: EventStore, deliver_url: str | None,
                 policy: RetryPolicy | None = None,
                 timeout: float = 2.0,
                 concurrency: int = 4,
                 poll_interval: float = 0.05,
                 batch_size: int = 32) -> None:
        self._store = store
        self._deliver_url = deliver_url
        self._policy = policy or RetryPolicy()
        self._timeout = timeout
        self._concurrency = concurrency
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._client: httpx.AsyncClient | None = None
        self._loop_task: asyncio.Task | None = None
        self._workers: set[asyncio.Task] = set()
        self._receiver_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._semaphore: asyncio.Semaphore | None = None
        self._stopping = False

    async def start(self) -> None:
        """启动调度循环；先做崩溃恢复（in_flight -> pending）。"""
        recovered = self._store.recover_in_flight()
        if recovered:
            logger.warning("recovered %d in-flight events after restart", recovered)
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._stopping = False
        self._loop_task = asyncio.create_task(self._loop(), name="dispatch-loop")

    async def stop(self) -> None:
        """停止调度：等待在途投递结束后关闭。"""
        self._stopping = True
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        if self._client:
            await self._client.aclose()

    async def _loop(self) -> None:
        while not self._stopping:
            try:
                due = self._store.claim_due(
                    time.time(), self._batch_size,
                    allow_unknown_target=self._deliver_url is not None)
            except Exception:
                logger.exception("claim_due failed")
                due = []
            by_receiver: dict[str, list[EventRecord]] = defaultdict(list)
            for rec in due:
                by_receiver[rec.receiver].append(rec)
            for receiver, records in by_receiver.items():
                self._spawn_worker(receiver, records)
            await asyncio.sleep(self._poll_interval)

    def _spawn_worker(self, receiver: str, records: list[EventRecord]) -> None:
        task = asyncio.create_task(
            self._deliver_sequential(receiver, records),
            name=f"deliver:{receiver}",
        )
        self._workers.add(task)
        task.add_done_callback(self._workers.discard)

    async def _deliver_sequential(self, receiver: str,
                                  records: list[EventRecord]) -> None:
        """同一接收端串行投递，保证顺序约束。"""
        lock = self._receiver_locks[receiver]
        async with lock:
            for rec in records:
                if self._stopping:
                    # 停止时把未处理事件退回 pending，等待下次恢复
                    self._store.reschedule(
                        rec.event_id, FailureKind.NONE, "dispatcher stopped",
                        rec.attempts, time.time(), time.time())
                    continue
                async with self._semaphore:
                    await self._deliver_one(rec)

    def resolve_url(self, receiver: str) -> str | None:
        """按接收端名字解析投递地址：已登记目标优先，否则回落默认地址。"""
        target = self._store.get_target(receiver)
        if target is not None:
            return target.url if target.enabled else None
        return self._deliver_url

    async def _deliver_one(self, rec: EventRecord) -> None:
        attempt = rec.attempts + 1
        url = self.resolve_url(rec.receiver)
        if url is None:
            # 目标在认领后被停用，或无路由可用：退回 pending 等待恢复，
            # 不消耗重试次数、不判失败
            self._store.reschedule(rec.event_id, FailureKind.NONE,
                                   "no route: target disabled or missing",
                                   rec.attempts, time.time() + self._poll_interval * 10,
                                   time.time())
            logger.info("event=%s receiver=%s result=deferred (no route)",
                        rec.event_id, rec.receiver)
            return
        now = time.time()
        exc: BaseException | None = None
        status_code: int | None = None
        body: bytes | None = None
        try:
            resp = await self._client.post(
                url,
                json={
                    "event_id": rec.event_id,
                    "event_type": rec.event_type,
                    "receiver": rec.receiver,
                    "payload": rec.payload,
                    "attempt": attempt,
                },
            )
            status_code = resp.status_code
            body = resp.content
        except BaseException as e:  # 超时/连接错误等
            exc = e

        if exc is None and status_code is not None and 200 <= status_code < 300:
            self._store.complete(rec.event_id, EventStatus.DELIVERED,
                                 FailureKind.NONE, f"http {status_code}",
                                 attempt, time.time())
            logger.info("event=%s attempt=%d result=delivered http=%d",
                        rec.event_id, attempt, status_code)
            return

        kind = self.classify(exc, status_code, body)
        detail = self._detail(exc, status_code, body)
        retriable = kind in RETRIABLE_KINDS
        if retriable and attempt < self._policy.max_attempts:
            delay = self._policy.delay_for(attempt)
            self._store.reschedule(rec.event_id, kind, detail, attempt,
                                   time.time() + delay, time.time())
            decision = f"retry in {delay:.3f}s"
        else:
            self._store.complete(rec.event_id, EventStatus.FAILED, kind,
                                 detail, attempt, time.time())
            decision = ("give up: attempts exhausted" if retriable
                        else "give up: non-retriable failure")
        logger.info("event=%s attempt=%d result=failed kind=%s detail=%r -> %s",
                    rec.event_id, attempt, kind.value, detail, decision)

    def classify(self, exc: BaseException | None, status_code: int | None,
                 body: bytes | None) -> FailureKind:
        """把一次投递结果分类为失败类别（判定依据可解释）。"""
        if exc is not None:
            if isinstance(exc, httpx.TimeoutException):
                return FailureKind.TIMEOUT
            if isinstance(exc, httpx.TransportError):
                return FailureKind.CONNECTION
            return FailureKind.CONNECTION  # 未知异常按连接类瞬时故障处理
        if status_code is None:
            return FailureKind.CONNECTION
        if status_code == 429:
            return FailureKind.RATE_LIMITED
        if 500 <= status_code < 600:
            return FailureKind.SERVER_5XX
        if 400 <= status_code < 500:
            if status_code == 422 and body and b'"rejected"' in body:
                return FailureKind.RECEIVER_REJECTED
            return FailureKind.CLIENT_4XX
        return FailureKind.CLIENT_4XX

    @staticmethod
    def _detail(exc: BaseException | None, status_code: int | None,
                body: bytes | None) -> str:
        if exc is not None:
            return f"{type(exc).__name__}: {exc}"
        text = (body or b"")[:200].decode("utf-8", "replace")
        return f"http {status_code}: {text}"
