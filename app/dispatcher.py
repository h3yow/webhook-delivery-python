"""投递调度器：工作线程池按到期时间领取事件并并发投递，单事件超时隔离。"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from .models import DeliveryAttempt, Event, FailureKind, RETRYABLE_KINDS, compute_backoff
from .store import EventStore

logger = logging.getLogger("dispatcher")


class DeliveryResult:
    """单次投递的结果分类。"""

    def __init__(self, ok: bool, kind: FailureKind | None = None,
                 http_status: int | None = None, detail: str | None = None,
                 duration_ms: float = 0.0) -> None:
        self.ok = ok
        self.kind = kind
        self.http_status = http_status
        self.detail = detail
        self.duration_ms = duration_ms


def deliver_once(event: Event, timeout: float) -> DeliveryResult:
    """执行一次 HTTP 投递并分类结果（成功 / 超时 / 连接错误 / 4xx / 5xx）。"""
    start = time.monotonic()
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(
                event.endpoint,
                json={
                    "event_id": event.event_id,
                    "idempotency_key": event.idempotency_key,
                    "payload": event.payload,
                },
                headers={"X-Idempotency-Key": event.idempotency_key},
            )
        duration_ms = (time.monotonic() - start) * 1000
        status = resp.status_code
        if 200 <= status < 300:
            return DeliveryResult(True, http_status=status, duration_ms=duration_ms)
        if 400 <= status < 500:
            return DeliveryResult(False, FailureKind.HTTP_4XX, status,
                                  f"receiver returned {status}", duration_ms)
        return DeliveryResult(False, FailureKind.HTTP_5XX, status,
                              f"receiver returned {status}", duration_ms)
    except httpx.TimeoutException as exc:
        duration_ms = (time.monotonic() - start) * 1000
        return DeliveryResult(False, FailureKind.TIMEOUT, None,
                              f"timeout after {duration_ms:.0f}ms: {type(exc).__name__}",
                              duration_ms)
    except httpx.TransportError as exc:
        duration_ms = (time.monotonic() - start) * 1000
        return DeliveryResult(False, FailureKind.CONNECTION, None,
                              f"connection error: {type(exc).__name__}", duration_ms)


class Dispatcher:
    """后台投递调度器：固定大小线程池 + 到期扫描循环。

    - 并发上限由线程池大小决定，领取批量不超过空闲容量，资源有界。
    - 单个事件投递超时由 httpx timeout 隔离，不阻塞其他事件。
    """

    def __init__(self, store: EventStore, workers: int = 4,
                 poll_interval: float = 0.02, timeout: float = 2.0,
                 ordered_per_endpoint: bool = False) -> None:
        self._store = store
        self._workers = workers
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._ordered_per_endpoint = ordered_per_endpoint
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=workers,
                                            thread_name_prefix="deliver")
        self._in_flight = 0
        self._count_lock = threading.Lock()
        self._loop_thread: threading.Thread | None = None

    def start(self) -> None:
        self._loop_thread = threading.Thread(target=self._run_loop,
                                             name="dispatch-loop", daemon=True)
        self._loop_thread.start()

    def stop(self, wait: bool = True) -> None:
        self._stop.set()
        self._wakeup.set()
        if wait:
            if self._loop_thread is not None:
                self._loop_thread.join(timeout=5)
            self._executor.shutdown(wait=True)

    def notify(self) -> None:
        """唤醒调度循环，尽快领取新事件。"""
        self._wakeup.set()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            with self._count_lock:
                capacity = self._workers - self._in_flight
            if capacity > 0:
                events = self._store.claim_due_events(
                    time.time(), capacity,
                    ordered_per_endpoint=self._ordered_per_endpoint)
                for event in events:
                    with self._count_lock:
                        self._in_flight += 1
                    self._executor.submit(self._process, event)
            self._wakeup.wait(self._poll_interval)
            self._wakeup.clear()

    def _process(self, event: Event) -> None:
        """投递单个事件并根据结果更新状态与退避。"""
        try:
            result = deliver_once(event, self._timeout)
            attempt_no = event.attempts + 1
            if result.ok:
                attempt = DeliveryAttempt(event.event_id, attempt_no, "success",
                                          http_status=result.http_status,
                                          duration_ms=result.duration_ms)
                self._store.mark_delivered(event.event_id, attempt)
                logger.info("event=%s key=%s attempt=%d 判定=成功 http=%s",
                            event.event_id, event.idempotency_key,
                            attempt_no, result.http_status)
                return

            kind = result.kind or FailureKind.CONNECTION
            retryable = kind in RETRYABLE_KINDS and attempt_no < event.max_attempts
            attempt = DeliveryAttempt(
                event.event_id, attempt_no, "failure",
                failure_kind=kind.value, http_status=result.http_status,
                duration_ms=result.duration_ms, detail=result.detail,
            )
            if retryable:
                backoff = compute_backoff(attempt_no)
                next_at = time.time() + backoff
                self._store.mark_retry(event.event_id, attempt, next_at)
                logger.info("event=%s key=%s attempt=%d 判定=可重试 原因=%s 退避=%.3fs",
                            event.event_id, event.idempotency_key,
                            attempt_no, kind.value, backoff)
            else:
                # 终态失败：保留真实失败原因（超时/连接/5xx/4xx），
                # 重试耗尽由 attempts == max_attempts 体现
                self._store.mark_dead(event.event_id, attempt)
                logger.info("event=%s key=%s attempt=%d 判定=终态失败 原因=%s",
                            event.event_id, event.idempotency_key,
                            attempt_no, kind.value)
        finally:
            with self._count_lock:
                self._in_flight -= 1
            self._wakeup.set()
