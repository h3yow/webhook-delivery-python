"""HTTP API：事件提交（幂等）、状态查询、模拟接收端。"""
from __future__ import annotations

import asyncio
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .dispatcher import Dispatcher
from .models import Event
from .store import EventStore


class SubmitEventRequest(BaseModel):
    """提交事件请求体。"""

    idempotency_key: str = Field(min_length=1, max_length=255)
    endpoint: str = Field(min_length=1)
    payload: dict[str, Any]
    max_attempts: int = Field(default=5, ge=1, le=20)


class SubmitEventResponse(BaseModel):
    event_id: str
    status: str
    duplicated: bool = False


def _event_view(store: EventStore, event: Event) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "idempotency_key": event.idempotency_key,
        "endpoint": event.endpoint,
        "payload": event.payload,
        "status": event.status.value,
        "attempts": event.attempts,
        "max_attempts": event.max_attempts,
        "last_failure_kind": event.last_failure_kind,
        "last_failure_detail": event.last_failure_detail,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
    }


def create_app(db_path: str = ":memory:", workers: int = 4,
               poll_interval: float = 0.02, timeout: float = 2.0,
               ordered_per_endpoint: bool = False) -> FastAPI:
    """构建 FastAPI 应用：装配存储、调度器与路由。"""
    store = EventStore(db_path)
    dispatcher = Dispatcher(store, workers=workers,
                            poll_interval=poll_interval, timeout=timeout,
                            ordered_per_endpoint=ordered_per_endpoint)
    # 模拟接收端的内存收件箱（仅用于本地验证与测试）
    receiver_inbox: list[dict[str, Any]] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 重启恢复：崩溃/重启残留的 in_flight 事件重置为 pending，不丢失不误判
        recovered = store.recover_in_flight()
        if recovered:
            app.state.recovered = recovered
        dispatcher.start()
        yield
        dispatcher.stop()
        store.close()

    app = FastAPI(title="webhook-delivery", lifespan=lifespan)
    app.state.store = store
    app.state.dispatcher = dispatcher
    app.state.receiver_inbox = receiver_inbox

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(status_code=500,
                            content={"error": "internal", "detail": str(exc)})

    @app.post("/events", status_code=201)
    def submit_event(req: SubmitEventRequest) -> Any:
        if not req.endpoint.startswith(("http://", "https://")):
            return JSONResponse(status_code=422, content={
                "error": "invalid_endpoint",
                "detail": "endpoint must be an http(s) URL",
            })

        def duplicate_response(existing: Event) -> JSONResponse:
            if existing.endpoint == req.endpoint and existing.payload == req.payload:
                # 重复提交：返回已存在事件，不产生重复投递
                return JSONResponse(status_code=200, content=SubmitEventResponse(
                    event_id=existing.event_id, status=existing.status.value,
                    duplicated=True,
                ).model_dump())
            # 幂等标识冲突：相同 key 但内容不同
            return JSONResponse(status_code=409, content={
                "error": "idempotency_conflict",
                "detail": "idempotency_key already used with different content",
                "event_id": existing.event_id,
            })

        existing = store.get_by_idempotency_key(req.idempotency_key)
        if existing is not None:
            return duplicate_response(existing)
        event = Event(
            event_id=Event.new_id(),
            idempotency_key=req.idempotency_key,
            endpoint=req.endpoint,
            payload=req.payload,
            max_attempts=req.max_attempts,
        )
        try:
            store.insert_event(event)
        except sqlite3.IntegrityError:
            # 并发下唯一约束兜底：另一线程已插入同一幂等标识
            winner = store.get_by_idempotency_key(req.idempotency_key)
            assert winner is not None
            return duplicate_response(winner)
        dispatcher.notify()
        return SubmitEventResponse(event_id=event.event_id,
                                   status=event.status.value)

    @app.get("/events")
    def list_events(endpoint: str | None = None, status: str | None = None) -> Any:
        return {"events": [_event_view(store, e)
                           for e in store.list_events(endpoint, status)]}

    @app.get("/events/{event_id}")
    def get_event(event_id: str) -> Any:
        event = store.get_event(event_id)
        if event is None:
            return JSONResponse(status_code=404,
                                content={"error": "not_found"})
        view = _event_view(store, event)
        view["attempts_log"] = [
            {
                "attempt_no": a.attempt_no,
                "outcome": a.outcome,
                "failure_kind": a.failure_kind,
                "http_status": a.http_status,
                "duration_ms": a.duration_ms,
                "detail": a.detail,
                "ts": a.ts,
            }
            for a in store.list_attempts(event_id)
        ]
        return view

    @app.get("/stats")
    def get_stats() -> Any:
        return store.stats()

    # --- 本地模拟接收端：不依赖外部服务 ---
    @app.post("/_receiver")
    async def receiver(request: Request) -> Any:
        body = await request.json()
        receiver_inbox.append({
            "event_id": body.get("event_id"),
            "idempotency_key": body.get("idempotency_key"),
            "ts": time.time(),
        })
        simulate = (body.get("payload") or {}).get("simulate", "ok")
        if simulate == "timeout":
            try:
                await asyncio.sleep(10)  # 超过投递超时，由客户端判定超时失败
            except asyncio.CancelledError:  # 服务关闭时安静退出
                return JSONResponse(status_code=499, content={"error": "shutdown"})
        elif simulate == "fail500":
            return JSONResponse(status_code=500, content={"error": "simulated 5xx"})
        elif simulate == "fail400":
            return JSONResponse(status_code=400, content={"error": "simulated 4xx"})
        elif isinstance(simulate, dict) and "flaky" in simulate:
            # 前 N 次 5xx，之后成功
            key = body.get("idempotency_key")
            seen = sum(1 for r in receiver_inbox if r["idempotency_key"] == key)
            if seen <= int(simulate["flaky"]):
                return JSONResponse(status_code=500,
                                    content={"error": f"flaky fail #{seen}"})
        return {"received": True}

    @app.get("/_receiver/inbox")
    def receiver_inbox_view() -> Any:
        return {"received": list(receiver_inbox), "count": len(receiver_inbox)}

    return app
