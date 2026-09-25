"""主应用装配：事件 API + 挂载模拟接收端 + 调度器生命周期。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response

from .dispatcher import Dispatcher, RetryPolicy
from .models import EventAccepted, EventIn, EventOut, EventRecord
from .receiver import create_receiver_app
from .store import DuplicateEventError, EventStore


def _to_out(rec: EventRecord) -> EventOut:
    return EventOut(
        event_id=rec.event_id,
        event_type=rec.event_type,
        receiver=rec.receiver,
        status=rec.status,
        attempts=rec.attempts,
        max_attempts=rec.max_attempts,
        last_failure_kind=rec.last_failure_kind,
        last_failure_detail=rec.last_failure_detail,
        next_retry_at=rec.next_retry_at,
        history=rec.history,
    )


def create_app(db_path: str = ":memory:", deliver_url: str = "http://127.0.0.1:9/recv",
               policy: RetryPolicy | None = None,
               receiver_app: FastAPI | None = None) -> FastAPI:
    """创建主应用。

    - db_path: SQLite 路径，":memory:" 表示纯内存；
    - deliver_url: 投递目标（测试指向本地模拟接收端）；
    - receiver_app: 为 None 时挂载内置模拟接收端到 /_receiver。
    """
    store = EventStore(db_path)
    dispatcher = Dispatcher(store, deliver_url, policy=policy)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await dispatcher.start()
        yield
        await dispatcher.stop()
        store.close()

    app = FastAPI(title="webhook-delivery", lifespan=lifespan)
    app.state.store = store
    app.state.dispatcher = dispatcher
    if receiver_app is None:
        receiver_app = create_receiver_app()
    app.mount("/_receiver", receiver_app)

    @app.get("/")
    def read_root() -> dict:
        return {"Hello": "World"}

    @app.post("/events", status_code=201, response_model=EventAccepted)
    def submit_event(event: EventIn, response: Response) -> EventAccepted:
        """接收事件并入队。

        - 201: 首次受理；
        - 200 + duplicated=true: 同一幂等标识重复提交（内容一致），不重复投递；
        - 409: 幂等标识冲突（同一 event_id 但内容不同）；
        - 422: 参数不合法（由校验层自动拒绝）。
        """
        existing = store.get(event.event_id)
        if existing is not None:
            same = (existing.event_type == event.event_type
                    and existing.receiver == event.receiver
                    and existing.payload == event.payload)
            if not same:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "reason": "idempotency_conflict",
                        "message": f"event_id {event.event_id!r} 已存在且内容不同",
                    },
                )
            response.status_code = 200
            return EventAccepted(event_id=event.event_id,
                                 status=existing.status, duplicated=True)
        record = EventRecord(
            event_id=event.event_id,
            event_type=event.event_type,
            receiver=event.receiver,
            payload=event.payload,
            max_attempts=(policy or RetryPolicy()).max_attempts,
        )
        try:
            store.insert(record)
        except DuplicateEventError:
            # 并发下唯一约束兜底：与上面 get 的竞态由数据库裁决
            raise HTTPException(
                status_code=409,
                detail={
                    "reason": "idempotency_conflict",
                    "message": f"event_id {event.event_id!r} 已存在",
                },
            )
        return EventAccepted(event_id=event.event_id, status=record.status)

    @app.get("/events/{event_id}", response_model=EventOut)
    def get_event(event_id: str) -> EventOut:
        rec = store.get(event_id)
        if rec is None:
            raise HTTPException(
                status_code=404,
                detail={"reason": "not_found",
                        "message": f"event {event_id!r} 不存在"},
            )
        return _to_out(rec)

    @app.get("/events", response_model=list[EventOut])
    def list_events() -> list[EventOut]:
        return [_to_out(r) for r in store.list_events()]

    return app
