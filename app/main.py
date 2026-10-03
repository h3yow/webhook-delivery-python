"""主应用装配：事件 API + 投递目标注册 + 挂载模拟接收端 + 调度器生命周期。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response

from .dispatcher import Dispatcher, RetryPolicy
from .models import (EventAccepted, EventIn, EventOut, EventRecord,
                     TargetIn, TargetOut, TargetRecord, TargetUpdate)
from .receiver import create_receiver_app
from .store import (CapacityExceededError, DuplicateEventError,
                    EventStore, TargetAddressConflictError)

# 默认待投递事件上限：背压，防止积压无界增长
DEFAULT_MAX_OUTSTANDING = 1000


def _to_out(rec: EventRecord) -> EventOut:
    return EventOut(
        event_id=rec.event_id,
        event_type=rec.event_type,
        receiver=rec.receiver,
        target=rec.receiver,
        status=rec.status,
        attempts=rec.attempts,
        max_attempts=rec.max_attempts,
        last_failure_kind=rec.last_failure_kind,
        last_failure_detail=rec.last_failure_detail,
        next_retry_at=rec.next_retry_at,
        history=rec.history,
    )


def _to_target_out(rec: TargetRecord) -> TargetOut:
    return TargetOut(
        name=rec.name,
        url=rec.url,
        enabled=rec.enabled,
        created_at=rec.created_at,
        updated_at=rec.updated_at,
        duplicated=rec.duplicated,
    )


def create_app(db_path: str = ":memory:",
               deliver_url: str | None = None,
               policy: RetryPolicy | None = None,
               timeout: float = 2.0,
               receiver_app: FastAPI | None = None,
               targets: dict[str, str] | None = None,
               max_outstanding: int | None = DEFAULT_MAX_OUTSTANDING) -> FastAPI:
    """创建主应用。

    两种装配方式：

    - **路由模式**（推荐/默认启动）：``targets`` 预置具名目标，事件按其
      ``receiver`` 字段路由到登记地址；未登记名字的事件被拒绝。
    - **固定地址模式**（兼容旧用例）：传入 ``deliver_url`` 且不预置目标，
      所有事件投往同一固定地址，不做名字校验、不做容量背压。

    - db_path: SQLite 路径，":memory:" 表示纯内存；
    - receiver_app: 为 None 时挂载内置模拟接收端到 /_receiver。
    """
    routed = bool(targets) or deliver_url is None
    store = EventStore(db_path,
                       max_outstanding=max_outstanding if routed else None)
    dispatcher = Dispatcher(store,
                            deliver_url=deliver_url,
                            policy=policy,
                            timeout=timeout,
                            routed=routed)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 预置目标在恢复之前登记（INSERT OR IGNORE，重启不覆盖既有登记）
        for name, url in (targets or {}).items():
            store.upsert_target(TargetRecord(name=name, url=url))
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

    # ---- 投递目标注册与维护 ----

    @app.put("/targets/{name}", response_model=TargetOut)
    def register_target(name: str, body: TargetIn,
                        response: Response) -> TargetOut:
        """登记（或幂等重复登记）一个具名投递目标。

        - 201: 首次登记；
        - 200 + duplicated=true: 同名同址重复登记；
        - 409: 同名已指向另一个地址（target_address_conflict）。
        """
        if not name.strip():
            raise HTTPException(
                status_code=422,
                detail={"reason": "invalid_target",
                        "message": "目标名不能为空"})
        try:
            rec = store.upsert_target(
                TargetRecord(name=name, url=body.url))
        except TargetAddressConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "reason": "target_address_conflict",
                    "message": str(exc),
                    "name": exc.name,
                    "existing_url": exc.existing_url,
                    "new_url": exc.new_url,
                },
            )
        if rec.duplicated:
            response.status_code = 200
        else:
            response.status_code = 201
            dispatcher.notify()
        return _to_target_out(rec)

    @app.get("/targets", response_model=list[TargetOut])
    def list_targets() -> list[TargetOut]:
        return [_to_target_out(t) for t in store.list_targets()]

    @app.get("/targets/{name}", response_model=TargetOut)
    def get_target(name: str) -> TargetOut:
        rec = store.get_target(name)
        if rec is None:
            raise HTTPException(
                status_code=404,
                detail={"reason": "target_not_found",
                        "message": f"target {name!r} 未登记"},
            )
        return _to_target_out(rec)

    @app.patch("/targets/{name}", response_model=TargetOut)
    def update_target(name: str, update: TargetUpdate) -> TargetOut:
        """临时停用 / 重新启用目标；地址不可通过此接口更改。"""
        rec = store.set_target_enabled(name, update.enabled)
        if rec is None:
            raise HTTPException(
                status_code=404,
                detail={"reason": "target_not_found",
                        "message": f"target {name!r} 未登记"},
            )
        dispatcher.notify()  # 重新启用后尽快续投积压事件
        return _to_target_out(rec)

    # ---- 事件受理与状态查询 ----

    @app.post("/events", status_code=201, response_model=EventAccepted)
    def submit_event(event: EventIn, response: Response) -> EventAccepted:
        """接收事件并入队。

        - 201: 首次受理；
        - 200 + duplicated=true: 同一幂等标识重复提交（内容一致），不重复投递；
        - 409: 幂等标识冲突（同一 event_id 但内容不同）；
        - 422: 参数不合法，或 receiver 名字未登记（reason 可区分）；
        - 503: 待投递事件数已达上限（capacity_exceeded）。
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
        if routed and store.get_target(event.receiver) is None:
            raise HTTPException(
                status_code=422,
                detail={
                    "reason": "target_not_registered",
                    "message": f"receiver {event.receiver!r} 未登记为投递目标",
                    "receiver": event.receiver,
                },
            )
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
        except CapacityExceededError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "reason": "capacity_exceeded",
                    "message": "待投递事件过多，请稍后重试",
                    "limit": exc.limit,
                },
            )
        dispatcher.notify()
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
