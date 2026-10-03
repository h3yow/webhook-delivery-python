"""主应用装配：事件 API + 投递目标注册/路由 + 模拟接收端 + 调度器生命周期。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request, Response

from .dispatcher import Dispatcher, RetryPolicy
from .models import (EventAccepted, EventIn, EventOut, EventRecord,
                     TargetIn, TargetOut, TargetPatch, TargetRecord)
from .receiver import create_receiver_app
from .store import (DuplicateEventError, DuplicateTargetError, EventStore)


def _validate_target_url(url: str) -> str | None:
    """校验目标地址合法性，返回错误消息；合法返回 None。"""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return f"url {url!r} 非法：必须是带主机名的 http(s) 地址"
    return None


def create_app(db_path: str = ":memory:", deliver_url: str | None = None,
               policy: RetryPolicy | None = None,
               timeout: float = 2.0,
               receiver_app: FastAPI | None = None,
               default_targets: dict[str, str] | None = None,
               max_pending: int = 1000) -> FastAPI:
    """创建主应用。

    - db_path: SQLite 路径，":memory:" 表示纯内存；
    - deliver_url: 兼容模式下的固定投递地址；为 None 时进入注册表模式，
      未登记名字的接收端在提交期被拒绝；
    - default_targets: 启动时播种的默认目标（幂等，已存在则跳过）；
    - max_pending: 待投递事件（pending + in_flight）上限，超出拒绝受理；
    - receiver_app: 为 None 时挂载内置模拟接收端到 /_receiver。
    """
    store = EventStore(db_path)
    dispatcher = Dispatcher(store, deliver_url, policy=policy, timeout=timeout)

    for name, url in (default_targets or {}).items():
        if store.get_target(name) is None:
            store.insert_target(TargetRecord(name=name, url=url))

    def target_url_of(receiver: str) -> str | None:
        target = store.get_target(receiver)
        if target is not None:
            return target.url
        return deliver_url

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
            target_url=target_url_of(rec.receiver),
            history=rec.history,
        )

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

    # ---- 投递目标注册与维护 ----

    @app.post("/targets", status_code=201, response_model=TargetOut)
    def register_target(target: TargetIn, response: Response) -> TargetOut:
        """登记具名投递目标。

        - 201: 首次登记；
        - 200: 同名同地址重复登记（幂等），不变更；
        - 409: 同名但指向另一个地址（reason=target_conflict）；
        - 422: 登记信息不合法（reason=invalid_target）。
        """
        if err := _validate_target_url(target.url):
            raise HTTPException(
                status_code=422,
                detail={"reason": "invalid_target", "message": err})
        existing = store.get_target(target.name)
        if existing is not None:
            if existing.url != target.url:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "reason": "target_conflict",
                        "message": f"目标 {target.name!r} 已登记为"
                                   f" {existing.url!r}，不能再指向 {target.url!r}",
                    })
            response.status_code = 200
            return TargetOut(**existing.__dict__)
        record = TargetRecord(name=target.name, url=target.url)
        try:
            store.insert_target(record)
        except DuplicateTargetError:
            # 并发下唯一约束兜底
            raise HTTPException(
                status_code=409,
                detail={"reason": "target_conflict",
                        "message": f"目标 {target.name!r} 已存在"})
        return TargetOut(**record.__dict__)

    @app.get("/targets", response_model=list[TargetOut])
    def list_targets() -> list[TargetOut]:
        return [TargetOut(**t.__dict__) for t in store.list_targets()]

    @app.get("/targets/{name}", response_model=TargetOut)
    def get_target(name: str) -> TargetOut:
        target = store.get_target(name)
        if target is None:
            raise HTTPException(
                status_code=404,
                detail={"reason": "target_not_found",
                        "message": f"目标 {name!r} 未登记"})
        return TargetOut(**target.__dict__)

    @app.patch("/targets/{name}", response_model=TargetOut)
    def update_target(name: str, patch: TargetPatch) -> TargetOut:
        """更新目标地址或临时停用/重新启用。"""
        if patch.url is not None:
            if err := _validate_target_url(patch.url):
                raise HTTPException(
                    status_code=422,
                    detail={"reason": "invalid_target", "message": err})
        target = store.update_target(name, url=patch.url,
                                     enabled=patch.enabled)
        if target is None:
            raise HTTPException(
                status_code=404,
                detail={"reason": "target_not_found",
                        "message": f"目标 {name!r} 未登记"})
        return TargetOut(**target.__dict__)

    # ---- 事件提交与查询 ----

    @app.post("/events", status_code=201, response_model=EventAccepted)
    def submit_event(event: EventIn, response: Response) -> EventAccepted:
        """接收事件并按其 receiver 名字路由到已登记目标入队。

        - 201: 首次受理；
        - 200 + duplicated=true: 同一幂等标识重复提交（内容一致），不重复投递；
        - 409: 幂等标识冲突（同一 event_id 但内容不同）；
        - 422: 参数不合法，或接收端名字未登记（注册表模式，
          reason=unknown_receiver）；
        - 429: 待投递事件超过容量上限（reason=capacity_exceeded）。
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
        if deliver_url is None and store.get_target(event.receiver) is None:
            raise HTTPException(
                status_code=422,
                detail={
                    "reason": "unknown_receiver",
                    "message": f"接收端 {event.receiver!r} 未登记投递目标",
                })
        if store.count_pending() >= max_pending:
            raise HTTPException(
                status_code=429,
                detail={
                    "reason": "capacity_exceeded",
                    "message": f"待投递事件已达上限 {max_pending}，请稍后重试",
                })
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
