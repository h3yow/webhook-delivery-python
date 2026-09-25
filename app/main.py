"""主应用装配（骨架）：事件 API + 挂载模拟接收端 + 调度器生命周期。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .dispatcher import Dispatcher, RetryPolicy
from .models import EventAccepted, EventIn, EventOut
from .receiver import create_receiver_app
from .store import EventStore


def create_app(db_path: str = ":memory:", deliver_url: str = "http://127.0.0.1:9/recv",
               policy: RetryPolicy | None = None) -> FastAPI:
    """创建主应用。deliver_url 默认指向不可达地址，测试中会改为挂载的接收端。"""
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
    app.mount("/_receiver", create_receiver_app())

    @app.get("/")
    def read_root() -> dict:
        return {"Hello": "World"}

    @app.post("/events", status_code=201, response_model=EventAccepted)
    def submit_event(event: EventIn) -> EventAccepted:
        """接收事件并入队；幂等标识冲突返回 409，参数非法返回 422。"""
        raise NotImplementedError

    @app.get("/events/{event_id}", response_model=EventOut)
    def get_event(event_id: str) -> EventOut:
        """查询事件投递进度与状态。"""
        raise NotImplementedError

    @app.get("/events", response_model=list[EventOut])
    def list_events() -> list[EventOut]:
        """列出全部事件。"""
        raise NotImplementedError

    return app
