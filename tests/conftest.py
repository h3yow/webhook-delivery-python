"""测试设施：本地模拟接收端（真实 HTTP）+ 被测服务（ASGI 直挂）。"""
from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import socket
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
import pytest
import uvicorn

from app.dispatcher import RetryPolicy
from app.main import create_app
from app.receiver import create_receiver_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("tests")


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@dataclass
class ReceiverHandle:
    url: str
    client: httpx.AsyncClient
    server: uvicorn.Server
    task: asyncio.Task

    async def program(self, **cfg) -> None:
        r = await self.client.post("/program", json=cfg)
        assert r.status_code == 200, r.text

    async def deliveries(self) -> list[dict]:
        r = await self.client.get("/deliveries")
        return r.json()["deliveries"]


async def start_receiver() -> ReceiverHandle:
    """启动一个真实 HTTP 模拟接收端（独立端口）。"""
    port = _free_port()
    config = uvicorn.Config(create_receiver_app(), host="127.0.0.1",
                            port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    url = f"http://127.0.0.1:{port}"
    client = httpx.AsyncClient(base_url=url)
    return ReceiverHandle(url=url, client=client, server=server, task=task)


async def stop_receiver(h: ReceiverHandle) -> None:
    await h.client.aclose()
    h.server.should_exit = True
    await h.task


@pytest.fixture
async def receiver_factory():
    handles: list[ReceiverHandle] = []

    async def factory() -> ReceiverHandle:
        h = await start_receiver()
        handles.append(h)
        return h

    yield factory
    for h in handles:
        await stop_receiver(h)


@pytest.fixture
async def receiver(receiver_factory) -> ReceiverHandle:
    return await receiver_factory()


@dataclass
class ServiceHandle:
    client: httpx.AsyncClient
    db_path: str
    app: object

    async def submit(self, **event) -> httpx.Response:
        return await self.client.post("/events", json=event)

    async def status(self, event_id: str) -> dict:
        r = await self.client.get(f"/events/{event_id}")
        assert r.status_code == 200, r.text
        return r.json()

    async def register(self, name: str, url: str) -> httpx.Response:
        return await self.client.put(f"/targets/{name}", json={"url": url})

    async def set_enabled(self, name: str, enabled: bool) -> httpx.Response:
        return await self.client.patch(f"/targets/{name}",
                                       json={"enabled": enabled})

    async def targets(self) -> list[dict]:
        return (await self.client.get("/targets")).json()


def make_service_params(tmp_path, deliver_url: str | None, **overrides) -> dict:
    policy = RetryPolicy(base_delay=0.05, factor=2.0, max_delay=0.5,
                         max_attempts=overrides.pop("max_attempts", 4))
    return {
        "db_path": str(tmp_path / "events.db"),
        "deliver_url": deliver_url,
        "policy": policy,
        **overrides,
    }


@pytest.fixture
async def service_factory(tmp_path):
    """服务工厂。

    - 传 deliver_url：固定地址模式（旧语义，兼容旧用例）；
    - 传 targets={name: url}：路由模式，按事件 receiver 名字选目标。
    """
    stacks: list = []
    counter = itertools.count()

    async def factory(**overrides) -> ServiceHandle:
        deliver_url = overrides.pop("deliver_url", None)
        params = make_service_params(tmp_path, deliver_url, **overrides)
        params["db_path"] = str(
            tmp_path / f"events-{next(counter)}.db")  # 每实例独立 db
        app = create_app(**params)
        cm = app.router.lifespan_context(app)
        await cm.__aenter__()
        stacks.append(cm)
        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://svc")
        stacks.append(client)
        return ServiceHandle(client=client, db_path=params["db_path"], app=app)

    yield factory
    for cm in reversed(stacks):
        if isinstance(cm, httpx.AsyncClient):
            await cm.aclose()
        else:
            await cm.__aexit__(None, None, None)


@pytest.fixture
async def service(service_factory, receiver) -> ServiceHandle:
    return await service_factory(deliver_url=f"{receiver.url}/recv")


@pytest.fixture
async def routed_service(service_factory, receiver_factory) -> ServiceHandle:
    """路由模式服务：预置两个具名目标 ta/tb，分别指向两个本地接收端。"""
    ra = await receiver_factory()
    rb = await receiver_factory()
    svc = await service_factory(targets={
        "ta": f"{ra.url}/recv",
        "tb": f"{rb.url}/recv",
    })
    svc.receivers = {"ta": ra, "tb": rb}
    return svc


async def wait_for(predicate, timeout: float = 5.0, interval: float = 0.02):
    """轮询直到条件满足，返回条件值；超时抛 AssertionError。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return value
        await asyncio.sleep(interval)
    raise AssertionError("wait_for timed out")
