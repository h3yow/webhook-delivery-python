"""测试设施：本地模拟接收端（真实 HTTP）+ 被测服务（ASGI 直挂）。"""
from __future__ import annotations

import asyncio
import inspect
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


@pytest.fixture
async def receiver() -> AsyncIterator[ReceiverHandle]:
    """启动一个真实 HTTP 模拟接收端（独立端口）。"""
    port = _free_port()
    config = uvicorn.Config(create_receiver_app(), host="127.0.0.1",
                            port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    url = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient(base_url=url) as client:
        yield ReceiverHandle(url=url, client=client, server=server, task=task)
    server.should_exit = True
    await task


@dataclass
class ServiceHandle:
    client: httpx.AsyncClient
    db_path: str

    async def submit(self, **event) -> httpx.Response:
        return await self.client.post("/events", json=event)

    async def status(self, event_id: str) -> dict:
        r = await self.client.get(f"/events/{event_id}")
        assert r.status_code == 200, r.text
        return r.json()


def make_service_params(tmp_path, deliver_url: str, **overrides) -> dict:
    policy = RetryPolicy(base_delay=0.05, factor=2.0, max_delay=0.5,
                         max_attempts=overrides.pop("max_attempts", 4))
    return {
        "db_path": str(tmp_path / "events.db"),
        "deliver_url": deliver_url,
        "policy": policy,
        **overrides,
    }


@pytest.fixture
async def service(tmp_path, receiver) -> AsyncIterator[ServiceHandle]:
    """被测服务：文件型 SQLite + 指向模拟接收端的调度器。"""
    params = make_service_params(tmp_path, f"{receiver.url}/recv")
    app = create_app(**params)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://svc") as client:
            yield ServiceHandle(client=client, db_path=params["db_path"])


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
