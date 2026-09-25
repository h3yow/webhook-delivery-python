"""测试夹具：在本机 ephemeral 端口启动真实 uvicorn 服务，互不依赖外部服务。"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request

import pytest
import uvicorn

from app.api import create_app

logger = logging.getLogger("tests")


class ServerHandle:
    def __init__(self, app, server, thread, port):
        self.app = app
        self.server = server
        self.thread = thread
        self.port = port
        self.base = f"http://127.0.0.1:{port}"

    # --- 极简 HTTP 客户端 ---
    def post(self, path, body):
        req = urllib.request.Request(
            self.base + path, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=10) as r:
            return json.loads(r.read())

    def wait_status(self, event_id, finals=("delivered", "dead"), timeout=15.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            ev = self.get(f"/events/{event_id}")
            if ev["status"] in finals:
                return ev
            time.sleep(0.02)
        raise TimeoutError(f"event {event_id} not final within {timeout}s")

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=10)


def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(**kwargs) -> ServerHandle:
    app = create_app(**kwargs)
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port,
                            log_level="error", timeout_graceful_shutdown=0)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("server failed to start")
        time.sleep(0.02)
    return ServerHandle(app, server, thread, port)


@pytest.fixture()
def server():
    h = start_server(db_path=":memory:", poll_interval=0.01, timeout=0.5)
    yield h
    h.stop()


def log_event_outcome(event: dict) -> None:
    """按任务要求在单测日志中打印幂等标识、重试次数与判定依据。"""
    logger.info(
        "判定结果 idempotency_key=%s status=%s attempts=%d 终态原因=%s",
        event["idempotency_key"], event["status"], event["attempts"],
        event.get("last_failure_kind"),
    )
    for a in event.get("attempts_log", []):
        logger.info(
            "  尝试#%d outcome=%s 判定依据(failure_kind)=%s http=%s 详情=%s",
            a["attempt_no"], a["outcome"], a["failure_kind"],
            a["http_status"], a["detail"],
        )
