"""本地模拟接收端：可编程行为，用于验证投递语义，不依赖外部服务。"""
from __future__ import annotations

import asyncio
import itertools
import time
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# 支持的行为模式
MODES = {
    "always_ok",           # 总是 200
    "always_500",          # 总是 500（可重试）
    "always_400",          # 总是 400（不可重试）
    "fail_times_then_ok",  # 前 fail_times 次 500，之后 200
    "slow",                # 延迟 delay 秒后 200（用于触发超时）
    "reject",              # 422 业务拒绝（不可重试）
}


class ProgramIn(BaseModel):
    mode: str
    fail_times: int = 0
    delay: float = 0.0


class ReceiverState:
    """接收端运行状态：当前行为 + 已收到的投递记录。"""

    def __init__(self) -> None:
        self.mode = "always_ok"
        self.fail_times = 0
        self.delay = 0.0
        self.deliveries: list[dict[str, Any]] = []
        self._seq = itertools.count(1)

    def reset(self) -> None:
        self.__init__()


def create_receiver_app() -> FastAPI:
    app = FastAPI(title="mock-receiver")
    app.state.receiver = ReceiverState()

    @app.post("/recv")
    async def recv(request: Request) -> JSONResponse:
        st: ReceiverState = request.app.state.receiver
        body = await request.json()
        seq = next(st._seq)
        st.deliveries.append({
            "seq": seq,
            "event_id": body.get("event_id"),
            "event_type": body.get("event_type"),
            "receiver": body.get("receiver"),
            "attempt": body.get("attempt"),
            "payload": body.get("payload"),
            "at": time.time(),
        })
        n = len(st.deliveries)
        if st.mode == "always_500":
            return JSONResponse({"error": "boom"}, status_code=500)
        if st.mode == "always_400":
            return JSONResponse({"error": "bad event"}, status_code=400)
        if st.mode == "fail_times_then_ok" and n <= st.fail_times:
            return JSONResponse({"error": f"boom {n}/{st.fail_times}"},
                                status_code=500)
        if st.mode == "slow":
            await asyncio.sleep(st.delay)
        if st.mode == "reject":
            return JSONResponse({"rejected": True, "reason": "not subscribed"},
                                status_code=422)
        return JSONResponse({"ok": True, "seq": seq})

    @app.post("/program")
    async def program(cfg: ProgramIn, request: Request) -> dict:
        if cfg.mode not in MODES:
            return JSONResponse({"error": f"unknown mode {cfg.mode!r}"},
                                status_code=400)
        st: ReceiverState = request.app.state.receiver
        st.mode = cfg.mode
        st.fail_times = cfg.fail_times
        st.delay = cfg.delay
        return {"mode": st.mode, "fail_times": st.fail_times, "delay": st.delay}

    @app.get("/deliveries")
    async def deliveries(request: Request) -> dict:
        st: ReceiverState = request.app.state.receiver
        return {"count": len(st.deliveries), "deliveries": list(st.deliveries)}

    @app.post("/reset")
    async def reset(request: Request) -> dict:
        request.app.state.receiver.reset()
        return {"ok": True}

    return app
