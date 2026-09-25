"""本地模拟接收端（骨架）：可编程行为，用于验证投递语义。"""
from __future__ import annotations

from fastapi import FastAPI


def create_receiver_app() -> FastAPI:
    """创建模拟接收端应用。

    路由（骨架，空实现）：
    - POST /recv        接收投递，按编程行为返回
    - POST /program     设置行为（always_ok / always_500 / always_400 /
                        fail_times_then_ok / slow / reject）
    - GET  /deliveries  查询已收到的投递（含幂等标识与顺序）
    - POST /reset       清空状态
    """
    app = FastAPI(title="mock-receiver")

    @app.post("/recv")
    async def recv() -> dict:  # TODO: 按编程行为响应
        raise NotImplementedError

    @app.post("/program")
    async def program() -> dict:  # TODO: 设置行为
        raise NotImplementedError

    @app.get("/deliveries")
    async def deliveries() -> dict:  # TODO: 返回收到记录
        raise NotImplementedError

    @app.post("/reset")
    async def reset() -> dict:  # TODO: 清空
        raise NotImplementedError

    return app
