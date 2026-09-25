"""进程重启场景：未完成事件不丢失、不被误判为成功。"""
from __future__ import annotations

import logging
import time

import httpx

from app.main import create_app
from app.models import EventRecord, EventStatus
from app.store import EventStore

from .conftest import _free_port, make_service_params, wait_for

logger = logging.getLogger("tests.restart")


async def test_store_recovers_in_flight_after_restart(tmp_path):
    """存储层：in_flight 事件在重启后恢复为 pending，且不被误判为成功。"""
    db = str(tmp_path / "events.db")
    s1 = EventStore(db)
    s1.insert(EventRecord(event_id="r-1", event_type="t", receiver="r1",
                          payload={}))
    claimed = s1.claim_due(time.time(), 10)
    assert claimed[0].status == EventStatus.IN_FLIGHT
    s1.close()  # 模拟进程退出：事件停留在 in_flight

    s2 = EventStore(db)  # 重启后重新打开同一文件
    rec = s2.get("r-1")
    assert rec.status == EventStatus.IN_FLIGHT, "重启前不得误判为成功"
    assert s2.recover_in_flight() == 1
    assert s2.get("r-1").status == EventStatus.PENDING, "恢复后应可重新投递"
    assert s2.claim_due(time.time(), 10)[0].event_id == "r-1"
    logger.info("event_id=r-1 恢复路径=in_flight->pending, basis=重启恢复扫描")
    s2.close()


async def test_restart_redelivers_unfinished_event(tmp_path, receiver):
    """端到端：首次运行投递失败（连接拒绝），重启后同一事件继续投递直至成功。"""
    dead_url = f"http://127.0.0.1:{_free_port()}/recv"  # 未监听端口
    params1 = make_service_params(tmp_path, dead_url, max_attempts=5)
    app1 = create_app(**params1)
    async with app1.router.lifespan_context(app1):
        transport = httpx.ASGITransport(app=app1)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://svc") as client:
            r = await client.post("/events", json={
                "event_id": "restart-1", "event_type": "t",
                "receiver": "r1", "payload": {}})
            assert r.status_code == 201

            async def tried():
                st = (await client.get("/events/restart-1")).json()
                return st if st["attempts"] >= 1 else None

            st = await wait_for(tried)
            logger.info("event_id=restart-1 重启前 attempts=%d kind=%s",
                        st["attempts"], st["last_failure_kind"])
            assert st["last_failure_kind"] == "connection"
            assert st["status"] in ("pending", "in_flight"), "未完成不得为终态"

    # 进程“重启”：同一 db 文件，新实例指向可用接收端
    params2 = make_service_params(tmp_path, f"{receiver.url}/recv",
                                  max_attempts=5)
    params2["db_path"] = params1["db_path"]
    app2 = create_app(**params2)
    async with app2.router.lifespan_context(app2):
        transport = httpx.ASGITransport(app=app2)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://svc") as client:
            async def done():
                st = (await client.get("/events/restart-1")).json()
                return st if st["status"] == "delivered" else None

            st = await wait_for(done)
            deliveries = await receiver.deliveries()
            logger.info("event_id=restart-1 重启后 status=%s attempts=%d "
                        "deliveries=%d basis=持久化状态恢复",
                        st["status"], st["attempts"], len(deliveries))
            assert st["status"] == "delivered", "重启后未完成事件不得丢失"
            assert st["attempts"] >= 2, "重启前的失败尝试应计入历史"
            assert len(deliveries) == 1, "重启后只需补投一次"


async def test_restart_recovers_in_flight_e2e(tmp_path, receiver):
    """端到端：崩溃时处于 in_flight 的事件，重启后恢复并成功投递。"""
    params = make_service_params(tmp_path, f"{receiver.url}/recv")
    # 预置一个“崩溃现场”：事件已认领（in_flight）但进程退出
    store = EventStore(params["db_path"])
    store.insert(EventRecord(event_id="crash-1", event_type="t",
                             receiver="r1", payload={}))
    store.claim_due(time.time(), 10)
    store.close()

    app = create_app(**params)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://svc") as client:
            async def done():
                st = (await client.get("/events/crash-1")).json()
                return st if st["status"] == "delivered" else None

            st = await wait_for(done)
            logger.info("event_id=crash-1 恢复后 status=%s basis=in_flight 恢复",
                        st["status"])
            assert st["status"] == "delivered"
    deliveries = await receiver.deliveries()
    assert [d["event_id"] for d in deliveries] == ["crash-1"]
