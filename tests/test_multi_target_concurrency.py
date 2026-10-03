"""多目标下的顺序/并发、容量背压、并发幂等与重启恢复正确性验证。"""
from __future__ import annotations

import asyncio
import logging

import httpx

from app.main import create_app

from .conftest import _free_port, wait_for

logger = logging.getLogger("tests.multi")


async def test_same_target_order_cross_target_concurrency(routed_service):
    """业务：同一目标严格按入队顺序；不同目标并发，互不拖慢。"""
    svc = routed_service
    ra, rb = svc.receivers["ta"], svc.receivers["tb"]
    # tb 变慢；ta 仍应快速按序完成，证明跨目标并发、不互相阻塞
    await rb.program(mode="slow", delay=0.5)

    n = 6
    for i in range(n):
        await svc.submit(event_id=f"ord-a-{i}", event_type="t", receiver="ta",
                         payload={"i": i})
        await svc.submit(event_id=f"ord-b-{i}", event_type="t", receiver="tb",
                         payload={"i": i})

    await wait_for(lambda: _all(svc, "ord-a", n, "delivered"), timeout=8)
    da = await ra.deliveries()
    seq_a = [d["event_id"] for d in da]
    logger.info("target=ta 顺序=%s basis=目标内按入队序串行", seq_a)
    assert seq_a == [f"ord-a-{i}" for i in range(n)]

    # rb 慢，但最终也全部送达（慢目标不丢事件）
    await wait_for(lambda: _all(svc, "ord-b", n, "delivered"), timeout=15)
    db = await rb.deliveries()
    seq_b = [d["event_id"] for d in db]
    logger.info("target=rb 慢目标顺序=%s basis=慢目标不拖累ta且自身不丢", seq_b)
    assert seq_b == [f"ord-b-{i}" for i in range(n)]


async def test_capacity_backpressure(service_factory, receiver):
    """业务：待投递事件数有明确上限，超限被拒（503），成功投递后腾出名额。"""
    # 指向一个未监听端口：事件持续重试不终结，快速占满容量
    dead = f"http://127.0.0.1:{_free_port()}/recv"
    svc = await service_factory(targets={"dead": dead}, max_outstanding=3)
    for i in range(3):
        r = await svc.submit(event_id=f"cap-{i}", event_type="t",
                             receiver="dead", payload={"i": i})
        assert r.status_code == 201, r.text
    r = await svc.submit(event_id="cap-over", event_type="t", receiver="dead",
                         payload={})
    assert r.status_code == 503
    detail = r.json()["detail"]
    assert detail["reason"] == "capacity_exceeded" and detail["limit"] == 3
    logger.info("event_id=cap-over 拒绝原因=%s limit=%d basis=待投递上限",
                detail["reason"], detail["limit"])


async def test_concurrent_same_idempotency_key_stable(routed_service):
    """业务：高并发下同一幂等标识+相同内容重复提交，受理结果稳定不重复投递。"""
    svc = routed_service
    ra = svc.receivers["ta"]
    body = {"event_id": "idem-hot", "event_type": "t", "receiver": "ta",
            "payload": {"v": 1}}

    async def submit():
        return await svc.client.post("/events", json=body)

    responses = await asyncio.gather(*(submit() for _ in range(20)))
    codes = sorted(r.status_code for r in responses)
    dup = sum(1 for r in responses if r.json().get("duplicated") is True)
    first = sum(1 for r in responses if r.status_code == 201)
    logger.info("event_id=idem-hot 并发=20 首次201=%d 重复200=%d 拒绝=%d "
                "basis=唯一约束+内容比对", first, dup,
                len(responses) - first - dup)
    assert codes[0] == 200 and codes[-1] == 201
    assert first == 1 and dup == 19, "恰有一次首次受理，其余全部幂等重复"

    st = await wait_for(lambda: _is(svc, "idem-hot", "delivered"))
    deliveries = await ra.deliveries()
    assert st["attempts"] == 1 and len(deliveries) == 1, "并发重复提交不得重复投递"


async def test_restart_preserves_targets_and_unfinished(tmp_path,
                                                        receiver_factory):
    """业务：重启后目标登记仍在；未完成事件接着投，绝不当成已成功。"""
    ra = await receiver_factory()
    db = str(tmp_path / "routed.db")
    url_a = f"{ra.url}/recv"
    # 首次运行前预置“崩溃现场”：目标已登记 + 一条 in_flight 未完成事件
    import time

    from app.models import EventRecord, TargetRecord
    from app.store import EventStore
    pre = EventStore(db)
    pre.upsert_target(TargetRecord(name="ta", url=url_a))
    pre.insert(EventRecord(event_id="resume-1", event_type="t", receiver="ta",
                           payload={}))
    pre.claim_due(time.time(), 10)  # 认领后即“崩溃”
    pre.close()

    app = create_app(db_path=db, targets={"ta": url_a})
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://svc") as client:
            # 目标登记扛得住重启
            targets = (await client.get("/targets")).json()
            assert {t["name"]: t["url"] for t in targets} == {"ta": url_a}
            logger.info("重启后目标登记=%s basis=targets 表持久化",
                        [t["name"] for t in targets])

            st = await wait_for(
                lambda: _status(client, "resume-1", "delivered"))
            deliveries = await ra.deliveries()
            logger.info("event_id=resume-1 重启后 status=%s attempts=%d "
                        "deliveries=%d basis=in_flight 恢复续投",
                        st["status"], st["attempts"], len(deliveries))
            assert st["attempts"] == 1
            assert [d["event_id"] for d in deliveries] == ["resume-1"]


async def _all(svc, prefix, n, status):
    events = (await svc.client.get("/events")).json()
    mine = [e for e in events if e["event_id"].startswith(f"{prefix}-")]
    return len(mine) == n and all(e["status"] == status for e in mine)


async def _is(svc, event_id, status):
    st = await svc.status(event_id)
    return st if st["status"] == status else None


async def _status(client, event_id, status):
    st = (await client.get(f"/events/{event_id}")).json()
    return st if st["status"] == status else None
