"""投递目标注册、按名路由、多目标顺序/并发与停用恢复的验证。"""
from __future__ import annotations

import asyncio
import logging

import httpx

from app.main import create_app

from .conftest import _free_port, make_service_params, wait_for

logger = logging.getLogger("tests.targets")


async def _register(svc, name: str, url: str) -> httpx.Response:
    return await svc.client.post("/targets", json={"name": name, "url": url})


async def _patch(svc, name: str, **fields) -> httpx.Response:
    return await svc.client.patch(f"/targets/{name}", json=fields)


async def _delivered(svc, event_id: str):
    st = await svc.status(event_id)
    return st if st["status"] == "delivered" else None


async def test_register_and_route_by_receiver_name(service_factory,
                                                   receiver_factory):
    """注册两个具名目标后，事件按 receiver 名字路由到各自地址。"""
    ra = await receiver_factory()
    rb = await receiver_factory()
    svc = await service_factory(deliver_url=None)  # 注册表模式

    r = await _register(svc, "ta", f"{ra.url}/recv")
    assert r.status_code == 201, r.text
    r = await _register(svc, "tb", f"{rb.url}/recv")
    assert r.status_code == 201, r.text

    await svc.submit(event_id="route-1", event_type="t", receiver="ta",
                     payload={})
    await svc.submit(event_id="route-2", event_type="t", receiver="tb",
                     payload={})

    st1 = await wait_for(lambda: _delivered(svc, "route-1"))
    st2 = await wait_for(lambda: _delivered(svc, "route-2"))
    da = await ra.deliveries()
    db = await rb.deliveries()
    logger.info("event_id=route-1 target=ta url=%s attempts=%d; "
                "event_id=route-2 target=tb attempts=%d; "
                "ra收到=%s rb收到=%s basis=按 receiver 名路由",
                st1["target_url"], st1["attempts"], st2["attempts"],
                [d["event_id"] for d in da], [d["event_id"] for d in db])
    assert [d["event_id"] for d in da] == ["route-1"]
    assert [d["event_id"] for d in db] == ["route-2"]
    assert st1["target_url"] == f"{ra.url}/recv"
    assert st2["target_url"] == f"{rb.url}/recv"


async def test_target_registration_rejections(service_factory, receiver):
    """不合法/冲突/重复登记的拒绝原因可区分；同名同地址幂等。"""
    svc = await service_factory(deliver_url=None)

    r = await _register(svc, "bad", "ftp://no-scheme-check")
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "invalid_target"
    logger.info("name=bad 拒绝=%s", r.json()["detail"]["reason"])

    r = await svc.client.post("/targets", json={"name": "", "url": "http://x"})
    assert r.status_code == 422, "空名字必须拒绝"

    url = f"{receiver.url}/recv"
    r = await _register(svc, "t1", url)
    assert r.status_code == 201
    r = await _register(svc, "t1", url)
    assert r.status_code == 200, "同名同地址应幂等受理"
    logger.info("name=t1 重复登记(同地址) -> %d", r.status_code)

    r = await _register(svc, "t1", "http://127.0.0.1:1/other")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "target_conflict"
    logger.info("name=t1 同名异址 -> %d reason=%s", r.status_code,
                r.json()["detail"]["reason"])

    r = await svc.client.get("/targets/ghost")
    assert r.status_code == 404
    assert r.json()["detail"]["reason"] == "target_not_found"
    r = await _patch(svc, "ghost", enabled=False)
    assert r.status_code == 404
    r = await _patch(svc, "t1", url="not-a-url")
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "invalid_target"

    # 更新地址是显式操作：允许，且之后按新地址投递
    r = await _patch(svc, "t1", url=f"{receiver.url}/recv")
    assert r.status_code == 200 and r.json()["url"].endswith("/recv")


async def test_unknown_receiver_rejected(service_factory):
    """注册表模式下，未登记的接收端名字在提交期被拒绝且原因可区分。"""
    svc = await service_factory(deliver_url=None)
    r = await svc.submit(event_id="ghost-1", event_type="t",
                         receiver="ghost", payload={})
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "unknown_receiver"
    logger.info("event_id=ghost-1 拒绝=%s basis=接收端名未登记",
                r.json()["detail"]["reason"])
    events = (await svc.client.get("/events")).json()
    assert events == [], "被拒绝的事件不得入队"


async def test_per_target_order_cross_target_concurrency(service_factory,
                                                         receiver_factory):
    """同一目标内按入队顺序串行；慢目标不拖住其他目标。"""
    slow = await receiver_factory()
    fast = await receiver_factory()
    await slow.program(mode="slow", delay=0.3)
    svc = await service_factory(deliver_url=None)
    await _register(svc, "ts", f"{slow.url}/recv")
    await _register(svc, "tf", f"{fast.url}/recv")

    for i in range(4):
        await svc.submit(event_id=f"s-{i}", event_type="t", receiver="ts",
                         payload={"i": i})
    for i in range(3):
        await svc.submit(event_id=f"f-{i}", event_type="t", receiver="tf",
                         payload={"i": i})

    async def fast_done():
        ds = await fast.deliveries()
        return ds if len(ds) == 3 else None

    df = await wait_for(fast_done, timeout=2)
    ds_mid = await slow.deliveries()
    logger.info("快目标已收=%s 时慢目标仅收=%s basis=目标间并发隔离",
                [d["event_id"] for d in df],
                [d["event_id"] for d in ds_mid])
    assert len(ds_mid) < 4, "慢目标不得阻塞快目标，且自身串行投递"

    async def slow_done():
        ds = await slow.deliveries()
        return ds if len(ds) == 4 else None

    ds = await wait_for(slow_done, timeout=10)
    order = [d["event_id"] for d in ds]
    logger.info("慢目标投递顺序=%s basis=同一目标按入队顺序", order)
    assert order == [f"s-{i}" for i in range(4)]


async def test_disabled_target_defers_then_resumes(service_factory, receiver):
    """目标停用期间受理的事件不投不丢，重新启用后按序补投。"""
    svc = await service_factory(deliver_url=None)
    await _register(svc, "t", f"{receiver.url}/recv")
    r = await _patch(svc, "t", enabled=False)
    assert r.status_code == 200 and r.json()["enabled"] is False

    for i in range(3):
        resp = await svc.submit(event_id=f"d-{i}", event_type="t",
                                receiver="t", payload={"i": i})
        assert resp.status_code == 201, "目标停用不应拒绝受理"

    await asyncio.sleep(0.4)  # 远超一次调度周期
    ds = await receiver.deliveries()
    st = await svc.status("d-0")
    logger.info("停用期间 deliveries=%d event_id=d-0 status=%s attempts=%d "
                "basis=停用目标不被认领", len(ds), st["status"], st["attempts"])
    assert ds == [], "停用目标不得投递"
    assert st["status"] == "pending" and st["attempts"] == 0

    await _patch(svc, "t", enabled=True)

    async def all_done():
        ds = await receiver.deliveries()
        return ds if len(ds) == 3 else None

    ds = await wait_for(all_done)
    order = [d["event_id"] for d in ds]
    logger.info("恢复后补投顺序=%s basis=重新启用后继续投递", order)
    assert order == [f"d-{i}" for i in range(3)], "补投不得丢失或乱序"


async def test_registry_and_events_survive_restart(tmp_path,
                                                   receiver_factory):
    """重启后目标登记仍在；指错地址的未完成事件在修正地址后续投成功。"""
    live = await receiver_factory()
    dead_url = f"http://127.0.0.1:{_free_port()}/recv"
    params1 = make_service_params(tmp_path, None, max_attempts=5)
    app1 = create_app(**params1)
    async with app1.router.lifespan_context(app1):
        transport = httpx.ASGITransport(app=app1)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://svc") as client:
            r = await client.post("/targets",
                                  json={"name": "t", "url": dead_url})
            assert r.status_code == 201
            r = await client.post("/events", json={
                "event_id": "re-1", "event_type": "t", "receiver": "t",
                "payload": {}})
            assert r.status_code == 201

            async def tried():
                st = (await client.get("/events/re-1")).json()
                return st if st["attempts"] >= 1 else None

            st = await wait_for(tried)
            logger.info("event_id=re-1 重启前 attempts=%d kind=%s",
                        st["attempts"], st["last_failure_kind"])
            assert st["last_failure_kind"] == "connection"

    # 重启：同一 db，新实例；目标登记应仍在，事件不得被误判成功
    params2 = make_service_params(tmp_path, None, max_attempts=5)
    params2["db_path"] = params1["db_path"]
    app2 = create_app(**params2)
    async with app2.router.lifespan_context(app2):
        transport = httpx.ASGITransport(app=app2)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://svc") as client:
            targets = (await client.get("/targets")).json()
            logger.info("重启后 targets=%s", targets)
            assert [t["name"] for t in targets] == ["t"], "目标登记必须持久化"
            st = (await client.get("/events/re-1")).json()
            assert st["status"] in ("pending", "in_flight"), "未完成不得为终态"

            # 修正目标地址后，未完成事件续投成功
            r = await client.patch(f"/targets/t",
                                   json={"url": f"{live.url}/recv"})
            assert r.status_code == 200

            async def done():
                st = (await client.get("/events/re-1")).json()
                return st if st["status"] == "delivered" else None

            st = await wait_for(done)
            ds = await live.deliveries()
            logger.info("event_id=re-1 重启后 status=%s attempts=%d "
                        "deliveries=%d basis=目标持久化+未完成续投",
                        st["status"], st["attempts"], len(ds))
            assert st["attempts"] >= 2, "重启前的失败尝试应计入历史"
            assert len(ds) == 1


async def test_capacity_limit_rejects_when_backlog_full(service_factory,
                                                        receiver):
    """待投递事件超过上限时拒绝受理（429），恢复后已受理事件照常投递。"""
    svc = await service_factory(deliver_url=None, max_pending=2)
    await _register(svc, "t", f"{receiver.url}/recv")
    await _patch(svc, "t", enabled=False)  # 让事件积压

    for i in range(2):
        r = await svc.submit(event_id=f"cap-{i}", event_type="t",
                             receiver="t", payload={})
        assert r.status_code == 201
    r = await svc.submit(event_id="cap-2", event_type="t", receiver="t",
                         payload={})
    assert r.status_code == 429
    assert r.json()["detail"]["reason"] == "capacity_exceeded"
    logger.info("event_id=cap-2 拒绝=%s basis=pending+in_flight 达到上限 2",
                r.json()["detail"]["reason"])

    # 幂等重报不受容量限制：同一事件重复提交仍返回 duplicated
    r = await svc.submit(event_id="cap-0", event_type="t", receiver="t",
                         payload={})
    assert r.status_code == 200 and r.json()["duplicated"] is True

    await _patch(svc, "t", enabled=True)

    async def drained():
        ds = await receiver.deliveries()
        return ds if len(ds) == 2 else None

    ds = await wait_for(drained)
    logger.info("恢复后补投=%s", [d["event_id"] for d in ds])
    assert [d["event_id"] for d in ds] == ["cap-0", "cap-1"]


async def test_concurrent_duplicate_submit_stable(service_factory, receiver):
    """注册表模式 + 高并发：同一幂等标识相同内容并发提交，受理结果稳定。"""
    svc = await service_factory(deliver_url=None)
    await _register(svc, "t", f"{receiver.url}/recv")
    event = {"event_id": "conc-1", "event_type": "t", "receiver": "t",
             "payload": {"v": 1}}

    responses = await asyncio.gather(*(
        svc.client.post("/events", json=event) for _ in range(20)))
    codes = sorted(r.status_code for r in responses)
    n_created = sum(1 for r in responses if r.status_code == 201)
    n_dup = sum(1 for r in responses
                if r.status_code == 200 and r.json()["duplicated"])
    logger.info("event_id=conc-1 并发=20 201=%d 200dup=%d codes=%s "
                "basis=唯一约束兜底+内容比对", n_created, n_dup, codes)
    assert n_created == 1, "恰好一次首次受理"
    assert n_created + n_dup == 20, "相同内容重复提交不得报冲突"

    st = await wait_for(lambda: _delivered(svc, "conc-1"))
    ds = await receiver.deliveries()
    logger.info("event_id=conc-1 attempts=%d deliveries=%d",
                st["attempts"], len(ds))
    assert len(ds) == 1, "并发重复提交不得产生重复投递"
