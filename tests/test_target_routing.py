"""投递目标注册、按名路由、拒绝原因区分与停用恢复补投的端到端验证。"""
from __future__ import annotations

import logging

from .conftest import wait_for

logger = logging.getLogger("tests.targets")


async def test_target_register_list_and_duplicate(routed_service):
    """业务：运行时登记具名目标，可查看；同名同址重复登记幂等。"""
    svc = routed_service
    # 预置目标随服务启动即可见
    targets = {t["name"]: t for t in await svc.targets()}
    assert set(targets) == {"ta", "tb"}
    assert all(t["enabled"] for t in targets.values())

    # 运行中再登记一个新目标 -> 201
    extra_url = svc.receivers["ta"].url + "/recv"
    r = await svc.register("tc", extra_url)
    assert r.status_code == 201 and r.json()["duplicated"] is False
    logger.info("target=tc action=register url=%s basis=首次登记返回201", extra_url)

    # 同名同址重复登记 -> 200 duplicated=true，不视为冲突
    r2 = await svc.register("tc", extra_url)
    assert r2.status_code == 200 and r2.json()["duplicated"] is True
    logger.info("target=tc action=re-register same url -> 200 duplicated=true")
    assert len(await svc.targets()) == 3


async def test_invalid_target_registration_rejected(service_factory):
    """业务：登记信息不合法（空名/空 URL/缺字段）必须 422，原因可区分。"""
    svc = await service_factory(targets={})
    # 空名字落在路径里会 307/404，这里用空白名字（FastAPI 不裁剪）验 422
    r = await svc.client.put("/targets/%20", json={"url": "http://x/recv"})
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "invalid_target"
    # URL 为空 / 缺字段：请求体校验 422
    r = await svc.register("bad", "")
    assert r.status_code == 422
    r = await svc.client.put("/targets/bad", json={})
    assert r.status_code == 422
    logger.info("target=bad action=register invalid body -> 422 依据=校验层拒绝")


async def test_same_name_other_address_conflicts(routed_service):
    """业务：同一个名字指向另一个地址必须明确拒绝（409，与其他原因可区分）。"""
    svc = routed_service
    old_url = svc.receivers["ta"].url + "/recv"
    r = await svc.register("ta", "http://127.0.0.1:9/recv")  # 改指别的地址
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "target_address_conflict"
    assert detail["existing_url"] == old_url
    assert detail["new_url"] == "http://127.0.0.1:9/recv"
    logger.info("target=ta 拒绝原因=%s existing=%s new=%s basis=名字地址一一对应",
                detail["reason"], detail["existing_url"], detail["new_url"])
    # 原登记不受影响，仍可投递
    await svc.submit(event_id="conf-1", event_type="t", receiver="ta",
                     payload={})
    st = await wait_for(lambda: _is(svc, "conf-1", "delivered"))
    assert st["attempts"] == 1


async def test_event_to_unregistered_target_rejected(routed_service):
    """业务：事件声明未登记的接收端名字必须拒绝，且与参数非法/幂等冲突可区分。"""
    svc = routed_service
    r = await svc.submit(event_id="unk-1", event_type="t", receiver="ghost",
                         payload={})
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["reason"] == "target_not_registered"
    assert detail["receiver"] == "ghost"
    logger.info("event_id=unk-1 receiver=ghost 拒绝原因=%s basis=名字未登记",
                detail["reason"])
    # 参数非法（空 event_id）同样是 422，但 reason 来自校验层结构，可与此区分
    r_bad = await svc.submit(event_id="", event_type="t", receiver="ta",
                             payload={})
    assert r_bad.status_code == 422
    assert isinstance(r_bad.json()["detail"], list)  # 校验层 422 为数组明细


async def test_routes_to_named_targets(routed_service):
    """业务：按事件声明的名字投递到对应地址，状态里能看出归属目标与进度。"""
    svc = routed_service
    await svc.submit(event_id="route-a", event_type="t", receiver="ta",
                     payload={"k": "a"})
    await svc.submit(event_id="route-b", event_type="t", receiver="tb",
                     payload={"k": "b"})
    sta = await wait_for(lambda: _is(svc, "route-a", "delivered"))
    stb = await wait_for(lambda: _is(svc, "route-b", "delivered"))
    assert sta["target"] == "ta" and stb["target"] == "tb"

    da = await svc.receivers["ta"].deliveries()
    db = await svc.receivers["tb"].deliveries()
    logger.info("event_id=route-a target=%s -> ta收到%d; event_id=route-b "
                "target=%s -> tb收到%d", "ta", len(da), "tb", len(db))
    assert [d["event_id"] for d in da] == ["route-a"]
    assert [d["event_id"] for d in db] == ["route-b"]
    # 投递报文带 target 字段，接收端可识别
    assert da[0]["receiver"] == "ta"


async def test_target_disable_then_enable_resumes(routed_service):
    """业务：目标停用期间受理的事件不丢，重新启用后继续按序补投。"""
    svc = routed_service
    ra = svc.receivers["ta"]
    # 停用 ta：已受理/停用期间受理的事件都应排队等待，不计失败次数
    assert (await svc.set_enabled("ta", False)).status_code == 200
    for i in range(3):
        r = await svc.submit(event_id=f"park-{i}", event_type="t",
                             receiver="ta", payload={"i": i})
        assert r.status_code == 201
    # 停用期间查询目标状态可见 enabled=false
    ta = {t["name"]: t for t in await svc.targets()}["ta"]
    assert ta["enabled"] is False

    # 另一目标 tb 不受 ta 停用影响，照常投递（不被拖住）
    await svc.submit(event_id="park-b", event_type="t", receiver="tb",
                     payload={})
    stb = await wait_for(lambda: _is(svc, "park-b", "delivered"), timeout=3)
    assert stb["attempts"] == 1
    logger.info("ta停用期间 event_id=park-b target=tb 仍成功 basis=目标间相互隔离")

    # 给 ta 一段轮询窗口确认事件确实积压未投
    import asyncio
    await asyncio.sleep(0.3)
    assert await ra.deliveries() == []
    parked = await svc.status("park-0")
    assert parked["status"] == "pending" and parked["attempts"] == 0, \
        "停用目标不应产生投递尝试"

    # 重新启用 -> 积压事件按入队顺序补投
    assert (await svc.set_enabled("ta", True)).status_code == 200
    for i in range(3):
        await wait_for(lambda i=i: _is(svc, f"park-{i}", "delivered"), timeout=5)
    da = await ra.deliveries()
    order = [d["event_id"] for d in da]
    logger.info("ta恢复后补投顺序=%s basis=按入队顺序且不丢不重", order)
    assert order == ["park-0", "park-1", "park-2"]


async def test_target_outage_recovers_without_losing_events(
        service_factory):
    """业务：目标地址不可达期间事件持续重试，接收端在同址恢复后补投成功。"""
    from .conftest import _free_port, start_receiver, stop_receiver

    port = _free_port()  # 此刻该端口无监听 -> 模拟目标宕机
    url = f"http://127.0.0.1:{port}/recv"
    svc = await service_factory(targets={"ta": url}, max_attempts=100)
    for i in range(3):
        r = await svc.submit(event_id=f"down-{i}", event_type="t",
                             receiver="ta", payload={"i": i})
        assert r.status_code == 201

    # 宕机期间：发生连接失败、事件保持未终态，绝不能被判成成功
    import asyncio
    for _ in range(50):
        if (await svc.status("down-0"))["attempts"] >= 1:
            break
        await asyncio.sleep(0.05)
    st0 = await svc.status("down-0")
    assert st0["last_failure_kind"] == "connection"
    assert st0["status"] in ("pending", "in_flight"), "不可达期间不得误判成功"
    logger.info("event_id=down-0 宕机期间 attempts=%d kind=%s status=%s "
                "basis=连接失败可重试且不终结",
                st0["attempts"], st0["last_failure_kind"], st0["status"])

    # 接收端在“同一地址”上线（模拟目标恢复，名字->地址绑定不变）
    import uvicorn
    from app.receiver import create_receiver_app
    config = uvicorn.Config(create_receiver_app(), host="127.0.0.1",
                            port=port, log_level="error")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    try:
        for i in range(3):
            st = await wait_for(lambda i=i: _is(svc, f"down-{i}", "delivered"),
                                timeout=8)
            assert st["attempts"] >= 1 if i == 0 else st["attempts"] >= 1
        logger.info("目标同址恢复后 down-0/1/2 全部 delivered basis=续投不丢")
    finally:
        server.should_exit = True
        await task


async def _is(svc, event_id, status):
    st = await svc.status(event_id)
    return st if st["status"] == status else None
