"""并发投递、顺序约束与单次投递超时的验证。"""
from __future__ import annotations

import asyncio
import logging
import time

from .conftest import wait_for

logger = logging.getLogger("tests.concurrency")


async def test_concurrent_receivers_per_receiver_order(service_factory,
                                                       receiver_factory):
    """不同接收端并发投递；同一接收端严格按提交顺序到达。"""
    ra = await receiver_factory()
    rb = await receiver_factory()
    # 两个服务实例分别指向两个接收端，共享各自 db
    svc_a = await service_factory(deliver_url=f"{ra.url}/recv")
    svc_b = await service_factory(deliver_url=f"{rb.url}/recv")

    n = 8
    for i in range(n):
        await svc_a.submit(event_id=f"a-{i}", event_type="t", receiver="ra",
                           payload={"i": i})
        await svc_b.submit(event_id=f"b-{i}", event_type="t", receiver="rb",
                           payload={"i": i})

    await wait_for(lambda: _all_delivered(svc_a, "a", n))
    await wait_for(lambda: _all_delivered(svc_b, "b", n))

    da = await ra.deliveries()
    db = await rb.deliveries()
    seq_a = [d["event_id"] for d in da]
    seq_b = [d["event_id"] for d in db]
    logger.info("receiver=ra order=%s; receiver=rb order=%s", seq_a, seq_b)
    assert seq_a == [f"a-{i}" for i in range(n)], "同一接收端必须按序投递"
    assert seq_b == [f"b-{i}" for i in range(n)]
    assert len(da) == n and len(db) == n, "并发下不得重复或丢失投递"


async def test_high_concurrency_no_duplication(service, receiver):
    """并发提交 + 并发投递：无重复、无丢失、状态不错乱。"""
    n = 30

    async def submit(i):
        return await service.submit(event_id=f"c-{i}", event_type="t",
                                    receiver="r1", payload={"i": i})

    responses = await asyncio.gather(*(submit(i) for i in range(n)))
    assert all(r.status_code == 201 for r in responses)

    await wait_for(lambda: _all_delivered(service, "c", n), timeout=15)
    deliveries = await receiver.deliveries()
    ids = [d["event_id"] for d in deliveries]
    logger.info("submitted=%d delivered=%d unique=%d", n, len(ids),
                len(set(ids)))
    assert len(ids) == n and len(set(ids)) == n, "每个事件恰好投递一次"
    # 并发提交的入队顺序本身不确定，顺序约束由顺序提交的用例验证
    events = (await service.client.get("/events")).json()
    assert all(e["status"] == "delivered" for e in events)


async def test_timeout_is_failure_and_does_not_block(service_factory,
                                                     receiver_factory):
    """单次投递超时判定为失败（可重试），且慢接收端不阻塞其他事件。"""
    slow = await receiver_factory()
    fast = await receiver_factory()
    await slow.program(mode="slow", delay=0.6)
    svc = await service_factory(deliver_url=f"{slow.url}/recv", timeout=0.15,
                                max_attempts=2)
    svc_fast = await service_factory(deliver_url=f"{fast.url}/recv")

    t0 = time.monotonic()
    await svc.submit(event_id="slow-1", event_type="t", receiver="slow",
                     payload={})
    await svc_fast.submit(event_id="fast-1", event_type="t", receiver="fast",
                          payload={})

    # 快接收端应立即送达，不被慢接收端的超时投递阻塞
    st_fast = await wait_for(lambda: _status_is(svc_fast, "fast-1",
                                                "delivered"), timeout=2)
    fast_elapsed = time.monotonic() - t0
    logger.info("event_id=fast-1 delivered in %.2fs (慢接收端延迟 0.6s)",
                fast_elapsed)
    assert fast_elapsed < 0.6, "慢接收端不得阻塞其他事件的投递"

    # 慢接收端：每次尝试都超时 -> 可重试 -> 达到 max_attempts 后 failed
    st_slow = await wait_for(lambda: _status_is(svc, "slow-1", "failed"),
                             timeout=5)
    logger.info("event_id=slow-1 attempts=%d kind=%s basis=单次投递超时>0.15s",
                st_slow["attempts"], st_slow["last_failure_kind"])
    assert st_slow["attempts"] == 2
    assert st_slow["last_failure_kind"] == "timeout"
    assert st_fast["status"] == "delivered"


async def _all_delivered(service, prefix, n):
    events = (await service.client.get("/events")).json()
    mine = [e for e in events if e["event_id"].startswith(f"{prefix}-")]
    return len(mine) == n and all(e["status"] == "delivered" for e in mine)


async def _status_is(service, event_id, status):
    st = await service.status(event_id)
    return st if st["status"] == status else None
