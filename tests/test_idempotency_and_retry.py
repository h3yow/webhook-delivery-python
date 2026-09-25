"""幂等提交、瞬时失败重试、不可重试失败的端到端验证。"""
from __future__ import annotations

import logging

from .conftest import wait_for

logger = logging.getLogger("tests.idempotency")


async def test_duplicate_submit_no_double_delivery(service, receiver):
    """同一幂等标识重复提交：第二次返回 duplicated，且只投递一次。"""
    event = {"event_id": "idem-1", "event_type": "user.created",
             "receiver": "r1", "payload": {"v": 1}}
    r1 = await service.submit(**event)
    assert r1.status_code == 201, r1.text
    r2 = await service.submit(**event)
    assert r2.status_code == 200 and r2.json()["duplicated"] is True

    st = await wait_for(lambda: _delivered(service, "idem-1"))
    deliveries = await receiver.deliveries()
    logger.info("event_id=%s attempts=%d deliveries=%d basis=唯一约束+内容比对",
                "idem-1", st["attempts"], len(deliveries))
    assert st["status"] == "delivered"
    assert len(deliveries) == 1, "重复提交不得产生重复投递"


async def test_idempotency_conflict_rejected(service):
    """同一幂等标识但内容不同：409 且原因可区分。"""
    await service.submit(event_id="idem-2", event_type="t", receiver="r1",
                         payload={"v": 1})
    r = await service.submit(event_id="idem-2", event_type="t", receiver="r1",
                             payload={"v": 2})
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "idempotency_conflict"
    logger.info("event_id=%s 拒绝原因=%s", "idem-2",
                r.json()["detail"]["reason"])


async def test_invalid_params_rejected(service):
    """参数不合法：422，与幂等冲突（409）可区分。"""
    r = await service.submit(event_id="", event_type="t", receiver="r1",
                             payload={})
    assert r.status_code == 422
    # 缺 receiver 必须拒绝；payload 有缺省值，可缺省
    r = await service.client.post(
        "/events", json={"event_id": "x", "event_type": "t"})
    assert r.status_code == 422
    r = await service.client.post("/events", json={"event_type": "t"})
    assert r.status_code == 422


async def test_transient_failure_retries_then_succeeds(service, receiver):
    """瞬时 5xx：按退避策略重试，最终成功，次数与历史可预期。"""
    await receiver.program(mode="fail_times_then_ok", fail_times=2)
    await service.submit(event_id="retry-1", event_type="t", receiver="r1",
                         payload={})
    st = await wait_for(lambda: _delivered(service, "retry-1"))
    deliveries = await receiver.deliveries()
    kinds = [h["kind"] for h in st["history"]]
    logger.info("event_id=%s retries=%d kinds=%s basis=500->可重试, 2xx->成功",
                "retry-1", st["attempts"] - 1, kinds)
    assert st["status"] == "delivered"
    assert st["attempts"] == 3, "前 2 次 500 + 第 3 次成功"
    assert kinds == ["server_5xx", "server_5xx", "none"]
    assert len(deliveries) == 3


async def test_non_retriable_4xx_not_retried(service, receiver):
    """4xx 不可重试：只投递一次即终态 failed，原因可区分。"""
    await receiver.program(mode="always_400")
    await service.submit(event_id="bad-1", event_type="t", receiver="r1",
                         payload={})
    st = await wait_for(lambda: _failed(service, "bad-1"))
    deliveries = await receiver.deliveries()
    logger.info("event_id=%s attempts=%d kind=%s basis=400->不可重试",
                "bad-1", st["attempts"], st["last_failure_kind"])
    assert st["status"] == "failed"
    assert st["attempts"] == 1, "不可重试失败不得反复重试"
    assert st["last_failure_kind"] == "client_4xx"
    assert len(deliveries) == 1


async def test_receiver_rejection_not_retried(service, receiver):
    """接收端业务拒绝（422 rejected）：不可重试，类别与 4xx 可区分。"""
    await receiver.program(mode="reject")
    await service.submit(event_id="rej-1", event_type="t", receiver="r1",
                         payload={})
    st = await wait_for(lambda: _failed(service, "rej-1"))
    logger.info("event_id=%s attempts=%d kind=%s basis=422+rejected->业务拒绝",
                "rej-1", st["attempts"], st["last_failure_kind"])
    assert st["last_failure_kind"] == "receiver_rejected"
    assert st["attempts"] == 1


async def test_retriable_exhaustion_gives_up(service, receiver):
    """持续 5xx：重试至 max_attempts 后放弃，终态 failed。"""
    await receiver.program(mode="always_500")
    await service.submit(event_id="exh-1", event_type="t", receiver="r1",
                         payload={})
    st = await wait_for(lambda: _failed(service, "exh-1"))
    deliveries = await receiver.deliveries()
    logger.info("event_id=%s attempts=%d max=%d basis=重试次数耗尽",
                "exh-1", st["attempts"], st["max_attempts"])
    assert st["status"] == "failed"
    assert st["attempts"] == st["max_attempts"] == 4
    assert st["last_failure_kind"] == "server_5xx"
    assert len(deliveries) == 4


async def _delivered(service, event_id):
    st = await service.status(event_id)
    return st if st["status"] == "delivered" else None


async def _failed(service, event_id):
    st = await service.status(event_id)
    return st if st["status"] == "failed" else None
