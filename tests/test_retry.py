"""失败分类与重试：瞬时失败按退避重试，不可重试失败不反复重试，超时隔离。"""
import logging
import threading
import time

from .conftest import log_event_outcome

logger = logging.getLogger("tests.retry")


def _submit(server, key, payload, max_attempts=5):
    return server.post("/events", {
        "idempotency_key": key,
        "endpoint": f"{server.base}/_receiver",
        "payload": payload,
        "max_attempts": max_attempts,
    })


def test_transient_failure_retries_then_succeeds(server):
    """flaky 接收端前 2 次 5xx，第 3 次成功：可重试失败按退避重试。"""
    st, body = _submit(server, "flaky-key", {"simulate": {"flaky": 2}})
    assert st == 201
    ev = server.wait_status(body["event_id"])
    log_event_outcome(ev)

    assert ev["status"] == "delivered"
    assert ev["attempts"] == 3, "前两次 5xx 应触发恰好两次重试"
    kinds = [a["failure_kind"] for a in ev["attempts_log"]]
    assert kinds[:2] == ["http_5xx", "http_5xx"]
    assert ev["attempts_log"][2]["outcome"] == "success"
    # 退避可解释：相邻尝试的时间差应随次数增长（指数退避）
    ts = [a["ts"] for a in ev["attempts_log"]]
    gaps = [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]
    logger.info("退避间隔: %s", [f"{g:.3f}s" for g in gaps])
    assert gaps[1] > gaps[0], "退避间隔应递增（指数退避）"


def test_non_retryable_4xx_not_retried(server):
    """4xx 为不可重试失败：一次尝试即进入终态，不得反复重试。"""
    st, body = _submit(server, "bad-request-key", {"simulate": "fail400"})
    assert st == 201
    ev = server.wait_status(body["event_id"])
    log_event_outcome(ev)

    assert ev["status"] == "dead"
    assert ev["attempts"] == 1, "不可重试失败不得重试"
    assert ev["last_failure_kind"] == "http_4xx"
    # 稳定后再观察一段时间，确认没有额外尝试
    time.sleep(0.3)
    ev2 = server.get(f"/events/{body['event_id']}")
    assert ev2["attempts"] == 1


def test_retryable_5xx_exhausts_to_dead(server):
    """持续 5xx：按 max_attempts 重试耗尽后进入终态，原因可区分。"""
    st, body = _submit(server, "always-500-key", {"simulate": "fail500"},
                       max_attempts=3)
    assert st == 201
    ev = server.wait_status(body["event_id"])
    log_event_outcome(ev)

    assert ev["status"] == "dead"
    assert ev["attempts"] == 3, "应恰好尝试 max_attempts 次"
    # 终态保留真实失败原因，重试耗尽由 attempts == max_attempts 体现
    assert ev["last_failure_kind"] == "http_5xx"
    kinds = [a["failure_kind"] for a in ev["attempts_log"]]
    assert kinds == ["http_5xx", "http_5xx", "http_5xx"]


def test_connection_refused_is_retryable(server):
    """连接错误（接收端不存在）分类为 connection，可重试并最终耗尽。"""
    st, body = server.post("/events", {
        "idempotency_key": "conn-refused-key",
        "endpoint": "http://127.0.0.1:1/unreachable",
        "payload": {},
        "max_attempts": 2,
    })
    assert st == 201
    ev = server.wait_status(body["event_id"])
    log_event_outcome(ev)

    assert ev["status"] == "dead" and ev["attempts"] == 2
    assert ev["attempts_log"][0]["failure_kind"] == "connection"
    assert ev["last_failure_kind"] == "connection"


def test_timeout_is_failure_and_does_not_block_others(server):
    """单次投递超时判定为失败（可重试），且慢接收端不阻塞其他事件投递。"""
    # 慢事件：接收端睡眠 10s，投递超时 0.5s
    st, slow = _submit(server, "slow-key", {"simulate": "timeout"}, max_attempts=1)
    assert st == 201

    # 同时提交一批正常事件
    fast_ids = []
    for i in range(4):
        st, body = _submit(server, f"fast-key-{i}", {"n": i})
        assert st == 201
        fast_ids.append(body["event_id"])

    # 正常事件应迅速投递完成，不被慢事件阻塞
    start = time.time()
    for eid in fast_ids:
        ev = server.wait_status(eid, timeout=5)
        assert ev["status"] == "delivered"
    elapsed = time.time() - start
    logger.info("4 个正常事件在慢事件挂起期间完成，耗时 %.2fs", elapsed)
    assert elapsed < 5, "慢事件不得阻塞其他事件的投递"

    # 慢事件单次超时判定为失败（max_attempts=1 直接终态）
    ev = server.wait_status(slow["event_id"])
    log_event_outcome(ev)
    assert ev["status"] == "dead"
    assert ev["attempts"] == 1
    assert ev["last_failure_kind"] == "timeout"
    assert ev["attempts_log"][0]["duration_ms"] < 5000, "超时判定应发生在投递超时附近"
