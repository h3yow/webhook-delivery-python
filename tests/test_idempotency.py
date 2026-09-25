"""幂等与参数校验：重复提交不产生重复投递，冲突与非法请求被可区分地拒绝。"""
import logging

from .conftest import log_event_outcome

logger = logging.getLogger("tests.idempotency")


def _submit(server, key, payload=None, endpoint=None, max_attempts=5):
    body = {
        "idempotency_key": key,
        "endpoint": endpoint or f"{server.base}/_receiver",
        "payload": payload if payload is not None else {"n": 1},
        "max_attempts": max_attempts,
    }
    return server.post("/events", body)


def test_duplicate_submission_delivers_once(server):
    st, first = _submit(server, "dup-key-1")
    assert st == 201 and first["duplicated"] is False

    # 相同内容重复提交：200 + duplicated，不产生新事件
    st, dup = _submit(server, "dup-key-1")
    assert st == 200
    assert dup["duplicated"] is True
    assert dup["event_id"] == first["event_id"]

    ev = server.wait_status(first["event_id"])
    log_event_outcome(ev)
    assert ev["status"] == "delivered" and ev["attempts"] == 1

    inbox = server.get("/_receiver/inbox")
    assert inbox["count"] == 1, "重复提交不得产生重复投递"
    assert server.get("/events")["events"].__len__() == 1


def test_idempotency_conflict_rejected(server):
    st, first = _submit(server, "conflict-key", payload={"n": 1})
    assert st == 201

    # 相同幂等标识但内容不同：409，原因可区分
    st, resp = _submit(server, "conflict-key", payload={"n": 2})
    assert st == 409
    assert resp["error"] == "idempotency_conflict"
    assert resp["event_id"] == first["event_id"]
    logger.info("幂等冲突被拒绝 key=%s 原因=%s", "conflict-key", resp["error"])


def test_invalid_params_rejected(server):
    # 非 http(s) 端点
    st, resp = _submit(server, "bad-ep", endpoint="ftp://example.com/hook")
    assert st == 422 and resp["error"] == "invalid_endpoint"
    # 空幂等标识（schema 校验）
    st, resp = server.post("/events", {
        "idempotency_key": "", "endpoint": f"{server.base}/_receiver",
        "payload": {}})
    assert st == 422
    # 缺少 payload
    st, resp = server.post("/events", {
        "idempotency_key": "k", "endpoint": f"{server.base}/_receiver"})
    assert st == 422
    # max_attempts 越界
    st, resp = _submit(server, "bad-attempts", max_attempts=0)
    assert st == 422
    logger.info("非法参数均被 422 拒绝，与 409 幂等冲突可区分")


def test_unknown_event_404(server):
    st, resp = server.post("/events", {
        "idempotency_key": "k-404-check", "endpoint": f"{server.base}/_receiver",
        "payload": {}})
    assert st == 201
    import urllib.error, urllib.request
    try:
        server.get("/events/does-not-exist")
        raise AssertionError("expected 404")
    except urllib.error.HTTPError as e:
        assert e.code == 404
