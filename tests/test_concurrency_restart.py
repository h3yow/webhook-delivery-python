"""并发投递、顺序约束与进程重启恢复。"""
import logging
import threading
import time

from app.models import DeliveryStatus, Event
from app.store import EventStore

from .conftest import log_event_outcome, start_server

logger = logging.getLogger("tests.concurrency")


def test_concurrent_submission_and_delivery(server):
    """多线程并发提交 30 个事件：全部恰好投递一次，状态一致。"""
    n = 30
    ids, errors = [], []

    def submit(i):
        try:
            st, body = server.post("/events", {
                "idempotency_key": f"conc-key-{i}",
                "endpoint": f"{server.base}/_receiver",
                "payload": {"n": i},
            })
            assert st == 201
            ids.append(body["event_id"])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and len(ids) == n

    for eid in ids:
        ev = server.wait_status(eid)
        assert ev["status"] == "delivered" and ev["attempts"] == 1

    inbox = server.get("/_receiver/inbox")
    assert inbox["count"] == n, "并发下每个事件应恰好投递一次"
    assert len({r["event_id"] for r in inbox["received"]}) == n

    stats = server.get("/stats")
    logger.info("并发投递完成 stats=%s", stats)
    assert stats == {"pending": 0, "in_flight": 0, "delivered": n, "dead": 0}


def test_concurrent_duplicate_submission_single_event(server):
    """并发提交相同幂等标识：只产生一个事件、一次投递。"""
    results = []

    def submit():
        results.append(server.post("/events", {
            "idempotency_key": "same-key",
            "endpoint": f"{server.base}/_receiver",
            "payload": {"x": 1},
        }))

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    statuses = sorted(st for st, _ in results)
    assert statuses.count(201) == 1, "恰好一个请求创建事件"
    assert statuses.count(200) == 7, "其余为幂等重复"
    event_ids = {body["event_id"] for _, body in results}
    assert len(event_ids) == 1

    ev = server.wait_status(event_ids.pop())
    log_event_outcome(ev)
    assert server.get("/_receiver/inbox")["count"] == 1


def test_per_endpoint_ordering(tmp_path):
    """开启 ordered_per_endpoint 后，同一接收端的事件按提交顺序串行投递。"""
    server = start_server(db_path=str(tmp_path / "ordered.db"),
                          poll_interval=0.01, timeout=0.5,
                          ordered_per_endpoint=True)
    try:
        n = 6
        ids = []
        for i in range(n):
            st, body = server.post("/events", {
                "idempotency_key": f"ord-{i:02d}",
                "endpoint": f"{server.base}/_receiver",
                "payload": {"seq": i},
            })
            assert st == 201
            ids.append(body["event_id"])
        for eid in ids:
            assert server.wait_status(eid)["status"] == "delivered"

        inbox = server.get("/_receiver/inbox")["received"]
        got_keys = [r["idempotency_key"] for r in inbox]
        expected = [f"ord-{i:02d}" for i in range(n)]
        logger.info("接收顺序=%s 期望=%s", got_keys, expected)
        assert got_keys == expected, "同一接收端应按提交顺序投递"
    finally:
        server.stop()


def test_restart_recovers_in_flight_and_pending(tmp_path):
    """进程重启：in_flight 残留被恢复重投，pending 不丢失，delivered 不重投。"""
    db = str(tmp_path / "restart.db")

    # 模拟崩溃现场：直接向库中写入三种状态的事件
    store = EventStore(db)
    delivered = Event(event_id=Event.new_id(), idempotency_key="done-key",
                      endpoint="http://127.0.0.1:1/x", payload={},
                      status=DeliveryStatus.DELIVERED, attempts=1)
    inflight = Event(event_id=Event.new_id(), idempotency_key="inflight-key",
                     endpoint="", payload={}, status=DeliveryStatus.IN_FLIGHT,
                     attempts=1)
    pending = Event(event_id=Event.new_id(), idempotency_key="pending-key",
                    endpoint="", payload={}, status=DeliveryStatus.PENDING)
    store.insert_event(delivered)
    store.insert_event(inflight)
    store.insert_event(pending)
    store.close()

    # 重启：新进程加载同一数据库
    server = start_server(db_path=db, poll_interval=0.01, timeout=0.5)
    try:
        assert server.app.state.recovered == 1, "应恢复 1 个 in_flight 事件"

        # 修正两个未完成事件的端点指向本服务的模拟接收端
        # （通过直接改库模拟同一事件在新进程中继续投递）
        st, _ = server.post("/events", {
            "idempotency_key": "inflight-key",
            "endpoint": f"{server.base}/_receiver", "payload": {}})
        assert st == 409, "in_flight 事件已存在，重复提交不得产生新事件"

        # 直接更新库中端点（模拟崩溃前写入的事件在新进程中被调度）
        server.app.state.store._conn.execute(
            "UPDATE events SET endpoint=? WHERE idempotency_key IN ('inflight-key','pending-key')",
            (f"{server.base}/_receiver",))
        server.app.state.store._conn.commit()
        server.app.state.dispatcher.notify()

        ev1 = server.wait_status(inflight.event_id)
        ev2 = server.wait_status(pending.event_id)
        log_event_outcome(ev1)
        log_event_outcome(ev2)
        assert ev1["status"] == "delivered", "in_flight 残留应被恢复并完成投递"
        assert ev2["status"] == "delivered", "pending 事件重启后不得丢失"

        # delivered 事件不得被误判或重投
        ev0 = server.get(f"/events/{delivered.event_id}")
        assert ev0["status"] == "delivered" and ev0["attempts"] == 1
        time.sleep(0.3)
        inbox = server.get("/_receiver/inbox")
        assert inbox["count"] == 2, "已投递事件重启后不得重投"
    finally:
        server.stop()
