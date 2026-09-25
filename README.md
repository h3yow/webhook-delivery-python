# webhook-delivery-python

面向外部接收端的可靠事件投递服务。接收端由内置的本地模拟端点（`/_receiver`）承担，
不依赖任何外部服务或真实凭据，使重复提交、失败重试与并发投递下的最终结果一致且可解释。

## 运行

```bash
uv sync
uv run python main.py        # 监听 127.0.0.1:8000，数据落盘 webhook_delivery.db
uv run pytest                # 运行全部测试
uv run pytest --log-cli-level=INFO   # 查看幂等标识 / 重试次数 / 判定依据日志
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 提交事件并入队（幂等） |
| GET | `/events` | 列表查询，支持 `endpoint`、`status` 过滤 |
| GET | `/events/{event_id}` | 单事件状态与全部投递尝试记录 |
| GET | `/stats` | 各状态事件计数 |
| POST | `/_receiver` | 本地模拟接收端（见下文） |
| GET | `/_receiver/inbox` | 模拟接收端收件箱（验证用） |

### 提交事件

```json
{
  "idempotency_key": "order-123",
  "endpoint": "http://127.0.0.1:8000/_receiver",
  "payload": {"simulate": "ok"},
  "max_attempts": 5
}
```

响应语义（状态码可区分拒绝原因）：

- `201`：新事件入队，返回 `event_id`。
- `200` + `duplicated: true`：相同幂等标识且内容一致的重复提交，返回原事件，**不产生重复投递**。
- `409` `idempotency_conflict`：相同幂等标识但 `endpoint`/`payload` 不同，拒绝。
- `422` `invalid_endpoint` / schema 校验失败：参数不合法（非 http(s) 端点、空幂等标识、
  `max_attempts` 越界等），拒绝。

## 投递语义

- **至少一次投递 + 提交侧幂等**：同一幂等标识只对应一个事件；事件状态机为
  `pending → in_flight → delivered | dead`。
- **失败分类**（`failure_kind`，可在事件详情与尝试记录中查询）：
  - 可重试：`timeout`（单次投递超时）、`connection`（连接错误）、`http_5xx`；
  - 不可重试：`http_4xx`，一次尝试即进入 `dead`，绝不反复重试。
- **退避策略**：指数退避 `0.05s × 2^(attempt-1)`，封顶 5s；重试耗尽
  （`attempts == max_attempts`）后进入 `dead`，`last_failure_kind` 保留真实失败原因。
- **超时隔离**：单次投递超时（默认 2s）即判定为失败，由独立工作线程承担，
  不阻塞其他事件的投递。
- **并发约束**：固定大小线程池（默认 4）并发投递，领取批量不超过空闲容量，资源有界；
  事件按 `(next_attempt_at, created_at)` 顺序出队。`create_app(ordered_per_endpoint=True)`
  可开启同一接收端的串行有序投递（每个接收端同一时刻至多一个在途事件）。
- **重启恢复**：状态持久化于 SQLite。进程启动时将残留的 `in_flight` 事件重置为
  `pending` 重新调度——未完成事件不丢失、不被误判为成功；`delivered`/`dead` 终态事件
  不会被重投。

## 模拟接收端

`POST /_receiver` 按 `payload.simulate` 模拟接收端行为：

| 值 | 行为 |
| --- | --- |
| `"ok"`（默认） | 返回 200 |
| `{"flaky": N}` | 前 N 次返回 500，之后返回 200（瞬时故障） |
| `"fail500"` | 恒返回 500（可重试失败） |
| `"fail400"` | 恒返回 400（不可重试失败） |
| `"timeout"` | 睡眠 10s（触发投递超时） |

## 验证方法

`tests/` 覆盖任务要求的关键场景，日志打印幂等标识、重试次数与判定依据：

- `test_idempotency.py`：重复提交只投递一次；并发重复提交只创建一个事件；
  幂等冲突返回 409；非法参数返回 422。
- `test_retry.py`：瞬时失败（flaky）按递增退避重试后成功；4xx 不重试；
  持续 5xx / 连接失败按 `max_attempts` 耗尽进入 `dead`；超时判定为失败且不阻塞其他事件。
- `test_concurrency_restart.py`：30 事件并发提交全部恰好投递一次；同一接收端顺序约束；
  进程重启后 `in_flight` 残留被恢复重投、`pending` 不丢失、`delivered` 不重投。
