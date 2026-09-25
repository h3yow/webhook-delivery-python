# Webhook 可靠事件投递服务

在现有 FastAPI 服务基础上补齐面向外部接收端的可靠事件投递能力。接收端由本地可模拟端点承担（`app/receiver.py`），不依赖任何外部服务或真实凭据。

## 架构

```
POST /events ──> EventStore (SQLite) ──> Dispatcher ──> 接收端 (POST deliver_url)
                    │                        │
                    │ 持久化状态机            │ 按接收端串行、跨接收端并发
                    ▼                        ▼
            pending → in_flight → delivered / failed
                       ↑________│ 可重试失败回到 pending（带退避时间）
```

- `app/models.py`：状态机、失败分类、请求/响应模型
- `app/store.py`：SQLite 持久化（幂等插入、原子认领、崩溃恢复）
- `app/dispatcher.py`：后台调度循环（失败分类、指数退避、并发控制）
- `app/receiver.py`：可编程行为的本地模拟接收端
- `app/main.py`：HTTP API 与生命周期装配

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/events` | 提交事件并入队。`201` 首次受理；`200 + duplicated=true` 幂等重复；`409` 幂等冲突；`422` 参数非法 |
| `GET` | `/events/{event_id}` | 查询投递进度：状态、尝试次数、失败类别、下次重试时间、逐次历史 |
| `GET` | `/events` | 列出全部事件 |
| `GET` | `/` | 原有根路由，响应结构保持不变 |

模拟接收端挂载于 `/_receiver`：`POST /_receiver/program` 设置行为
（`always_ok` / `always_500` / `always_400` / `fail_times_then_ok` / `slow` / `reject`），
`GET /_receiver/deliveries` 查询已收到的投递，`POST /_receiver/reset` 重置。

## 投递语义

- **至少一次（at-least-once）**：事件持久化后才算受理；投递成功（2xx）才进入终态
  `delivered`；进程重启后未完成事件（`pending` / `in_flight`）一律恢复重投，
  不会丢失，也不会把 `in_flight` 误判为成功。
- **状态机**：`pending → in_flight → delivered | failed`；可重试失败时
  `in_flight → pending`（携带 `next_retry_at`）。`failed` 与 `delivered` 均为终态。

## 幂等规则

- `event_id` 为幂等标识，数据库唯一约束兜底。
- 同一 `event_id` 且**内容一致**的重复提交：返回 `200 duplicated=true`，不产生重复投递。
- 同一 `event_id` 但**内容不同**：返回 `409`，`detail.reason = "idempotency_conflict"`。
- 参数不合法（缺字段、空串等）：返回 `422`。三种拒绝原因可明确区分。

## 失败分类与重试

| 类别 | 判定依据 | 是否重试 |
|---|---|---|
| `timeout` | 单次投递超过超时阈值 | 是 |
| `connection` | 连接错误等传输层异常 | 是 |
| `server_5xx` | HTTP 5xx | 是 |
| `rate_limited` | HTTP 429 | 是 |
| `client_4xx` | 其他 HTTP 4xx | 否 |
| `receiver_rejected` | HTTP 422 且响应含 `"rejected"` | 否 |

- **退避策略**：`delay = base_delay × factor^(attempt-1)`，封顶 `max_delay`
  （默认 0.5s × 2^n，封顶 30s，最多 5 次，测试中缩小以提速）。
- 达到 `max_attempts` 或遭遇不可重试失败即进入终态 `failed`，不再重试。
- 单次投递有独立超时，超时判定为失败；慢接收端不会阻塞其他事件的投递。

## 并发约束

- **同一接收端**的事件严格按入队顺序串行投递（per-receiver 锁）。
- **不同接收端**之间并发投递。
- 全局信号量限制同时在途的投递数（默认 4），批量认领有上限（默认 32），
  并发升高时不会出现状态错乱或资源无界增长。
- 所有存储写操作经互斥锁序列化；认领使用 `BEGIN IMMEDIATE` 事务保证原子性。

## 运行与验证

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动服务（默认 127.0.0.1:8000）
uv run pytest -q        # 运行全部测试
uv run pytest -q -s     # 查看投递日志：幂等标识、重试次数、判定依据
```

测试覆盖（`tests/`）：

- `test_idempotency_and_retry.py`：重复提交、幂等冲突、参数非法、
  瞬时 5xx 重试成功、4xx/业务拒绝不重试、重试耗尽放弃；
- `test_concurrency_timeout.py`：跨接收端并发、同接收端顺序、
  高并发恰好一次、超时判失败且不阻塞他人；
- `test_restart_recovery.py`：存储层与端到端的重启恢复
  （`in_flight` 恢复、未完成事件续投）。

日志样例：

```
INFO dispatcher event=retry-1 attempt=1 result=failed kind=server_5xx detail='http 500: ...' -> retry in 0.050s
INFO dispatcher event=retry-1 attempt=3 result=delivered http=200
INFO tests event_id=retry-1 retries=2 kinds=['server_5xx','server_5xx','none'] basis=500->可重试, 2xx->成功
```
