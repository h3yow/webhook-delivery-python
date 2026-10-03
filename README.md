# Webhook 可靠事件投递服务

在 FastAPI 服务上实现面向多个下游接收端的可靠事件投递：事件在运行时按其声明的
**接收端名字**路由到各自登记的地址；接收端由本地可模拟端点承担（`app/receiver.py`），
不依赖任何外部服务或真实凭据。

## 架构

```
                  ┌──────────────── targets 注册表（SQLite，持久化）────────────────┐
PUT /targets/{n} ─┤  name(唯一) -> url, enabled；名字与地址一一对应、不可改址        │
                  └───────────────────────────────────────────────────────────────┘
POST /events ──> 校验 receiver 名字已登记 ──> EventStore (SQLite)
                                                    │
                                                    ▼
            Dispatcher：按目标队首认领（每目标同时至多一条在途）
                                                    │
                    ┌───────────────┬───────────────┴───────────────┐
                    ▼               ▼                               ▼
              目标 ta（串行）   目标 tb（串行）              目标 tc（停用→排队）
            pending → in_flight → delivered / failed
                       ↑________│ 可重试失败回到 pending（带退避时间）
```

- `app/models.py`：状态机、失败分类、事件/目标请求响应模型
- `app/store.py`：SQLite 持久化（事件幂等插入、原子认领、崩溃恢复、目标注册表、容量上限）
- `app/dispatcher.py`：后台调度循环（按名解析地址、失败分类、指数退避、并发控制）
- `app/receiver.py`：可编程行为的本地模拟接收端
- `app/main.py`：HTTP API 与生命周期装配

## API

### 事件

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/events` | 提交事件并入队。`201` 首次受理；`200 + duplicated=true` 幂等重复；`409` 幂等冲突；`422` 参数非法或**接收端名字未登记**；`503` 待投递量超上限 |
| `GET` | `/events/{event_id}` | 查询投递进度：归属目标 `target`、状态、尝试次数、失败类别、下次重试时间、逐次历史 |
| `GET` | `/events` | 列出全部事件 |
| `GET` | `/` | 原有根路由，响应结构保持不变 |

### 投递目标

| 方法 | 路径 | 说明 |
|---|---|---|
| `PUT` | `/targets/{name}` | 登记具名目标（请求体 `{"url": ...}`）。`201` 首次登记；`200 + duplicated=true` 同名同址幂等重复；`409` 同名指向另一地址 |
| `GET` | `/targets` | 查看全部目标（名字、地址、启用状态、时间戳） |
| `GET` | `/targets/{name}` | 查看单个目标；不存在 `404` |
| `PATCH` | `/targets/{name}` | 临时停用 / 重新启用（请求体 `{"enabled": false|true}`）；不存在 `404` |

模拟接收端挂载于 `/_receiver`：`POST /_receiver/program` 设置行为
（`always_ok` / `always_500` / `always_400` / `fail_times_then_ok` / `slow` / `reject`），
`GET /_receiver/deliveries` 查询已收到的投递，`POST /_receiver/reset` 重置。

## 目标注册与路由规则

- 事件体里的 `receiver` 字段即**路由键（目标名字）**：调度器据此从注册表解析投递 URL。
- **名字 ↔ 地址一一对应，且不可改址**：
  - 同名 + 同址重复登记：幂等成功（`200 duplicated=true`）；
  - 同名 + 不同地址：明确拒绝 `409`，`detail.reason = "target_address_conflict"`，
    响应同时给出 `existing_url` / `new_url`，原登记不受影响；
  - 目标停用/启用只改 `enabled`，地址始终不变。
- **三类拒绝原因可明确区分**：
  - 登记信息不合法（空名、空 URL、缺字段）：`422`（目标接口返回
    `detail.reason = "invalid_target"`；请求体校验失败为校验层数组明细）；
  - 同名改址冲突：`409 target_address_conflict`；
  - 事件提交给未登记名字：`422 detail.reason = "target_not_registered"`。
- 停用的目标仍可继续受理事件（排队等待），只是不会被调度投递。

## 多目标下的顺序 / 并发 / 重试语义

- **目标内严格有序**：认领时每个目标只取其队首（入队顺序以 SQLite `rowid` 为准）的
  到期事件，同一目标同一时刻至多一条在途；后续事件必须等前一条到达终态或退回
  `pending` 才能被认领，因此严格按入队顺序投递。
- **跨目标并发、相互隔离**：不同目标的投递在各自任务中并发进行，并由全局信号量
  （默认 4）限制总在途数。某个目标慢、超时或暂时不可用，只会让它自己的队列等待，
  不会拖住其他目标。
- **失败分类与退避重试在每个目标、每个事件上独立维护**（见下表）；
  `delay = base_delay × factor^(attempt-1)`，封顶 `max_delay`，默认最多 5 次。
- 目标**停用/未登记/认领后状态变化**时，在途事件以 `defer` 退回 `pending`：
  **不增加尝试次数、不记失败历史**；目标重新启用（或恢复登记）后自动续投，事件不丢。

| 类别 | 判定依据 | 是否重试 |
|---|---|---|
| `timeout` | 单次投递超过超时阈值 | 是 |
| `connection` | 连接错误等传输层异常（含目标宕机） | 是 |
| `server_5xx` | HTTP 5xx | 是 |
| `rate_limited` | HTTP 429 | 是 |
| `client_4xx` | 其他 HTTP 4xx | 否 |
| `receiver_rejected` | HTTP 422 且响应含 `"rejected"` | 否 |

达到 `max_attempts` 或遭遇不可重试失败即进入终态 `failed`，不再重试。

## 投递与幂等规则

- **至少一次（at-least-once）**：事件持久化后才算受理；投递成功（2xx）才进入终态
  `delivered`。高并发下同一 `event_id` + 相同内容的重复提交，恰有一次 `201`，
  其余返回 `200 duplicated=true`，不会重复投递；同一 `event_id` 内容不同返回 `409`。
- **状态机**：`pending → in_flight → delivered | failed`；可重试失败时
  `in_flight → pending`（携带 `next_retry_at`）。`failed` 与 `delivered` 均为终态。

## 重启恢复与容量上限

- 目标登记与事件都保存在 SQLite 文件中（默认 `data/webhook.db`），**进程重启后**：
  - 已登记目标（含启用/停用状态）仍然存在；预置目标用幂等登记，不覆盖既有记录；
  - 未完成事件（`pending` 以及崩溃时停留在 `in_flight` 的事件）一律恢复重投，
    **绝不会把没投成的当成投成功**，重启前的失败尝试仍计入历史。
- **容量上限（背压）**：路由模式下待投递（`pending` + `in_flight`）事件数默认上限
  **1000**（`create_app(max_outstanding=...)` 可调）。计数与插入在同一事务内完成，
  并发提交也不会超限；超限返回 `503 detail.reason = "capacity_exceeded"`。
  事件到达终态后自动腾出名额。
- **单事件重试上限**：`max_attempts`（默认 5），达到即终态 `failed`，不会无限重试。
- 所有存储写操作经互斥锁序列化；认领使用 `BEGIN IMMEDIATE` 事务保证原子性，
  并发升高时不会出现状态错乱、重复投递或资源无界增长。

## 运行与验证

默认方式直接启动即可完成一次真实的提交与投递（内置模拟接收端与服务同进程，
预置具名目标 `default`，无需任何手动配置）：

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动服务（127.0.0.1:8000，SQLite 落在 data/webhook.db）
```

另开一个终端：

```bash
# 事件声明 receiver=default -> 投到内置接收端 http://127.0.0.1:8000/_receiver/recv
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"demo-1","event_type":"demo.hello","receiver":"default","payload":{"v":1}}'

curl -s localhost:8000/events/demo-1            # 查看投递进度（target/status/attempts）
curl -s localhost:8000/_receiver/deliveries     # 接收端实际收到的投递
curl -s localhost:8000/targets                  # 已登记目标
```

对接多个本地接收端时，先登记具名目标，再让事件声明对应名字：

```bash
uvicorn app.receiver:create_receiver_app --factory --port 9001 &
uvicorn app.receiver:create_receiver_app --factory --port 9002 &
curl -s -X PUT localhost:8000/targets/billing -H 'Content-Type: application/json' \
  -d '{"url":"http://127.0.0.1:9001/recv"}'
curl -s -X PUT localhost:8000/targets/notify  -H 'Content-Type: application/json' \
  -d '{"url":"http://127.0.0.1:9002/recv"}'
curl -s -X PATCH localhost:8000/targets/billing -H 'Content-Type: application/json' \
  -d '{"enabled":false}'      # 临时停用；期间事件排队
curl -s -X PATCH localhost:8000/targets/billing -H 'Content-Type: application/json' \
  -d '{"enabled":true}'       # 重新启用；积压事件按序补投
```

可用环境变量 `WEBHOOK_BASE_URL`、`WEBHOOK_DB` 覆盖默认自身地址与数据库路径。

```bash
uv run pytest -q        # 运行全部测试
uv run pytest -q -s     # 查看日志：每个用例验什么业务、幂等标识、重试次数、判定依据
```

测试覆盖（`tests/`）：

- `test_idempotency_and_retry.py`：重复提交、幂等冲突、参数非法、
  瞬时 5xx 重试成功、4xx/业务拒绝不重试、重试耗尽放弃；
- `test_concurrency_timeout.py`：跨接收端并发、同接收端顺序、
  高并发恰好一次、超时判失败且不阻塞他人；
- `test_restart_recovery.py`：存储层与端到端的重启恢复
  （`in_flight` 恢复、未完成事件续投）；
- `test_target_routing.py`：目标登记/查看/幂等重复、不合法登记与同名改址冲突、
  未登记名字拒绝、按名路由、停用再恢复补投、目标宕机同址恢复不丢事件；
- `test_multi_target_concurrency.py`：同目标顺序与跨目标并发（慢目标不拖累他人）、
  容量背压 503、高并发同一幂等标识受理稳定、重启后目标与未完成事件恢复。

日志样例：

```
INFO dispatcher event=down-0 target=ta attempt=1 result=failed kind=connection detail='ConnectError: ...' -> retry in 0.050s
INFO dispatcher event=down-0 target=ta attempt=3 result=delivered http=200
INFO tests.targets event_id=down-0 宕机期间 attempts=2 kind=connection status=pending basis=连接失败可重试且不终结
INFO tests.multi event_id=idem-hot 并发=20 首次201=1 重复200=19 拒绝=0 basis=唯一约束+内容比对
```
