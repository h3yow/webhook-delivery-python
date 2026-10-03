# Webhook 可靠事件投递服务

在现有 FastAPI 服务基础上补齐面向外部接收端的可靠事件投递能力：投递目标注册 +
按接收端名路由，一个实例可同时对接多个本地可模拟的接收端点
（`app/receiver.py`），不依赖任何外部服务或真实凭据。

## 架构

```
POST /targets ──> 目标注册表 (SQLite) ─┐
                                       │ 按 receiver 名解析地址
POST /events ──> EventStore (SQLite) ──> Dispatcher ──> 各目标接收端 (POST url)
                    │                        │
                    │ 持久化状态机            │ 按目标串行、跨目标并发
                    ▼                        ▼
            pending → in_flight → delivered / failed
                       ↑________│ 可重试失败回到 pending（带退避时间）
```

- `app/models.py`：状态机、失败分类、目标/事件请求响应模型
- `app/store.py`：SQLite 持久化（事件 + 目标注册表、幂等插入、原子认领、崩溃恢复）
- `app/dispatcher.py`：后台调度循环（按名路由、失败分类、指数退避、并发控制）
- `app/receiver.py`：可编程行为的本地模拟接收端
- `app/main.py`：HTTP API 与生命周期装配

## API

### 投递目标

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/targets` | 登记具名目标 `{name, url}`。`201` 首次登记；`200` 同名同地址幂等；`409` 同名异址（`reason=target_conflict`）；`422` 登记信息不合法（`reason=invalid_target`） |
| `GET` | `/targets` / `/targets/{name}` | 查看目标（未登记 `404`，`reason=target_not_found`） |
| `PATCH` | `/targets/{name}` | 更新地址或临时停用/重新启用 `{url?, enabled?}` |

### 事件

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/events` | 提交事件并按 `receiver` 名路由入队。`201` 首次受理；`200 + duplicated=true` 幂等重复；`409` 幂等冲突；`422` 参数非法或接收端未登记；`429` 超出容量上限 |
| `GET` | `/events/{event_id}` | 查询投递进度：所属目标地址、状态、尝试次数、失败类别、下次重试时间、逐次历史 |
| `GET` | `/events` | 列出全部事件 |
| `GET` | `/` | 原有根路由，响应结构保持不变 |

模拟接收端挂载于 `/_receiver`：`POST /_receiver/program` 设置行为
（`always_ok` / `always_500` / `always_400` / `fail_times_then_ok` / `slow` / `reject`），
`GET /_receiver/deliveries` 查询已收到的投递，`POST /_receiver/reset` 重置。

## 目标注册与路由规则

- 事件按 `receiver` 字段（接收端名）路由到同名目标登记的地址；
  **同一个名字必须始终对应同一个地址**：`POST /targets` 拿同名指向另一个地址
  会被 `409 target_conflict` 拒绝，改地址只能显式 `PATCH /targets/{name}`。
- 拒绝原因可明确区分：
  - `unknown_receiver`（422）：注册表模式下提交了未登记的接收端名；
  - `invalid_target`（422）：目标名/地址不合法（地址必须是带主机名的 http(s)）；
  - `target_conflict`（409）：同名异址登记；
  - `target_not_found`（404）：查看/更新未登记的目标；
  - `idempotency_conflict`（409）：同一 `event_id` 内容不同；
  - `capacity_exceeded`（429）：待投递事件达到上限。
- **兼容模式**：`create_app(deliver_url=...)` 传入固定地址时，未登记的名字
  回落到该地址（旧行为不变）；`deliver_url=None` 为纯注册表模式，
  未登记名字一律拒绝。已登记的名字在两种模式下都优先路由到登记地址。

## 投递语义

- **至少一次（at-least-once）**：事件持久化后才算受理；投递成功（2xx）才进入终态
  `delivered`；进程重启后未完成事件（`pending` / `in_flight`）一律恢复重投，
  不会丢失，也不会把 `in_flight` 误判为成功。
- **状态机**：`pending → in_flight → delivered | failed`；可重试失败时
  `in_flight → pending`（携带 `next_retry_at`）。`failed` 与 `delivered` 均为终态。
- **目标停用**：停用期间已受理的事件留在 `pending`，不被认领、不消耗重试次数、
  不判失败；重新启用后按入队顺序补投，绝不丢弃。

## 多目标下的顺序 / 并发 / 重试

- **同一目标**内的事件严格按入队顺序串行投递（per-receiver 锁）。
- **不同目标**之间并发投递；某个目标慢或暂时不可用（停用、连接失败）时，
  只影响自己的目标队列，不拖住其他目标。
- 每个目标各自维护投递进度：认领按目标过滤（停用目标的事件不被认领），
  重试退避、单次超时都按事件独立计算。
- 全局信号量限制同时在途的投递数（默认 4），批量认领有上限（默认 32）。

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

## 重启与容量约束

- 目标登记信息与事件状态都持久化在同一 SQLite 文件中：重启后已登记目标仍在，
  未完成投递接着投，`in_flight` 一律恢复为 `pending` 重投，绝不误判为成功。
- **容量上限**：待投递事件数（`pending + in_flight`）达到 `max_pending`
  （默认 1000）后，新事件提交返回 `429 capacity_exceeded`；
  幂等重报（重复提交已受理事件）不受容量限制，保证受理结果稳定。
- 单个事件重试次数上限为 `max_attempts`；全局并发信号量与批量认领上限
  保证并发升高时不出现状态错乱、重复投递或资源无界增长。
- 所有存储写操作经互斥锁序列化；认领使用 `BEGIN IMMEDIATE` 事务保证原子性。

## 运行与验证

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动服务（默认 127.0.0.1:8000，状态存 ./events.db）
uv run pytest -q        # 运行全部测试
uv run pytest -q -s     # 查看投递日志：幂等标识、重试次数、判定依据
```

默认启动即可跑通一次真实提交与投递（无需手动改配置）：服务预置目标
`default` 指向内置模拟接收端，直接提交即可：

```bash
curl -X POST localhost:8000/events -H 'content-type: application/json' -d \
  '{"event_id":"demo-1","event_type":"demo","receiver":"default","payload":{"hello":"world"}}'
curl localhost:8000/events/demo-1        # status=delivered, target_url=.../_receiver/recv
curl localhost:8000/targets              # 查看已登记目标
```

对接多个接收端时，先登记再按名提交：

```bash
curl -X POST localhost:8000/targets -H 'content-type: application/json' -d \
  '{"name":"billing","url":"http://127.0.0.1:9001/recv"}'
curl -X PATCH localhost:8000/targets/billing -H 'content-type: application/json' -d \
  '{"enabled":false}'   # 临时停用；恢复后积压事件自动补投
```

测试覆盖（`tests/`）：

- `test_idempotency_and_retry.py`：重复提交、幂等冲突、参数非法、
  瞬时 5xx 重试成功、4xx/业务拒绝不重试、重试耗尽放弃；
- `test_concurrency_timeout.py`：跨接收端并发、同接收端顺序、
  高并发恰好一次、超时判失败且不阻塞他人；
- `test_restart_recovery.py`：存储层与端到端的重启恢复
  （`in_flight` 恢复、未完成事件续投）；
- `test_targets_routing.py`：目标注册与按名路由、未登记名/不合法/冲突登记的
  拒绝原因区分、同目标顺序与跨目标并发、停用再恢复后的补投、
  目标与事件的重启持久化、容量上限、注册表模式下并发重复提交的稳定性。

日志样例：

```
INFO dispatcher event=retry-1 attempt=1 result=failed kind=server_5xx detail='http 500: ...' -> retry in 0.050s
INFO dispatcher event=retry-1 attempt=3 result=delivered http=200
INFO tests event_id=retry-1 retries=2 kinds=['server_5xx','server_5xx','none'] basis=500->可重试, 2xx->成功
INFO tests event_id=conc-1 并发=20 201=1 200dup=19 basis=唯一约束兜底+内容比对
INFO tests 恢复后补投顺序=['d-0','d-1','d-2'] basis=重新启用后继续投递
```
