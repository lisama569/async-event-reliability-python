# 异步事件入站与可靠处理（本地可运行）

一个基于 FastAPI 的本地示例：用 **SQLite（WAL 模式，单文件）** 同时模拟
消息代理（到期队列 + 租约）与处理状态库，演示事件接入的**幂等提交**、消费侧的
**退避重试 / 死信 / 人工重放**、多进程**并发消费不重复**，以及**可查询、单调**的
状态语义。所有状态落盘，服务/消费进程重启后不丢失。

## 架构

```
HTTP 入站 (FastAPI)                         消费进程（可多开）
POST /events ──► EventStore.submit()        Worker.run 循环：
  · 幂等键 (tenant_id, business_key)          1) claim_due   BEGIN IMMEDIATE 原子认领 + 租约
  · 请求哈希区分"重复"与"键冲突"              2) processor.process（注入式失败）
  · 每租户硬配额                              3) 成功 complete / 可恢复 requeue(退避)
  · 非法事件校验                              4) 致命或重试耗尽 dead_letter
        │                                                │
        ▼                                                ▼
            SQLite 文件（events / state_transitions / side_effects）
                          ▲
        GET 状态/迁移/副作用（只读索引查询，不拿写锁）
        POST /events/{id}/replay（人工重放死信）
```

- `app/api.py`：HTTP 接口与入站校验，应用内默认启动一个消费协程。
- `app/storage.py`：持久层与状态机，`BEGIN IMMEDIATE` 保证跨线程/跨进程认领串行化。
- `app/processor.py`：业务处理器，支持注入失败模式（可恢复 / 致命 / 始终可恢复）。
- `app/sideeffects.py`：副作用幂等账本（唯一约束去重）。
- `app/worker.py`：消费循环、指数退避、死信判定、租约栅栏。
- `app/cli.py`：`serve`（API + 内置 worker）与 `worker`（独立消费进程）入口。

## 运行

依赖 Python 3.14 与 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync

# 终端 1：API + 内置一个消费协程（单进程即可本地演示）
uv run python main.py serve --port 8000

# 终端 2..N：再开独立消费进程，共享同一个 DB（验证并发消费）
EVENT_DB_PATH=data/events.db uv run python main.py worker --id worker-1
EVENT_DB_PATH=data/events.db uv run python main.py worker --id worker-2
```

关键环境变量（均有默认值）：`EVENT_DB_PATH`、`MAX_ATTEMPTS`（默认 5）、
`RETRY_BASE_DELAY`（0.5s）、`RETRY_BACKOFF_FACTOR`（2）、`RETRY_MAX_DELAY`（30s）、
`LEASE_SECONDS`（5s）、`TENANT_QUOTA`（100）、`WORKER_POLL_INTERVAL`、
`FAIL_SPEC_PATH`（`data/fail_spec.json`）。

### 本地验证步骤

```bash
# 1) 提交事件（201）
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "tenant_id":"t1","business_key":"order-1",
  "event_type":"order.created","payload":{"amount":100}}'

# 2) 同样的请求再发一次 -> 200 且 duplicate=true，只产生一次结果
curl -s -X POST localhost:8000/events -d '{ ...同上... }'

# 3) 同业务键但请求体不同 -> 409 key_conflict
#    非法 event_type / 缺字段 -> 400 invalid_event
#    超过每租户配额 -> 429 quota_exceeded

# 4) 查询状态、状态迁移、副作用
curl localhost:8000/events/<event_id>
curl localhost:8000/events/<event_id>/transitions
curl localhost:8000/events/<event_id>/side-effects

# 5) 死信列表与人工重放（重放不产生重复副作用）
curl localhost:8000/dead-letters
curl -s -X POST localhost:8000/events/<event_id>/replay
```

制造失败以观察重试与死信（独立 worker 进程通过文件读取失败规格，键为 event_id）：

```jsonc
// data/fail_spec.json
{
  "<event_id>": {"mode": "recoverable", "times": 2, "message": "下游抖动"},
  "<event_id>": {"mode": "recoverable_always", "message": "持续故障"},
  "<event_id>": {"mode": "fatal", "times": 1, "message": "不可恢复"}
}
```

`recoverable` 在第 1..`times` 次尝试抛可恢复错误、之后成功；
`recoverable_always` 始终失败直到耗尽 `MAX_ATTEMPTS`；`fatal` 立即进死信。

## 语义说明

### 幂等提交

- 幂等键为 `(tenant_id, business_key)`，数据库唯一约束兜底；提交在
  `BEGIN IMMEDIATE` 事务内完成，并发重复请求只有一个写入成功，其余返回**同一
  event_id** 与 `duplicate=true`，且不消耗配额、不产生新副作用。
- 用 `event_type + payload` 的稳定哈希区分两种"重复"：
  - 同键 + 同请求体：良性重复 → `200 {"duplicate": true}`；
  - 同键 + 不同请求体：键冲突 → `409 key_conflict`。

### 拒绝原因（可区分）

| 情况 | HTTP | error 码 |
|---|---|---|
| 缺字段 / 空键 / 不支持的 event_type / 请求体过大 | 400 | `invalid_event` |
| 同 `(tenant,business_key)` 但请求体不同 | 409 | `key_conflict` |
| 超过每租户已接纳事件硬配额 | 429 | `quota_exceeded` |
| 查询不存在的事件 / 重放非死信 | 404 / 409 | `not_found` / `invalid_state` |

配额是"已接纳有效事件数"的硬上限，重复提交不累加。

### 异步消费、退避重试、死信

- 状态机：`PENDING → PROCESSING → COMPLETED`；失败时
  `PROCESSING → PENDING`（退避到期后再次可见），超过 `MAX_ATTEMPTS` 或遇到不可恢复
  错误则 `PROCESSING → DEAD_LETTER`。
- 退避：`delay(attempt) = min(RETRY_BASE_DELAY * FACTOR^(attempt-1), RETRY_MAX_DELAY)`。
- 死信事件可经 `POST /events/{id}/replay` 人工重放（`DEAD_LETTER → PENDING`，
  `replay_count+1`、attempt 归零）；非死信状态重放返回 `invalid_state`。

### 副作用 exactly-once 表现

处理器在"至少一次"语义下可能被重复执行，但每个副作用以
`(event_id, effect_key)` 唯一约束记入 `side_effects` 账本：重试/租约重投/人工重放
再次执行时，重复副作用被抑制，对外表现为**恰好一次**。`GET /events/{id}/side-effects`
可核验。

### 并发消费不重复

- `claim_due` 用单条 `BEGIN IMMEDIATE` 事务在到期集合（`state ∈ {PENDING,
  PROCESSING}` 且 `visible_at <= now`，按到期时间排序）中认领一条并立即写成
  `PROCESSING + owner + attempt`。SQLite 写锁使多进程/多线程中只有一个认领成功。
- 每次认领写入**租约到期时间**：消费进程崩溃后，租约到期的 `PROCESSING` 事件会自动
  重新到期并被其他进程重投一次（attempt 递增）。
- 完成/退避/死信都带**栅栏**：只有 `state=PROCESSING 且 attempt 与 owner 同时匹配`
 才能落库。崩溃前残留的"迟到"完成会被拒绝，无法覆盖新的处理结果。

### 状态查询与迟到/重复消息

- 所有迁移追加写入 `state_transitions`（仅追加、不可改），状态只沿合法方向迁移，
  已 `COMPLETED` 的结果不允许被任何迟到/重复消息回滚或覆盖（重复提交返回既有结果；
  迟到的 complete/requeue/dead_letter 返回 false）。
- 状态查询是走索引的只读事务（WAL 下不阻塞写者、不拿写锁），不会明显拖慢消费进度
  （`tests/test_state_semantics.py::test_status_reads_do_not_block_consumption`
  在持续高并发读下验证事件照常被消费完）。

### 重启恢复

事件、状态、结果、迁移历史、副作用均在 SQLite 文件中持久化。重启后：
`PENDING` 继续被消费、`DEAD_LETTER` 仍可重放、`COMPLETED` 结果与副作用不变，
重复提交依旧幂等、键冲突依旧可识别。

## 测试

```bash
uv run pytest -s
```

覆盖场景（共 24 个用例）：

- 并发幂等提交（12 路并发同请求只有 1 个有效事件）；重复 vs 键冲突；非法事件、
  配额、404 的错误码区分。
- 可恢复错误按指数退避重试后成功；重试耗尽进死信；致命错误立即进死信；
  死信人工重放成功且**副作用不重复**；退避递增与封顶。
- 6 个并发工作者（各自独立连接，模拟多进程）消费 30 条事件：每条恰好认领、恰好处理
  一次、恰好一条副作用；租约过期后重投 + 迟到完成被栅栏拒绝；API 内置 worker 与
  额外 worker 并发不重复。
- 重启（新建 store/连接）后继续消费、各状态与迁移历史保留、死信可重放、幂等仍成立。
- 状态单调性、迁移序列与尝试次数、重复入站不回滚完成结果、高并发读不阻塞消费。

测试日志（`-s` 默认开启）会打印：
幂等键（`idempotency_key=tenant/business_key`、`idempotent-hit/miss`）、
尝试次数（`attempt=N/M`）、以及每次状态迁移
（`PROCESSING->COMPLETED` / `PROCESSING->PENDING` / `PROCESSING->DEAD_LETTER`、
`late-*-rejected`、`side-effect-deduped` 等）。
