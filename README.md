# 异步事件可靠处理（本地可运行 / FastAPI + SQLite）

用本地存储模拟消息代理与处理进程，提供事件幂等提交、异步消费、退避重试、
死信恢复与单调状态查询。无需任何外部中间件，一个 SQLite 文件即代表全部状态。

## 快速开始

```bash
uv sync --group dev

# 启动服务（默认 2 个并发消费进程，SQLite 落到 ./data/events.db）
uv run uvicorn main:app --host 127.0.0.1 --port 8000

# 或直接
uv run python main.py
```

可用环境变量：`EVENT_DB_PATH`、`WORKER_CONCURRENCY`、`TENANT_QUOTA`、
`MAX_ATTEMPTS`。

### 提交一个事件

```bash
curl -s -X POST http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"t1","business_key":"order-1",
       "event_type":"order.created",
       "payload":{"mode":"ok"}}'
# 202 {"event_id":1,"idempotency_key":"t1:order-1","status":"ACCEPTED",...}
```

`payload` 是给内置**模拟处理器**的指令（仅用于本地演示/测试）：

| 字段 | 含义 |
| --- | --- |
| `mode` | `ok`（默认）/ `transient`（可恢复失败）/ `permanent`（不可恢复失败） |
| `fail_times` | `transient` 时前 N 次尝试失败，之后成功 |
| `fail_until_replay` | `true` 时重放前一直失败，死信重放后成功 |
| `effects` | 需要去重的外部副作用名列表，如 `["send-mail"]` |

### 查询状态 / 重放死信

```bash
curl http://127.0.0.1:8000/events/1            # 查询状态
curl -X POST http://127.0.0.1:8000/events/1/replay  # 仅 DEAD_LETTER 可重放
```

### 运行测试

```bash
uv run pytest -q
```

测试覆盖：并发幂等提交、三类拒绝原因区分、退避重试与重试耗尽、
不可恢复错误立即进死信、死信人工重放（副作用不重复）、租约过期接管、
服务重启恢复、状态单调不被迟到/重复消息覆盖，以及 HTTP 层行为。
日志中固定打印 `幂等键 / attempt 尝试次数 / 状态迁移`。

## 语义说明

### 1. 幂等提交

幂等键 = `tenant_id:business_key`，在 `events` 表上有 UNIQUE 约束。
同一租户同一业务键的并发重复提交通过一个 `BEGIN IMMEDIATE` 事务串行化：
**恰好 1 次成功，其余全部以 `DUPLICATE_KEY` 拒绝**，并且指向同一条事件，
不会产生第二条记录、也不会触发第二次处理。

拒绝原因通过 HTTP 状态码与响应体 `reason` 明确区分：

| reason | HTTP | 触发条件 |
| --- | --- | --- |
| `DUPLICATE_KEY` | 409 | 幂等键已存在（重复提交的确定性判定，优先于配额） |
| `QUOTA_EXCEEDED` | 429 | 租户在滑动窗口（默认 60s）内事件数达到配额（默认 100） |
| `INVALID_EVENT` | 422 | 字段校验失败 / 不支持的 `event_type` / 对非死信事件重放 |

### 2. 状态机与单调性

```
ACCEPTED ──claim──▶ PROCESSING ──成功──────────────▶ SUCCEEDED   (终态)
                        │
                        ├──可恢复错误 + 仍有预算──▶ RETRYING ──到期再 claim──▶ PROCESSING
                        │
                        ├──可恢复错误 + 预算耗尽──▶ DEAD_LETTER (终态)
                        └──不可恢复错误──────────▶ DEAD_LETTER (终态)

DEAD_LETTER ──人工 replay（CAS）──▶ ACCEPTED（replayed=1，重新计数 attempts）
```

所有状态推进都是**带条件的单条 UPDATE（CAS）**：
只有 `status='PROCESSING'` 且 `lease_owner=当前消费者` 才能落结果。
因此迟到/重复消息无法把 `SUCCEEDED`/`DEAD_LETTER` 改回中间态，
状态只单调向前，已完成结果永不被覆盖或回滚。

### 3. 异步消费与并发不重复

每个 worker 是一个独立 asyncio 任务（用 `WORKER_CONCURRENCY` 模拟多个处理进程），
也可以用多个 OS 进程打开同一个 SQLite 文件。领取在单个 `BEGIN IMMEDIATE`
事务内完成「挑到期消息 + 写租约」，所以**同一条事件不会被两个消费者同时处理**。

### 4. 退避重试与死信

* 可恢复错误（`TransientError`）：按
  `delay = min(base_delay * multiplier**(attempt-1), max_delay)` 指数退避，
  到 `visible_at` 后才可再次被领取。
* 尝试次数达到 `max_attempts`（默认 3）：进入 `DEAD_LETTER`，
  `error_kind=RETRY_EXHAUSTED`。
* 不可恢复错误（`PermanentError`）：不消耗重试预算，立即 `DEAD_LETTER`，
  `error_kind=PERMANENT`。两类死信原因可区分。

### 5. 死信人工重放与副作用去重

`POST /events/{id}/replay` 仅对 `DEAD_LETTER` 生效（CAS 保证只有一条重放请求成功），
事件回到 `ACCEPTED`、`replayed=1`、重新获得完整尝试预算。

外部副作用通过 `side_effects` 表按 `(idempotency_key, effect_key)` 去重：
处理器执行副作用前先 `record_effect` 登记，只有首次登记真正执行；
因此重试、租约接管、人工重放导致的重复执行都**不会产生第二次副作用**。

### 6. 失败恢复（崩溃 / 重启）

处理中崩溃的事件停留在 `PROCESSING` 并持有租约；租约过期
（`lease_expires_at < now`）后可被任一健康消费者接管，崩溃前那次尝试计入
`attempts`。事件、状态、错误信息、副作用记录全部在 SQLite 中持久化
（WAL 模式），服务重启后不丢失；重启后重复提交仍被拒绝。

### 7. 状态查询不拖慢消费

`GET /events/{id}` 是只读主键点查（WAL 下读写不互斥），不参与领取事务，
不持锁、不影响消费进度；测试在消费/重试期间高频查询验证了两者并行推进。

## 代码结构

```
app/
  models.py       枚举/模型/退避策略
  errors.py       RejectError（三类拒绝原因）、Transient/Permanent 错误
  storage.py      SQLite：事件表、唯一幂等键、原子领取+租约、CAS 推进、副作用去重
  processor.py    领取后处理状态机（成功/重试/死信）与统一日志
  sim_handler.py  按 payload 指令模拟成功/失败与幂等副作用
  service.py      幂等提交、配额、查询、死信重放
  worker.py       异步消费进程 / 进程组
  api.py          FastAPI 路由与应用装配
main.py           uvicorn 入口
tests/            并发幂等/重试/死信/重启/单调/HTTP 测试
```
