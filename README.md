# qrunnr

qrunnr (pronounced "queue runner") is a self-hosted distributed task queue. Services enqueue JSON tasks over a REST API; stateless workers claim them, send heartbeats, and report success or failure. qrunnr handles priorities, delayed delivery, retries with exponential backoff, dead-lettering, per-queue rate limits, crashed-worker recovery, metrics, and an operator dashboard.

## Status

The v1 scope in [PRD.md](PRD.md) and [User-Stories.md](User-Stories.md) is implemented:

| Area | What exists |
| --- | --- |
| REST API | Queues, tasks, worker protocol, dead-letter queue, stats, active workers, health checks |
| Storage | Postgres (durable, multi-instance) with automatic migrations; in-memory store for local development |
| Dispatch | Atomic claims via `SELECT … FOR UPDATE SKIP LOCKED`, ordered by priority then schedule |
| Retries | `min(base·2ⁿ + rand(0, base), max)` backoff, dead-lettering when `max_retries` is exhausted |
| Watchdog | Reclaims tasks whose heartbeat expired; a reclaim counts as a failed attempt |
| Rate limiting | Per-queue token bucket; atomic Redis Lua script shared by all API instances, in-process fallback without Redis |
| Observability | Prometheus `/metrics`, JSON logs for every request and every task state transition, per-task history |
| Admin dashboard | `/admin`: live queue health (SSE), task inspector, task history, DLQ browser with bulk replay/discard, worker status |

## Quick start

```sh
make up            # API + Postgres + Redis via Docker Compose
open http://localhost:8080/admin/
```

Create a queue, enqueue work, and run a worker:

```sh
curl -X POST localhost:8080/queues -d '{"name":"email_jobs","max_retries":5,"rate_limit":10}'
curl -X POST localhost:8080/queues/email_jobs/tasks -d '{"payload":{"user_id":"abc123"},"priority":3}'
QRUNNR_API_URL=http://localhost:8080 WORKER_ID=worker-1 make worker
```

Without `DATABASE_URL`, `make run` starts the API with in-memory storage. Data is lost on restart.

## Configuration

API server (`cmd/qrunnr`):

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8080` | HTTP port |
| `DATABASE_URL` | empty | Postgres URL. Empty uses in-memory storage. Migrations run on startup. |
| `REDIS_URL` | empty | Redis URL for cluster-wide rate limits. Empty enforces limits per instance. |
| `HEARTBEAT_TIMEOUT` | `30s` | Executing tasks without a heartbeat for this long are reclaimed. Also the claim lease length. |
| `WATCHDOG_INTERVAL` | `15s` | How often the watchdog sweeps for expired tasks |
| `RETRY_BASE_DELAY` | `1s` | Backoff base |
| `RETRY_MAX_DELAY` | `1h` | Backoff cap |
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | empty | Set both to protect `/admin` with HTTP basic auth |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |

Invalid durations or a lone `ADMIN_USERNAME`/`ADMIN_PASSWORD` stop startup with an error.

Reference worker (`cmd/worker`):

| Variable | Default | Description |
| --- | --- | --- |
| `QRUNNR_API_URL` | `http://localhost:8080` | API base URL |
| `WORKER_ID` | host name | Worker identity sent to qrunnr |
| `QUEUE` | empty | Optional queue filter |
| `WORKER_POLL_INTERVAL` | `1s` | Delay between claim attempts |
| `HEARTBEAT_INTERVAL` | `10s` | Heartbeat cadence while executing |
| `WORKER_REQUEST_TIMEOUT` | `15s` | HTTP client timeout |

The reference worker treats payloads as JSON objects. It succeeds by default, sleeps when `sleep_ms` is present, and nacks when `fail: true` is present.

## API

| Area | Endpoints |
| --- | --- |
| Queues | `POST /queues`, `GET /queues`, `PATCH /queues/{name}`, `DELETE /queues/{name}` |
| Tasks | `POST /queues/{name}/tasks`, `GET /queues/{name}/tasks/{id}`, `DELETE /queues/{name}/tasks/{id}` |
| Workers | `POST /workers/claim`, `POST /workers/heartbeat`, `POST /workers/ack`, `POST /workers/nack`, `GET /workers` |
| DLQ | `GET /queues/{name}/dlq`, `POST /queues/{name}/dlq/{id}/replay`, `DELETE /queues/{name}/dlq/{id}` |
| Stats | `GET /queues/{name}/stats` |
| Operations | `GET /healthz`, `GET /readyz`, `GET /metrics`, `/admin/` |

A Postman collection is in [postman_collection.json](postman_collection.json).

Enqueue a delayed task:

```http
POST /queues/email_jobs/tasks
Content-Type: application/json

{
  "payload": { "user_id": "abc123", "action": "send_welcome_email" },
  "scheduled_at": "2026-05-20T12:00:00Z",
  "priority": 1
}
```

Claim work, then ack or nack it:

```http
POST /workers/claim
{ "worker_id": "worker-1", "queue": "email_jobs" }

POST /workers/ack
{ "task_id": "…", "worker_id": "worker-1", "result": { "ok": true } }

POST /workers/nack
{ "task_id": "…", "worker_id": "worker-1", "error": "downstream timeout" }
```

`POST /workers/claim` returns `204` when nothing is available, and `404` if the named queue does not exist.

## Behaviour notes

- Queue names are 1–64 characters: letters, numbers, `-` and `_`. `max_retries` is 0–25.
- Payloads and ack results must be JSON objects. Priority is `1` (highest) to `10`, default `5`.
- Tasks in `retry` are claimable again once their backoff elapses. A task whose attempts reach `max_retries` is moved to the DLQ on its next failure. This applies to both nacks and watchdog reclaims.
- When one queue's rate limit is exhausted, a worker that is not bound to a queue is given work from other queues instead of waiting.
- A worker counts as active if it was seen in the last 60s, including idle polls. It is stale after 20s without contact.
- Deleting a queue requires no `pending`, `claimed`, `executing`, or `retry` tasks. Its finished tasks, dead letters, and history are removed with it.
- Stats: `done_last_minute` and `failed_last_minute` count tasks that completed or failed in the last 60 seconds. Dead-lettered tasks count as failed.

## Observability

`/metrics` exposes, in Prometheus format:

| Metric | Type | Labels |
| --- | --- | --- |
| `qrunnr_tasks_enqueued_total` | counter | `queue` |
| `qrunnr_tasks_completed_total` | counter | `queue`, `status` (`done`, `retry`, `dead`) |
| `qrunnr_task_execution_duration_seconds` | histogram | `queue` |
| `qrunnr_queue_depth` | gauge | `queue`, `status` |
| `qrunnr_dlq_depth` | gauge | `queue` |
| `qrunnr_worker_heartbeat_age_seconds` | gauge | `worker_id` |

Gauges are read from the database at scrape time, so every instance reports the same values. Counters and the histogram are per instance; sum them across instances.

Every state transition is logged as JSON with `task_id`, `queue`, `worker_id`, `from_status`, `to_status`, and `duration_ms`. Transitions and heartbeats are also stored in `task_events`, which backs the dashboard's task history.

## Admin dashboard

Served at `/admin/` by the API server. It is built with [templ](https://templ.guide), [htmx](https://htmx.org) and Tailwind CSS. The generated `*_templ.go` files, the compiled `app.css`, and the vendored htmx scripts are committed, so `go build` needs neither the templ CLI nor Node.

- **Overview:** opens with a one-sentence health verdict, then live queue depth, throughput, error rate, oldest pending age, DLQ depth, and worker counts, pushed over server-sent events every 2s. Queues needing attention sort first. You can also create queues, change rate limits, and delete queues here.
- **Tasks:** filter by queue, status, worker, task ID, and creation time. Each task page shows the payload, result, last error, full status history and heartbeats, and lets you cancel pending tasks.
- **Dead letters:** filter by queue and time, then replay or discard tasks one at a time or in bulk. Shows replay counts.
- **Workers:** live list with the current task and last heartbeat. Stale workers are highlighted.

Dashboard actions only accept requests sent by htmx (they must carry the `HX-Request` header), which blocks cross-site form posts. Set `ADMIN_USERNAME` and `ADMIN_PASSWORD` before exposing the dashboard. The REST API itself has no authentication; keep it on a private network.

The visual system (tokens, type, component rules) is documented in [DESIGN.md](DESIGN.md). After editing `.templ` files or Tailwind classes, run `make generate`.

## Development

```sh
make test               # unit tests; Postgres/Redis tests skip themselves
make test-integration   # all tests against disposable Postgres + Redis (Docker)
make generate           # templ + Tailwind
make build              # bin/qrunnr and bin/worker
```

`make test-integration` runs:

- the shared storage contract suite (`internal/storage/storetest`) against both the in-memory and the Postgres store,
- the Redis rate limiter tests,
- an end-to-end test (`test/integration`) that drives the real API and the reference worker binary through retries, a crashed-worker reclaim, dead-lettering, replay, metrics, and the dashboard.

## Repository layout

```text
cmd/qrunnr/          API server entrypoint
cmd/worker/             reference worker
internal/app/           wiring: storage, observability, dashboard, router, watchdog
internal/api/           HTTP router, handlers, middleware
internal/admin/         dashboard handlers; views/ holds templ components
internal/model/         domain types, validation, retry policy
internal/observability/ JSON logging, transition logs, Prometheus metrics
internal/ratelimit/     token bucket interface and in-process limiter
internal/scheduler/     heartbeat watchdog
internal/storage/       memory, postgres, redis, and the shared storetest suite
migrations/             embedded Postgres migrations, applied at startup
web/admin/              dashboard static assets and Tailwind source
test/integration/       end-to-end tests
```
