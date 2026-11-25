# qrunnr — Product Requirements Document

**Version:** 1.0  
**Status:** Draft  
**Author:** Bernard Ograh
**Last updated:** 2026-11-25

---

## 1. Overview

qrunnr is a self-hosted distributed task queue service inspired by Google Cloud Tasks. It provides a durable, observable, and configurable system for background job execution across distributed services. It exposes an HTTP API for enqueuing tasks, manages retries and scheduling, and routes work to a pool of stateless workers.

The primary goal is a production-grade internal tool that a small engineering team can deploy, operate, and extend without depending on a managed cloud service.

---

## 2. Problem statement

As a backend system scales across multiple services, the need for reliable asynchronous work execution increases. Common pain points without a centralised task queue:

- Work is lost when a service crashes mid-execution.
- Retry logic is duplicated across codebases, inconsistently.
- There is no observability into what jobs are running, failing, or stuck.
- Rate-limiting against downstream services is handled ad hoc.
- Developers have no way to inspect, cancel, or replay failed jobs.

qrunnr solves all of the above in a single deployable service.

---

## 3. Goals

- Provide a reliable, at-least-once delivery guarantee for all enqueued tasks.
- Support configurable retry policies with exponential backoff and jitter.
- Handle delayed/scheduled task delivery with second-level precision.
- Detect and reclaim tasks from crashed or stuck workers via heartbeat expiry.
- Enforce per-queue rate limits to protect downstream systems.
- Expose a real-time admin dashboard for queue health and task inspection.
- Be deployable via Docker Compose with Postgres and Redis as the only dependencies.

## 4. Non-goals

- qrunnr is not a message broker (no pub/sub fan-out, no topic subscriptions).
- It does not execute tasks itself — workers are separate processes owned by the caller.
- It does not provide a managed SaaS offering or multi-tenancy.
- It does not guarantee exactly-once delivery in the initial version.
- It does not support task dependencies or DAG-style workflows in v1.

---

## 5. Users

| User | Description |
|---|---|
| Backend engineer | Enqueues tasks via the HTTP API from application services. |
| Platform/infra engineer | Deploys and operates qrunnr; configures queues and rate limits. |
| On-call engineer | Uses the admin dashboard to inspect failures and replay DLQ tasks. |

---

## 6. Architecture

### 6.1 Components

**API server** — Exposes the REST API for queue management, task enqueuing, inspection, and cancellation. Stateless; horizontally scalable.

**Dispatch** — Workers claim work with `POST /workers/claim`. The claim atomically selects the highest-priority due task (`status IN ('pending','retry') AND scheduled_at <= now()`, ordered by priority then `scheduled_at`) with `SELECT … FOR UPDATE SKIP LOCKED`, checks the queue's rate limit, and marks the task `executing` in one transaction. Delayed and retrying tasks become claimable as soon as their `scheduled_at` passes, so no separate promotion loop is needed.

> **Design decision (2026-11):** The original design staged tasks in a Redis sorted-set buffer fed by a dispatcher and scheduler loop. v1 claims directly from Postgres instead. `SKIP LOCKED` already gives exclusive, priority-ordered claims across any number of API instances, and a second copy of task state in Redis would need dual writes and reconciliation without improving any acceptance criterion. Redis is used only for rate limiting. Revisit if claim throughput on Postgres becomes a bottleneck.

**Worker pool** — Stateless worker processes external to qrunnr that poll the dispatch API, execute payloads, and report results via `ack`/`nack`.

**Heartbeat watchdog** — Background loop that identifies tasks whose `heartbeat_at` has not been updated within the timeout window and reclaims them.

**Admin dashboard** — Web UI served by the API server. Shows live queue stats, task history, and a dead-letter queue browser with replay controls.

### 6.2 Storage

| Store | Role |
|---|---|
| Postgres | Durable source of truth for all tasks, queues, and DLQ entries. |
| Redis | Per-queue rate-limit token buckets (atomic Lua script). Optional: without it, limits are enforced per API instance. |

### 6.3 Task lifecycle

```
pending → claimed → executing → done
                             ↘ retry → pending (loop)
                                     ↘ dead-letter (max retries exceeded)
                             ↘ timeout → retry
```

A task is created as `pending`. A worker's claim takes it atomically (via `SELECT FOR UPDATE SKIP LOCKED`). The worker begins execution and sends heartbeats. On success, it calls `ack`; on failure, `nack`. Failed tasks are rescheduled with exponential backoff until `max_retries` is exceeded, at which point they are moved to the dead-letter queue.

---

## 7. Data model

### `queues`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | Primary key |
| `name` | TEXT | Unique, human-readable identifier |
| `max_retries` | INT | Default: 3 |
| `rate_limit` | INT | Max tasks dispatched per second; null = unlimited |
| `created_at` | TIMESTAMPTZ | — |

### `tasks`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | Primary key |
| `queue_id` | UUID | FK → queues |
| `payload` | JSONB | Arbitrary caller-defined data |
| `status` | TEXT | `pending`, `claimed`, `executing`, `done`, `retry`, `dead` |
| `priority` | INT | 1 (highest) to 10 (lowest); default 5 |
| `scheduled_at` | TIMESTAMPTZ | Earliest dispatch time |
| `claimed_at` | TIMESTAMPTZ | When dispatcher last claimed |
| `claimed_by` | TEXT | Worker ID |
| `heartbeat_at` | TIMESTAMPTZ | Last heartbeat from worker |
| `attempts` | INT | Number of execution attempts so far |
| `last_error` | TEXT | Error message from most recent failure |
| `result` | JSONB | Worker-supplied result on success |
| `created_at` | TIMESTAMPTZ | — |

Indexed on `(queue_id, status, priority, scheduled_at)`.

Implementation additions: `completed_at` and `last_failed_at` (used for throughput and error-rate stats).

### `dead_letter_tasks`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | Matches original task ID |
| `queue_id` | UUID | — |
| `payload` | JSONB | — |
| `last_error` | TEXT | — |
| `attempts` | INT | — |
| `died_at` | TIMESTAMPTZ | — |
| `replayed_at` | TIMESTAMPTZ | Set when manually re-enqueued |
| `replay_count` | INT | Number of times the entry was replayed |

### `workers`

Last time each worker was seen (claim, heartbeat, ack, nack, or idle poll) and its current task. Shared by all API instances.

### `task_events`

Append-only history of status transitions and heartbeats per task, shown in the dashboard's task inspector. It survives dead-lettering and DLQ discard and is removed when the queue is deleted.

---

## 8. API

### Queues

```
POST   /queues              Create a queue
GET    /queues              List all queues
DELETE /queues/:name        Delete a queue (must be empty)
```

### Tasks

```
POST   /queues/:name/tasks          Enqueue a task
GET    /queues/:name/tasks/:id      Inspect a task
DELETE /queues/:name/tasks/:id      Cancel a pending task
GET    /queues/:name/stats          Queue depth, throughput, error rate
```

**Enqueue request body:**

```json
{
  "payload": { "user_id": "abc123", "action": "send_welcome_email" },
  "priority": 3,
  "scheduled_at": "2026-05-19T15:00:00Z"
}
```

### Worker protocol

```
POST   /workers/claim               Claim the next task (204 when none)
GET    /workers                     Workers seen in the last 60s
POST   /workers/heartbeat           Extend execution lease (every 10s)
POST   /workers/ack                 Report success + optional result
POST   /workers/nack                Report failure + error message
```

### Dead-letter queue

```
GET    /queues/:name/dlq            List dead-letter tasks
POST   /queues/:name/dlq/:id/replay Re-enqueue a dead task
DELETE /queues/:name/dlq/:id        Discard a dead task permanently
```

---

## 9. Retry policy

Backoff delay for attempt `n` is calculated as:

```
delay_ms = min(base_ms * 2^n + rand(0, base_ms), max_delay_ms)
```

Default values: `base_ms = 1000`, `max_delay_ms = 3_600_000` (1 hour).

Jitter (the `rand` term) is added to prevent thundering herd — without it, all retrying tasks would attempt re-execution at the same moment.

After `max_retries` attempts, the task is moved to the dead-letter queue and will not be retried automatically.

---

## 10. Rate limiting

Rate limits are enforced per queue at the dispatcher level using a Redis token bucket. A Lua script atomically checks and decrements the bucket on each dispatch decision, ensuring no race conditions under concurrent dispatchers.

Workers are never aware of rate limiting — it is transparent at the dispatch layer.

---

## 11. Heartbeat and timeout

Workers must send a `POST /workers/heartbeat` with their task ID every 10 seconds while executing. The watchdog loop checks every 15 seconds for tasks where:

```sql
status = 'executing' AND heartbeat_at < now() - interval '30 seconds'
```

These tasks are returned to `retry` status and rescheduled with the normal backoff; the reclaim counts as an attempt. A task that has already used its retry budget is moved to the dead-letter queue instead. The worker that was executing them will receive a rejection if it later attempts to `ack` or `nack` a task it no longer owns.

Timeout threshold is configurable per deployment (default: 30 seconds).

---

## 12. Admin dashboard

The dashboard is served by the API server at `/admin` and requires no separate deployment. It is built with templ, htmx and Tailwind CSS. It can be protected with HTTP basic auth (`ADMIN_USERNAME`/`ADMIN_PASSWORD`).

**Queue overview panel:** Shows all queues with live metrics — depth, tasks/sec, error rate, oldest pending task age. Updates via server-sent events.

**Task inspector:** Search or filter tasks by queue, status, worker ID, or time range. View full payload, all status transitions, heartbeat history, and error messages.

**Dead-letter browser:** Lists DLQ tasks with filter by queue and time. Supports single and bulk replay. Shows how many times a task has been replayed.

**Worker status:** Lists active workers by ID, last heartbeat, and current task. Highlights workers that have not sent a heartbeat recently.

---

## 13. Implementation milestones

| Milestone | Description | Estimated effort |
|---|---|---|
| 1 — Naive queue | Postgres-only, single dispatcher, basic ack/nack | 1–2 days |
| 2 — Retry engine | Attempt counter, backoff formula, DLQ promotion | 1 day |
| 3 — Scheduler loop | Delayed task delivery, two-tier Redis buffer | 1 day |
| 4 — Heartbeat watchdog | Worker crash detection, task reclaim | 1 day |
| 5 — Priority lanes | Redis sorted set dispatch by priority score | 1 day |
| 6 — Rate limiting | Per-queue token bucket via Redis Lua script | 1 day |
| 7 — Admin dashboard | SSE metrics stream, task inspector, DLQ browser | 2 days |

All milestones are complete. Milestones 3 and 5 were delivered with Postgres claims instead of a Redis buffer (see §6.1).

Total estimated build time: **8–10 days** for a working v1.

---

## 14. Observability

The API server exposes a `/metrics` endpoint in Prometheus format with the following metrics:

- `qrunnr_tasks_enqueued_total` — counter by queue
- `qrunnr_tasks_completed_total` — counter by queue and status
- `qrunnr_task_execution_duration_seconds` — histogram by queue
- `qrunnr_queue_depth` — gauge by queue and status
- `qrunnr_dlq_depth` — gauge by queue
- `qrunnr_worker_heartbeat_age_seconds` — gauge by worker ID

Structured JSON logs are emitted for every state transition with `task_id`, `queue`, `worker_id`, `from_status`, `to_status`, and `duration_ms`.

---

## 15. Deployment

Minimum viable deployment via Docker Compose:

```yaml
services:
  api:
    build: .
    ports: ["8080:8080"]
    environment:
      DATABASE_URL: postgres://...
      REDIS_URL: redis://redis:6379
    depends_on: [postgres, redis]
  
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: qrunnr
  
  redis:
    image: redis:7-alpine
```

Workers are separate processes deployed by the caller and configured to point at `QRUNNR_API_URL`.

---

## 16. Open questions

- Should task payloads be encrypted at rest in Postgres for sensitive use cases?
- Is a webhook delivery mode (qrunnr pushes to a URL rather than workers polling) in scope for v2?
- Should queue configuration support per-task-type retry overrides, or only queue-level defaults?
- What is the retention policy for completed tasks? Should there be an archival or TTL mechanism?

---

## 17. Out of scope (v2 candidates)

- Exactly-once delivery semantics
- Task dependency graphs (fan-in/fan-out DAGs)
- Webhook push delivery mode
- Multi-tenant / team isolation
- Task result streaming (currently result is only available after `ack`)
