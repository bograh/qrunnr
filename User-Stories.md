# TaskForge — User Stories & Acceptance Criteria

**Version:** 1.0  
**Status:** Draft  
**Last updated:** 2026-05-19

---

## Roles

| Role | Description |
|---|---|
| **Service** | A backend application that enqueues tasks via the HTTP API. |
| **Worker** | A stateless process that claims, executes, and acknowledges tasks. |
| **Operator** | An engineer who configures queues and deploys TaskForge. |
| **On-call engineer** | An engineer who monitors and responds to queue failures at runtime. |

---

## Epic 1 — Queue management

### US-01 Create a queue

**As an** operator,  
**I want to** create a named queue with retry and rate-limit configuration,  
**so that** services can start enqueuing tasks to a well-defined channel.

**Acceptance criteria:**

- `POST /queues` with a valid `name`, `max_retries`, and optional `rate_limit` returns `201 Created` with the queue object.
- Queue names are unique; a duplicate name returns `409 Conflict` with a descriptive error message.
- `name` must be 1–64 characters, alphanumeric plus hyphens and underscores; invalid names return `400 Bad Request`.
- `max_retries` must be an integer between 0 and 25; values outside this range return `400`.
- `rate_limit` is optional; when omitted, the queue is unlimited.
- The created queue is immediately available for task enqueuing.

---

### US-02 List queues

**As an** operator,  
**I want to** list all queues with their current configuration,  
**so that** I can audit what queues exist and how they are set up.

**Acceptance criteria:**

- `GET /queues` returns `200 OK` with an array of all queues and their config fields.
- Each queue object includes `id`, `name`, `max_retries`, `rate_limit`, and `created_at`.
- An empty system returns `200` with an empty array, not a `404`.

---

### US-03 Delete a queue

**As an** operator,  
**I want to** delete a queue that is no longer needed,  
**so that** the system stays clean and services are not enqueuing to stale queues.

**Acceptance criteria:**

- `DELETE /queues/:name` on an empty queue returns `204 No Content`.
- Deleting a queue with tasks in `pending`, `claimed`, or `executing` status returns `409 Conflict` with a message indicating tasks must be drained first.
- Deleting a non-existent queue returns `404 Not Found`.
- A successfully deleted queue is no longer returned by `GET /queues`.

---

## Epic 2 — Task enqueuing

### US-04 Enqueue an immediate task

**As a** service,  
**I want to** enqueue a task with a JSON payload for immediate execution,  
**so that** a worker picks it up as soon as one is available.

**Acceptance criteria:**

- `POST /queues/:name/tasks` with a valid payload returns `201 Created` with the task object including a unique `id`.
- The task is created with `status: pending` and `scheduled_at` set to the current time.
- `payload` must be a valid JSON object; non-object values return `400`.
- Enqueueing to a non-existent queue returns `404 Not Found`.
- The task is claimable by a worker within 1 second of creation under normal load.

---

### US-05 Enqueue a delayed task

**As a** service,  
**I want to** enqueue a task with a future `scheduled_at` timestamp,  
**so that** it is not dispatched before that time.

**Acceptance criteria:**

- A task enqueued with a `scheduled_at` in the future is created with `status: pending` but is not dispatched until `now() >= scheduled_at`.
- A task with `scheduled_at` in the past is treated as immediately dispatchable.
- `scheduled_at` must be a valid ISO 8601 timestamp; invalid values return `400`.
- The task appears in `GET /queues/:name/tasks/:id` with the correct `scheduled_at` value.

---

### US-06 Enqueue a high-priority task

**As a** service,  
**I want to** assign a priority to a task,  
**so that** urgent work is dispatched before lower-priority items in the same queue.

**Acceptance criteria:**

- A task enqueued with `priority: 1` is dispatched before a task enqueued with `priority: 10` when both are pending in the same queue.
- `priority` must be an integer from 1 to 10; values outside this range return `400`.
- When no priority is specified, it defaults to `5`.
- Priority ordering is best-effort under concurrent dispatch; it is not guaranteed to be strictly ordered across all workers.

---

### US-07 Cancel a pending task

**As a** service,  
**I want to** cancel a task that has not yet been claimed,  
**so that** I can abort work that is no longer needed.

**Acceptance criteria:**

- `DELETE /queues/:name/tasks/:id` on a task with `status: pending` returns `204 No Content` and sets the task status to `cancelled`.
- Cancelling a task with `status: claimed` or `executing` returns `409 Conflict`; the task cannot be cancelled mid-flight.
- Cancelling a task with `status: done` or `dead` returns `409 Conflict`.
- Cancelling a non-existent task returns `404 Not Found`.
- A cancelled task is not dispatched to any worker.

---

## Epic 3 — Task execution (worker protocol)

### US-08 Worker claims and executes a task

**As a** worker,  
**I want to** receive a task to execute,  
**so that** I can process the payload and report the result.

**Acceptance criteria:**

- The dispatcher atomically claims one task per worker request; two workers requesting simultaneously never receive the same task.
- The claimed task transitions from `pending` to `claimed` to `executing`; these state changes are persisted to Postgres before the task is returned to the worker.
- The task response includes `id`, `queue`, `payload`, `priority`, `attempts`, and a `lease_expires_at` timestamp.
- If no tasks are available, the worker receives a `204 No Content` response.

---

### US-09 Worker sends heartbeats

**As a** worker,  
**I want to** periodically signal that I am still alive while executing a task,  
**so that** the system does not prematurely reclaim my task as stuck.

**Acceptance criteria:**

- `POST /workers/heartbeat` with a valid `task_id` and `worker_id` returns `200 OK` and updates `heartbeat_at` on the task.
- A heartbeat on a task the worker does not own (i.e. `claimed_by` does not match) returns `403 Forbidden`.
- A heartbeat on a task with `status` other than `executing` returns `409 Conflict`.
- A heartbeat on a non-existent task returns `404 Not Found`.

---

### US-10 Worker acknowledges success

**As a** worker,  
**I want to** report that I completed a task successfully,  
**so that** the task is marked done and any optional result is stored.

**Acceptance criteria:**

- `POST /workers/ack` with `task_id`, `worker_id`, and optional `result` (JSON object) returns `200 OK`.
- The task transitions to `status: done` and `result` is stored.
- Acknowledging a task the worker does not own returns `403 Forbidden`.
- Acknowledging a task that has been reclaimed by the watchdog returns `409 Conflict` with a message indicating the lease expired.
- A done task is never re-dispatched.

---

### US-11 Worker reports failure

**As a** worker,  
**I want to** report that a task failed with an error message,  
**so that** the system can schedule a retry or move the task to the dead-letter queue.

**Acceptance criteria:**

- `POST /workers/nack` with `task_id`, `worker_id`, and `error` (string) returns `200 OK`.
- If `attempts < max_retries`, the task transitions to `retry`, `attempts` is incremented, `last_error` is updated, and `scheduled_at` is set to `now() + backoff_delay`.
- If `attempts >= max_retries`, the task is moved to the dead-letter queue and removed from the active tasks table.
- Nacking a task the worker does not own returns `403 Forbidden`.
- The `error` field is required; an empty string returns `400`.

---

## Epic 4 — Retry and backoff

### US-12 Exponential backoff on retry

**As a** worker,  
**I want** failed tasks to be rescheduled with increasing delays,  
**so that** transient failures do not cause an immediate retry storm.

**Acceptance criteria:**

- Retry delay for attempt `n` follows: `delay_ms = min(base_ms × 2^n + rand(0, base_ms), max_delay_ms)`.
- Default `base_ms` is 1000; default `max_delay_ms` is 3,600,000 (1 hour).
- `scheduled_at` on a retried task is set to `now() + delay_ms`.
- The delay formula is applied consistently; two tasks with the same attempt count receive delays within the jitter range of each other, not identical delays.
- Retry delays are visible in the task's `scheduled_at` field via `GET /queues/:name/tasks/:id`.

---

### US-13 Dead-letter promotion

**As an** on-call engineer,  
**I want** tasks that exceed their retry budget to be moved to a dead-letter queue,  
**so that** I can inspect them and decide whether to replay or discard them.

**Acceptance criteria:**

- When `attempts` reaches `max_retries` and the worker nacks, the task is inserted into `dead_letter_tasks` and removed from `tasks`.
- The DLQ entry preserves `id`, `queue_id`, `payload`, `last_error`, `attempts`, and `died_at`.
- The task no longer appears in `GET /queues/:name/tasks/:id` (returns `404`).
- The task appears in `GET /queues/:name/dlq`.

---

## Epic 5 — Stuck worker detection

### US-14 Reclaim timed-out tasks

**As an** operator,  
**I want** tasks from crashed workers to be automatically reclaimed,  
**so that** work is never permanently lost due to a worker dying mid-execution.

**Acceptance criteria:**

- The watchdog runs on a configurable interval (default: every 15 seconds).
- Any task with `status: executing` and `heartbeat_at < now() - timeout_threshold` is reclaimed to `retry` status.
- Default `timeout_threshold` is 30 seconds; it is configurable at deployment time.
- `attempts` is incremented when the task is reclaimed; the reclaim counts as a failed attempt.
- `last_error` is set to `"watchdog: heartbeat expired"` on reclaim.
- If the original worker later calls `ack` or `nack` on a reclaimed task, it receives `409 Conflict`.

---

## Epic 6 — Rate limiting

### US-15 Enforce per-queue rate limits

**As an** operator,  
**I want to** set a maximum dispatch rate on a queue,  
**so that** downstream services are not overwhelmed by task execution.

**Acceptance criteria:**

- A queue with `rate_limit: N` dispatches at most `N` tasks per second to workers.
- Rate limiting is enforced atomically via a Redis token bucket; concurrent dispatchers do not exceed the limit collectively.
- Tasks are not dropped when the rate limit is hit — they remain `pending` and are dispatched in the next available window.
- A queue with no `rate_limit` set has no dispatch ceiling.
- Rate limit changes via `PATCH /queues/:name` take effect within one scheduler tick (≤ 1 second).

---

## Epic 7 — Observability and admin

### US-16 Inspect a task

**As an** on-call engineer,  
**I want to** look up any task by ID and see its full history,  
**so that** I can diagnose why it failed or understand its current state.

**Acceptance criteria:**

- `GET /queues/:name/tasks/:id` returns the full task object including `status`, `attempts`, `last_error`, `claimed_by`, `heartbeat_at`, `scheduled_at`, and `result`.
- The response is available for tasks in all statuses including `done` and `dead`.
- A non-existent task returns `404 Not Found`.
- Tasks moved to the DLQ are no longer accessible via this endpoint; they must be accessed via the DLQ endpoint.

---

### US-17 View queue stats

**As an** on-call engineer,  
**I want to** see real-time metrics for a queue,  
**so that** I can assess its health and identify backlogs or failure spikes.

**Acceptance criteria:**

- `GET /queues/:name/stats` returns: `depth` (pending count), `executing_count`, `done_last_minute`, `failed_last_minute`, `dlq_depth`, and `oldest_pending_age_seconds`.
- Stats are accurate to within 5 seconds.
- The admin dashboard polls this endpoint and renders the values in real time via server-sent events.
- A queue with no activity returns all zero values, not an error.

---

### US-18 Replay a dead-letter task

**As an** on-call engineer,  
**I want to** re-enqueue a task from the dead-letter queue,  
**so that** I can retry work that failed due to a transient infrastructure issue.

**Acceptance criteria:**

- `POST /queues/:name/dlq/:id/replay` creates a new task in `pending` status with the same `payload` and a fresh `attempts: 0`.
- The DLQ entry is updated with `replayed_at` set to the current time but is not deleted.
- Replaying an already-replayed task is allowed; `replayed_at` is overwritten.
- Replaying a non-existent DLQ entry returns `404 Not Found`.
- The replayed task is assigned a new `id` and is not linked to the original.

---

### US-19 Discard a dead-letter task

**As an** on-call engineer,  
**I want to** permanently delete a task from the dead-letter queue,  
**so that** I can discard work that I know should not be retried.

**Acceptance criteria:**

- `DELETE /queues/:name/dlq/:id` removes the DLQ entry and returns `204 No Content`.
- Discarded tasks are not recoverable.
- Deleting a non-existent DLQ entry returns `404 Not Found`.
- The deletion is reflected immediately in `GET /queues/:name/stats` (`dlq_depth` decrements).

---

### US-20 Monitor active workers

**As an** on-call engineer,  
**I want to** see which workers are active and what tasks they are currently executing,  
**so that** I can identify workers that are stuck or failing to heartbeat.

**Acceptance criteria:**

- The admin dashboard's worker panel lists all workers seen in the last 60 seconds, identified by `worker_id`.
- Each worker entry shows: `worker_id`, `current_task_id`, `last_heartbeat_at`, and a `stale` indicator when `last_heartbeat_at > 20 seconds ago`.
- Workers not seen in 60 seconds are automatically removed from the panel.
- The panel updates in real time without requiring a page refresh.

---

## Appendix — Story map

| Epic | Stories | Priority |
|---|---|---|
| Queue management | US-01, US-02, US-03 | Must have |
| Task enqueuing | US-04, US-05, US-06, US-07 | Must have |
| Worker protocol | US-08, US-09, US-10, US-11 | Must have |
| Retry and backoff | US-12, US-13 | Must have |
| Stuck worker detection | US-14 | Must have |
| Rate limiting | US-15 | Should have |
| Observability and admin | US-16, US-17, US-18, US-19, US-20 | Should have |
