# TaskForge

TaskForge is a self-hosted distributed task queue service. It is being built around a REST API for queue configuration, task enqueueing, worker leasing, retries, dead-letter queues, and operational inspection.

## Current Status

The core API handlers, router, and in-memory stores are implemented and covered by tests. The `cmd/taskforge` entrypoint is not yet wired to start an HTTP server, and the Postgres/Redis stores are still mostly stubs. Docker Compose provisions Postgres and Redis for the intended architecture, but the current tested runtime path is the in-memory implementation used by `internal/api/router.NewRouter`.

Implemented API surface:

| Area | Endpoints |
| --- | --- |
| Queues | `POST /queues`, `GET /queues`, `PATCH /queues/{name}`, `DELETE /queues/{name}` |
| Tasks | `POST /queues/{name}/tasks`, `GET /queues/{name}/tasks/{id}`, `DELETE /queues/{name}/tasks/{id}` |
| Workers | `POST /workers/claim`, `POST /workers/heartbeat`, `POST /workers/ack`, `POST /workers/nack`, `GET /workers` |
| DLQ | `GET /queues/{name}/dlq`, `POST /queues/{name}/dlq/{id}/replay`, `DELETE /queues/{name}/dlq/{id}` |
| Stats | `GET /queues/{name}/stats` |

Known gaps:

- `cmd/taskforge` does not start the API server yet.
- Durable Postgres persistence is not wired into the API.
- Redis-backed cross-process rate limiting is not implemented; rate limiting is currently atomic within the in-memory store only.
- The admin dashboard UI is not present yet.

## Requirements

- Go 1.25.7 or newer, matching `go.mod`
- Docker and Docker Compose, only needed for the planned Postgres/Redis stack

## Development

Run the test suite:

```sh
make test
```

Build the API binary:

```sh
make build
```

Run the current command entrypoint:

```sh
make run
```

At the moment, `make run` prints a startup message only. It does not bind an HTTP port.

Start the supporting Docker services:

```sh
make up
```

Stop them:

```sh
make down
```

## API Examples

Create a queue:

```http
POST /queues
Content-Type: application/json

{
  "name": "email_jobs",
  "max_retries": 5,
  "rate_limit": 10
}
```

Enqueue an immediate task:

```http
POST /queues/email_jobs/tasks
Content-Type: application/json

{
  "payload": {
    "user_id": "abc123",
    "action": "send_welcome_email"
  },
  "priority": 5
}
```

Enqueue a delayed task:

```http
POST /queues/email_jobs/tasks
Content-Type: application/json

{
  "payload": {
    "user_id": "abc123",
    "action": "send_welcome_email"
  },
  "scheduled_at": "2026-05-20T12:00:00Z",
  "priority": 1
}
```

Claim work as a worker:

```http
POST /workers/claim
Content-Type: application/json

{
  "worker_id": "worker-1",
  "queue": "email_jobs"
}
```

Report success:

```http
POST /workers/ack
Content-Type: application/json

{
  "task_id": "task-id",
  "worker_id": "worker-1",
  "result": {
    "ok": true
  }
}
```

Report failure:

```http
POST /workers/nack
Content-Type: application/json

{
  "task_id": "task-id",
  "worker_id": "worker-1",
  "error": "downstream timeout"
}
```

## Behavior Notes

- Queue names must be 1-64 characters and may contain letters, numbers, hyphens, and underscores.
- `max_retries` must be between `0` and `25`.
- Task payloads and ack results must be JSON objects.
- Task priority ranges from `1` highest to `10` lowest and defaults to `5`.
- Retry backoff uses `min(base_ms * 2^attempt + rand(0, base_ms), max_delay_ms)` with a 1 second base and 1 hour max.
- The watchdog default interval is 15 seconds, with a 30 second heartbeat timeout.

## Repository Layout

```text
cmd/                  command entrypoints
internal/api/         HTTP router, handlers, and middleware
internal/model/       domain models and validation helpers
internal/storage/     in-memory, Postgres, and Redis storage packages
internal/scheduler/   retry and watchdog scheduling logic
migrations/           Postgres schema initialization
```
