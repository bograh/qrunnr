-- Timestamps used for throughput/error-rate stats.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_failed_at TIMESTAMPTZ;

ALTER TABLE dead_letter_tasks ADD COLUMN IF NOT EXISTS replay_count INT NOT NULL DEFAULT 0;

-- Claim path: due tasks ordered by priority then schedule.
CREATE INDEX IF NOT EXISTS tasks_claimable_idx
  ON tasks (priority, scheduled_at) WHERE status IN ('pending', 'retry');
-- Watchdog scan.
CREATE INDEX IF NOT EXISTS tasks_executing_heartbeat_idx
  ON tasks (heartbeat_at) WHERE status = 'executing';
-- Task inspector filters.
CREATE INDEX IF NOT EXISTS tasks_claimed_by_idx ON tasks (claimed_by);
CREATE INDEX IF NOT EXISTS tasks_created_at_idx ON tasks (created_at);
CREATE INDEX IF NOT EXISTS dead_letter_tasks_queue_died_idx ON dead_letter_tasks (queue_id, died_at);

-- Workers seen by any API instance.
CREATE TABLE IF NOT EXISTS workers (
  worker_id       TEXT PRIMARY KEY,
  current_task_id UUID,
  last_seen_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS workers_last_seen_at_idx ON workers (last_seen_at);

-- Task history: status transitions and heartbeats.
CREATE TABLE IF NOT EXISTS task_events (
  id          BIGSERIAL PRIMARY KEY,
  task_id     UUID NOT NULL,
  queue_id    UUID,
  kind        TEXT NOT NULL,          -- transition | heartbeat
  from_status TEXT,
  to_status   TEXT,
  worker_id   TEXT,
  message     TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS task_events_task_id_idx ON task_events (task_id, id);
CREATE INDEX IF NOT EXISTS task_events_queue_id_idx ON task_events (queue_id);
