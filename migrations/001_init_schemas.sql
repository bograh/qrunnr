CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Queues: named channels with config
CREATE TABLE IF NOT EXISTS queues (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name         TEXT UNIQUE NOT NULL,
  max_retries  INT NOT NULL DEFAULT 3,
  rate_limit   INT,             -- max tasks/sec dispatched
  created_at   TIMESTAMPTZ DEFAULT now()
);

-- Tasks: the unit of work
CREATE TABLE IF NOT EXISTS tasks (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_id       UUID REFERENCES queues(id),
  payload        JSONB NOT NULL,
  status         TEXT NOT NULL DEFAULT 'pending',
                 -- pending | claimed | executing | done | retry | dead | cancelled
  priority       INT NOT NULL DEFAULT 5,   -- 1 (high) to 10 (low)
  scheduled_at   TIMESTAMPTZ DEFAULT now(),
  claimed_at     TIMESTAMPTZ,
  claimed_by     TEXT,          -- worker ID
  heartbeat_at   TIMESTAMPTZ,
  attempts       INT NOT NULL DEFAULT 0,
  last_error     TEXT,
  result         JSONB,
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tasks_queue_id_status_priority_scheduled_at_idx
  ON tasks (queue_id, status, priority, scheduled_at);

-- Dead-letter: tasks that exhausted retries
CREATE TABLE IF NOT EXISTS dead_letter_tasks (
  id          UUID PRIMARY KEY,
  queue_id    UUID,
  payload     JSONB,
  last_error  TEXT,
  attempts    INT,
  died_at     TIMESTAMPTZ DEFAULT now(),
  replayed_at TIMESTAMPTZ  -- set when manually re-enqueued
);
