package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge.dev/internal/model"
	"taskforge.dev/internal/ratelimit"
)

// maxClaimAttempts bounds how many rate-limited queues a single claim skips
// before giving up and reporting no work available.
const maxClaimAttempts = 16

const taskColumns = `t.id::text, t.queue_id::text, t.payload, t.status, t.priority, t.scheduled_at,
	t.claimed_at, t.claimed_by, t.heartbeat_at, t.attempts, t.last_error, t.result, t.created_at, t.completed_at`

const upsertWorkerSQL = `
	INSERT INTO workers (worker_id, current_task_id, last_seen_at)
	VALUES ($1, $2::uuid, now())
	ON CONFLICT (worker_id) DO UPDATE
	SET current_task_id = EXCLUDED.current_task_id, last_seen_at = EXCLUDED.last_seen_at`

const insertEventSQL = `
	INSERT INTO task_events (task_id, queue_id, kind, from_status, to_status, worker_id, message)
	VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''))`

// TaskStore is the durable Postgres implementation of the task store.
type TaskStore struct {
	pool        *pgxpool.Pool
	rateLimiter ratelimit.Limiter
	retry       model.RetryPolicy
	observer    model.Observer
}

func NewTaskStore(pool *pgxpool.Pool) *TaskStore {
	return &TaskStore{
		pool:        pool,
		rateLimiter: ratelimit.NewLocal(),
		retry:       model.DefaultRetryPolicy(),
		observer:    model.NopObserver{},
	}
}

// SetRateLimiter replaces the default in-process limiter, e.g. with the Redis
// limiter so limits hold across API instances.
func (s *TaskStore) SetRateLimiter(rateLimiter ratelimit.Limiter) {
	if rateLimiter != nil {
		s.rateLimiter = rateLimiter
	}
}

func (s *TaskStore) SetRetryPolicy(policy model.RetryPolicy) { s.retry = policy }

func (s *TaskStore) SetObserver(observer model.Observer) {
	if observer == nil {
		observer = model.NopObserver{}
	}
	s.observer = observer
}

func (s *TaskStore) emit(transitions []model.Transition) {
	for _, t := range transitions {
		s.observer.TaskTransition(t)
	}
}

func (s *TaskStore) Create(ctx context.Context, queueName string, task model.Task) (model.Task, error) {
	row := s.pool.QueryRow(ctx, `
		WITH q AS (
			SELECT id FROM queues WHERE name = $1
		), ins AS (
			INSERT INTO tasks (queue_id, payload, status, priority, scheduled_at, attempts, created_at)
			SELECT q.id, $2, $3, $4, $5, $6, $7 FROM q
			RETURNING *
		), ev AS (
			INSERT INTO task_events (task_id, queue_id, kind, to_status)
			SELECT id, queue_id, 'transition', status FROM ins
		)
		SELECT `+taskColumns+` FROM ins t
	`, queueName, task.Payload, task.Status, task.Priority, task.ScheduledAt, task.Attempts, task.CreatedAt)

	created, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Task{}, model.ErrQueueNotFound
	}
	if err != nil {
		return model.Task{}, err
	}
	s.emit([]model.Transition{{
		TaskID: created.ID, Queue: queueName, To: created.Status, At: created.CreatedAt,
	}})
	return created, nil
}

func (s *TaskStore) Get(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	if !isUUID(taskID) {
		return model.Task{}, model.ErrTaskNotFound
	}
	row := s.pool.QueryRow(ctx, `
		SELECT `+taskColumns+`
		FROM tasks t
		JOIN queues q ON q.id = t.queue_id
		WHERE q.name = $1 AND t.id = $2
	`, queueName, taskID)

	task, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Task{}, model.ErrTaskNotFound
	}
	return task, err
}

func (s *TaskStore) GetTaskByID(ctx context.Context, taskID string) (model.TaskSummary, error) {
	if !isUUID(taskID) {
		return model.TaskSummary{}, model.ErrTaskNotFound
	}
	row := s.pool.QueryRow(ctx, `
		SELECT `+taskColumns+`, q.name
		FROM tasks t
		JOIN queues q ON q.id = t.queue_id
		WHERE t.id = $1
	`, taskID)

	summary, err := scanTaskSummary(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.TaskSummary{}, model.ErrTaskNotFound
	}
	return summary, err
}

func (s *TaskStore) ListTasks(ctx context.Context, filter model.TaskFilter) ([]model.TaskSummary, error) {
	where := []string{"TRUE"}
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if filter.TaskID != "" {
		if !isUUID(filter.TaskID) {
			return []model.TaskSummary{}, nil
		}
		where = append(where, "t.id = "+arg(filter.TaskID))
	}
	if filter.Queue != "" {
		where = append(where, "q.name = "+arg(filter.Queue))
	}
	if filter.Status != "" {
		where = append(where, "t.status = "+arg(string(filter.Status)))
	}
	if filter.WorkerID != "" {
		where = append(where, "t.claimed_by = "+arg(filter.WorkerID))
	}
	if filter.CreatedAfter != nil {
		where = append(where, "t.created_at >= "+arg(*filter.CreatedAfter))
	}
	if filter.CreatedBefore != nil {
		where = append(where, "t.created_at < "+arg(*filter.CreatedBefore))
	}
	limit := arg(model.ClampLimit(filter.Limit))
	offset := arg(max(filter.Offset, 0))

	rows, err := s.pool.Query(ctx, `
		SELECT `+taskColumns+`, q.name
		FROM tasks t
		JOIN queues q ON q.id = t.queue_id
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY t.created_at DESC, t.id
		LIMIT `+limit+` OFFSET `+offset, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := []model.TaskSummary{}
	for rows.Next() {
		summary, err := scanTaskSummary(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, summary)
	}
	return tasks, rows.Err()
}

func (s *TaskStore) TaskEvents(ctx context.Context, taskID string) ([]model.TaskEvent, error) {
	if !isUUID(taskID) {
		return []model.TaskEvent{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, task_id::text, kind, COALESCE(from_status, ''), COALESCE(to_status, ''),
		       COALESCE(worker_id, ''), COALESCE(message, ''), created_at
		FROM task_events
		WHERE task_id = $1
		ORDER BY id
	`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []model.TaskEvent{}
	for rows.Next() {
		var e model.TaskEvent
		if err := rows.Scan(&e.ID, &e.TaskID, &e.Kind, &e.FromStatus, &e.ToStatus, &e.WorkerID, &e.Message, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *TaskStore) Cancel(ctx context.Context, queueName string, taskID string) error {
	if !isUUID(taskID) {
		return model.ErrTaskNotFound
	}
	var createdAt time.Time
	err := s.pool.QueryRow(ctx, `
		WITH u AS (
			UPDATE tasks t
			SET status = 'cancelled'
			FROM queues q
			WHERE q.id = t.queue_id AND q.name = $1 AND t.id = $2 AND t.status = 'pending'
			RETURNING t.id, t.queue_id, t.created_at
		), ev AS (
			INSERT INTO task_events (task_id, queue_id, kind, from_status, to_status)
			SELECT id, queue_id, 'transition', 'pending', 'cancelled' FROM u
		)
		SELECT created_at FROM u
	`, queueName, taskID).Scan(&createdAt)
	if err == nil {
		now := time.Now().UTC()
		s.emit([]model.Transition{{
			TaskID: taskID, Queue: queueName, From: model.TaskStatusPending, To: model.TaskStatusCancelled,
			Duration: now.Sub(createdAt), At: now,
		}})
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	if _, err := s.Get(ctx, queueName, taskID); err != nil {
		return err
	}
	return model.ErrTaskNotCancellable
}

func (s *TaskStore) ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error) {
	if queueName != "" {
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM queues WHERE name = $1)`, queueName).Scan(&exists); err != nil {
			return model.ClaimedTask{}, false, err
		}
		if !exists {
			return model.ClaimedTask{}, false, model.ErrQueueNotFound
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.ClaimedTask{}, false, err
	}
	defer tx.Rollback(ctx)

	// Queues whose rate limit is exhausted are skipped so they do not starve
	// other queues when the worker is not bound to a single queue.
	limitedQueues := []string{}
	for range maxClaimAttempts {
		var task model.Task
		var selectedQueue string
		var rateLimit *int
		err = tx.QueryRow(ctx, `
			SELECT `+taskColumns+`, q.name, q.rate_limit
			FROM tasks t
			JOIN queues q ON q.id = t.queue_id
			WHERE t.status IN ('pending', 'retry')
			  AND t.scheduled_at <= now()
			  AND ($1 = '' OR q.name = $1)
			  AND NOT (q.name = ANY($2))
			ORDER BY t.priority ASC, t.scheduled_at ASC
			LIMIT 1
			FOR UPDATE OF t SKIP LOCKED
		`, queueName, limitedQueues).Scan(
			&task.ID, &task.QueueID, &task.Payload, &task.Status, &task.Priority, &task.ScheduledAt,
			&task.ClaimedAt, &task.ClaimedBy, &task.HeartbeatAt, &task.Attempts, &task.LastError, &task.Result,
			&task.CreatedAt, &task.CompletedAt, &selectedQueue, &rateLimit,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Rollback(ctx)
			return model.ClaimedTask{}, false, s.recordIdlePoll(ctx, workerID)
		}
		if err != nil {
			return model.ClaimedTask{}, false, err
		}

		if rateLimit != nil {
			allowed, err := s.rateLimiter.Allow(ctx, selectedQueue, *rateLimit)
			if err != nil {
				return model.ClaimedTask{}, false, err
			}
			if !allowed {
				limitedQueues = append(limitedQueues, selectedQueue)
				continue
			}
		}

		var now time.Time
		if err := tx.QueryRow(ctx, `
			UPDATE tasks
			SET status = 'executing', claimed_at = now(), claimed_by = $2, heartbeat_at = now()
			WHERE id = $1
			RETURNING claimed_at
		`, task.ID, workerID).Scan(&now); err != nil {
			return model.ClaimedTask{}, false, err
		}
		if _, err := tx.Exec(ctx, insertEventSQL, task.ID, task.QueueID, model.TaskEventTransition,
			string(task.Status), string(model.TaskStatusClaimed), workerID, ""); err != nil {
			return model.ClaimedTask{}, false, err
		}
		if _, err := tx.Exec(ctx, insertEventSQL, task.ID, task.QueueID, model.TaskEventTransition,
			string(model.TaskStatusClaimed), string(model.TaskStatusExecuting), workerID, ""); err != nil {
			return model.ClaimedTask{}, false, err
		}
		if _, err := tx.Exec(ctx, upsertWorkerSQL, workerID, task.ID); err != nil {
			return model.ClaimedTask{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return model.ClaimedTask{}, false, err
		}

		now = now.UTC()
		s.emit([]model.Transition{
			{TaskID: task.ID, Queue: selectedQueue, WorkerID: workerID, From: task.Status, To: model.TaskStatusClaimed,
				Duration: waitDuration(task, now), At: now},
			{TaskID: task.ID, Queue: selectedQueue, WorkerID: workerID, From: model.TaskStatusClaimed, To: model.TaskStatusExecuting, At: now},
		})
		return model.ClaimedTask{
			ID:             task.ID,
			Queue:          selectedQueue,
			Payload:        task.Payload,
			Priority:       task.Priority,
			Attempts:       task.Attempts,
			LeaseExpiresAt: now.Add(leaseDuration),
		}, true, nil
	}
	_ = tx.Rollback(ctx)
	return model.ClaimedTask{}, false, s.recordIdlePoll(ctx, workerID)
}

// recordIdlePoll marks a worker that polled and found no work as seen, so
// idle workers stay visible. Writes are throttled to one per worker every
// five seconds to keep polling cheap.
func (s *TaskStore) recordIdlePoll(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (worker_id, current_task_id, last_seen_at)
		VALUES ($1, NULL, now())
		ON CONFLICT (worker_id) DO UPDATE
		SET current_task_id = NULL, last_seen_at = EXCLUDED.last_seen_at
		WHERE workers.last_seen_at < now() - interval '5 seconds'
	`, workerID)
	return err
}

func (s *TaskStore) Ack(ctx context.Context, taskID string, workerID string, result []byte) error {
	if !isUUID(taskID) {
		return model.ErrTaskNotFound
	}
	var queueName string
	var claimedAt *time.Time
	var completedAt time.Time
	err := s.pool.QueryRow(ctx, `
		WITH u AS (
			UPDATE tasks t
			SET status = 'done', result = $3, completed_at = now()
			FROM queues q
			WHERE t.id = $1 AND t.claimed_by = $2 AND t.status = 'executing' AND q.id = t.queue_id
			RETURNING t.id, t.queue_id, q.name, t.claimed_at, t.completed_at
		), ev AS (
			INSERT INTO task_events (task_id, queue_id, kind, from_status, to_status, worker_id)
			SELECT id, queue_id, 'transition', 'executing', 'done', $2 FROM u
		), w AS (
			INSERT INTO workers (worker_id, current_task_id, last_seen_at)
			SELECT $2, NULL::uuid, now() FROM u
			ON CONFLICT (worker_id) DO UPDATE
			SET current_task_id = NULL, last_seen_at = EXCLUDED.last_seen_at
		)
		SELECT name, claimed_at, completed_at FROM u
	`, taskID, workerID, result).Scan(&queueName, &claimedAt, &completedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.operationError(ctx, taskID, workerID)
	}
	if err != nil {
		return err
	}

	completedAt = completedAt.UTC()
	s.emit([]model.Transition{{
		TaskID: taskID, Queue: queueName, WorkerID: workerID, From: model.TaskStatusExecuting, To: model.TaskStatusDone,
		Duration: sinceOrZero(claimedAt, completedAt), At: completedAt,
	}})
	return nil
}

func (s *TaskStore) Nack(ctx context.Context, taskID string, workerID string, errMessage string) error {
	if !isUUID(taskID) {
		return model.ErrTaskNotFound
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var task model.Task
	var queueName string
	var maxRetries int
	err = tx.QueryRow(ctx, `
		SELECT `+taskColumns+`, q.name, q.max_retries
		FROM tasks t
		JOIN queues q ON q.id = t.queue_id
		WHERE t.id = $1
		FOR UPDATE OF t
	`, taskID).Scan(
		&task.ID, &task.QueueID, &task.Payload, &task.Status, &task.Priority, &task.ScheduledAt,
		&task.ClaimedAt, &task.ClaimedBy, &task.HeartbeatAt, &task.Attempts, &task.LastError, &task.Result,
		&task.CreatedAt, &task.CompletedAt, &queueName, &maxRetries,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrTaskNotFound
	}
	if err != nil {
		return err
	}
	if task.ClaimedBy == nil || *task.ClaimedBy != workerID {
		return model.ErrTaskNotOwned
	}
	if task.Status != model.TaskStatusExecuting {
		if task.Status == model.TaskStatusRetry {
			return model.ErrTaskLeaseExpired
		}
		return model.ErrTaskInvalidStatus
	}

	transition, err := s.failLocked(ctx, tx, task, queueName, maxRetries, errMessage, workerID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, upsertWorkerSQL, workerID, nil); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.emit([]model.Transition{transition})
	return nil
}

// failLocked records a failed attempt for an executing task whose row is
// locked by tx. Tasks with retry budget left are rescheduled with backoff;
// exhausted tasks are moved to the dead-letter queue.
func (s *TaskStore) failLocked(ctx context.Context, tx pgx.Tx, task model.Task, queueName string, maxRetries int, errMessage string, workerID string) (model.Transition, error) {
	now := time.Now().UTC()
	transition := model.Transition{
		TaskID: task.ID, Queue: queueName, WorkerID: workerID, From: model.TaskStatusExecuting,
		Duration: sinceOrZero(task.ClaimedAt, now), At: now,
	}

	if task.Attempts >= maxRetries {
		if _, err := tx.Exec(ctx, `
			INSERT INTO dead_letter_tasks (id, queue_id, payload, last_error, attempts, died_at)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, task.ID, task.QueueID, task.Payload, errMessage, task.Attempts, now); err != nil {
			return model.Transition{}, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM tasks WHERE id = $1`, task.ID); err != nil {
			return model.Transition{}, err
		}
		if _, err := tx.Exec(ctx, insertEventSQL, task.ID, task.QueueID, model.TaskEventTransition,
			string(model.TaskStatusExecuting), string(model.TaskStatusDead), workerID, errMessage); err != nil {
			return model.Transition{}, err
		}
		transition.To = model.TaskStatusDead
		return transition, nil
	}

	attempts := task.Attempts + 1
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'retry', attempts = $2, last_error = $3, scheduled_at = $4, last_failed_at = $5
		WHERE id = $1
	`, task.ID, attempts, errMessage, now.Add(s.retry.Delay(attempts)), now); err != nil {
		return model.Transition{}, err
	}
	if _, err := tx.Exec(ctx, insertEventSQL, task.ID, task.QueueID, model.TaskEventTransition,
		string(model.TaskStatusExecuting), string(model.TaskStatusRetry), workerID, errMessage); err != nil {
		return model.Transition{}, err
	}
	transition.To = model.TaskStatusRetry
	return transition, nil
}

func (s *TaskStore) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	if !isUUID(taskID) {
		return model.ErrTaskNotFound
	}
	var updated string
	err := s.pool.QueryRow(ctx, `
		WITH u AS (
			UPDATE tasks
			SET heartbeat_at = now()
			WHERE id = $1 AND claimed_by = $2 AND status = 'executing'
			RETURNING id, queue_id
		), ev AS (
			INSERT INTO task_events (task_id, queue_id, kind, worker_id)
			SELECT id, queue_id, 'heartbeat', $2 FROM u
		), w AS (
			INSERT INTO workers (worker_id, current_task_id, last_seen_at)
			SELECT $2, id, now() FROM u
			ON CONFLICT (worker_id) DO UPDATE
			SET current_task_id = EXCLUDED.current_task_id, last_seen_at = EXCLUDED.last_seen_at
		)
		SELECT id::text FROM u
	`, taskID, workerID).Scan(&updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.operationError(ctx, taskID, workerID)
	}
	return err
}

// ReclaimExpired returns executing tasks whose heartbeat is older than
// heartbeatTimeout to the retry state (or the DLQ when their retry budget is
// spent). Each reclaim counts as a failed attempt.
func (s *TaskStore) ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT `+taskColumns+`, q.name, q.max_retries
		FROM tasks t
		JOIN queues q ON q.id = t.queue_id
		WHERE t.status = 'executing'
		  AND t.heartbeat_at < now() - make_interval(secs => $1)
		ORDER BY t.heartbeat_at
		LIMIT 500
		FOR UPDATE OF t SKIP LOCKED
	`, heartbeatTimeout.Seconds())
	if err != nil {
		return nil, err
	}

	type expired struct {
		task       model.Task
		queueName  string
		maxRetries int
	}
	candidates := []expired{}
	for rows.Next() {
		var e expired
		if err := rows.Scan(
			&e.task.ID, &e.task.QueueID, &e.task.Payload, &e.task.Status, &e.task.Priority, &e.task.ScheduledAt,
			&e.task.ClaimedAt, &e.task.ClaimedBy, &e.task.HeartbeatAt, &e.task.Attempts, &e.task.LastError, &e.task.Result,
			&e.task.CreatedAt, &e.task.CompletedAt, &e.queueName, &e.maxRetries,
		); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	reclaimed := make([]model.Task, 0, len(candidates))
	transitions := make([]model.Transition, 0, len(candidates))
	for _, e := range candidates {
		workerID := ""
		if e.task.ClaimedBy != nil {
			workerID = *e.task.ClaimedBy
		}
		transition, err := s.failLocked(ctx, tx, e.task, e.queueName, e.maxRetries, model.WatchdogError, workerID)
		if err != nil {
			return nil, err
		}
		task := e.task
		task.Status = transition.To
		if transition.To == model.TaskStatusRetry {
			task.Attempts++
		}
		msg := model.WatchdogError
		task.LastError = &msg
		reclaimed = append(reclaimed, task)
		transitions = append(transitions, transition)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.emit(transitions)
	return reclaimed, nil
}

func (s *TaskStore) Stats(ctx context.Context, queueName string) (model.QueueStats, error) {
	var stats model.QueueStats
	err := s.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE t.status IN ('pending', 'retry'))::int,
			count(*) FILTER (WHERE t.status = 'executing')::int,
			count(*) FILTER (WHERE t.status = 'done' AND t.completed_at >= now() - interval '1 minute')::int,
			(count(*) FILTER (WHERE t.last_failed_at >= now() - interval '1 minute')
			  + (SELECT count(*) FROM dead_letter_tasks d WHERE d.queue_id = q.id AND d.died_at >= now() - interval '1 minute'))::int,
			(SELECT count(*) FROM dead_letter_tasks d WHERE d.queue_id = q.id)::int,
			COALESCE(EXTRACT(EPOCH FROM now() - min(t.scheduled_at) FILTER (
				WHERE t.status IN ('pending', 'retry') AND t.scheduled_at <= now()
			)), 0)::int
		FROM queues q
		LEFT JOIN tasks t ON t.queue_id = q.id
		WHERE q.name = $1
		GROUP BY q.id
	`, queueName).Scan(&stats.Depth, &stats.ExecutingCount, &stats.DoneLastMinute, &stats.FailedLastMinute, &stats.DLQDepth, &stats.OldestPendingAgeSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.QueueStats{}, model.ErrQueueNotFound
	}
	return stats, err
}

// StatusCounts returns task counts by queue and status, with dead-letter
// entries reported under the "dead" status.
func (s *TaskStore) StatusCounts(ctx context.Context) ([]model.StatusCount, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT q.name, t.status, count(*)::int
		FROM tasks t JOIN queues q ON q.id = t.queue_id
		GROUP BY q.name, t.status
		UNION ALL
		SELECT q.name, 'dead', count(*)::int
		FROM dead_letter_tasks d JOIN queues q ON q.id = d.queue_id
		GROUP BY q.name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := []model.StatusCount{}
	for rows.Next() {
		var c model.StatusCount
		if err := rows.Scan(&c.Queue, &c.Status, &c.Count); err != nil {
			return nil, err
		}
		counts = append(counts, c)
	}
	return counts, rows.Err()
}

func (s *TaskStore) ListDeadLetterTasks(ctx context.Context, queueName string) ([]model.DeadLetterTask, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM queues WHERE name = $1)`, queueName).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, model.ErrQueueNotFound
	}
	return s.ListDeadLetters(ctx, model.DeadLetterFilter{Queue: queueName, Limit: 500})
}

func (s *TaskStore) ListDeadLetters(ctx context.Context, filter model.DeadLetterFilter) ([]model.DeadLetterTask, error) {
	where := []string{"TRUE"}
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if filter.TaskID != "" {
		if !isUUID(filter.TaskID) {
			return []model.DeadLetterTask{}, nil
		}
		where = append(where, "d.id = "+arg(filter.TaskID))
	}
	if filter.Queue != "" {
		where = append(where, "q.name = "+arg(filter.Queue))
	}
	if filter.DiedAfter != nil {
		where = append(where, "d.died_at >= "+arg(*filter.DiedAfter))
	}
	if filter.DiedBefore != nil {
		where = append(where, "d.died_at < "+arg(*filter.DiedBefore))
	}
	limit := arg(model.ClampLimit(filter.Limit))
	offset := arg(max(filter.Offset, 0))

	rows, err := s.pool.Query(ctx, `
		SELECT d.id::text, d.queue_id::text, q.name, d.payload, COALESCE(d.last_error, ''), COALESCE(d.attempts, 0),
		       d.died_at, d.replayed_at, d.replay_count
		FROM dead_letter_tasks d
		JOIN queues q ON q.id = d.queue_id
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY d.died_at, d.id
		LIMIT `+limit+` OFFSET `+offset, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := []model.DeadLetterTask{}
	for rows.Next() {
		var d model.DeadLetterTask
		if err := rows.Scan(&d.ID, &d.QueueID, &d.QueueName, &d.Payload, &d.LastError, &d.Attempts, &d.DiedAt, &d.ReplayedAt, &d.ReplayCount); err != nil {
			return nil, err
		}
		tasks = append(tasks, d)
	}
	return tasks, rows.Err()
}

func (s *TaskStore) ReplayDeadLetterTask(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	if !isUUID(taskID) {
		return model.Task{}, model.ErrDeadLetterTaskNotFound
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Task{}, err
	}
	defer tx.Rollback(ctx)

	var queueID string
	var payload []byte
	err = tx.QueryRow(ctx, `
		UPDATE dead_letter_tasks d
		SET replayed_at = now(), replay_count = d.replay_count + 1
		FROM queues q
		WHERE q.id = d.queue_id AND q.name = $1 AND d.id = $2
		RETURNING d.queue_id::text, d.payload
	`, queueName, taskID).Scan(&queueID, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Task{}, model.ErrDeadLetterTaskNotFound
	}
	if err != nil {
		return model.Task{}, err
	}

	task := model.NewTask(queueID, payload, time.Now().UTC(), 5)
	created, err := scanTask(tx.QueryRow(ctx, `
		INSERT INTO tasks (queue_id, payload, status, priority, scheduled_at, attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, 0, $6)
		RETURNING id::text, queue_id::text, payload, status, priority, scheduled_at, claimed_at, claimed_by,
		          heartbeat_at, attempts, last_error, result, created_at, completed_at
	`, task.QueueID, task.Payload, task.Status, task.Priority, task.ScheduledAt, task.CreatedAt))
	if err != nil {
		return model.Task{}, err
	}
	if _, err := tx.Exec(ctx, insertEventSQL, created.ID, queueID, model.TaskEventTransition,
		"", string(model.TaskStatusPending), "", "replayed from dead-letter task "+taskID); err != nil {
		return model.Task{}, err
	}
	if _, err := tx.Exec(ctx, insertEventSQL, taskID, queueID, model.TaskEventReplay,
		"", "", "", "replayed as task "+created.ID); err != nil {
		return model.Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Task{}, err
	}

	s.emit([]model.Transition{{TaskID: created.ID, Queue: queueName, To: model.TaskStatusPending, At: created.CreatedAt}})
	return created, nil
}

func (s *TaskStore) DeleteDeadLetterTask(ctx context.Context, queueName string, taskID string) error {
	if !isUUID(taskID) {
		return model.ErrDeadLetterTaskNotFound
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM dead_letter_tasks d
		USING queues q
		WHERE q.id = d.queue_id AND q.name = $1 AND d.id = $2
	`, queueName, taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return model.ErrDeadLetterTaskNotFound
	}
	return nil
}

// ActiveWorkers lists workers seen by any API instance in the last 60 seconds.
func (s *TaskStore) ActiveWorkers(ctx context.Context) ([]model.WorkerStatus, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT worker_id, COALESCE(current_task_id::text, ''), last_seen_at,
		       last_seen_at < now() - interval '20 seconds' AS stale
		FROM workers
		WHERE last_seen_at >= now() - interval '60 seconds'
		ORDER BY worker_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workers := []model.WorkerStatus{}
	for rows.Next() {
		var w model.WorkerStatus
		if err := rows.Scan(&w.WorkerID, &w.CurrentTaskID, &w.LastHeartbeatAt, &w.Stale); err != nil {
			return nil, err
		}
		w.LastHeartbeatAt = w.LastHeartbeatAt.UTC()
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

// PruneWorkers deletes workers not seen within olderThan.
func (s *TaskStore) PruneWorkers(ctx context.Context, olderThan time.Duration) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM workers WHERE last_seen_at < now() - make_interval(secs => $1)`, olderThan.Seconds())
	return err
}

func (s *TaskStore) operationError(ctx context.Context, taskID string, workerID string) error {
	var status model.TaskStatus
	var claimedBy *string
	err := s.pool.QueryRow(ctx, `SELECT status, claimed_by FROM tasks WHERE id = $1`, taskID).Scan(&status, &claimedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrTaskNotFound
	}
	if err != nil {
		return err
	}
	if claimedBy == nil || *claimedBy != workerID {
		return model.ErrTaskNotOwned
	}
	if status == model.TaskStatusRetry {
		return model.ErrTaskLeaseExpired
	}
	return model.ErrTaskInvalidStatus
}

type taskScanner interface {
	Scan(dest ...any) error
}

func scanTask(row taskScanner) (model.Task, error) {
	var task model.Task
	if err := row.Scan(
		&task.ID, &task.QueueID, &task.Payload, &task.Status, &task.Priority, &task.ScheduledAt,
		&task.ClaimedAt, &task.ClaimedBy, &task.HeartbeatAt, &task.Attempts, &task.LastError, &task.Result,
		&task.CreatedAt, &task.CompletedAt,
	); err != nil {
		return model.Task{}, err
	}
	return normalizeTask(task), nil
}

func scanTaskSummary(row taskScanner) (model.TaskSummary, error) {
	var s model.TaskSummary
	if err := row.Scan(
		&s.ID, &s.QueueID, &s.Payload, &s.Status, &s.Priority, &s.ScheduledAt,
		&s.ClaimedAt, &s.ClaimedBy, &s.HeartbeatAt, &s.Attempts, &s.LastError, &s.Result,
		&s.CreatedAt, &s.CompletedAt, &s.QueueName,
	); err != nil {
		return model.TaskSummary{}, err
	}
	s.Task = normalizeTask(s.Task)
	return s, nil
}

func normalizeTask(t model.Task) model.Task {
	t.ScheduledAt = t.ScheduledAt.UTC()
	t.CreatedAt = t.CreatedAt.UTC()
	for _, p := range []**time.Time{&t.ClaimedAt, &t.HeartbeatAt, &t.CompletedAt} {
		if *p != nil {
			v := (*p).UTC()
			*p = &v
		}
	}
	return t
}

func waitDuration(task model.Task, now time.Time) time.Duration {
	start := task.CreatedAt
	if task.ScheduledAt.After(start) {
		start = task.ScheduledAt
	}
	if now.Before(start) {
		return 0
	}
	return now.Sub(start)
}

func sinceOrZero(start *time.Time, now time.Time) time.Duration {
	if start == nil || now.Before(*start) {
		return 0
	}
	return now.Sub(*start)
}

// isUUID reports whether s is a canonical 8-4-4-4-12 hex UUID. IDs that are
// not UUIDs cannot exist, and passing them to Postgres would raise an error.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}
