package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge.dev/internal/model"
)

const (
	defaultRetryBaseDelay = time.Second
	defaultRetryMaxDelay  = time.Hour
)

type RateLimiter interface {
	Allow(ctx context.Context, queueName string, limit int) (bool, error)
}

type TaskStore struct {
	pool        *pgxpool.Pool
	rateLimiter RateLimiter
	workersMu   sync.Mutex
	workers     map[string]workerState
}

type workerState struct {
	currentTaskID   string
	lastHeartbeatAt time.Time
}

func NewTaskStore(pool *pgxpool.Pool) *TaskStore {
	return &TaskStore{
		pool:    pool,
		workers: make(map[string]workerState),
	}
}

func (s *TaskStore) SetRateLimiter(rateLimiter RateLimiter) {
	s.rateLimiter = rateLimiter
}

func (s *TaskStore) Create(ctx context.Context, queueName string, task model.Task) (model.Task, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO tasks (queue_id, payload, status, priority, scheduled_at, attempts, created_at)
		SELECT q.id, $2, $3, $4, $5, $6, $7
		FROM queues q
		WHERE q.name = $1
		RETURNING id::text, queue_id::text, payload, status, priority, scheduled_at, claimed_at, claimed_by, heartbeat_at, attempts, last_error, result, created_at
	`, queueName, task.Payload, task.Status, task.Priority, task.ScheduledAt, task.Attempts, task.CreatedAt)

	created, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Task{}, model.ErrQueueNotFound
	}
	return created, err
}

func (s *TaskStore) Get(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT t.id::text, t.queue_id::text, t.payload, t.status, t.priority, t.scheduled_at,
		       t.claimed_at, t.claimed_by, t.heartbeat_at, t.attempts, t.last_error, t.result, t.created_at
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

func (s *TaskStore) Cancel(ctx context.Context, queueName string, taskID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks t
		SET status = 'cancelled'
		FROM queues q
		WHERE q.id = t.queue_id AND q.name = $1 AND t.id = $2 AND t.status = 'pending'
	`, queueName, taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	task, err := s.Get(ctx, queueName, taskID)
	if errors.Is(err, model.ErrTaskNotFound) {
		return model.ErrTaskNotFound
	}
	if err != nil {
		return err
	}
	if task.Status != model.TaskStatusPending {
		return model.ErrTaskNotCancellable
	}
	return nil
}

func (s *TaskStore) ClaimPending(ctx context.Context, limit int) ([]model.Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, queue_id::text, payload, status, priority, scheduled_at,
		       claimed_at, claimed_by, heartbeat_at, attempts, last_error, result, created_at
		FROM tasks
		WHERE status = 'pending' AND scheduled_at <= now()
		ORDER BY priority ASC, scheduled_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := []model.Task{}
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *TaskStore) ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.ClaimedTask{}, false, err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()

	var task model.Task
	var selectedQueueName string
	var rateLimit *int
	err = tx.QueryRow(ctx, `
		SELECT t.id::text, t.queue_id::text, t.payload, t.status, t.priority, t.scheduled_at,
		       t.claimed_at, t.claimed_by, t.heartbeat_at, t.attempts, t.last_error, t.result, t.created_at,
		       q.name, q.rate_limit
		FROM tasks t
		JOIN queues q ON q.id = t.queue_id
		WHERE t.status = 'pending'
		  AND t.scheduled_at <= now()
		  AND ($1 = '' OR q.name = $1)
		ORDER BY t.priority ASC, t.scheduled_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, queueName).Scan(
		&task.ID, &task.QueueID, &task.Payload, &task.Status, &task.Priority, &task.ScheduledAt,
		&task.ClaimedAt, &task.ClaimedBy, &task.HeartbeatAt, &task.Attempts, &task.LastError, &task.Result, &task.CreatedAt,
		&selectedQueueName, &rateLimit,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ClaimedTask{}, false, nil
	}
	if err != nil {
		return model.ClaimedTask{}, false, err
	}

	if rateLimit != nil && s.rateLimiter != nil {
		allowed, err := s.rateLimiter.Allow(ctx, selectedQueueName, *rateLimit)
		if err != nil {
			return model.ClaimedTask{}, false, err
		}
		if !allowed {
			return model.ClaimedTask{}, false, tx.Commit(ctx)
		}
	}

	leaseExpiresAt := now.Add(leaseDuration)
	_, err = tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'executing', claimed_at = $2, claimed_by = $3, heartbeat_at = $2
		WHERE id = $1
	`, task.ID, now, workerID)
	if err != nil {
		return model.ClaimedTask{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.ClaimedTask{}, false, err
	}

	s.recordWorker(workerID, task.ID, now)
	return model.ClaimedTask{
		ID:             task.ID,
		Queue:          selectedQueueName,
		Payload:        task.Payload,
		Priority:       task.Priority,
		Attempts:       task.Attempts,
		LeaseExpiresAt: leaseExpiresAt,
	}, true, nil
}

func (s *TaskStore) MarkExecuting(ctx context.Context, taskID string, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'executing', claimed_by = $2, claimed_at = now(), heartbeat_at = now()
		WHERE id = $1
	`, taskID, workerID)
	return err
}

func (s *TaskStore) Ack(ctx context.Context, taskID string, workerID string, result []byte) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'done', result = $3
		WHERE id = $1 AND claimed_by = $2 AND status = 'executing'
	`, taskID, workerID, result)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		s.recordWorker(workerID, "", time.Now().UTC())
		return nil
	}
	return s.operationError(ctx, taskID, workerID)
}

func (s *TaskStore) Nack(ctx context.Context, taskID string, workerID string, errMessage string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var task model.Task
	var queueName string
	var maxRetries int
	err = tx.QueryRow(ctx, `
		SELECT t.id::text, t.queue_id::text, t.payload, t.status, t.priority, t.scheduled_at,
		       t.claimed_at, t.claimed_by, t.heartbeat_at, t.attempts, t.last_error, t.result, t.created_at,
		       q.name, q.max_retries
		FROM tasks t
		JOIN queues q ON q.id = t.queue_id
		WHERE t.id = $1
		FOR UPDATE
	`, taskID).Scan(
		&task.ID, &task.QueueID, &task.Payload, &task.Status, &task.Priority, &task.ScheduledAt,
		&task.ClaimedAt, &task.ClaimedBy, &task.HeartbeatAt, &task.Attempts, &task.LastError, &task.Result, &task.CreatedAt,
		&queueName, &maxRetries,
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

	now := time.Now().UTC()
	if task.Attempts >= maxRetries {
		if _, err := tx.Exec(ctx, `
			INSERT INTO dead_letter_tasks (id, queue_id, payload, last_error, attempts, died_at)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, task.ID, task.QueueID, task.Payload, errMessage, task.Attempts, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM tasks WHERE id = $1`, task.ID); err != nil {
			return err
		}
		s.recordWorker(workerID, "", now)
		return tx.Commit(ctx)
	}

	attempts := task.Attempts + 1
	_, err = tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'retry', attempts = $2, last_error = $3, scheduled_at = $4
		WHERE id = $1
	`, task.ID, attempts, errMessage, now.Add(backoffDelay(attempts)))
	if err != nil {
		return err
	}
	s.recordWorker(workerID, "", now)
	_ = queueName
	return tx.Commit(ctx)
}

func (s *TaskStore) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	now := time.Now().UTC()
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET heartbeat_at = $3
		WHERE id = $1 AND claimed_by = $2 AND status = 'executing'
	`, taskID, workerID, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		s.recordWorker(workerID, taskID, now)
		return nil
	}
	return s.operationError(ctx, taskID, workerID)
}

func (s *TaskStore) ListDue(ctx context.Context, limit int) ([]model.Task, error) {
	return s.ClaimPending(ctx, limit)
}

func (s *TaskStore) Reschedule(ctx context.Context, taskID string, scheduledAt time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE tasks SET scheduled_at = $2 WHERE id = $1`, taskID, scheduledAt)
	return err
}

func (s *TaskStore) ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE tasks
		SET status = 'retry',
		    attempts = attempts + 1,
		    last_error = 'watchdog: heartbeat expired',
		    scheduled_at = now() + interval '1 second'
		WHERE status = 'executing'
		  AND heartbeat_at < now() - $1::interval
		RETURNING id::text, queue_id::text, payload, status, priority, scheduled_at,
		          claimed_at, claimed_by, heartbeat_at, attempts, last_error, result, created_at
	`, heartbeatTimeout.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := []model.Task{}
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *TaskStore) Stats(ctx context.Context, queueName string) (model.QueueStats, error) {
	var stats model.QueueStats
	err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(count(*) FILTER (WHERE t.status IN ('pending', 'retry')), 0)::int AS depth,
			COALESCE(count(*) FILTER (WHERE t.status = 'executing'), 0)::int AS executing_count,
			COALESCE(count(*) FILTER (WHERE t.status = 'done' AND t.created_at >= now() - interval '1 minute'), 0)::int AS done_last_minute,
			COALESCE(count(*) FILTER (WHERE t.last_error IS NOT NULL AND t.scheduled_at >= now() - interval '1 minute'), 0)::int AS failed_last_minute,
			COALESCE((SELECT count(*) FROM dead_letter_tasks d WHERE d.queue_id = q.id), 0)::int AS dlq_depth,
			COALESCE(EXTRACT(EPOCH FROM now() - min(t.scheduled_at)) FILTER (WHERE t.status IN ('pending', 'retry') AND t.scheduled_at <= now()), 0)::int AS oldest_pending_age_seconds
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

func (s *TaskStore) ListDeadLetterTasks(ctx context.Context, queueName string) ([]model.DeadLetterTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id::text, d.queue_id::text, d.payload, d.last_error, d.attempts, d.died_at, d.replayed_at
		FROM dead_letter_tasks d
		JOIN queues q ON q.id = d.queue_id
		WHERE q.name = $1
		ORDER BY d.died_at
	`, queueName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := []model.DeadLetterTask{}
	for rows.Next() {
		task, err := scanDeadLetterTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *TaskStore) ReplayDeadLetterTask(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Task{}, err
	}
	defer tx.Rollback(ctx)

	var dead model.DeadLetterTask
	err = tx.QueryRow(ctx, `
		UPDATE dead_letter_tasks d
		SET replayed_at = now()
		FROM queues q
		WHERE q.id = d.queue_id AND q.name = $1 AND d.id = $2
		RETURNING d.id::text, d.queue_id::text, d.payload, d.last_error, d.attempts, d.died_at, d.replayed_at
	`, queueName, taskID).Scan(&dead.ID, &dead.QueueID, &dead.Payload, &dead.LastError, &dead.Attempts, &dead.DiedAt, &dead.ReplayedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Task{}, model.ErrDeadLetterTaskNotFound
	}
	if err != nil {
		return model.Task{}, err
	}

	task := model.NewTask(dead.QueueID, dead.Payload, time.Now().UTC(), 5)
	row := tx.QueryRow(ctx, `
		INSERT INTO tasks (queue_id, payload, status, priority, scheduled_at, attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, 0, $6)
		RETURNING id::text, queue_id::text, payload, status, priority, scheduled_at, claimed_at, claimed_by, heartbeat_at, attempts, last_error, result, created_at
	`, task.QueueID, task.Payload, task.Status, task.Priority, task.ScheduledAt, task.CreatedAt)
	created, err := scanTask(row)
	if err != nil {
		return model.Task{}, err
	}
	return created, tx.Commit(ctx)
}

func (s *TaskStore) DeleteDeadLetterTask(ctx context.Context, queueName string, taskID string) error {
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

func (s *TaskStore) ActiveWorkers(ctx context.Context) ([]model.WorkerStatus, error) {
	now := time.Now().UTC()
	cutoff := now.Add(-60 * time.Second)
	staleCutoff := now.Add(-20 * time.Second)

	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	workers := make([]model.WorkerStatus, 0, len(s.workers))
	for workerID, worker := range s.workers {
		if worker.lastHeartbeatAt.Before(cutoff) {
			delete(s.workers, workerID)
			continue
		}
		workers = append(workers, model.WorkerStatus{
			WorkerID:        workerID,
			CurrentTaskID:   worker.currentTaskID,
			LastHeartbeatAt: worker.lastHeartbeatAt,
			Stale:           worker.lastHeartbeatAt.Before(staleCutoff),
		})
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].WorkerID < workers[j].WorkerID })
	return workers, nil
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

func (s *TaskStore) recordWorker(workerID string, taskID string, seenAt time.Time) {
	if workerID == "" {
		return
	}
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	s.workers[workerID] = workerState{currentTaskID: taskID, lastHeartbeatAt: seenAt}
}

type taskScanner interface {
	Scan(dest ...any) error
}

func scanTask(row taskScanner) (model.Task, error) {
	var task model.Task
	if err := row.Scan(
		&task.ID, &task.QueueID, &task.Payload, &task.Status, &task.Priority, &task.ScheduledAt,
		&task.ClaimedAt, &task.ClaimedBy, &task.HeartbeatAt, &task.Attempts, &task.LastError, &task.Result, &task.CreatedAt,
	); err != nil {
		return model.Task{}, err
	}
	return task, nil
}

func scanDeadLetterTask(row taskScanner) (model.DeadLetterTask, error) {
	var task model.DeadLetterTask
	if err := row.Scan(&task.ID, &task.QueueID, &task.Payload, &task.LastError, &task.Attempts, &task.DiedAt, &task.ReplayedAt); err != nil {
		return model.DeadLetterTask{}, err
	}
	return task, nil
}

func backoffDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}

	delay := defaultRetryBaseDelay
	for i := 0; i < attempts; i++ {
		delay *= 2
		if delay >= defaultRetryMaxDelay {
			delay = defaultRetryMaxDelay
			break
		}
	}

	delay += retryJitter(defaultRetryBaseDelay)
	if delay > defaultRetryMaxDelay {
		return defaultRetryMaxDelay
	}
	return delay
}

func retryJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}

	n, err := rand.Int(rand.Reader, big.NewInt(int64(base)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}
