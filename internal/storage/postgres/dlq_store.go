package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge.dev/internal/model"
)

type DLQStore struct {
	pool *pgxpool.Pool
}

func NewDLQStore(pool *pgxpool.Pool) *DLQStore {
	return &DLQStore{pool: pool}
}

func (s *DLQStore) List(ctx context.Context, queueName string) ([]model.DeadLetterTask, error) {
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

func (s *DLQStore) Replay(ctx context.Context, queueName string, taskID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var queueID string
	var payload []byte
	err = tx.QueryRow(ctx, `
		UPDATE dead_letter_tasks d
		SET replayed_at = now()
		FROM queues q
		WHERE q.id = d.queue_id AND q.name = $1 AND d.id = $2
		RETURNING d.queue_id::text, d.payload
	`, queueName, taskID).Scan(&queueID, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrDeadLetterTaskNotFound
	}
	if err != nil {
		return err
	}

	task := model.NewTask(queueID, payload, time.Now().UTC(), 5)
	if _, err := tx.Exec(ctx, `
		INSERT INTO tasks (queue_id, payload, status, priority, scheduled_at, attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, 0, $6)
	`, task.QueueID, task.Payload, task.Status, task.Priority, task.ScheduledAt, task.CreatedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *DLQStore) Delete(ctx context.Context, queueName string, taskID string) error {
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
