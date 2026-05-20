package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge.dev/internal/model"
)

type QueueStore struct {
	pool *pgxpool.Pool
}

func NewQueueStore(pool *pgxpool.Pool) *QueueStore {
	return &QueueStore{pool: pool}
}

func (s *QueueStore) Create(ctx context.Context, queue model.Queue) (model.Queue, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO queues (name, max_retries, rate_limit)
		VALUES ($1, $2, $3)
		RETURNING id::text, name, max_retries, rate_limit, created_at
	`, queue.Name, queue.MaxRetries, queue.RateLimit)

	created, err := scanQueue(row)
	if isUniqueViolation(err) {
		return model.Queue{}, model.ErrQueueAlreadyExists
	}
	return created, err
}

func (s *QueueStore) List(ctx context.Context) ([]model.Queue, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, name, max_retries, rate_limit, created_at
		FROM queues
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	queues := []model.Queue{}
	for rows.Next() {
		queue, err := scanQueue(rows)
		if err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, rows.Err()
}

func (s *QueueStore) GetByName(ctx context.Context, name string) (model.Queue, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id::text, name, max_retries, rate_limit, created_at
		FROM queues
		WHERE name = $1
	`, name)

	queue, err := scanQueue(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Queue{}, model.ErrQueueNotFound
	}
	return queue, err
}

func (s *QueueStore) UpdateRateLimit(ctx context.Context, name string, rateLimit *int) (model.Queue, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE queues
		SET rate_limit = $2
		WHERE name = $1
		RETURNING id::text, name, max_retries, rate_limit, created_at
	`, name, rateLimit)

	queue, err := scanQueue(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Queue{}, model.ErrQueueNotFound
	}
	return queue, err
}

func (s *QueueStore) Delete(ctx context.Context, name string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var queueID string
	err = tx.QueryRow(ctx, `SELECT id::text FROM queues WHERE name = $1`, name).Scan(&queueID)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrQueueNotFound
	}
	if err != nil {
		return err
	}

	var activeCount int
	err = tx.QueryRow(ctx, `
		SELECT count(*)
		FROM tasks
		WHERE queue_id = $1 AND status IN ('pending', 'claimed', 'executing')
	`, queueID).Scan(&activeCount)
	if err != nil {
		return err
	}
	if activeCount > 0 {
		return model.ErrQueueHasActiveTasks
	}

	if _, err := tx.Exec(ctx, `DELETE FROM queues WHERE id = $1`, queueID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type queueScanner interface {
	Scan(dest ...any) error
}

func scanQueue(row queueScanner) (model.Queue, error) {
	var queue model.Queue
	if err := row.Scan(&queue.ID, &queue.Name, &queue.MaxRetries, &queue.RateLimit, &queue.CreatedAt); err != nil {
		return model.Queue{}, err
	}
	return queue, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
