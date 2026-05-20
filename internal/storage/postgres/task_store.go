package postgres

import (
	"context"
	"time"

	"taskforge.dev/internal/model"
)

type TaskStore struct{}

func NewTaskStore() *TaskStore {
	return &TaskStore{}
}

func (s *TaskStore) Create(ctx context.Context, queueName string, task model.Task) (model.Task, error) {
	return model.Task{}, model.ErrNotImplemented
}

func (s *TaskStore) Get(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	return model.Task{}, model.ErrNotImplemented
}

func (s *TaskStore) Cancel(ctx context.Context, queueName string, taskID string) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) ClaimPending(ctx context.Context, limit int) ([]model.Task, error) {
	return nil, model.ErrNotImplemented
}

func (s *TaskStore) MarkExecuting(ctx context.Context, taskID string, workerID string) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) Ack(ctx context.Context, taskID string, workerID string, result []byte) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) Nack(ctx context.Context, taskID string, workerID string, errMessage string) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) ListDue(ctx context.Context, limit int) ([]model.Task, error) {
	return nil, model.ErrNotImplemented
}

func (s *TaskStore) Reschedule(ctx context.Context, taskID string, scheduledAt time.Time) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error) {
	return nil, model.ErrNotImplemented
}

func (s *TaskStore) Stats(ctx context.Context, queueName string) (model.QueueStats, error) {
	return model.QueueStats{}, model.ErrNotImplemented
}
