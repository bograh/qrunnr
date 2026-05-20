package redis

import (
	"context"

	"taskforge.dev/internal/model"
)

type DispatchStore struct{}

func NewDispatchStore() *DispatchStore {
	return &DispatchStore{}
}

func (s *DispatchStore) Enqueue(ctx context.Context, queueName string, task model.Task) error {
	return model.ErrNotImplemented
}

func (s *DispatchStore) Dequeue(ctx context.Context, queueName string, workerID string) (model.Task, error) {
	return model.Task{}, model.ErrNotImplemented
}

func (s *DispatchStore) Remove(ctx context.Context, queueName string, taskID string) error {
	return model.ErrNotImplemented
}
