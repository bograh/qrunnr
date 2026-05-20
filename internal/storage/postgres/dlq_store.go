package postgres

import (
	"context"

	"taskforge.dev/internal/model"
)

type DLQStore struct{}

func NewDLQStore() *DLQStore {
	return &DLQStore{}
}

func (s *DLQStore) List(ctx context.Context, queueName string) ([]model.DeadLetterTask, error) {
	return nil, model.ErrNotImplemented
}

func (s *DLQStore) Replay(ctx context.Context, queueName string, taskID string) error {
	return model.ErrNotImplemented
}

func (s *DLQStore) Delete(ctx context.Context, queueName string, taskID string) error {
	return model.ErrNotImplemented
}
