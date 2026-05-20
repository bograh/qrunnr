package postgres

import (
	"context"

	"taskforge.dev/internal/model"
)

type QueueStore struct{}

func NewQueueStore() *QueueStore {
	return &QueueStore{}
}

func (s *QueueStore) Create(ctx context.Context, queue model.Queue) (model.Queue, error) {
	return model.Queue{}, model.ErrNotImplemented
}

func (s *QueueStore) List(ctx context.Context) ([]model.Queue, error) {
	return nil, model.ErrNotImplemented
}

func (s *QueueStore) GetByName(ctx context.Context, name string) (model.Queue, error) {
	return model.Queue{}, model.ErrNotImplemented
}

func (s *QueueStore) Delete(ctx context.Context, name string) error {
	return model.ErrNotImplemented
}
