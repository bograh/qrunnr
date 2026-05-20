package dispatcher

import (
	"context"
	"time"

	"taskforge.dev/internal/model"
)

type TaskStore interface {
	ClaimPending(ctx context.Context, limit int) ([]model.Task, error)
	MarkExecuting(ctx context.Context, taskID string, workerID string) error
}

type DispatchStore interface {
	Enqueue(ctx context.Context, queueName string, task model.Task) error
	Dequeue(ctx context.Context, queueName string, workerID string) (model.Task, error)
}

type RateLimiter interface {
	Allow(ctx context.Context, queueName string, limit int) (bool, error)
}

type Dispatcher struct {
	TaskStore     TaskStore
	DispatchStore DispatchStore
	RateLimiter   RateLimiter
	Interval      time.Duration
}

func New(taskStore TaskStore, dispatchStore DispatchStore, rateLimiter RateLimiter) *Dispatcher {
	return &Dispatcher{
		TaskStore:     taskStore,
		DispatchStore: dispatchStore,
		RateLimiter:   rateLimiter,
	}
}

func (d *Dispatcher) Run(ctx context.Context) error {
	return model.ErrNotImplemented
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	return model.ErrNotImplemented
}
