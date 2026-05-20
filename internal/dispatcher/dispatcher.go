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
		Interval:      time.Second,
	}
}

func (d *Dispatcher) Run(ctx context.Context) error {
	interval := d.Interval
	if interval == 0 {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.DispatchOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	tasks, err := d.TaskStore.ClaimPending(ctx, 100)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		queueName := task.QueueID
		if err := d.DispatchStore.Enqueue(ctx, queueName, task); err != nil {
			return err
		}
	}
	return nil
}
