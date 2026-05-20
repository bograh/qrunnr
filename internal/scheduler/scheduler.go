package scheduler

import (
	"context"
	"time"

	"taskforge.dev/internal/model"
)

type TaskStore interface {
	ListDue(ctx context.Context, limit int) ([]model.Task, error)
	Reschedule(ctx context.Context, taskID string, scheduledAt time.Time) error
	ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error)
}

type Scheduler struct {
	TaskStore TaskStore
	Interval  time.Duration
}

func New(taskStore TaskStore, interval time.Duration) *Scheduler {
	return &Scheduler{
		TaskStore: taskStore,
		Interval:  interval,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	return model.ErrNotImplemented
}

func (s *Scheduler) SweepDue(ctx context.Context) error {
	return model.ErrNotImplemented
}

func (s *Scheduler) ReclaimExpired(ctx context.Context) error {
	return model.ErrNotImplemented
}
