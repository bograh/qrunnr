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
	TaskStore        TaskStore
	Interval         time.Duration
	TimeoutThreshold time.Duration
}

func New(taskStore TaskStore, interval time.Duration) *Scheduler {
	if interval == 0 {
		interval = 15 * time.Second
	}
	return &Scheduler{
		TaskStore:        taskStore,
		Interval:         interval,
		TimeoutThreshold: 30 * time.Second,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.ReclaimExpired(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *Scheduler) SweepDue(ctx context.Context) error {
	return model.ErrNotImplemented
}

func (s *Scheduler) ReclaimExpired(ctx context.Context) error {
	_, err := s.TaskStore.ReclaimExpired(ctx, s.TimeoutThreshold)
	return err
}
