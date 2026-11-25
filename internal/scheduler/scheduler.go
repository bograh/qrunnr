// Package scheduler runs the heartbeat watchdog that reclaims tasks from
// crashed or stuck workers.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"qrunnr.dev/internal/model"
)

type TaskStore interface {
	ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error)
}

// workerPruner is implemented by stores that persist worker state and need
// old rows removed periodically.
type workerPruner interface {
	PruneWorkers(ctx context.Context, olderThan time.Duration) error
}

// workerRetention is how long a silent worker is kept before it is pruned.
const workerRetention = 10 * time.Minute

type Scheduler struct {
	TaskStore        TaskStore
	Interval         time.Duration
	TimeoutThreshold time.Duration
	Logger           *slog.Logger
}

func New(taskStore TaskStore, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &Scheduler{
		TaskStore:        taskStore,
		Interval:         interval,
		TimeoutThreshold: 30 * time.Second,
		Logger:           slog.Default(),
	}
}

// Run sweeps for expired tasks every Interval until ctx is cancelled. A
// failed sweep is logged and retried on the next tick; it never stops the
// loop.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.Tick(ctx)
		}
	}
}

// Tick performs one watchdog sweep.
func (s *Scheduler) Tick(ctx context.Context) {
	logger := s.logger()

	reclaimed, err := s.TaskStore.ReclaimExpired(ctx, s.TimeoutThreshold)
	if err != nil {
		if ctx.Err() == nil {
			logger.Error("watchdog sweep failed", "error", err)
		}
	} else if len(reclaimed) > 0 {
		logger.Info("watchdog reclaimed expired tasks", "count", len(reclaimed))
	}

	if pruner, ok := s.TaskStore.(workerPruner); ok {
		if err := pruner.PruneWorkers(ctx, workerRetention); err != nil && ctx.Err() == nil {
			logger.Error("worker prune failed", "error", err)
		}
	}
}

// ReclaimExpired runs a single reclaim pass and returns its error.
func (s *Scheduler) ReclaimExpired(ctx context.Context) error {
	_, err := s.TaskStore.ReclaimExpired(ctx, s.TimeoutThreshold)
	return err
}

func (s *Scheduler) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}
