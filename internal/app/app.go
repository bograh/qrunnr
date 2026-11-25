// Package app assembles a qrunnr server from configuration: storage,
// rate limiting, observability, the admin dashboard, the HTTP router, and the
// heartbeat watchdog.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"qrunnr.dev/internal/admin"
	"qrunnr.dev/internal/api/router"
	"qrunnr.dev/internal/config"
	"qrunnr.dev/internal/model"
	"qrunnr.dev/internal/observability"
	"qrunnr.dev/internal/scheduler"
	"qrunnr.dev/internal/storage/memory"
	"qrunnr.dev/internal/storage/postgres"
	redisstore "qrunnr.dev/internal/storage/redis"
	"qrunnr.dev/migrations"
)

// TaskStore is what every storage backend provides to the rest of the server.
type TaskStore interface {
	admin.TaskStore
	observability.GaugeSource
	scheduler.TaskStore
	ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error)
	Create(ctx context.Context, queueName string, task model.Task) (model.Task, error)
	Get(ctx context.Context, queueName string, taskID string) (model.Task, error)
	Heartbeat(ctx context.Context, taskID string, workerID string) error
	Ack(ctx context.Context, taskID string, workerID string, result []byte) error
	Nack(ctx context.Context, taskID string, workerID string, errMessage string) error
	ListDeadLetterTasks(ctx context.Context, queueName string) ([]model.DeadLetterTask, error)
	SetObserver(model.Observer)
	SetRetryPolicy(model.RetryPolicy)
}

type QueueStore interface {
	admin.QueueStore
	GetByName(ctx context.Context, name string) (model.Queue, error)
}

// App is a fully wired server. Call Close when done.
type App struct {
	Handler   http.Handler
	Watchdog  *scheduler.Scheduler
	Dashboard *admin.Handler
	Queues    QueueStore
	Tasks     TaskStore

	closers []func()
}

// Close releases database connections and ends dashboard streams.
func (a *App) Close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
}

// New connects to the configured backends (applying Postgres migrations) and
// wires every component. With no DATABASE_URL it uses in-memory storage.
func New(ctx context.Context, cfg config.Config, logger *slog.Logger) (*App, error) {
	a := &App{}
	var checks []func(context.Context) error
	fail := func(err error) (*App, error) {
		a.Close()
		return nil, err
	}

	if cfg.Postgres.DatabaseURL != "" {
		pool, err := pgxpool.New(ctx, cfg.Postgres.DatabaseURL)
		if err != nil {
			return fail(fmt.Errorf("connect postgres: %w", err))
		}
		a.closers = append(a.closers, pool.Close)
		if err := pool.Ping(ctx); err != nil {
			return fail(fmt.Errorf("ping postgres: %w", err))
		}
		applied, err := postgres.Migrate(ctx, pool, migrations.FS)
		if err != nil {
			return fail(fmt.Errorf("migrate postgres: %w", err))
		}
		if len(applied) > 0 {
			logger.Info("applied migrations", "versions", applied)
		}
		checks = append(checks, pool.Ping)

		tasks := postgres.NewTaskStore(pool)
		if cfg.Redis.RedisURL != "" {
			options, err := redis.ParseURL(cfg.Redis.RedisURL)
			if err != nil {
				return fail(fmt.Errorf("parse REDIS_URL: %w", err))
			}
			client := redis.NewClient(options)
			a.closers = append(a.closers, func() { _ = client.Close() })
			if err := client.Ping(ctx).Err(); err != nil {
				return fail(fmt.Errorf("ping redis: %w", err))
			}
			tasks.SetRateLimiter(redisstore.NewRateLimiter(client))
			checks = append(checks, func(ctx context.Context) error { return client.Ping(ctx).Err() })
			logger.Info("storage ready", "backend", "postgres", "rate_limiter", "redis")
		} else {
			logger.Warn("REDIS_URL not set: rate limits are enforced per API instance, not cluster-wide",
				"backend", "postgres", "rate_limiter", "local")
		}
		a.Queues, a.Tasks = postgres.NewQueueStore(pool), tasks
	} else {
		logger.Warn("DATABASE_URL not set: using in-memory storage; all data is lost on restart", "backend", "memory")
		queues := memory.NewQueueStore()
		a.Queues, a.Tasks = queues, memory.NewTaskStore(queues)
	}

	metrics := observability.NewMetrics(a.Tasks, logger)
	a.Tasks.SetObserver(observability.Fanout{observability.TransitionLogger{Logger: logger}, metrics})
	a.Tasks.SetRetryPolicy(model.RetryPolicy{BaseDelay: cfg.Retry.BaseDelay, MaxDelay: cfg.Retry.MaxDelay})

	if cfg.Admin.AuthEnabled() {
		logger.Info("admin dashboard enabled with basic auth", "path", "/admin/")
	} else {
		logger.Warn("admin dashboard enabled without authentication; set ADMIN_USERNAME and ADMIN_PASSWORD to protect it", "path", "/admin/")
	}
	a.Dashboard = admin.New(admin.Options{
		Queues:   a.Queues,
		Tasks:    a.Tasks,
		Logger:   logger,
		Username: cfg.Admin.Username,
		Password: cfg.Admin.Password,
	})
	a.closers = append(a.closers, a.Dashboard.Close)

	a.Handler = router.New(router.Options{
		QueueStore:    a.Queues,
		TaskStore:     a.Tasks,
		Logger:        logger,
		LeaseDuration: cfg.Worker.HeartbeatTimeout,
		Metrics:       metrics.Handler(),
		Admin:         a.Dashboard.Routes(),
		Ready: func(ctx context.Context) error {
			for _, check := range checks {
				if err := check(ctx); err != nil {
					return err
				}
			}
			return nil
		},
	})

	a.Watchdog = scheduler.New(a.Tasks, cfg.Worker.WatchdogInterval)
	a.Watchdog.TimeoutThreshold = cfg.Worker.HeartbeatTimeout
	a.Watchdog.Logger = logger
	return a, nil
}
