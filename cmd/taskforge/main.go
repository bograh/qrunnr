package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"taskforge.dev/internal/api/router"
	"taskforge.dev/internal/config"
	"taskforge.dev/internal/scheduler"
	"taskforge.dev/internal/storage/memory"
	"taskforge.dev/internal/storage/postgres"
	redisstore "taskforge.dev/internal/storage/redis"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mux := router.NewRouter()
	var watchdog *scheduler.Scheduler

	if cfg.Postgres.DatabaseURL != "" {
		pool, err := pgxpool.New(ctx, cfg.Postgres.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		if err := pool.Ping(ctx); err != nil {
			return err
		}

		queueStore := postgres.NewQueueStore(pool)
		taskStore := postgres.NewTaskStore(pool)
		if cfg.Redis.RedisURL != "" {
			options, err := redis.ParseURL(cfg.Redis.RedisURL)
			if err != nil {
				return err
			}
			client := redis.NewClient(options)
			defer client.Close()
			if err := client.Ping(ctx).Err(); err != nil {
				return err
			}
			taskStore.SetRateLimiter(redisstore.NewRateLimiter(client))
		}

		mux = router.NewRouterWithStores(queueStore, taskStore)
		watchdog = scheduler.New(taskStore, 15*time.Second)
		watchdog.TimeoutThreshold = cfg.Worker.HeartbeatTimeout
	} else {
		queueStore := memory.NewQueueStore()
		taskStore := memory.NewTaskStore(queueStore)
		mux = router.NewRouterWithStores(queueStore, taskStore)
		watchdog = scheduler.New(taskStore, 15*time.Second)
		watchdog.TimeoutThreshold = cfg.Worker.HeartbeatTimeout
	}

	go func() {
		if err := watchdog.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("watchdog stopped: %v", err)
		}
	}()

	server := &http.Server{
		Addr:              ":" + cfg.HTTP.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("TaskForge API listening on %s", server.Addr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
