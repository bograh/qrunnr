package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taskforge.dev/internal/app"
	"taskforge.dev/internal/config"
	"taskforge.dev/internal/observability"
)

func main() {
	if err := run(); err != nil {
		slog.Error("taskforge exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer a.Close()

	go func() {
		if err := a.Watchdog.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("watchdog stopped", "error", err)
		}
	}()

	server := &http.Server{
		Addr:              ":" + cfg.HTTP.Port,
		Handler:           a.Handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout: /admin/events holds server-sent event streams open.
	}
	server.RegisterOnShutdown(a.Dashboard.Close)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("TaskForge API listening", "addr", server.Addr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
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
