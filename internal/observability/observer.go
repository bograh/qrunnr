// Package observability wires structured logging and Prometheus metrics to
// task state transitions.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"qrunnr.dev/internal/model"
)

// NewLogger returns a JSON slog logger writing to stderr at the given level
// ("debug", "info", "warn", "error"; default "info").
func NewLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

// TransitionLogger emits one structured log line per task state transition.
type TransitionLogger struct {
	Logger *slog.Logger
}

func (l TransitionLogger) TaskTransition(t model.Transition) {
	from := string(t.From)
	if from == "" {
		from = "none"
	}
	l.Logger.LogAttrs(context.Background(), slog.LevelInfo, "task transition",
		slog.String("task_id", t.TaskID),
		slog.String("queue", t.Queue),
		slog.String("worker_id", t.WorkerID),
		slog.String("from_status", from),
		slog.String("to_status", string(t.To)),
		slog.Int64("duration_ms", t.Duration.Milliseconds()),
	)
}

// Fanout forwards each transition to every observer in order.
type Fanout []model.Observer

func (f Fanout) TaskTransition(t model.Transition) {
	for _, o := range f {
		o.TaskTransition(t)
	}
}
