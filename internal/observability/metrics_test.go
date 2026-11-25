package observability

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"taskforge.dev/internal/model"
)

type fakeSource struct{}

func (fakeSource) StatusCounts(context.Context) ([]model.StatusCount, error) {
	return []model.StatusCount{
		{Queue: "email", Status: model.TaskStatusPending, Count: 7},
		{Queue: "email", Status: model.TaskStatusDead, Count: 2},
	}, nil
}

func (fakeSource) ActiveWorkers(context.Context) ([]model.WorkerStatus, error) {
	return []model.WorkerStatus{{WorkerID: "w1", LastHeartbeatAt: time.Now().Add(-3 * time.Second)}}, nil
}

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics(fakeSource{}, slog.New(slog.DiscardHandler))
	m.TaskTransition(model.Transition{Queue: "email", To: model.TaskStatusPending})
	m.TaskTransition(model.Transition{Queue: "email", From: model.TaskStatusPending, To: model.TaskStatusClaimed})
	m.TaskTransition(model.Transition{Queue: "email", From: model.TaskStatusExecuting, To: model.TaskStatusDone, Duration: 1500 * time.Millisecond})
	m.TaskTransition(model.Transition{Queue: "email", From: model.TaskStatusExecuting, To: model.TaskStatusRetry, Duration: time.Second})

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	out := string(body)

	for _, want := range []string{
		`taskforge_tasks_enqueued_total{queue="email"} 1`,
		`taskforge_tasks_completed_total{queue="email",status="done"} 1`,
		`taskforge_tasks_completed_total{queue="email",status="retry"} 1`,
		`taskforge_task_execution_duration_seconds_count{queue="email"} 2`,
		`taskforge_queue_depth{queue="email",status="pending"} 7`,
		`taskforge_queue_depth{queue="email",status="executing"} 0`,
		`taskforge_dlq_depth{queue="email"} 2`,
		`taskforge_worker_heartbeat_age_seconds{worker_id="w1"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
	if t.Failed() {
		t.Log(out)
	}
}

func TestTransitionLoggerFields(t *testing.T) {
	var buf strings.Builder
	l := TransitionLogger{Logger: slog.New(slog.NewJSONHandler(&buf, nil))}
	l.TaskTransition(model.Transition{TaskID: "t1", Queue: "q", WorkerID: "w", From: model.TaskStatusExecuting, To: model.TaskStatusDone, Duration: 42 * time.Millisecond})
	for _, want := range []string{`"task_id":"t1"`, `"queue":"q"`, `"worker_id":"w"`, `"from_status":"executing"`, `"to_status":"done"`, `"duration_ms":42`} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("expected %s in %s", want, buf.String())
		}
	}
}
