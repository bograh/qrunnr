// Package integration runs TaskForge end to end against real Postgres and
// Redis with the reference worker binary. Set TASKFORGE_TEST_DATABASE_URL and
// TASKFORGE_TEST_REDIS_URL (see `make test-integration`).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge.dev/internal/app"
	"taskforge.dev/internal/config"
	"taskforge.dev/internal/model"
)

type harness struct {
	t   *testing.T
	url string
}

func (h harness) call(method, path string, body any) (int, []byte) {
	h.t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, h.url+path, r)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func (h harness) mustCall(method, path string, body any, wantStatus int, into any) {
	h.t.Helper()
	status, out := h.call(method, path, body)
	if status != wantStatus {
		h.t.Fatalf("%s %s: expected %d, got %d: %s", method, path, wantStatus, status, out)
	}
	if into != nil {
		if err := json.Unmarshal(out, into); err != nil {
			h.t.Fatalf("%s %s: decode %s: %v", method, path, out, err)
		}
	}
}

func (h harness) stats(queue string) model.QueueStats {
	var s model.QueueStats
	h.mustCall(http.MethodGet, "/queues/"+queue+"/stats", nil, http.StatusOK, &s)
	return s
}

func (h harness) waitFor(what string, timeout time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

func TestEndToEnd(t *testing.T) {
	dbURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	redisURL := os.Getenv("TASKFORGE_TEST_REDIS_URL")
	if dbURL == "" || redisURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL and TASKFORGE_TEST_REDIS_URL must be set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Postgres: config.PostgresConfig{DatabaseURL: dbURL},
		Redis:    config.RedisConfig{RedisURL: redisURL},
		Worker: config.WorkerConfig{
			HeartbeatTimeout: time.Second,
			WatchdogInterval: 100 * time.Millisecond,
		},
		Retry: config.RetryConfig{BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond},
	}
	a, err := app.New(ctx, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	resetDatabase(t, dbURL)

	srv := httptest.NewServer(a.Handler)
	defer srv.Close()
	go func() { _ = a.Watchdog.Run(ctx) }()
	h := harness{t: t, url: srv.URL}

	// Health.
	h.mustCall(http.MethodGet, "/healthz", nil, http.StatusOK, nil)
	h.mustCall(http.MethodGet, "/readyz", nil, http.StatusOK, nil)

	// Queues.
	h.mustCall(http.MethodPost, "/queues", map[string]any{"name": "email_jobs", "max_retries": 2, "rate_limit": 50}, http.StatusCreated, nil)
	h.mustCall(http.MethodPost, "/queues", map[string]any{"name": "fragile", "max_retries": 0}, http.StatusCreated, nil)
	h.mustCall(http.MethodPost, "/queues", map[string]any{"name": "stuck", "max_retries": 2}, http.StatusCreated, nil)
	h.mustCall(http.MethodPost, "/queues", map[string]any{"name": "email_jobs", "max_retries": 2}, http.StatusConflict, nil)

	// A task claimed by a worker that then crashes: it never heartbeats, so
	// the watchdog must reclaim it for another worker.
	var stuck model.Task
	h.mustCall(http.MethodPost, "/queues/stuck/tasks", map[string]any{"payload": map[string]any{"job": "stuck"}}, http.StatusCreated, &stuck)
	var ghost model.ClaimedTask
	h.mustCall(http.MethodPost, "/workers/claim", map[string]any{"worker_id": "ghost", "queue": "stuck"}, http.StatusOK, &ghost)
	if ghost.ID != stuck.ID {
		t.Fatalf("expected ghost to claim the stuck task, got %+v", ghost)
	}
	if lease := time.Until(ghost.LeaseExpiresAt); lease <= 0 || lease > 2*time.Second {
		t.Fatalf("expected lease to match the 1s heartbeat timeout, got %s", lease)
	}

	// Regular work, a high-priority task, a delayed task, and a poison task.
	for i := range 4 {
		h.mustCall(http.MethodPost, "/queues/email_jobs/tasks", map[string]any{"payload": map[string]any{"n": i}}, http.StatusCreated, nil)
	}
	h.mustCall(http.MethodPost, "/queues/email_jobs/tasks", map[string]any{
		"payload": map[string]any{"n": "later"}, "scheduled_at": time.Now().Add(700 * time.Millisecond).UTC().Format(time.RFC3339Nano),
	}, http.StatusCreated, nil)
	var poison model.Task
	h.mustCall(http.MethodPost, "/queues/fragile/tasks", map[string]any{"payload": map[string]any{"fail": true}}, http.StatusCreated, &poison)

	// Run the real worker binary against the API.
	worker := startWorker(t, srv.URL)
	defer worker()

	h.waitFor("all email_jobs tasks done", 10*time.Second, func() bool {
		s := h.stats("email_jobs")
		return s.DoneLastMinute == 5 && s.Depth == 0 && s.ExecutingCount == 0
	})
	h.waitFor("poison task dead-lettered", 10*time.Second, func() bool { return h.stats("fragile").DLQDepth == 1 })
	h.waitFor("stuck task reclaimed and completed", 10*time.Second, func() bool {
		var task model.Task
		status, out := h.call(http.MethodGet, "/queues/stuck/tasks/"+stuck.ID, nil)
		return status == http.StatusOK && json.Unmarshal(out, &task) == nil && task.Status == model.TaskStatusDone
	})

	// The crashed worker's late ack is rejected.
	h.mustCall(http.MethodPost, "/workers/ack", map[string]any{"task_id": stuck.ID, "worker_id": "ghost"}, http.StatusForbidden, nil)
	var stuckTask model.Task
	h.mustCall(http.MethodGet, "/queues/stuck/tasks/"+stuck.ID, nil, http.StatusOK, &stuckTask)
	if stuckTask.Attempts != 1 || stuckTask.LastError == nil || *stuckTask.LastError != model.WatchdogError {
		t.Fatalf("expected reclaim recorded on the task, got %+v", stuckTask)
	}

	// DLQ inspection and replay through the API.
	var dlq []model.DeadLetterTask
	h.mustCall(http.MethodGet, "/queues/fragile/dlq", nil, http.StatusOK, &dlq)
	if len(dlq) != 1 || dlq[0].ID != poison.ID || !strings.Contains(dlq[0].LastError, "fail") {
		t.Fatalf("unexpected DLQ: %+v", dlq)
	}
	h.mustCall(http.MethodGet, "/queues/fragile/tasks/"+poison.ID, nil, http.StatusNotFound, nil)
	h.mustCall(http.MethodPost, "/queues/fragile/dlq/"+poison.ID+"/replay", nil, http.StatusCreated, nil)
	h.waitFor("replayed poison task dead-lettered again", 10*time.Second, func() bool { return h.stats("fragile").DLQDepth == 2 })
	status, page := h.call(http.MethodGet, "/admin/dlq?queue=fragile", nil)
	if status != http.StatusOK || !strings.Contains(string(page), "1×") {
		t.Fatalf("dlq page missing replay count (%d)", status)
	}
	h.mustCall(http.MethodDelete, "/queues/fragile/dlq/"+poison.ID, nil, http.StatusNoContent, nil)
	if s := h.stats("fragile"); s.DLQDepth != 1 {
		t.Fatalf("expected DLQ depth 1 after discard, got %+v", s)
	}

	// Workers are tracked in Postgres.
	var workers []model.WorkerStatus
	h.mustCall(http.MethodGet, "/workers", nil, http.StatusOK, &workers)
	found := false
	for _, w := range workers {
		found = found || w.WorkerID == "e2e-worker"
	}
	if !found {
		t.Fatalf("expected e2e-worker in active workers, got %+v", workers)
	}

	// Prometheus metrics reflect the run.
	status, metrics := h.call(http.MethodGet, "/metrics", nil)
	if status != http.StatusOK {
		t.Fatalf("metrics status %d", status)
	}
	for _, want := range []string{
		`taskforge_tasks_enqueued_total{queue="email_jobs"} 5`,
		`taskforge_tasks_completed_total{queue="email_jobs",status="done"} 5`,
		`taskforge_tasks_completed_total{queue="fragile",status="dead"} 2`,
		`taskforge_tasks_completed_total{queue="stuck",status="retry"} 1`,
		`taskforge_dlq_depth{queue="fragile"} 1`,
		`taskforge_queue_depth{queue="email_jobs",status="done"} 5`,
		`taskforge_worker_heartbeat_age_seconds{worker_id="e2e-worker"}`,
		`taskforge_task_execution_duration_seconds_bucket`,
	} {
		if !strings.Contains(string(metrics), want) {
			t.Errorf("metrics missing %q", want)
		}
	}

	// The dashboard renders against Postgres, including task history.
	status, page = h.call(http.MethodGet, "/admin/", nil)
	if status != http.StatusOK || !strings.Contains(string(page), "email_jobs") {
		t.Fatalf("overview: %d", status)
	}
	status, page = h.call(http.MethodGet, "/admin/tasks/"+stuck.ID, nil)
	if status != http.StatusOK || !strings.Contains(string(page), model.WatchdogError) || !strings.Contains(string(page), "executing → retry") {
		t.Fatalf("task detail missing watchdog history (%d)", status)
	}

	// Queues with only finished work can be deleted.
	h.mustCall(http.MethodDelete, "/queues/email_jobs", nil, http.StatusNoContent, nil)
}

func resetDatabase(t *testing.T, url string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(context.Background(), `TRUNCATE task_events, workers, dead_letter_tasks, tasks, queues`); err != nil {
		t.Fatal(err)
	}
}

// startWorker builds and runs cmd/worker against apiURL and returns a stop
// function.
func startWorker(t *testing.T, apiURL string) func() {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	bin := filepath.Join(t.TempDir(), "worker")

	build := exec.Command("go", "build", "-o", bin, "./cmd/worker")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}

	var logs bytes.Buffer
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"TASKFORGE_API_URL="+apiURL,
		"WORKER_ID=e2e-worker",
		"QUEUE=",
		"WORKER_POLL_INTERVAL=50ms",
		"HEARTBEAT_INTERVAL=200ms",
	)
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
		if t.Failed() {
			fmt.Fprintf(os.Stderr, "worker output:\n%s\n", logs.String())
		}
	}
}
