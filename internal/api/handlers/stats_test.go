package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/model"
	"taskforge.dev/internal/storage/memory"
)

func TestQueueStatsNoActivity(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	statsHandler := NewStatsHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	rec := getQueueStats(statsHandler, "email_jobs")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected stats status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var stats model.QueueStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats != (model.QueueStats{}) {
		t.Fatalf("expected zero stats, got %+v", stats)
	}
}

func TestQueueStatsCountsActivity(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)
	statsHandler := NewStatsHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":0}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}
	pending := postTask(taskHandler, "email_jobs", `{"payload":{"action":"pending"}}`)
	if pending.Code != http.StatusCreated {
		t.Fatalf("expected pending enqueue status %d, got %d", http.StatusCreated, pending.Code)
	}
	executing := postTask(taskHandler, "email_jobs", `{"payload":{"action":"executing"}}`)
	if executing.Code != http.StatusCreated {
		t.Fatalf("expected executing enqueue status %d, got %d", http.StatusCreated, executing.Code)
	}
	done := postTask(taskHandler, "email_jobs", `{"payload":{"action":"done"}}`)
	if done.Code != http.StatusCreated {
		t.Fatalf("expected done enqueue status %d, got %d", http.StatusCreated, done.Code)
	}

	claimExecuting := postClaim(workerHandler, `{"worker_id":"worker-1","queue":"email_jobs"}`)
	if claimExecuting.Code != http.StatusOK {
		t.Fatalf("expected claim status %d, got %d: %s", http.StatusOK, claimExecuting.Code, claimExecuting.Body.String())
	}
	claimDone := postClaim(workerHandler, `{"worker_id":"worker-2","queue":"email_jobs"}`)
	if claimDone.Code != http.StatusOK {
		t.Fatalf("expected claim status %d, got %d: %s", http.StatusOK, claimDone.Code, claimDone.Body.String())
	}
	var doneClaim model.ClaimedTask
	if err := json.NewDecoder(claimDone.Body).Decode(&doneClaim); err != nil {
		t.Fatalf("decode claim: %v", err)
	}
	ack := postAck(workerHandler, `{"task_id":"`+doneClaim.ID+`","worker_id":"worker-2"}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("expected ack status %d, got %d: %s", http.StatusOK, ack.Code, ack.Body.String())
	}

	rec := getQueueStats(statsHandler, "email_jobs")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected stats status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var stats model.QueueStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Depth != 1 || stats.ExecutingCount != 1 || stats.DoneLastMinute != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func getQueueStats(handler *StatsHandler, queueName string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/queues/"+queueName+"/stats", nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("name", queueName)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	handler.GetQueueStats(rec, req)
	return rec
}
