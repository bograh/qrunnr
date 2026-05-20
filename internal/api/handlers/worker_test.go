package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"taskforge.dev/internal/model"
	"taskforge.dev/internal/storage/memory"
)

func TestWorkerClaimsTask(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}
	enqueue := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"},"priority":1}`)
	if enqueue.Code != http.StatusCreated {
		t.Fatalf("expected enqueue status %d, got %d: %s", http.StatusCreated, enqueue.Code, enqueue.Body.String())
	}

	rec := postClaim(workerHandler, `{"worker_id":"worker-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var claimed model.ClaimedTask
	if err := json.NewDecoder(rec.Body).Decode(&claimed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if claimed.ID == "" {
		t.Fatal("expected task ID")
	}
	if claimed.Queue != "email_jobs" {
		t.Fatalf("expected queue email_jobs, got %q", claimed.Queue)
	}
	if claimed.Priority != 1 {
		t.Fatalf("expected priority 1, got %d", claimed.Priority)
	}
	if claimed.LeaseExpiresAt.Before(time.Now().UTC()) {
		t.Fatalf("expected future lease expiry, got %s", claimed.LeaseExpiresAt)
	}

	task, err := taskStore.Get(context.Background(), "email_jobs", claimed.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusExecuting {
		t.Fatalf("expected task status %q, got %q", model.TaskStatusExecuting, task.Status)
	}
	if task.ClaimedBy == nil || *task.ClaimedBy != "worker-1" {
		t.Fatalf("expected claimed_by worker-1, got %v", task.ClaimedBy)
	}
}

func TestWorkerClaimNoTasks(t *testing.T) {
	workerHandler := NewWorkerHandler(memory.NewTaskStore(memory.NewQueueStore()))

	rec := postClaim(workerHandler, `{"worker_id":"worker-1"}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNoContent, rec.Code, rec.Body.String())
	}
}

func TestConcurrentWorkersDoNotClaimSameTask(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}
	enqueue := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"}}`)
	if enqueue.Code != http.StatusCreated {
		t.Fatalf("expected enqueue status %d, got %d: %s", http.StatusCreated, enqueue.Code, enqueue.Body.String())
	}

	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, 2)
	for _, workerID := range []string{"worker-1", "worker-2"} {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			results <- postClaim(workerHandler, `{"worker_id":"`+workerID+`"}`)
		}(workerID)
	}
	wg.Wait()
	close(results)

	claimedCount := 0
	claimedIDs := map[string]bool{}
	for rec := range results {
		switch rec.Code {
		case http.StatusOK:
			claimedCount++
			var claimed model.ClaimedTask
			if err := json.NewDecoder(rec.Body).Decode(&claimed); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if claimedIDs[claimed.ID] {
				t.Fatalf("task %s was claimed twice", claimed.ID)
			}
			claimedIDs[claimed.ID] = true
		case http.StatusNoContent:
		default:
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}
	}
	if claimedCount != 1 {
		t.Fatalf("expected exactly one worker to claim the task, got %d", claimedCount)
	}
}

func postClaim(handler *WorkerHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/workers/claim", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.ClaimTask(rec, req)
	return rec
}
