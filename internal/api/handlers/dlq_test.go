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

func TestDeadLetterPromotionAppearsInDLQ(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)
	dlqHandler := NewDLQHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":0}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}
	enqueue := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"}}`)
	if enqueue.Code != http.StatusCreated {
		t.Fatalf("expected enqueue status %d, got %d: %s", http.StatusCreated, enqueue.Code, enqueue.Body.String())
	}
	claim := postClaim(workerHandler, `{"worker_id":"worker-1"}`)
	if claim.Code != http.StatusOK {
		t.Fatalf("expected claim status %d, got %d: %s", http.StatusOK, claim.Code, claim.Body.String())
	}

	var claimed model.ClaimedTask
	if err := json.NewDecoder(claim.Body).Decode(&claimed); err != nil {
		t.Fatalf("decode claim: %v", err)
	}

	nack := postNack(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1","error":"permanent failure"}`)
	if nack.Code != http.StatusOK {
		t.Fatalf("expected nack status %d, got %d: %s", http.StatusOK, nack.Code, nack.Body.String())
	}

	get := getTask(taskHandler, "email_jobs", claimed.ID)
	if get.Code != http.StatusNotFound {
		t.Fatalf("expected active task lookup status %d, got %d: %s", http.StatusNotFound, get.Code, get.Body.String())
	}

	list := getDLQ(dlqHandler, "email_jobs")
	if list.Code != http.StatusOK {
		t.Fatalf("expected DLQ status %d, got %d: %s", http.StatusOK, list.Code, list.Body.String())
	}

	var tasks []model.DeadLetterTask
	if err := json.NewDecoder(list.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode DLQ response: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 DLQ task, got %d", len(tasks))
	}

	dead := tasks[0]
	if dead.ID != claimed.ID {
		t.Fatalf("expected DLQ id %s, got %s", claimed.ID, dead.ID)
	}
	if dead.QueueID == "" {
		t.Fatal("expected queue_id to be preserved")
	}
	if string(dead.Payload) != `{"action":"send_email"}` {
		t.Fatalf("expected payload to be preserved, got %s", string(dead.Payload))
	}
	if dead.LastError != "permanent failure" {
		t.Fatalf("expected last_error permanent failure, got %q", dead.LastError)
	}
	if dead.Attempts != 0 {
		t.Fatalf("expected attempts 0, got %d", dead.Attempts)
	}
	if dead.DiedAt.IsZero() {
		t.Fatal("expected died_at to be set")
	}
}

func getDLQ(handler *DLQHandler, queueName string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/queues/"+queueName+"/dlq", nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("name", queueName)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	handler.ListDeadLetterTasks(rec, req)
	return rec
}
