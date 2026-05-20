package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/model"
	"taskforge.dev/internal/storage/memory"
)

func TestEnqueueImmediateTask(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	before := time.Now().UTC()
	rec := postTask(taskHandler, "email_jobs", `{"payload":{"user_id":"abc123","action":"send_welcome_email"}}`)
	after := time.Now().UTC()

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	var task model.Task
	if err := json.NewDecoder(rec.Body).Decode(&task); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if task.ID == "" {
		t.Fatal("expected task ID to be set")
	}
	if task.QueueID == "" {
		t.Fatal("expected queue ID to be set")
	}
	if task.Status != model.TaskStatusPending {
		t.Fatalf("expected status %q, got %q", model.TaskStatusPending, task.Status)
	}
	if task.ScheduledAt.Before(before) || task.ScheduledAt.After(after.Add(time.Second)) {
		t.Fatalf("expected scheduled_at to be current time, got %s", task.ScheduledAt)
	}
	if string(task.Payload) != `{"user_id":"abc123","action":"send_welcome_email"}` {
		t.Fatalf("expected payload to round-trip, got %s", string(task.Payload))
	}
}

func TestEnqueueTaskRejectsNonObjectPayload(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	tests := []string{
		`{"payload":"send_email"}`,
		`{"payload":123}`,
		`{"payload":true}`,
		`{"payload":null}`,
		`{"payload":["send_email"]}`,
		`{}`,
	}

	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			rec := postTask(taskHandler, "email_jobs", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestEnqueueTaskQueueNotFound(t *testing.T) {
	taskHandler := NewTaskHandler(memory.NewTaskStore(memory.NewQueueStore()))

	rec := postTask(taskHandler, "missing", `{"payload":{"action":"send_email"}}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNotFound, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "not found") {
		t.Fatalf("expected not found message, got %s", rec.Body.String())
	}
}

func TestEnqueuedTaskIsClaimableImmediately(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	rec := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	var created model.Task
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		tasks, err := taskStore.ClaimPending(context.Background(), 1)
		if err != nil {
			t.Fatalf("claim pending: %v", err)
		}
		if len(tasks) == 1 && tasks[0].ID == created.ID {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("expected enqueued task to be claimable within 1 second")
}

func TestEnqueueDelayedTaskIsNotClaimableBeforeScheduledAt(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	scheduledAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	rec := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"},"scheduled_at":"`+scheduledAt+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	tasks, err := taskStore.ClaimPending(context.Background(), 1)
	if err != nil {
		t.Fatalf("claim pending: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no claimable tasks before scheduled_at, got %d", len(tasks))
	}
}

func TestEnqueuePastScheduledTaskIsClaimableImmediately(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	scheduledAt := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	rec := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"},"scheduled_at":"`+scheduledAt+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	tasks, err := taskStore.ClaimPending(context.Background(), 1)
	if err != nil {
		t.Fatalf("claim pending: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected past-scheduled task to be claimable, got %d tasks", len(tasks))
	}
}

func TestEnqueueTaskRejectsInvalidScheduledAt(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	rec := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"},"scheduled_at":"not-a-time"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
}

func TestGetTaskIncludesScheduledAt(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	scheduledAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	rec := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"},"scheduled_at":"`+scheduledAt.Format(time.RFC3339)+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	var created model.Task
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	get := getTask(taskHandler, "email_jobs", created.ID)
	if get.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, get.Code, get.Body.String())
	}

	var found model.Task
	if err := json.NewDecoder(get.Body).Decode(&found); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !found.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("expected scheduled_at %s, got %s", scheduledAt, found.ScheduledAt)
	}
}

func TestEnqueueTaskDefaultPriority(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	rec := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	var task model.Task
	if err := json.NewDecoder(rec.Body).Decode(&task); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if task.Priority != 5 {
		t.Fatalf("expected default priority 5, got %d", task.Priority)
	}
}

func TestEnqueueTaskRejectsInvalidPriority(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	tests := []string{
		`{"payload":{"action":"send_email"},"priority":0}`,
		`{"payload":{"action":"send_email"},"priority":11}`,
	}

	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			rec := postTask(taskHandler, "email_jobs", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHighPriorityTaskIsClaimedBeforeLowPriorityTask(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	low := postTask(taskHandler, "email_jobs", `{"payload":{"action":"low"},"priority":10}`)
	if low.Code != http.StatusCreated {
		t.Fatalf("expected low priority create status %d, got %d", http.StatusCreated, low.Code)
	}
	high := postTask(taskHandler, "email_jobs", `{"payload":{"action":"high"},"priority":1}`)
	if high.Code != http.StatusCreated {
		t.Fatalf("expected high priority create status %d, got %d", http.StatusCreated, high.Code)
	}

	var highTask model.Task
	if err := json.NewDecoder(high.Body).Decode(&highTask); err != nil {
		t.Fatalf("decode high priority response: %v", err)
	}

	tasks, err := taskStore.ClaimPending(context.Background(), 2)
	if err != nil {
		t.Fatalf("claim pending: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 claimable tasks, got %d", len(tasks))
	}
	if tasks[0].ID != highTask.ID {
		t.Fatalf("expected high priority task first, got task %s with priority %d", tasks[0].ID, tasks[0].Priority)
	}
}

func postTask(handler *TaskHandler, queueName string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/queues/"+queueName+"/tasks", bytes.NewBufferString(body))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("name", queueName)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	handler.EnqueueTask(rec, req)
	return rec
}

func getTask(handler *TaskHandler, queueName string, taskID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/queues/"+queueName+"/tasks/"+taskID, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("name", queueName)
	routeCtx.URLParams.Add("id", taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	handler.GetTask(rec, req)
	return rec
}
