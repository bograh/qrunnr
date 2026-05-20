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

func TestReplayDeadLetterTask(t *testing.T) {
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

	replay := postReplayDLQ(dlqHandler, "email_jobs", claimed.ID)
	if replay.Code != http.StatusCreated {
		t.Fatalf("expected replay status %d, got %d: %s", http.StatusCreated, replay.Code, replay.Body.String())
	}
	var replayed model.Task
	if err := json.NewDecoder(replay.Body).Decode(&replayed); err != nil {
		t.Fatalf("decode replayed task: %v", err)
	}
	if replayed.ID == claimed.ID {
		t.Fatal("expected replayed task to have a new id")
	}
	if replayed.Status != model.TaskStatusPending || replayed.Attempts != 0 {
		t.Fatalf("expected fresh pending task, got status %q attempts %d", replayed.Status, replayed.Attempts)
	}
	if string(replayed.Payload) != `{"action":"send_email"}` {
		t.Fatalf("expected payload to be preserved, got %s", string(replayed.Payload))
	}

	list := getDLQ(dlqHandler, "email_jobs")
	if list.Code != http.StatusOK {
		t.Fatalf("expected DLQ status %d, got %d: %s", http.StatusOK, list.Code, list.Body.String())
	}
	var tasks []model.DeadLetterTask
	if err := json.NewDecoder(list.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode DLQ response: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ReplayedAt == nil {
		t.Fatalf("expected original DLQ entry to remain with replayed_at set, got %+v", tasks)
	}
}

func TestReplayDeadLetterTaskNotFound(t *testing.T) {
	queueStore := memory.NewQueueStore()
	queueHandler := NewQueueHandler(queueStore)
	dlqHandler := NewDLQHandler(memory.NewTaskStore(queueStore))

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":0}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	replay := postReplayDLQ(dlqHandler, "email_jobs", "missing")
	if replay.Code != http.StatusNotFound {
		t.Fatalf("expected replay status %d, got %d: %s", http.StatusNotFound, replay.Code, replay.Body.String())
	}
}

func TestDeleteDeadLetterTask(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)
	dlqHandler := NewDLQHandler(taskStore)
	statsHandler := NewStatsHandler(taskStore)

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

	beforeStats := getQueueStats(statsHandler, "email_jobs")
	var before model.QueueStats
	if err := json.NewDecoder(beforeStats.Body).Decode(&before); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if before.DLQDepth != 1 {
		t.Fatalf("expected dlq depth 1 before delete, got %d", before.DLQDepth)
	}

	del := deleteDLQ(dlqHandler, "email_jobs", claimed.ID)
	if del.Code != http.StatusNoContent {
		t.Fatalf("expected delete status %d, got %d: %s", http.StatusNoContent, del.Code, del.Body.String())
	}

	list := getDLQ(dlqHandler, "email_jobs")
	var tasks []model.DeadLetterTask
	if err := json.NewDecoder(list.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode DLQ response: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected empty DLQ after delete, got %d", len(tasks))
	}

	afterStats := getQueueStats(statsHandler, "email_jobs")
	var after model.QueueStats
	if err := json.NewDecoder(afterStats.Body).Decode(&after); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if after.DLQDepth != 0 {
		t.Fatalf("expected dlq depth 0 after delete, got %d", after.DLQDepth)
	}
}

func TestDeleteDeadLetterTaskNotFound(t *testing.T) {
	queueStore := memory.NewQueueStore()
	queueHandler := NewQueueHandler(queueStore)
	dlqHandler := NewDLQHandler(memory.NewTaskStore(queueStore))

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":0}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}

	del := deleteDLQ(dlqHandler, "email_jobs", "missing")
	if del.Code != http.StatusNotFound {
		t.Fatalf("expected delete status %d, got %d: %s", http.StatusNotFound, del.Code, del.Body.String())
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

func postReplayDLQ(handler *DLQHandler, queueName string, taskID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/queues/"+queueName+"/dlq/"+taskID+"/replay", nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("name", queueName)
	routeCtx.URLParams.Add("id", taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	handler.ReplayDeadLetterTask(rec, req)
	return rec
}

func deleteDLQ(handler *DLQHandler, queueName string, taskID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, "/queues/"+queueName+"/dlq/"+taskID, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("name", queueName)
	routeCtx.URLParams.Add("id", taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	handler.DeleteDeadLetterTask(rec, req)
	return rec
}
