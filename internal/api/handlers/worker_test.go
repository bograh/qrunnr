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

func TestWorkerHeartbeat(t *testing.T) {
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
	claim := postClaim(workerHandler, `{"worker_id":"worker-1"}`)
	if claim.Code != http.StatusOK {
		t.Fatalf("expected claim status %d, got %d: %s", http.StatusOK, claim.Code, claim.Body.String())
	}

	var claimed model.ClaimedTask
	if err := json.NewDecoder(claim.Body).Decode(&claimed); err != nil {
		t.Fatalf("decode claim: %v", err)
	}
	before := time.Now().UTC()

	rec := postHeartbeat(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected heartbeat status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	task, err := taskStore.Get(context.Background(), "email_jobs", claimed.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.HeartbeatAt == nil || task.HeartbeatAt.Before(before) {
		t.Fatalf("expected heartbeat_at to be updated after %s, got %v", before, task.HeartbeatAt)
	}
}

func TestWorkerHeartbeatWrongOwner(t *testing.T) {
	_, taskStore, _, _, workerHandler, claimed := setupClaimedTask(t)

	rec := postHeartbeat(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-2"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d: %s", http.StatusForbidden, rec.Code, rec.Body.String())
	}

	_ = taskStore
}

func TestWorkerHeartbeatInvalidStatus(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	workerHandler := NewWorkerHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}
	task := model.NewTask("", json.RawMessage(`{"action":"send_email"}`), time.Now().UTC(), 5)
	workerID := "worker-1"
	task.Status = model.TaskStatusClaimed
	task.ClaimedBy = &workerID
	created, err := taskStore.Create(context.Background(), "email_jobs", task)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	rec := postHeartbeat(workerHandler, `{"task_id":"`+created.ID+`","worker_id":"worker-1"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d: %s", http.StatusConflict, rec.Code, rec.Body.String())
	}
}

func TestWorkerHeartbeatTaskNotFound(t *testing.T) {
	workerHandler := NewWorkerHandler(memory.NewTaskStore(memory.NewQueueStore()))

	rec := postHeartbeat(workerHandler, `{"task_id":"missing","worker_id":"worker-1"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNotFound, rec.Code, rec.Body.String())
	}
}

func TestWorkerAck(t *testing.T) {
	_, taskStore, _, _, workerHandler, claimed := setupClaimedTask(t)

	rec := postAck(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1","result":{"ok":true}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	task, err := taskStore.Get(context.Background(), "email_jobs", claimed.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusDone {
		t.Fatalf("expected status %q, got %q", model.TaskStatusDone, task.Status)
	}
	if string(task.Result) != `{"ok":true}` {
		t.Fatalf("expected result to be stored, got %s", string(task.Result))
	}
}

func TestWorkerAckWrongOwner(t *testing.T) {
	_, _, _, _, workerHandler, claimed := setupClaimedTask(t)

	rec := postAck(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-2"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d: %s", http.StatusForbidden, rec.Code, rec.Body.String())
	}
}

func TestWorkerAckLeaseExpired(t *testing.T) {
	workerHandler := NewWorkerHandler(expiredAckStore{})

	rec := postAck(workerHandler, `{"task_id":"task-1","worker_id":"worker-1"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d: %s", http.StatusConflict, rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("lease expired")) {
		t.Fatalf("expected lease expired message, got %s", rec.Body.String())
	}
}

func TestDoneTaskIsNotRedispatched(t *testing.T) {
	_, taskStore, _, _, workerHandler, claimed := setupClaimedTask(t)

	rec := postAck(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected ack status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	tasks, err := taskStore.ClaimPending(context.Background(), 1)
	if err != nil {
		t.Fatalf("claim pending: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected done task not to be redispatched, got %d tasks", len(tasks))
	}
}

func postClaim(handler *WorkerHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/workers/claim", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.ClaimTask(rec, req)
	return rec
}

func postAck(handler *WorkerHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/workers/ack", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.Ack(rec, req)
	return rec
}

func postHeartbeat(handler *WorkerHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/workers/heartbeat", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.Heartbeat(rec, req)
	return rec
}

func setupClaimedTask(t *testing.T) (*memory.QueueStore, *memory.TaskStore, *QueueHandler, *TaskHandler, *WorkerHandler, model.ClaimedTask) {
	t.Helper()

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
	claim := postClaim(workerHandler, `{"worker_id":"worker-1"}`)
	if claim.Code != http.StatusOK {
		t.Fatalf("expected claim status %d, got %d: %s", http.StatusOK, claim.Code, claim.Body.String())
	}

	var claimed model.ClaimedTask
	if err := json.NewDecoder(claim.Body).Decode(&claimed); err != nil {
		t.Fatalf("decode claim: %v", err)
	}

	return queueStore, taskStore, queueHandler, taskHandler, workerHandler, claimed
}

type expiredAckStore struct{}

func (s expiredAckStore) ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error) {
	return model.ClaimedTask{}, false, model.ErrNotImplemented
}

func (s expiredAckStore) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	return model.ErrNotImplemented
}

func (s expiredAckStore) Ack(ctx context.Context, taskID string, workerID string, result []byte) error {
	return model.ErrTaskLeaseExpired
}
