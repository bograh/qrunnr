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

func TestWorkerNackSchedulesRetry(t *testing.T) {
	_, taskStore, _, _, workerHandler, claimed := setupClaimedTask(t)
	before := time.Now().UTC()

	rec := postNack(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1","error":"downstream failed"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	task, err := taskStore.Get(context.Background(), "email_jobs", claimed.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusRetry {
		t.Fatalf("expected status %q, got %q", model.TaskStatusRetry, task.Status)
	}
	if task.Attempts != 1 {
		t.Fatalf("expected attempts 1, got %d", task.Attempts)
	}
	if task.LastError == nil || *task.LastError != "downstream failed" {
		t.Fatalf("expected last_error downstream failed, got %v", task.LastError)
	}
	if !task.ScheduledAt.After(before) {
		t.Fatalf("expected scheduled_at after nack time, got %s", task.ScheduledAt)
	}
}

func TestWorkerNackAppliesExponentialBackoffWithJitter(t *testing.T) {
	_, taskStoreA, _, taskHandlerA, workerHandlerA, claimedA := setupClaimedTask(t)
	_, taskStoreB, _, taskHandlerB, workerHandlerB, claimedB := setupClaimedTask(t)

	beforeA := time.Now().UTC()
	recA := postNack(workerHandlerA, `{"task_id":"`+claimedA.ID+`","worker_id":"worker-1","error":"temporary failure"}`)
	if recA.Code != http.StatusOK {
		t.Fatalf("expected first nack status %d, got %d: %s", http.StatusOK, recA.Code, recA.Body.String())
	}
	beforeB := time.Now().UTC()
	recB := postNack(workerHandlerB, `{"task_id":"`+claimedB.ID+`","worker_id":"worker-1","error":"temporary failure"}`)
	if recB.Code != http.StatusOK {
		t.Fatalf("expected second nack status %d, got %d: %s", http.StatusOK, recB.Code, recB.Body.String())
	}

	taskA := mustGetTask(t, taskHandlerA, "email_jobs", claimedA.ID)
	taskB := mustGetTask(t, taskHandlerB, "email_jobs", claimedB.ID)

	delayA := taskA.ScheduledAt.Sub(beforeA)
	delayB := taskB.ScheduledAt.Sub(beforeB)
	minDelay := 2 * time.Second
	maxDelay := 3 * time.Second
	if delayA < minDelay || delayA > maxDelay {
		t.Fatalf("expected first attempt delay in [2s, 3s], got %s", delayA)
	}
	if delayB < minDelay || delayB > maxDelay {
		t.Fatalf("expected second attempt delay in [2s, 3s], got %s", delayB)
	}
	if taskA.ScheduledAt.Equal(taskB.ScheduledAt) {
		t.Fatalf("expected jittered scheduled_at values to differ, both were %s", taskA.ScheduledAt)
	}

	storedA, err := taskStoreA.Get(context.Background(), "email_jobs", claimedA.ID)
	if err != nil {
		t.Fatalf("get first task: %v", err)
	}
	storedB, err := taskStoreB.Get(context.Background(), "email_jobs", claimedB.ID)
	if err != nil {
		t.Fatalf("get second task: %v", err)
	}
	if !storedA.ScheduledAt.Equal(taskA.ScheduledAt) || !storedB.ScheduledAt.Equal(taskB.ScheduledAt) {
		t.Fatal("expected retry scheduled_at to be visible through GET")
	}
}

func TestWorkerNackMovesTaskToDeadLetter(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)

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

	rec := postNack(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1","error":"permanent failure"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	if _, err := taskStore.Get(context.Background(), "email_jobs", claimed.ID); err != model.ErrTaskNotFound {
		t.Fatalf("expected task removed from active table, got err %v", err)
	}
	dead, ok := taskStore.DeadLetterTask(claimed.ID)
	if !ok {
		t.Fatal("expected dead-letter task")
	}
	if dead.LastError != "permanent failure" {
		t.Fatalf("expected last error permanent failure, got %q", dead.LastError)
	}
}

func mustGetTask(t *testing.T, handler *TaskHandler, queueName string, taskID string) model.Task {
	t.Helper()

	rec := getTask(handler, queueName, taskID)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected get task status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var task model.Task
	if err := json.NewDecoder(rec.Body).Decode(&task); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	return task
}

func TestWorkerNackWrongOwner(t *testing.T) {
	_, _, _, _, workerHandler, claimed := setupClaimedTask(t)

	rec := postNack(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-2","error":"failed"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d: %s", http.StatusForbidden, rec.Code, rec.Body.String())
	}
}

func TestWorkerNackRequiresError(t *testing.T) {
	_, _, _, _, workerHandler, claimed := setupClaimedTask(t)

	rec := postNack(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1","error":""}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
}

func TestReclaimExpiredTask(t *testing.T) {
	_, taskStore, _, _, workerHandler, claimed := setupClaimedTask(t)

	task, err := taskStore.Get(context.Background(), "email_jobs", claimed.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	expiredHeartbeat := time.Now().UTC().Add(-time.Minute)
	task.HeartbeatAt = &expiredHeartbeat
	if err := taskStore.ReplaceForTest(context.Background(), task); err != nil {
		t.Fatalf("replace task: %v", err)
	}

	reclaimed, err := taskStore.ReclaimExpired(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatalf("reclaim expired: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("expected 1 reclaimed task, got %d", len(reclaimed))
	}

	reclaimedTask, err := taskStore.Get(context.Background(), "email_jobs", claimed.ID)
	if err != nil {
		t.Fatalf("get reclaimed task: %v", err)
	}
	if reclaimedTask.Status != model.TaskStatusRetry {
		t.Fatalf("expected status %q, got %q", model.TaskStatusRetry, reclaimedTask.Status)
	}
	if reclaimedTask.Attempts != 1 {
		t.Fatalf("expected attempts 1, got %d", reclaimedTask.Attempts)
	}
	if reclaimedTask.LastError == nil || *reclaimedTask.LastError != "watchdog: heartbeat expired" {
		t.Fatalf("expected watchdog last_error, got %v", reclaimedTask.LastError)
	}

	ack := postAck(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1"}`)
	if ack.Code != http.StatusConflict {
		t.Fatalf("expected stale ack status %d, got %d: %s", http.StatusConflict, ack.Code, ack.Body.String())
	}
	nack := postNack(workerHandler, `{"task_id":"`+claimed.ID+`","worker_id":"worker-1","error":"late failure"}`)
	if nack.Code != http.StatusConflict {
		t.Fatalf("expected stale nack status %d, got %d: %s", http.StatusConflict, nack.Code, nack.Body.String())
	}
}

func TestQueueRateLimitCapsDispatchPerSecond(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5,"rate_limit":1}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}
	for i := 0; i < 2; i++ {
		enqueue := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"}}`)
		if enqueue.Code != http.StatusCreated {
			t.Fatalf("expected enqueue status %d, got %d: %s", http.StatusCreated, enqueue.Code, enqueue.Body.String())
		}
	}

	first := postClaim(workerHandler, `{"worker_id":"worker-1","queue":"email_jobs"}`)
	if first.Code != http.StatusOK {
		t.Fatalf("expected first claim status %d, got %d: %s", http.StatusOK, first.Code, first.Body.String())
	}
	second := postClaim(workerHandler, `{"worker_id":"worker-2","queue":"email_jobs"}`)
	if second.Code != http.StatusNoContent {
		t.Fatalf("expected second claim status %d while rate limited, got %d: %s", http.StatusNoContent, second.Code, second.Body.String())
	}

	time.Sleep(time.Second)
	third := postClaim(workerHandler, `{"worker_id":"worker-3","queue":"email_jobs"}`)
	if third.Code != http.StatusOK {
		t.Fatalf("expected claim after next window status %d, got %d: %s", http.StatusOK, third.Code, third.Body.String())
	}
}

func TestQueueWithoutRateLimitHasNoDispatchCeiling(t *testing.T) {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	queueHandler := NewQueueHandler(queueStore)
	taskHandler := NewTaskHandler(taskStore)
	workerHandler := NewWorkerHandler(taskStore)

	create := postQueue(queueHandler, `{"name":"email_jobs","max_retries":5}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected queue create status %d, got %d", http.StatusCreated, create.Code)
	}
	for i := 0; i < 2; i++ {
		enqueue := postTask(taskHandler, "email_jobs", `{"payload":{"action":"send_email"}}`)
		if enqueue.Code != http.StatusCreated {
			t.Fatalf("expected enqueue status %d, got %d: %s", http.StatusCreated, enqueue.Code, enqueue.Body.String())
		}
	}

	first := postClaim(workerHandler, `{"worker_id":"worker-1","queue":"email_jobs"}`)
	second := postClaim(workerHandler, `{"worker_id":"worker-2","queue":"email_jobs"}`)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("expected both unlimited claims to succeed, got %d and %d", first.Code, second.Code)
	}
}

func postClaim(handler *WorkerHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/workers/claim", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.ClaimTask(rec, req)
	return rec
}

func postNack(handler *WorkerHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/workers/nack", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.Nack(rec, req)
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

func (s expiredAckStore) Nack(ctx context.Context, taskID string, workerID string, errMessage string) error {
	return model.ErrNotImplemented
}
