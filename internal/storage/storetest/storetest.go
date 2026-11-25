// Package storetest is a behavioural test suite shared by every storage
// backend, so the in-memory and Postgres stores are held to the same contract.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"taskforge.dev/internal/model"
	"taskforge.dev/internal/ratelimit"
)

type QueueStore interface {
	Create(ctx context.Context, queue model.Queue) (model.Queue, error)
	List(ctx context.Context) ([]model.Queue, error)
	GetByName(ctx context.Context, name string) (model.Queue, error)
	UpdateRateLimit(ctx context.Context, name string, rateLimit *int) (model.Queue, error)
	Delete(ctx context.Context, name string) error
}

type TaskStore interface {
	Create(ctx context.Context, queueName string, task model.Task) (model.Task, error)
	Get(ctx context.Context, queueName string, taskID string) (model.Task, error)
	GetTaskByID(ctx context.Context, taskID string) (model.TaskSummary, error)
	ListTasks(ctx context.Context, filter model.TaskFilter) ([]model.TaskSummary, error)
	TaskEvents(ctx context.Context, taskID string) ([]model.TaskEvent, error)
	Cancel(ctx context.Context, queueName string, taskID string) error
	ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error)
	Heartbeat(ctx context.Context, taskID string, workerID string) error
	Ack(ctx context.Context, taskID string, workerID string, result []byte) error
	Nack(ctx context.Context, taskID string, workerID string, errMessage string) error
	ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error)
	Stats(ctx context.Context, queueName string) (model.QueueStats, error)
	StatusCounts(ctx context.Context) ([]model.StatusCount, error)
	ListDeadLetterTasks(ctx context.Context, queueName string) ([]model.DeadLetterTask, error)
	ListDeadLetters(ctx context.Context, filter model.DeadLetterFilter) ([]model.DeadLetterTask, error)
	ReplayDeadLetterTask(ctx context.Context, queueName string, taskID string) (model.Task, error)
	DeleteDeadLetterTask(ctx context.Context, queueName string, taskID string) error
	ActiveWorkers(ctx context.Context) ([]model.WorkerStatus, error)
	SetObserver(model.Observer)
	SetRetryPolicy(model.RetryPolicy)
	SetRateLimiter(ratelimit.Limiter)
}

// Factory returns empty stores for one subtest.
type Factory func(t *testing.T) (QueueStore, TaskStore)

const lease = 30 * time.Second

// Run executes the full suite against stores produced by newStores.
func Run(t *testing.T, newStores Factory) {
	tests := []struct {
		name string
		fn   func(t *testing.T, qs QueueStore, ts TaskStore)
	}{
		{"QueueLifecycle", testQueueLifecycle},
		{"DeleteQueueRemovesFinishedTasks", testDeleteQueueRemovesFinishedTasks},
		{"DeleteQueueBlockedByRetryingTask", testDeleteQueueBlockedByRetryingTask},
		{"EnqueueAndGet", testEnqueueAndGet},
		{"UnknownIDsAreNotFound", testUnknownIDsAreNotFound},
		{"ClaimOrderAndSchedule", testClaimOrderAndSchedule},
		{"ClaimUnknownQueue", testClaimUnknownQueue},
		{"ConcurrentClaimsAreExclusive", testConcurrentClaimsAreExclusive},
		{"HeartbeatAckOwnership", testHeartbeatAckOwnership},
		{"RetriedTaskIsRedispatched", testRetriedTaskIsRedispatched},
		{"DeadLetterLifecycle", testDeadLetterLifecycle},
		{"WatchdogReclaim", testWatchdogReclaim},
		{"WatchdogDeadLettersExhaustedTask", testWatchdogDeadLettersExhaustedTask},
		{"RateLimitSkipsToOtherQueues", testRateLimitSkipsToOtherQueues},
		{"Cancel", testCancel},
		{"StatsAndCounts", testStatsAndCounts},
		{"ActiveWorkers", testActiveWorkers},
		{"ListTasksFilters", testListTasksFilters},
		{"HistoryAndObserver", testHistoryAndObserver},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			qs, ts := newStores(t)
			// Retries become due almost immediately so tests can re-claim them.
			ts.SetRetryPolicy(model.RetryPolicy{BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})
			tc.fn(t, qs, ts)
		})
	}
}

// ---------------------------------------------------------------------------
// helpers

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func intPtr(n int) *int { return &n }

func mustQueue(t *testing.T, qs QueueStore, name string, maxRetries int, rateLimit *int) model.Queue {
	t.Helper()
	q, err := qs.Create(ctx(t), model.NewQueue(name, maxRetries, rateLimit))
	if err != nil {
		t.Fatalf("create queue %s: %v", name, err)
	}
	return q
}

func mustEnqueue(t *testing.T, ts TaskStore, queue string, priority int, scheduledAt time.Time) model.Task {
	t.Helper()
	task, err := ts.Create(ctx(t), queue, model.NewTask("", json.RawMessage(`{"n":1}`), scheduledAt, priority))
	if err != nil {
		t.Fatalf("enqueue to %s: %v", queue, err)
	}
	return task
}

func mustClaim(t *testing.T, ts TaskStore, worker, queue string) model.ClaimedTask {
	t.Helper()
	claimed, ok, err := ts.ClaimForWorker(ctx(t), worker, queue, lease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !ok {
		t.Fatalf("expected a task to claim for %s on %q", worker, queue)
	}
	return claimed
}

func expectNoClaim(t *testing.T, ts TaskStore, worker, queue string) {
	t.Helper()
	claimed, ok, err := ts.ClaimForWorker(ctx(t), worker, queue, lease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if ok {
		t.Fatalf("expected no claimable task, got %s", claimed.ID)
	}
}

func expectErr(t *testing.T, got error, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("expected error %v, got %v", want, got)
	}
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

type recorder struct {
	mu          sync.Mutex
	transitions []model.Transition
}

func (r *recorder) TaskTransition(t model.Transition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions = append(r.transitions, t)
}

func (r *recorder) all() []model.Transition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]model.Transition{}, r.transitions...)
}

// ---------------------------------------------------------------------------
// tests

func testQueueLifecycle(t *testing.T, qs QueueStore, _ TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "beta", 3, nil)
	created := mustQueue(t, qs, "alpha", 5, intPtr(10))
	if created.ID == "" || created.Name != "alpha" || created.MaxRetries != 5 || *created.RateLimit != 10 {
		t.Fatalf("unexpected created queue: %+v", created)
	}

	_, err := qs.Create(c, model.NewQueue("alpha", 1, nil))
	expectErr(t, err, model.ErrQueueAlreadyExists)

	list, err := qs.List(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "alpha" || list[1].Name != "beta" {
		t.Fatalf("expected queues sorted by name, got %+v", list)
	}

	updated, err := qs.UpdateRateLimit(c, "alpha", nil)
	if err != nil || updated.RateLimit != nil {
		t.Fatalf("clear rate limit: %+v %v", updated, err)
	}
	_, err = qs.UpdateRateLimit(c, "missing", intPtr(1))
	expectErr(t, err, model.ErrQueueNotFound)

	if err := qs.Delete(c, "beta"); err != nil {
		t.Fatalf("delete empty queue: %v", err)
	}
	expectErr(t, qs.Delete(c, "beta"), model.ErrQueueNotFound)
	_, err = qs.GetByName(c, "beta")
	expectErr(t, err, model.ErrQueueNotFound)
}

func testDeleteQueueRemovesFinishedTasks(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 0, nil)
	pending := mustEnqueue(t, ts, "jobs", 5, time.Now())
	expectErr(t, qs.Delete(c, "jobs"), model.ErrQueueHasActiveTasks)

	// One task finishes, one is cancelled, one is dead-lettered.
	if err := ts.Cancel(c, "jobs", pending.ID); err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	done := mustClaim(t, ts, "w1", "jobs")
	if err := ts.Ack(c, done.ID, "w1", nil); err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	dead := mustClaim(t, ts, "w1", "jobs")
	if err := ts.Nack(c, dead.ID, "w1", "boom"); err != nil {
		t.Fatal(err)
	}

	if err := qs.Delete(c, "jobs"); err != nil {
		t.Fatalf("delete queue with only finished tasks: %v", err)
	}
	if _, err := ts.GetTaskByID(c, done.ID); !errors.Is(err, model.ErrTaskNotFound) {
		t.Fatalf("expected finished task removed with its queue, got %v", err)
	}
	dlq, err := ts.ListDeadLetters(c, model.DeadLetterFilter{})
	if err != nil || len(dlq) != 0 {
		t.Fatalf("expected dead letters removed with queue, got %d (%v)", len(dlq), err)
	}

	// The name can be reused.
	mustQueue(t, qs, "jobs", 1, nil)
}

func testDeleteQueueBlockedByRetryingTask(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 3, nil)
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	claimed := mustClaim(t, ts, "w1", "jobs")
	if err := ts.Nack(c, claimed.ID, "w1", "transient"); err != nil {
		t.Fatal(err)
	}
	expectErr(t, qs.Delete(c, "jobs"), model.ErrQueueHasActiveTasks)
}

func testEnqueueAndGet(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	q := mustQueue(t, qs, "jobs", 3, nil)
	scheduled := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	task := mustEnqueue(t, ts, "jobs", 2, scheduled)

	if task.ID == "" || task.QueueID != q.ID || task.Status != model.TaskStatusPending || task.Priority != 2 {
		t.Fatalf("unexpected task: %+v", task)
	}
	got, err := ts.Get(c, "jobs", task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ScheduledAt.Equal(scheduled) {
		t.Fatalf("expected scheduled_at %s, got %s", scheduled, got.ScheduledAt)
	}
	var payload map[string]int
	if err := json.Unmarshal(got.Payload, &payload); err != nil || payload["n"] != 1 {
		t.Fatalf("payload round trip: %s (%v)", got.Payload, err)
	}

	summary, err := ts.GetTaskByID(c, task.ID)
	if err != nil || summary.QueueName != "jobs" {
		t.Fatalf("get by id: %+v %v", summary, err)
	}

	_, err = ts.Create(c, "missing", model.NewTask("", json.RawMessage(`{}`), time.Now(), 5))
	expectErr(t, err, model.ErrQueueNotFound)

	mustQueue(t, qs, "other", 3, nil)
	_, err = ts.Get(c, "other", task.ID)
	expectErr(t, err, model.ErrTaskNotFound)
}

func testUnknownIDsAreNotFound(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 3, nil)
	for _, id := range []string{"not-a-uuid", "00000000-0000-4000-8000-000000000000"} {
		_, err := ts.Get(c, "jobs", id)
		expectErr(t, err, model.ErrTaskNotFound)
		_, err = ts.GetTaskByID(c, id)
		expectErr(t, err, model.ErrTaskNotFound)
		expectErr(t, ts.Cancel(c, "jobs", id), model.ErrTaskNotFound)
		expectErr(t, ts.Heartbeat(c, id, "w1"), model.ErrTaskNotFound)
		expectErr(t, ts.Ack(c, id, "w1", nil), model.ErrTaskNotFound)
		expectErr(t, ts.Nack(c, id, "w1", "x"), model.ErrTaskNotFound)
		_, err = ts.ReplayDeadLetterTask(c, "jobs", id)
		expectErr(t, err, model.ErrDeadLetterTaskNotFound)
		expectErr(t, ts.DeleteDeadLetterTask(c, "jobs", id), model.ErrDeadLetterTaskNotFound)
		events, err := ts.TaskEvents(c, id)
		if err != nil || len(events) != 0 {
			t.Fatalf("events for unknown id %q: %v %v", id, events, err)
		}
		tasks, err := ts.ListTasks(c, model.TaskFilter{TaskID: id})
		if err != nil || len(tasks) != 0 {
			t.Fatalf("list by unknown id %q: %v %v", id, tasks, err)
		}
	}
}

func testClaimOrderAndSchedule(t *testing.T, qs QueueStore, ts TaskStore) {
	mustQueue(t, qs, "jobs", 3, nil)
	now := time.Now()
	future := mustEnqueue(t, ts, "jobs", 1, now.Add(time.Hour))
	low := mustEnqueue(t, ts, "jobs", 10, now.Add(-time.Second))
	high := mustEnqueue(t, ts, "jobs", 1, now.Add(-time.Second))
	mid := mustEnqueue(t, ts, "jobs", 5, now.Add(-time.Second))

	for _, want := range []model.Task{high, mid, low} {
		got := mustClaim(t, ts, "w1", "")
		if got.ID != want.ID {
			t.Fatalf("expected task with priority %d (%s), got %s priority %d", want.Priority, want.ID, got.ID, got.Priority)
		}
		if got.Queue != "jobs" || got.LeaseExpiresAt.Before(time.Now()) {
			t.Fatalf("unexpected claim fields: %+v", got)
		}
	}
	expectNoClaim(t, ts, "w1", "")

	task, err := ts.Get(ctx(t), "jobs", future.ID)
	if err != nil || task.Status != model.TaskStatusPending {
		t.Fatalf("future task should still be pending: %+v %v", task, err)
	}
}

func testClaimUnknownQueue(t *testing.T, _ QueueStore, ts TaskStore) {
	_, _, err := ts.ClaimForWorker(ctx(t), "w1", "missing", lease)
	expectErr(t, err, model.ErrQueueNotFound)
}

func testConcurrentClaimsAreExclusive(t *testing.T, qs QueueStore, ts TaskStore) {
	mustQueue(t, qs, "jobs", 3, nil)
	const tasks = 40
	for range tasks {
		mustEnqueue(t, ts, "jobs", 5, time.Now().Add(-time.Second))
	}

	var mu sync.Mutex
	seen := map[string]string{}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := range 8 {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			for {
				claimed, ok, err := ts.ClaimForWorker(context.Background(), worker, "jobs", lease)
				if err != nil {
					errs <- err
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				if prev, dup := seen[claimed.ID]; dup {
					mu.Unlock()
					errs <- errors.New("task " + claimed.ID + " claimed by both " + prev + " and " + worker)
					return
				}
				seen[claimed.ID] = worker
				mu.Unlock()
			}
		}(string(rune('a' + w)))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(seen) != tasks {
		t.Fatalf("expected %d distinct claims, got %d", tasks, len(seen))
	}
}

func testHeartbeatAckOwnership(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 3, nil)
	pending := mustEnqueue(t, ts, "jobs", 9, time.Now().Add(time.Hour))
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	claimed := mustClaim(t, ts, "w1", "jobs")

	if err := ts.Heartbeat(c, claimed.ID, "w1"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	expectErr(t, ts.Heartbeat(c, claimed.ID, "w2"), model.ErrTaskNotOwned)
	expectErr(t, ts.Heartbeat(c, pending.ID, "w1"), model.ErrTaskNotOwned)
	expectErr(t, ts.Ack(c, claimed.ID, "w2", nil), model.ErrTaskNotOwned)
	expectErr(t, ts.Nack(c, claimed.ID, "w2", "x"), model.ErrTaskNotOwned)

	if err := ts.Ack(c, claimed.ID, "w1", []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("ack: %v", err)
	}
	task, err := ts.Get(c, "jobs", claimed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != model.TaskStatusDone || task.CompletedAt == nil || string(task.Result) == "" {
		t.Fatalf("expected done task with result and completed_at, got %+v", task)
	}
	expectErr(t, ts.Ack(c, claimed.ID, "w1", nil), model.ErrTaskInvalidStatus)
	expectErr(t, ts.Heartbeat(c, claimed.ID, "w1"), model.ErrTaskInvalidStatus)
	expectNoClaim(t, ts, "w1", "jobs")
}

// A nacked task with retry budget left must be dispatched again once its
// backoff elapses.
func testRetriedTaskIsRedispatched(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 3, nil)
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	first := mustClaim(t, ts, "w1", "jobs")
	before := time.Now().UTC()
	if err := ts.Nack(c, first.ID, "w1", "downstream timeout"); err != nil {
		t.Fatal(err)
	}

	task, err := ts.Get(c, "jobs", first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != model.TaskStatusRetry || task.Attempts != 1 || deref(task.LastError) != "downstream timeout" {
		t.Fatalf("unexpected task after nack: %+v", task)
	}
	if task.ScheduledAt.Before(before.Add(-time.Second)) {
		t.Fatalf("expected retry scheduled after the failure, got %s", task.ScheduledAt)
	}
	expectErr(t, ts.Ack(c, first.ID, "w1", nil), model.ErrTaskLeaseExpired)

	var again model.ClaimedTask
	if !eventually(t, 2*time.Second, func() bool {
		claimed, ok, err := ts.ClaimForWorker(c, "w2", "jobs", lease)
		if err != nil {
			t.Fatal(err)
		}
		again = claimed
		return ok
	}) {
		t.Fatal("retried task was never redispatched")
	}
	if again.ID != first.ID || again.Attempts != 1 {
		t.Fatalf("expected the retried task with attempts=1, got %+v", again)
	}
}

func testDeadLetterLifecycle(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 0, nil)
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	claimed := mustClaim(t, ts, "w1", "jobs")
	if err := ts.Nack(c, claimed.ID, "w1", "fatal"); err != nil {
		t.Fatal(err)
	}

	_, err := ts.Get(c, "jobs", claimed.ID)
	expectErr(t, err, model.ErrTaskNotFound)

	dlq, err := ts.ListDeadLetterTasks(c, "jobs")
	if err != nil || len(dlq) != 1 {
		t.Fatalf("expected 1 dead-letter task, got %d (%v)", len(dlq), err)
	}
	dead := dlq[0]
	if dead.ID != claimed.ID || dead.LastError != "fatal" || dead.QueueName != "jobs" || dead.ReplayCount != 0 {
		t.Fatalf("unexpected dead-letter entry: %+v", dead)
	}
	_, err = ts.ListDeadLetterTasks(c, "missing")
	expectErr(t, err, model.ErrQueueNotFound)

	byID, err := ts.ListDeadLetters(c, model.DeadLetterFilter{TaskID: dead.ID})
	if err != nil || len(byID) != 1 {
		t.Fatalf("filter dead letters by id: %d %v", len(byID), err)
	}

	replayed, err := ts.ReplayDeadLetterTask(c, "jobs", dead.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID == dead.ID || replayed.Status != model.TaskStatusPending || replayed.Attempts != 0 {
		t.Fatalf("unexpected replayed task: %+v", replayed)
	}
	if _, err := ts.ReplayDeadLetterTask(c, "jobs", dead.ID); err != nil {
		t.Fatalf("second replay: %v", err)
	}
	dlq, _ = ts.ListDeadLetterTasks(c, "jobs")
	if len(dlq) != 1 || dlq[0].ReplayCount != 2 || dlq[0].ReplayedAt == nil {
		t.Fatalf("expected entry kept with replay_count=2, got %+v", dlq)
	}

	if err := ts.DeleteDeadLetterTask(c, "jobs", dead.ID); err != nil {
		t.Fatal(err)
	}
	expectErr(t, ts.DeleteDeadLetterTask(c, "jobs", dead.ID), model.ErrDeadLetterTaskNotFound)
	_, err = ts.ReplayDeadLetterTask(c, "jobs", dead.ID)
	expectErr(t, err, model.ErrDeadLetterTaskNotFound)
}

func testWatchdogReclaim(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 3, nil)
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	claimed := mustClaim(t, ts, "w1", "jobs")

	reclaimed, err := ts.ReclaimExpired(c, time.Hour)
	if err != nil || len(reclaimed) != 0 {
		t.Fatalf("fresh heartbeat must not be reclaimed: %v %v", reclaimed, err)
	}

	time.Sleep(20 * time.Millisecond)
	reclaimed, err = ts.ReclaimExpired(c, 10*time.Millisecond)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].ID != claimed.ID {
		t.Fatalf("expected task reclaimed: %v %v", reclaimed, err)
	}

	task, err := ts.Get(c, "jobs", claimed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != model.TaskStatusRetry || task.Attempts != 1 || deref(task.LastError) != model.WatchdogError {
		t.Fatalf("unexpected reclaimed task: %+v", task)
	}
	expectErr(t, ts.Ack(c, claimed.ID, "w1", nil), model.ErrTaskLeaseExpired)
	expectErr(t, ts.Nack(c, claimed.ID, "w1", "late"), model.ErrTaskLeaseExpired)
	expectErr(t, ts.Heartbeat(c, claimed.ID, "w1"), model.ErrTaskLeaseExpired)
}

func testWatchdogDeadLettersExhaustedTask(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 0, nil)
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	claimed := mustClaim(t, ts, "w1", "jobs")
	time.Sleep(20 * time.Millisecond)

	reclaimed, err := ts.ReclaimExpired(c, 10*time.Millisecond)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Status != model.TaskStatusDead {
		t.Fatalf("expected task dead-lettered by the watchdog: %+v %v", reclaimed, err)
	}
	dlq, err := ts.ListDeadLetterTasks(c, "jobs")
	if err != nil || len(dlq) != 1 || dlq[0].ID != claimed.ID || dlq[0].LastError != model.WatchdogError {
		t.Fatalf("expected DLQ entry with watchdog error: %+v %v", dlq, err)
	}
	// The queue no longer has active work.
	if err := qs.Delete(c, "jobs"); err != nil {
		t.Fatalf("delete queue after task died: %v", err)
	}
}

func testRateLimitSkipsToOtherQueues(t *testing.T, qs QueueStore, ts TaskStore) {
	now := time.Now()
	clock := func() time.Time { return now }
	ts.SetRateLimiter(ratelimit.NewLocalWithClock(clock))

	mustQueue(t, qs, "limited", 3, intPtr(2))
	mustQueue(t, qs, "open", 3, nil)
	for range 4 {
		mustEnqueue(t, ts, "limited", 1, time.Now().Add(-time.Second))
	}
	openTask := mustEnqueue(t, ts, "open", 9, time.Now().Add(-time.Second))

	// The bucket allows a burst of 2, then the claim falls through to the
	// lower-priority task on the unlimited queue instead of starving it.
	for range 2 {
		if got := mustClaim(t, ts, "w1", ""); got.Queue != "limited" {
			t.Fatalf("expected limited queue first, got %s", got.Queue)
		}
	}
	if got := mustClaim(t, ts, "w1", ""); got.ID != openTask.ID {
		t.Fatalf("expected claim to skip the exhausted queue, got %+v", got)
	}
	expectNoClaim(t, ts, "w1", "limited")

	// Tokens refill at the rate limit; tasks are never dropped.
	now = now.Add(500 * time.Millisecond)
	mustClaim(t, ts, "w1", "limited")
	expectNoClaim(t, ts, "w1", "limited")

	stats, err := ts.Stats(ctx(t), "limited")
	if err != nil || stats.Depth != 1 {
		t.Fatalf("expected the remaining task to stay queued: %+v %v", stats, err)
	}
}

func testCancel(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 3, nil)
	pending := mustEnqueue(t, ts, "jobs", 9, time.Now().Add(time.Hour))
	mustEnqueue(t, ts, "jobs", 1, time.Now())
	running := mustClaim(t, ts, "w1", "jobs")

	if err := ts.Cancel(c, "jobs", pending.ID); err != nil {
		t.Fatal(err)
	}
	task, _ := ts.Get(c, "jobs", pending.ID)
	if task.Status != model.TaskStatusCancelled {
		t.Fatalf("expected cancelled, got %s", task.Status)
	}
	expectErr(t, ts.Cancel(c, "jobs", pending.ID), model.ErrTaskNotCancellable)
	expectErr(t, ts.Cancel(c, "jobs", running.ID), model.ErrTaskNotCancellable)
	// The HTTP layer maps both errors to 404.
	if err := ts.Cancel(c, "missing", running.ID); !errors.Is(err, model.ErrQueueNotFound) && !errors.Is(err, model.ErrTaskNotFound) {
		t.Fatalf("expected not found for unknown queue, got %v", err)
	}
}

func testStatsAndCounts(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 1, nil)
	mustQueue(t, qs, "idle", 1, nil)

	stats, err := ts.Stats(c, "idle")
	if err != nil || stats != (model.QueueStats{}) {
		t.Fatalf("expected zero stats for idle queue: %+v %v", stats, err)
	}
	_, err = ts.Stats(c, "missing")
	expectErr(t, err, model.ErrQueueNotFound)

	for range 4 {
		mustEnqueue(t, ts, "jobs", 5, time.Now().Add(-2*time.Second))
	}
	mustEnqueue(t, ts, "jobs", 5, time.Now().Add(time.Hour))

	done := mustClaim(t, ts, "w1", "jobs")
	if err := ts.Ack(c, done.ID, "w1", nil); err != nil {
		t.Fatal(err)
	}
	failing := mustClaim(t, ts, "w1", "jobs")
	if err := ts.Nack(c, failing.ID, "w1", "first failure"); err != nil {
		t.Fatal(err)
	}
	mustClaim(t, ts, "w2", "jobs") // stays executing

	stats, err = ts.Stats(c, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	// Remaining: 1 due pending + 1 future pending + 1 retry = depth 3.
	if stats.Depth != 3 || stats.ExecutingCount != 1 || stats.DoneLastMinute != 1 || stats.FailedLastMinute != 1 || stats.DLQDepth != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.OldestPendingAgeSeconds < 1 {
		t.Fatalf("expected oldest pending age >= 1s, got %d", stats.OldestPendingAgeSeconds)
	}

	// Exhaust the retrying task into the DLQ.
	if !eventually(t, 2*time.Second, func() bool {
		claimed, ok, err := ts.ClaimForWorker(c, "w3", "jobs", lease)
		if err != nil {
			t.Fatal(err)
		}
		if ok && claimed.ID != failing.ID {
			_ = ts.Ack(c, claimed.ID, "w3", nil)
			return false
		}
		return ok
	}) {
		t.Fatal("retry never became claimable")
	}
	if err := ts.Nack(c, failing.ID, "w3", "second failure"); err != nil {
		t.Fatal(err)
	}
	stats, _ = ts.Stats(c, "jobs")
	// failed_last_minute counts distinct tasks that failed in the window.
	if stats.DLQDepth != 1 || stats.FailedLastMinute != 1 {
		t.Fatalf("expected DLQ depth 1 and 1 recently failed task, got %+v", stats)
	}

	counts, err := ts.StatusCounts(c)
	if err != nil {
		t.Fatal(err)
	}
	byStatus := map[model.TaskStatus]int{}
	for _, sc := range counts {
		if sc.Queue != "jobs" {
			t.Fatalf("unexpected queue in counts: %+v", sc)
		}
		byStatus[sc.Status] = sc.Count
	}
	if byStatus[model.TaskStatusDead] != 1 || byStatus[model.TaskStatusExecuting] != 1 || byStatus[model.TaskStatusDone] < 1 {
		t.Fatalf("unexpected status counts: %+v", counts)
	}
}

func testActiveWorkers(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "jobs", 3, nil)
	mustEnqueue(t, ts, "jobs", 5, time.Now())
	claimed := mustClaim(t, ts, "w1", "jobs")

	workers, err := ts.ActiveWorkers(c)
	if err != nil || len(workers) != 1 || workers[0].WorkerID != "w1" || workers[0].CurrentTaskID != claimed.ID || workers[0].Stale {
		t.Fatalf("expected busy worker w1: %+v %v", workers, err)
	}
	if err := ts.Ack(c, claimed.ID, "w1", nil); err != nil {
		t.Fatal(err)
	}
	workers, _ = ts.ActiveWorkers(c)
	if len(workers) != 1 || workers[0].CurrentTaskID != "" {
		t.Fatalf("expected idle worker after ack: %+v", workers)
	}

	// A worker that polls and finds nothing is still alive.
	expectNoClaim(t, ts, "poller", "")
	workers, _ = ts.ActiveWorkers(c)
	if len(workers) != 2 || workers[0].WorkerID != "poller" || workers[0].CurrentTaskID != "" {
		t.Fatalf("expected idle poller listed: %+v", workers)
	}
}

func testListTasksFilters(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	mustQueue(t, qs, "a", 3, nil)
	mustQueue(t, qs, "b", 3, nil)
	start := time.Now().UTC().Add(-time.Second)
	for range 3 {
		mustEnqueue(t, ts, "a", 5, time.Now())
	}
	mustEnqueue(t, ts, "b", 5, time.Now())
	claimed := mustClaim(t, ts, "worker-x", "b")

	all, err := ts.ListTasks(c, model.TaskFilter{})
	if err != nil || len(all) != 4 {
		t.Fatalf("expected 4 tasks, got %d (%v)", len(all), err)
	}
	for i := 1; i < len(all); i++ {
		if all[i].CreatedAt.After(all[i-1].CreatedAt) {
			t.Fatal("expected newest first")
		}
	}

	check := func(name string, f model.TaskFilter, want int) {
		t.Helper()
		got, err := ts.ListTasks(c, f)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != want {
			t.Fatalf("%s: expected %d tasks, got %d", name, want, len(got))
		}
	}
	check("queue", model.TaskFilter{Queue: "a"}, 3)
	check("status", model.TaskFilter{Status: model.TaskStatusExecuting}, 1)
	check("worker", model.TaskFilter{WorkerID: "worker-x"}, 1)
	check("task id", model.TaskFilter{TaskID: claimed.ID}, 1)
	future := time.Now().Add(time.Hour)
	check("created after", model.TaskFilter{CreatedAfter: &future}, 0)
	check("created before", model.TaskFilter{CreatedBefore: &start}, 0)
	check("page 1", model.TaskFilter{Limit: 3}, 3)
	check("page 2", model.TaskFilter{Limit: 3, Offset: 3}, 1)

	got, _ := ts.ListTasks(c, model.TaskFilter{TaskID: claimed.ID})
	if got[0].QueueName != "b" {
		t.Fatalf("expected queue name on summary, got %+v", got[0])
	}
}

func testHistoryAndObserver(t *testing.T, qs QueueStore, ts TaskStore) {
	c := ctx(t)
	rec := &recorder{}
	ts.SetObserver(rec)
	mustQueue(t, qs, "jobs", 3, nil)
	task := mustEnqueue(t, ts, "jobs", 5, time.Now())
	claimed := mustClaim(t, ts, "w1", "jobs")
	if err := ts.Heartbeat(c, claimed.ID, "w1"); err != nil {
		t.Fatal(err)
	}
	if err := ts.Ack(c, claimed.ID, "w1", nil); err != nil {
		t.Fatal(err)
	}

	events, err := ts.TaskEvents(c, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	type step struct {
		kind     model.TaskEventKind
		from, to model.TaskStatus
	}
	want := []step{
		{model.TaskEventTransition, "", model.TaskStatusPending},
		{model.TaskEventTransition, model.TaskStatusPending, model.TaskStatusClaimed},
		{model.TaskEventTransition, model.TaskStatusClaimed, model.TaskStatusExecuting},
		{model.TaskEventHeartbeat, "", ""},
		{model.TaskEventTransition, model.TaskStatusExecuting, model.TaskStatusDone},
	}
	if len(events) != len(want) {
		t.Fatalf("expected %d events, got %+v", len(want), events)
	}
	for i, w := range want {
		e := events[i]
		if e.Kind != w.kind || e.FromStatus != w.from || e.ToStatus != w.to {
			t.Fatalf("event %d: expected %+v, got %+v", i, w, e)
		}
	}
	if events[4].WorkerID != "w1" {
		t.Fatalf("expected worker on ack event, got %+v", events[4])
	}

	transitions := rec.all()
	if len(transitions) != 4 {
		t.Fatalf("expected 4 observed transitions, got %+v", transitions)
	}
	last := transitions[3]
	if last.Queue != "jobs" || last.From != model.TaskStatusExecuting || last.To != model.TaskStatusDone || last.WorkerID != "w1" {
		t.Fatalf("unexpected final transition: %+v", last)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
