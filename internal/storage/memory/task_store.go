package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"qrunnr.dev/internal/model"
	"qrunnr.dev/internal/ratelimit"
)

// TaskStore is an in-process task store. It is used for local development
// and tests and mirrors the behaviour of the Postgres store.
type TaskStore struct {
	mu          sync.RWMutex
	queueStore  *QueueStore
	tasks       map[string]model.Task
	deadTasks   map[string]model.DeadLetterTask
	failedAt    map[string]time.Time
	events      map[string][]model.TaskEvent
	workers     map[string]workerState
	nextEventID int64

	rateLimiter ratelimit.Limiter
	retry       model.RetryPolicy
	observer    model.Observer
}

type workerState struct {
	currentTaskID   string
	lastHeartbeatAt time.Time
}

func NewTaskStore(queueStore *QueueStore) *TaskStore {
	s := &TaskStore{
		queueStore:  queueStore,
		tasks:       make(map[string]model.Task),
		deadTasks:   make(map[string]model.DeadLetterTask),
		failedAt:    make(map[string]time.Time),
		events:      make(map[string][]model.TaskEvent),
		workers:     make(map[string]workerState),
		rateLimiter: ratelimit.NewLocal(),
		retry:       model.DefaultRetryPolicy(),
		observer:    model.NopObserver{},
	}
	queueStore.onDelete = s.forgetQueue
	return s
}

func (s *TaskStore) SetRateLimiter(rateLimiter ratelimit.Limiter) {
	if rateLimiter != nil {
		s.rateLimiter = rateLimiter
	}
}

func (s *TaskStore) SetRetryPolicy(policy model.RetryPolicy) { s.retry = policy }

func (s *TaskStore) SetObserver(observer model.Observer) {
	if observer == nil {
		observer = model.NopObserver{}
	}
	s.observer = observer
}

func (s *TaskStore) emit(transitions ...model.Transition) {
	for _, t := range transitions {
		s.observer.TaskTransition(t)
	}
}

func (s *TaskStore) Create(ctx context.Context, queueName string, task model.Task) (model.Task, error) {
	if err := ctx.Err(); err != nil {
		return model.Task{}, err
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return model.Task{}, err
	}
	task.QueueID = queue.ID

	s.mu.Lock()
	s.tasks[task.ID] = task
	s.recordEventLocked(task.ID, model.TaskEventTransition, "", task.Status, "", "", task.CreatedAt)
	s.mu.Unlock()

	s.queueStore.addActiveTask(queueName)
	s.emit(model.Transition{TaskID: task.ID, Queue: queueName, To: task.Status, At: task.CreatedAt})
	return task, nil
}

func (s *TaskStore) Get(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	if err := ctx.Err(); err != nil {
		return model.Task{}, err
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return model.Task{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists || task.QueueID != queue.ID {
		return model.Task{}, model.ErrTaskNotFound
	}
	return task, nil
}

func (s *TaskStore) GetTaskByID(ctx context.Context, taskID string) (model.TaskSummary, error) {
	if err := ctx.Err(); err != nil {
		return model.TaskSummary{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return model.TaskSummary{}, model.ErrTaskNotFound
	}
	name, ok := s.queueStore.getNameByID(task.QueueID)
	if !ok {
		return model.TaskSummary{}, model.ErrTaskNotFound
	}
	return model.TaskSummary{Task: task, QueueName: name}, nil
}

func (s *TaskStore) ListTasks(ctx context.Context, filter model.TaskFilter) ([]model.TaskSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	matches := []model.TaskSummary{}
	for _, task := range s.tasks {
		if filter.TaskID != "" && task.ID != filter.TaskID {
			continue
		}
		if filter.Status != "" && task.Status != filter.Status {
			continue
		}
		if filter.WorkerID != "" && (task.ClaimedBy == nil || *task.ClaimedBy != filter.WorkerID) {
			continue
		}
		if filter.CreatedAfter != nil && task.CreatedAt.Before(*filter.CreatedAfter) {
			continue
		}
		if filter.CreatedBefore != nil && !task.CreatedAt.Before(*filter.CreatedBefore) {
			continue
		}
		name, ok := s.queueStore.getNameByID(task.QueueID)
		if !ok || (filter.Queue != "" && name != filter.Queue) {
			continue
		}
		matches = append(matches, model.TaskSummary{Task: task, QueueName: name})
	}
	s.mu.RUnlock()

	sort.Slice(matches, func(i, j int) bool {
		if !matches[i].CreatedAt.Equal(matches[j].CreatedAt) {
			return matches[i].CreatedAt.After(matches[j].CreatedAt)
		}
		return matches[i].ID < matches[j].ID
	})
	return paginate(matches, filter.Offset, filter.Limit), nil
}

func (s *TaskStore) TaskEvents(ctx context.Context, taskID string) ([]model.TaskEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]model.TaskEvent{}, s.events[taskID]...), nil
}

func (s *TaskStore) Cancel(ctx context.Context, queueName string, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return err
	}

	s.mu.Lock()
	task, exists := s.tasks[taskID]
	if !exists || task.QueueID != queue.ID {
		s.mu.Unlock()
		return model.ErrTaskNotFound
	}
	if task.Status != model.TaskStatusPending {
		s.mu.Unlock()
		return model.ErrTaskNotCancellable
	}

	now := time.Now().UTC()
	task.Status = model.TaskStatusCancelled
	s.tasks[taskID] = task
	s.recordEventLocked(taskID, model.TaskEventTransition, model.TaskStatusPending, model.TaskStatusCancelled, "", "", now)
	s.mu.Unlock()

	s.queueStore.removeActiveTask(queueName)
	s.emit(model.Transition{
		TaskID: taskID, Queue: queueName, From: model.TaskStatusPending, To: model.TaskStatusCancelled,
		Duration: now.Sub(task.CreatedAt), At: now,
	})
	return nil
}

// PeekDue returns up to limit dispatchable tasks in claim order without
// claiming them.
func (s *TaskStore) PeekDue(ctx context.Context, limit int) ([]model.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []model.Task{}, nil
	}

	now := time.Now().UTC()

	s.mu.RLock()
	tasks := make([]model.Task, 0)
	for _, task := range s.tasks {
		if isDue(task, now) {
			tasks = append(tasks, task)
		}
	}
	s.mu.RUnlock()

	sortClaimOrder(tasks)
	if len(tasks) > limit {
		tasks = tasks[:limit]
	}
	return tasks, nil
}

func (s *TaskStore) ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return model.ClaimedTask{}, false, err
	}

	if queueName != "" {
		if _, err := s.queueStore.GetByName(ctx, queueName); err != nil {
			return model.ClaimedTask{}, false, err
		}
	}

	now := time.Now().UTC()

	s.mu.Lock()
	candidates := make([]model.Task, 0)
	for _, task := range s.tasks {
		if !isDue(task, now) {
			continue
		}
		if queueName != "" {
			name, ok := s.queueStore.getNameByID(task.QueueID)
			if !ok || name != queueName {
				continue
			}
		}
		candidates = append(candidates, task)
	}
	sortClaimOrder(candidates)

	limited := map[string]bool{}
	for _, task := range candidates {
		queue, ok := s.queueStore.getByID(task.QueueID)
		if !ok || limited[queue.ID] {
			continue
		}
		if queue.RateLimit != nil {
			allowed, err := s.rateLimiter.Allow(ctx, queue.Name, *queue.RateLimit)
			if err != nil {
				s.mu.Unlock()
				return model.ClaimedTask{}, false, err
			}
			if !allowed {
				limited[queue.ID] = true
				continue
			}
		}

		from := task.Status
		claimedAt := now
		heartbeatAt := now
		task.Status = model.TaskStatusExecuting
		task.ClaimedAt = &claimedAt
		task.ClaimedBy = &workerID
		task.HeartbeatAt = &heartbeatAt
		s.tasks[task.ID] = task
		s.recordEventLocked(task.ID, model.TaskEventTransition, from, model.TaskStatusClaimed, workerID, "", now)
		s.recordEventLocked(task.ID, model.TaskEventTransition, model.TaskStatusClaimed, model.TaskStatusExecuting, workerID, "", now)
		s.recordWorkerLocked(workerID, task.ID, now)
		s.mu.Unlock()

		s.emit(
			model.Transition{TaskID: task.ID, Queue: queue.Name, WorkerID: workerID, From: from, To: model.TaskStatusClaimed,
				Duration: waitDuration(task, now), At: now},
			model.Transition{TaskID: task.ID, Queue: queue.Name, WorkerID: workerID, From: model.TaskStatusClaimed,
				To: model.TaskStatusExecuting, At: now},
		)
		return model.ClaimedTask{
			ID:             task.ID,
			Queue:          queue.Name,
			Payload:        task.Payload,
			Priority:       task.Priority,
			Attempts:       task.Attempts,
			LeaseExpiresAt: now.Add(leaseDuration),
		}, true, nil
	}
	// Idle polls keep the worker visible in ActiveWorkers.
	s.recordWorkerLocked(workerID, "", now)
	s.mu.Unlock()
	return model.ClaimedTask{}, false, nil
}

func (s *TaskStore) Ack(ctx context.Context, taskID string, workerID string, result []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	task, err := s.ownedExecutingLocked(taskID, workerID)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	now := time.Now().UTC()
	task.Status = model.TaskStatusDone
	task.Result = result
	task.CompletedAt = &now
	s.tasks[taskID] = task
	s.recordEventLocked(taskID, model.TaskEventTransition, model.TaskStatusExecuting, model.TaskStatusDone, workerID, "", now)
	s.recordWorkerLocked(workerID, "", now)
	queueName, _ := s.queueStore.getNameByID(task.QueueID)
	s.mu.Unlock()

	s.queueStore.removeActiveTask(queueName)
	s.emit(model.Transition{
		TaskID: taskID, Queue: queueName, WorkerID: workerID, From: model.TaskStatusExecuting, To: model.TaskStatusDone,
		Duration: sinceOrZero(task.ClaimedAt, now), At: now,
	})
	return nil
}

func (s *TaskStore) Nack(ctx context.Context, taskID string, workerID string, errMessage string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	task, err := s.ownedExecutingLocked(taskID, workerID)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	queue, ok := s.queueStore.getByID(task.QueueID)
	if !ok {
		s.mu.Unlock()
		return model.ErrQueueNotFound
	}

	transition := s.failLocked(task, queue, errMessage, workerID, time.Now().UTC())
	s.recordWorkerLocked(workerID, "", transition.At)
	s.mu.Unlock()

	if transition.To == model.TaskStatusDead {
		s.queueStore.removeActiveTask(queue.Name)
	}
	s.emit(transition)
	return nil
}

// failLocked records a failed attempt, scheduling a retry or moving the task
// to the dead-letter queue when its retry budget is spent.
func (s *TaskStore) failLocked(task model.Task, queue model.Queue, errMessage string, workerID string, now time.Time) model.Transition {
	transition := model.Transition{
		TaskID: task.ID, Queue: queue.Name, WorkerID: workerID, From: model.TaskStatusExecuting,
		Duration: sinceOrZero(task.ClaimedAt, now), At: now,
	}
	s.failedAt[task.ID] = now

	if task.Attempts >= queue.MaxRetries {
		s.deadTasks[task.ID] = model.DeadLetterTask{
			ID:        task.ID,
			QueueID:   task.QueueID,
			QueueName: queue.Name,
			Payload:   task.Payload,
			LastError: errMessage,
			Attempts:  task.Attempts,
			DiedAt:    now,
		}
		delete(s.tasks, task.ID)
		s.recordEventLocked(task.ID, model.TaskEventTransition, model.TaskStatusExecuting, model.TaskStatusDead, workerID, errMessage, now)
		transition.To = model.TaskStatusDead
		return transition
	}

	task.Attempts++
	task.Status = model.TaskStatusRetry
	task.LastError = &errMessage
	task.ScheduledAt = now.Add(s.retry.Delay(task.Attempts))
	s.tasks[task.ID] = task
	s.recordEventLocked(task.ID, model.TaskEventTransition, model.TaskStatusExecuting, model.TaskStatusRetry, workerID, errMessage, now)
	transition.To = model.TaskStatusRetry
	return transition
}

func (s *TaskStore) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, err := s.ownedExecutingLocked(taskID, workerID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	task.HeartbeatAt = &now
	s.tasks[taskID] = task
	s.recordEventLocked(taskID, model.TaskEventHeartbeat, "", "", workerID, "", now)
	s.recordWorkerLocked(workerID, taskID, now)
	return nil
}

func (s *TaskStore) ownedExecutingLocked(taskID string, workerID string) (model.Task, error) {
	task, exists := s.tasks[taskID]
	if !exists {
		return model.Task{}, model.ErrTaskNotFound
	}
	if task.ClaimedBy == nil || *task.ClaimedBy != workerID {
		return model.Task{}, model.ErrTaskNotOwned
	}
	if task.Status != model.TaskStatusExecuting {
		if task.Status == model.TaskStatusRetry {
			return model.Task{}, model.ErrTaskLeaseExpired
		}
		return model.Task{}, model.ErrTaskInvalidStatus
	}
	return task, nil
}

func (s *TaskStore) ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	cutoff := now.Add(-heartbeatTimeout)

	s.mu.Lock()
	reclaimed := make([]model.Task, 0)
	transitions := make([]model.Transition, 0)
	deadQueues := []string{}
	for _, task := range s.tasks {
		if task.Status != model.TaskStatusExecuting || task.HeartbeatAt == nil || !task.HeartbeatAt.Before(cutoff) {
			continue
		}
		queue, ok := s.queueStore.getByID(task.QueueID)
		if !ok {
			continue
		}
		workerID := ""
		if task.ClaimedBy != nil {
			workerID = *task.ClaimedBy
		}
		transition := s.failLocked(task, queue, model.WatchdogError, workerID, now)
		if updated, ok := s.tasks[task.ID]; ok {
			task = updated
		} else {
			task.Status = model.TaskStatusDead
			msg := model.WatchdogError
			task.LastError = &msg
			deadQueues = append(deadQueues, queue.Name)
		}
		reclaimed = append(reclaimed, task)
		transitions = append(transitions, transition)
	}
	s.mu.Unlock()

	for _, name := range deadQueues {
		s.queueStore.removeActiveTask(name)
	}
	s.emit(transitions...)
	return reclaimed, nil
}

func (s *TaskStore) Stats(ctx context.Context, queueName string) (model.QueueStats, error) {
	if err := ctx.Err(); err != nil {
		return model.QueueStats{}, err
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return model.QueueStats{}, err
	}

	now := time.Now().UTC()
	oneMinuteAgo := now.Add(-time.Minute)
	stats := model.QueueStats{}
	var oldestPending *time.Time

	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, task := range s.tasks {
		if task.QueueID != queue.ID {
			continue
		}
		switch task.Status {
		case model.TaskStatusPending, model.TaskStatusRetry:
			stats.Depth++
			if !task.ScheduledAt.After(now) && (oldestPending == nil || task.ScheduledAt.Before(*oldestPending)) {
				scheduledAt := task.ScheduledAt
				oldestPending = &scheduledAt
			}
		case model.TaskStatusExecuting:
			stats.ExecutingCount++
		case model.TaskStatusDone:
			if task.CompletedAt != nil && !task.CompletedAt.Before(oneMinuteAgo) {
				stats.DoneLastMinute++
			}
		}
		if failedAt, ok := s.failedAt[id]; ok && !failedAt.Before(oneMinuteAgo) {
			stats.FailedLastMinute++
		}
	}
	for id, task := range s.deadTasks {
		if task.QueueID != queue.ID {
			continue
		}
		stats.DLQDepth++
		if failedAt, ok := s.failedAt[id]; ok && !failedAt.Before(oneMinuteAgo) {
			stats.FailedLastMinute++
		}
	}
	if oldestPending != nil {
		stats.OldestPendingAgeSeconds = int(now.Sub(*oldestPending).Seconds())
	}
	return stats, nil
}

func (s *TaskStore) StatusCounts(ctx context.Context) ([]model.StatusCount, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	type key struct {
		queue  string
		status model.TaskStatus
	}
	counts := map[key]int{}

	s.mu.RLock()
	for _, task := range s.tasks {
		if name, ok := s.queueStore.getNameByID(task.QueueID); ok {
			counts[key{name, task.Status}]++
		}
	}
	for _, task := range s.deadTasks {
		if name, ok := s.queueStore.getNameByID(task.QueueID); ok {
			counts[key{name, model.TaskStatusDead}]++
		}
	}
	s.mu.RUnlock()

	result := make([]model.StatusCount, 0, len(counts))
	for k, n := range counts {
		result = append(result, model.StatusCount{Queue: k.queue, Status: k.status, Count: n})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Queue != result[j].Queue {
			return result[i].Queue < result[j].Queue
		}
		return result[i].Status < result[j].Status
	})
	return result, nil
}

// DeadLetterTask returns a dead-letter entry by ID. It is a test helper.
func (s *TaskStore) DeadLetterTask(taskID string) (model.DeadLetterTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, ok := s.deadTasks[taskID]
	return task, ok
}

// ReplaceForTest overwrites a stored task. It is a test helper.
func (s *TaskStore) ReplaceForTest(ctx context.Context, task model.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[task.ID]; !exists {
		return model.ErrTaskNotFound
	}
	s.tasks[task.ID] = task
	return nil
}

func (s *TaskStore) ListDeadLetterTasks(ctx context.Context, queueName string) ([]model.DeadLetterTask, error) {
	if _, err := s.queueStore.GetByName(ctx, queueName); err != nil {
		return nil, err
	}
	return s.ListDeadLetters(ctx, model.DeadLetterFilter{Queue: queueName, Limit: 500})
}

func (s *TaskStore) ListDeadLetters(ctx context.Context, filter model.DeadLetterFilter) ([]model.DeadLetterTask, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	tasks := []model.DeadLetterTask{}
	for _, task := range s.deadTasks {
		if filter.TaskID != "" && task.ID != filter.TaskID {
			continue
		}
		name, ok := s.queueStore.getNameByID(task.QueueID)
		if !ok || (filter.Queue != "" && name != filter.Queue) {
			continue
		}
		if filter.DiedAfter != nil && task.DiedAt.Before(*filter.DiedAfter) {
			continue
		}
		if filter.DiedBefore != nil && !task.DiedAt.Before(*filter.DiedBefore) {
			continue
		}
		task.QueueName = name
		tasks = append(tasks, task)
	}
	s.mu.RUnlock()

	sort.Slice(tasks, func(i, j int) bool {
		if !tasks[i].DiedAt.Equal(tasks[j].DiedAt) {
			return tasks[i].DiedAt.Before(tasks[j].DiedAt)
		}
		return tasks[i].ID < tasks[j].ID
	})
	return paginate(tasks, filter.Offset, filter.Limit), nil
}

func (s *TaskStore) ReplayDeadLetterTask(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	if err := ctx.Err(); err != nil {
		return model.Task{}, err
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return model.Task{}, err
	}

	s.mu.Lock()
	dead, exists := s.deadTasks[taskID]
	if !exists || dead.QueueID != queue.ID {
		s.mu.Unlock()
		return model.Task{}, model.ErrDeadLetterTaskNotFound
	}

	now := time.Now().UTC()
	dead.ReplayedAt = &now
	dead.ReplayCount++
	s.deadTasks[taskID] = dead

	task := model.NewTask(queue.ID, dead.Payload, now, 5)
	s.tasks[task.ID] = task
	s.recordEventLocked(task.ID, model.TaskEventTransition, "", model.TaskStatusPending, "", "replayed from dead-letter task "+taskID, now)
	s.recordEventLocked(taskID, model.TaskEventReplay, "", "", "", "replayed as task "+task.ID, now)
	s.mu.Unlock()

	s.queueStore.addActiveTask(queueName)
	s.emit(model.Transition{TaskID: task.ID, Queue: queueName, To: model.TaskStatusPending, At: now})
	return task, nil
}

func (s *TaskStore) DeleteDeadLetterTask(ctx context.Context, queueName string, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dead, exists := s.deadTasks[taskID]
	if !exists || dead.QueueID != queue.ID {
		return model.ErrDeadLetterTaskNotFound
	}

	delete(s.deadTasks, taskID)
	delete(s.failedAt, taskID)
	return nil
}

func (s *TaskStore) ActiveWorkers(ctx context.Context) ([]model.WorkerStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	cutoff := now.Add(-60 * time.Second)
	staleCutoff := now.Add(-20 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()

	workers := make([]model.WorkerStatus, 0, len(s.workers))
	for workerID, worker := range s.workers {
		if worker.lastHeartbeatAt.Before(cutoff) {
			delete(s.workers, workerID)
			continue
		}
		workers = append(workers, model.WorkerStatus{
			WorkerID:        workerID,
			CurrentTaskID:   worker.currentTaskID,
			LastHeartbeatAt: worker.lastHeartbeatAt,
			Stale:           worker.lastHeartbeatAt.Before(staleCutoff),
		})
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].WorkerID < workers[j].WorkerID })
	return workers, nil
}

// ReplaceWorkerForTest overwrites a worker's last-seen state. It is a test helper.
func (s *TaskStore) ReplaceWorkerForTest(workerID string, currentTaskID string, lastHeartbeatAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.workers[workerID] = workerState{currentTaskID: currentTaskID, lastHeartbeatAt: lastHeartbeatAt}
}

func (s *TaskStore) recordWorkerLocked(workerID string, taskID string, seenAt time.Time) {
	if workerID == "" {
		return
	}
	s.workers[workerID] = workerState{currentTaskID: taskID, lastHeartbeatAt: seenAt}
}

func (s *TaskStore) recordEventLocked(taskID string, kind model.TaskEventKind, from, to model.TaskStatus, workerID, message string, at time.Time) {
	s.nextEventID++
	s.events[taskID] = append(s.events[taskID], model.TaskEvent{
		ID:         s.nextEventID,
		TaskID:     taskID,
		Kind:       kind,
		FromStatus: from,
		ToStatus:   to,
		WorkerID:   workerID,
		Message:    message,
		CreatedAt:  at,
	})
}

// forgetQueue drops every task, dead-letter entry, and event belonging to a
// deleted queue. It is called by the queue store after a successful delete.
func (s *TaskStore) forgetQueue(queueID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, task := range s.tasks {
		if task.QueueID == queueID {
			delete(s.tasks, id)
			delete(s.events, id)
			delete(s.failedAt, id)
		}
	}
	for id, task := range s.deadTasks {
		if task.QueueID == queueID {
			delete(s.deadTasks, id)
			delete(s.events, id)
			delete(s.failedAt, id)
		}
	}
}

func isDue(task model.Task, now time.Time) bool {
	return (task.Status == model.TaskStatusPending || task.Status == model.TaskStatusRetry) && !task.ScheduledAt.After(now)
}

func sortClaimOrder(tasks []model.Task) {
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority < tasks[j].Priority
		}
		return tasks[i].ScheduledAt.Before(tasks[j].ScheduledAt)
	})
}

func waitDuration(task model.Task, now time.Time) time.Duration {
	start := task.CreatedAt
	if task.ScheduledAt.After(start) {
		start = task.ScheduledAt
	}
	if now.Before(start) {
		return 0
	}
	return now.Sub(start)
}

func sinceOrZero(start *time.Time, now time.Time) time.Duration {
	if start == nil || now.Before(*start) {
		return 0
	}
	return now.Sub(*start)
}

func paginate[T any](items []T, offset, limit int) []T {
	limit = model.ClampLimit(limit)
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []T{}
	}
	end := min(offset+limit, len(items))
	return items[offset:end]
}
