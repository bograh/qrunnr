package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"taskforge.dev/internal/model"
)

type TaskStore struct {
	mu         sync.RWMutex
	queueStore *QueueStore
	tasks      map[string]model.Task
	leases     map[string]time.Time
}

func NewTaskStore(queueStore *QueueStore) *TaskStore {
	return &TaskStore{
		queueStore: queueStore,
		tasks:      make(map[string]model.Task),
		leases:     make(map[string]time.Time),
	}
}

func (s *TaskStore) Create(ctx context.Context, queueName string, task model.Task) (model.Task, error) {
	select {
	case <-ctx.Done():
		return model.Task{}, ctx.Err()
	default:
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return model.Task{}, err
	}

	task.QueueID = queue.ID

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks[task.ID] = task
	s.queueStore.addActiveTask(queueName)
	return task, nil
}

func (s *TaskStore) Get(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	select {
	case <-ctx.Done():
		return model.Task{}, ctx.Err()
	default:
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return model.Task{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return model.Task{}, model.ErrTaskNotFound
	}
	if task.QueueID != queue.ID {
		return model.Task{}, model.ErrTaskNotFound
	}

	return task, nil
}

func (s *TaskStore) Cancel(ctx context.Context, queueName string, taskID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	queue, err := s.queueStore.GetByName(ctx, queueName)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists || task.QueueID != queue.ID {
		return model.ErrTaskNotFound
	}
	if task.Status != model.TaskStatusPending {
		return model.ErrTaskNotCancellable
	}

	task.Status = model.TaskStatusCancelled
	s.tasks[taskID] = task
	s.queueStore.removeActiveTask(queueName)
	return nil
}

func (s *TaskStore) ClaimPending(ctx context.Context, limit int) ([]model.Task, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if limit <= 0 {
		return []model.Task{}, nil
	}

	now := time.Now().UTC()

	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]model.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task.Status == model.TaskStatusPending && !task.ScheduledAt.After(now) {
			tasks = append(tasks, task)
		}
	}

	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority < tasks[j].Priority
		}
		return tasks[i].ScheduledAt.Before(tasks[j].ScheduledAt)
	})

	if len(tasks) > limit {
		tasks = tasks[:limit]
	}

	return tasks, nil
}

func (s *TaskStore) ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error) {
	select {
	case <-ctx.Done():
		return model.ClaimedTask{}, false, ctx.Err()
	default:
	}

	if queueName != "" {
		if _, err := s.queueStore.GetByName(ctx, queueName); err != nil {
			return model.ClaimedTask{}, false, err
		}
	}

	now := time.Now().UTC()
	leaseExpiresAt := now.Add(leaseDuration)

	s.mu.Lock()
	defer s.mu.Unlock()

	candidates := make([]model.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task.Status != model.TaskStatusPending || task.ScheduledAt.After(now) {
			continue
		}
		taskQueueName, ok := s.queueStore.getNameByID(task.QueueID)
		if !ok {
			continue
		}
		if queueName != "" && taskQueueName != queueName {
			continue
		}
		candidates = append(candidates, task)
	}
	if len(candidates) == 0 {
		return model.ClaimedTask{}, false, nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority < candidates[j].Priority
		}
		return candidates[i].ScheduledAt.Before(candidates[j].ScheduledAt)
	})

	task := candidates[0]
	claimedAt := now
	task.Status = model.TaskStatusClaimed
	task.ClaimedAt = &claimedAt
	task.ClaimedBy = &workerID
	s.tasks[task.ID] = task

	task.Status = model.TaskStatusExecuting
	heartbeatAt := now
	task.HeartbeatAt = &heartbeatAt
	s.tasks[task.ID] = task
	s.leases[task.ID] = leaseExpiresAt

	taskQueueName, _ := s.queueStore.getNameByID(task.QueueID)
	return model.ClaimedTask{
		ID:             task.ID,
		Queue:          taskQueueName,
		Payload:        task.Payload,
		Priority:       task.Priority,
		Attempts:       task.Attempts,
		LeaseExpiresAt: leaseExpiresAt,
	}, true, nil
}

func (s *TaskStore) MarkExecuting(ctx context.Context, taskID string, workerID string) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) Ack(ctx context.Context, taskID string, workerID string, result []byte) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) Nack(ctx context.Context, taskID string, workerID string, errMessage string) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return model.ErrTaskNotFound
	}
	if task.ClaimedBy == nil || *task.ClaimedBy != workerID {
		return model.ErrTaskNotOwned
	}
	if task.Status != model.TaskStatusExecuting {
		return model.ErrTaskInvalidStatus
	}

	heartbeatAt := time.Now().UTC()
	task.HeartbeatAt = &heartbeatAt
	s.tasks[taskID] = task
	return nil
}

func (s *TaskStore) ListDue(ctx context.Context, limit int) ([]model.Task, error) {
	return s.ClaimPending(ctx, limit)
}

func (s *TaskStore) Reschedule(ctx context.Context, taskID string, scheduledAt time.Time) error {
	return model.ErrNotImplemented
}

func (s *TaskStore) ReclaimExpired(ctx context.Context, heartbeatTimeout time.Duration) ([]model.Task, error) {
	return nil, model.ErrNotImplemented
}

func (s *TaskStore) Stats(ctx context.Context, queueName string) (model.QueueStats, error) {
	return model.QueueStats{}, model.ErrNotImplemented
}
