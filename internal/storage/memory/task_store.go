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
}

func NewTaskStore(queueStore *QueueStore) *TaskStore {
	return &TaskStore{
		queueStore: queueStore,
		tasks:      make(map[string]model.Task),
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
	return model.ErrNotImplemented
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
