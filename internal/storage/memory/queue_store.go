package memory

import (
	"context"
	"sort"
	"sync"

	"qrunnr.dev/internal/model"
)

type QueueStore struct {
	mu          sync.RWMutex
	queues      map[string]model.Queue
	activeTasks map[string]int
	// onDelete is called with the deleted queue's ID after the queue store
	// lock is released, so the task store can drop the queue's tasks.
	onDelete func(queueID string)
}

func NewQueueStore() *QueueStore {
	return &QueueStore{
		queues:      make(map[string]model.Queue),
		activeTasks: make(map[string]int),
	}
}

func (s *QueueStore) Create(ctx context.Context, queue model.Queue) (model.Queue, error) {
	select {
	case <-ctx.Done():
		return model.Queue{}, ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[queue.Name]; exists {
		return model.Queue{}, model.ErrQueueAlreadyExists
	}

	s.queues[queue.Name] = queue
	return queue, nil
}

func (s *QueueStore) List(ctx context.Context) ([]model.Queue, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	queues := make([]model.Queue, 0, len(s.queues))
	for _, queue := range s.queues {
		queues = append(queues, queue)
	}
	sort.Slice(queues, func(i, j int) bool {
		return queues[i].Name < queues[j].Name
	})

	return queues, nil
}

func (s *QueueStore) GetByName(ctx context.Context, name string) (model.Queue, error) {
	select {
	case <-ctx.Done():
		return model.Queue{}, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	queue, exists := s.queues[name]
	if !exists {
		return model.Queue{}, model.ErrQueueNotFound
	}

	return queue, nil
}

func (s *QueueStore) UpdateRateLimit(ctx context.Context, name string, rateLimit *int) (model.Queue, error) {
	select {
	case <-ctx.Done():
		return model.Queue{}, ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	queue, exists := s.queues[name]
	if !exists {
		return model.Queue{}, model.ErrQueueNotFound
	}
	queue.RateLimit = rateLimit
	s.queues[name] = queue
	return queue, nil
}

func (s *QueueStore) getNameByID(id string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for name, queue := range s.queues {
		if queue.ID == id {
			return name, true
		}
	}

	return "", false
}

func (s *QueueStore) getByID(id string) (model.Queue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, queue := range s.queues {
		if queue.ID == id {
			return queue, true
		}
	}

	return model.Queue{}, false
}

func (s *QueueStore) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	queue, exists := s.queues[name]
	if !exists {
		s.mu.Unlock()
		return model.ErrQueueNotFound
	}
	if s.activeTasks[name] > 0 {
		s.mu.Unlock()
		return model.ErrQueueHasActiveTasks
	}
	delete(s.queues, name)
	delete(s.activeTasks, name)
	onDelete := s.onDelete
	s.mu.Unlock()

	if onDelete != nil {
		onDelete(queue.ID)
	}
	return nil
}

func (s *QueueStore) addActiveTask(queueName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.activeTasks[queueName]++
}

func (s *QueueStore) removeActiveTask(queueName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeTasks[queueName] > 0 {
		s.activeTasks[queueName]--
	}
}
