package memory

import (
	"context"
	"sort"
	"sync"

	"taskforge.dev/internal/model"
)

type QueueStore struct {
	mu          sync.RWMutex
	queues      map[string]model.Queue
	activeTasks map[string]int
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

func (s *QueueStore) Delete(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[name]; !exists {
		return model.ErrQueueNotFound
	}
	if s.activeTasks[name] > 0 {
		return model.ErrQueueHasActiveTasks
	}

	delete(s.queues, name)
	return nil
}

func (s *QueueStore) addActiveTask(queueName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.activeTasks[queueName]++
}
