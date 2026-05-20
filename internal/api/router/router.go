package router

import (
	"context"
	"time"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/api/handlers"
	appmiddleware "taskforge.dev/internal/api/middleware"
	"taskforge.dev/internal/model"
	"taskforge.dev/internal/storage/memory"
)

func NewRouter() *chi.Mux {
	queueStore := memory.NewQueueStore()
	taskStore := memory.NewTaskStore(queueStore)
	return NewRouterWithStores(queueStore, taskStore)
}

func NewRouterWithQueueStore(queueStore handlers.QueueStore) *chi.Mux {
	taskStore, _ := queueStoreTaskStore(queueStore)
	return NewRouterWithStores(queueStore, taskStore)
}

func NewRouterWithStores(queueStore handlers.QueueStore, taskStore handlers.TaskStore) *chi.Mux {
	r := chi.NewRouter()
	r.Use(appmiddleware.Logger)
	r.Use(appmiddleware.Recovery)

	queueHandler := handlers.NewQueueHandler(queueStore)
	taskHandler := handlers.NewTaskHandler(taskStore)
	statsHandler := handlers.NewStatsHandler()
	workerHandler := handlers.NewWorkerHandler(taskStore)
	dlqHandler := handlers.NewDLQHandler()

	r.Post("/queues", queueHandler.CreateQueue)
	r.Get("/queues", queueHandler.ListQueues)
	r.Delete("/queues/{name}", queueHandler.DeleteQueue)

	r.Post("/queues/{name}/tasks", taskHandler.EnqueueTask)
	r.Get("/queues/{name}/tasks/{id}", taskHandler.GetTask)
	r.Delete("/queues/{name}/tasks/{id}", taskHandler.CancelTask)
	r.Get("/queues/{name}/stats", statsHandler.GetQueueStats)

	r.Post("/workers/claim", workerHandler.ClaimTask)
	r.Post("/workers/heartbeat", workerHandler.Heartbeat)
	r.Post("/workers/ack", workerHandler.Ack)
	r.Post("/workers/nack", workerHandler.Nack)

	r.Get("/queues/{name}/dlq", dlqHandler.ListDeadLetterTasks)
	r.Post("/queues/{name}/dlq/{id}/replay", dlqHandler.ReplayDeadLetterTask)
	r.Delete("/queues/{name}/dlq/{id}", dlqHandler.DeleteDeadLetterTask)

	return r
}

func queueStoreTaskStore(queueStore handlers.QueueStore) (handlers.TaskStore, bool) {
	store, ok := queueStore.(*memory.QueueStore)
	if !ok {
		return unavailableTaskStore{}, false
	}

	return memory.NewTaskStore(store), true
}

type unavailableTaskStore struct{}

func (s unavailableTaskStore) Create(ctx context.Context, queueName string, task model.Task) (model.Task, error) {
	return model.Task{}, model.ErrNotImplemented
}

func (s unavailableTaskStore) Get(ctx context.Context, queueName string, taskID string) (model.Task, error) {
	return model.Task{}, model.ErrNotImplemented
}

func (s unavailableTaskStore) Cancel(ctx context.Context, queueName string, taskID string) error {
	return model.ErrNotImplemented
}

func (s unavailableTaskStore) ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error) {
	return model.ClaimedTask{}, false, model.ErrNotImplemented
}

func (s unavailableTaskStore) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	return model.ErrNotImplemented
}
