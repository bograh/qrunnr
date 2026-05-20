package router

import (
	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/api/handlers"
	appmiddleware "taskforge.dev/internal/api/middleware"
	"taskforge.dev/internal/storage/memory"
)

func NewRouter() *chi.Mux {
	queueStore := memory.NewQueueStore()
	return NewRouterWithQueueStore(queueStore)
}

func NewRouterWithQueueStore(queueStore handlers.QueueStore) *chi.Mux {
	r := chi.NewRouter()
	r.Use(appmiddleware.Logger)
	r.Use(appmiddleware.Recovery)

	queueHandler := handlers.NewQueueHandler(queueStore)
	taskHandler := handlers.NewTaskHandler()
	statsHandler := handlers.NewStatsHandler()
	workerHandler := handlers.NewWorkerHandler()
	dlqHandler := handlers.NewDLQHandler()

	r.Post("/queues", queueHandler.CreateQueue)
	r.Get("/queues", queueHandler.ListQueues)
	r.Delete("/queues/{name}", queueHandler.DeleteQueue)

	r.Post("/queues/{name}/tasks", taskHandler.EnqueueTask)
	r.Get("/queues/{name}/tasks/{id}", taskHandler.GetTask)
	r.Delete("/queues/{name}/tasks/{id}", taskHandler.CancelTask)
	r.Get("/queues/{name}/stats", statsHandler.GetQueueStats)

	r.Post("/workers/heartbeat", workerHandler.Heartbeat)
	r.Post("/workers/ack", workerHandler.Ack)
	r.Post("/workers/nack", workerHandler.Nack)

	r.Get("/queues/{name}/dlq", dlqHandler.ListDeadLetterTasks)
	r.Post("/queues/{name}/dlq/{id}/replay", dlqHandler.ReplayDeadLetterTask)
	r.Delete("/queues/{name}/dlq/{id}", dlqHandler.DeleteDeadLetterTask)

	return r
}
