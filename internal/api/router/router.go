package router

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"taskforge.dev/internal/api/handlers"
	appmiddleware "taskforge.dev/internal/api/middleware"
	"taskforge.dev/internal/storage/memory"
)

// Options configures the HTTP router. QueueStore and TaskStore are required;
// everything else is optional.
type Options struct {
	QueueStore handlers.QueueStore
	TaskStore  handlers.TaskStore
	Logger     *slog.Logger
	// LeaseDuration is reported to workers as lease_expires_at on claim.
	LeaseDuration time.Duration
	// Metrics serves /metrics. When nil the endpoint is not registered.
	Metrics http.Handler
	// Admin is mounted at /admin. When nil the dashboard is not served.
	Admin http.Handler
	// Ready reports dependency health for /readyz.
	Ready func(ctx context.Context) error
}

// NewRouter returns a router backed by fresh in-memory stores.
func NewRouter() *chi.Mux {
	queueStore := memory.NewQueueStore()
	return NewRouterWithStores(queueStore, memory.NewTaskStore(queueStore))
}

func NewRouterWithStores(queueStore handlers.QueueStore, taskStore handlers.TaskStore) *chi.Mux {
	return New(Options{QueueStore: queueStore, TaskStore: taskStore})
}

func New(opts Options) *chi.Mux {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	r := chi.NewRouter()
	r.Use(chimiddleware.RequestID)
	r.Use(appmiddleware.Logger(logger))
	r.Use(appmiddleware.Recovery(logger))

	queueHandler := handlers.NewQueueHandler(opts.QueueStore)
	taskHandler := handlers.NewTaskHandler(opts.TaskStore)
	statsHandler := handlers.NewStatsHandler(opts.TaskStore)
	workerHandler := handlers.NewWorkerHandler(opts.TaskStore).WithLeaseDuration(opts.LeaseDuration)
	dlqHandler := handlers.NewDLQHandler(opts.TaskStore)

	r.Get("/healthz", handlers.Healthz)
	r.Get("/readyz", handlers.Readyz(opts.Ready))
	if opts.Metrics != nil {
		r.Method(http.MethodGet, "/metrics", opts.Metrics)
	}

	r.Post("/queues", queueHandler.CreateQueue)
	r.Get("/queues", queueHandler.ListQueues)
	r.Patch("/queues/{name}", queueHandler.PatchQueue)
	r.Delete("/queues/{name}", queueHandler.DeleteQueue)

	r.Post("/queues/{name}/tasks", taskHandler.EnqueueTask)
	r.Get("/queues/{name}/tasks/{id}", taskHandler.GetTask)
	r.Delete("/queues/{name}/tasks/{id}", taskHandler.CancelTask)
	r.Get("/queues/{name}/stats", statsHandler.GetQueueStats)

	r.Post("/workers/claim", workerHandler.ClaimTask)
	r.Get("/workers", workerHandler.ListWorkers)
	r.Post("/workers/heartbeat", workerHandler.Heartbeat)
	r.Post("/workers/ack", workerHandler.Ack)
	r.Post("/workers/nack", workerHandler.Nack)

	r.Get("/queues/{name}/dlq", dlqHandler.ListDeadLetterTasks)
	r.Post("/queues/{name}/dlq/{id}/replay", dlqHandler.ReplayDeadLetterTask)
	r.Delete("/queues/{name}/dlq/{id}", dlqHandler.DeleteDeadLetterTask)

	if opts.Admin != nil {
		r.Get("/admin", func(w http.ResponseWriter, req *http.Request) {
			http.Redirect(w, req, "/admin/", http.StatusMovedPermanently)
		})
		r.Mount("/admin/", opts.Admin)
	}
	return r
}
