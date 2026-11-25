// Package admin serves the operator dashboard at /admin: live queue health,
// a task inspector, a dead-letter browser with replay controls, and worker
// status. Pages are rendered with templ and enhanced with htmx; live panels
// are pushed over server-sent events.
package admin

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/admin/views"
	"taskforge.dev/internal/model"
	webadmin "taskforge.dev/web/admin"
)

// QueueStore is the queue persistence the dashboard needs.
type QueueStore interface {
	Create(ctx context.Context, queue model.Queue) (model.Queue, error)
	List(ctx context.Context) ([]model.Queue, error)
	UpdateRateLimit(ctx context.Context, name string, rateLimit *int) (model.Queue, error)
	Delete(ctx context.Context, name string) error
}

// TaskStore is the task persistence the dashboard needs.
type TaskStore interface {
	Stats(ctx context.Context, queueName string) (model.QueueStats, error)
	ListTasks(ctx context.Context, filter model.TaskFilter) ([]model.TaskSummary, error)
	GetTaskByID(ctx context.Context, taskID string) (model.TaskSummary, error)
	TaskEvents(ctx context.Context, taskID string) ([]model.TaskEvent, error)
	Cancel(ctx context.Context, queueName string, taskID string) error
	ListDeadLetters(ctx context.Context, filter model.DeadLetterFilter) ([]model.DeadLetterTask, error)
	ReplayDeadLetterTask(ctx context.Context, queueName string, taskID string) (model.Task, error)
	DeleteDeadLetterTask(ctx context.Context, queueName string, taskID string) error
	ActiveWorkers(ctx context.Context) ([]model.WorkerStatus, error)
}

type Options struct {
	Queues QueueStore
	Tasks  TaskStore
	Logger *slog.Logger
	// Username and Password enable HTTP basic auth when both are non-empty.
	Username string
	Password string
	// LiveInterval is how often SSE panels are re-rendered. Default 2s.
	LiveInterval time.Duration
}

type Handler struct {
	queues       QueueStore
	tasks        TaskStore
	logger       *slog.Logger
	username     string
	password     string
	liveInterval time.Duration

	// closing is cancelled by Close so open SSE streams end promptly when
	// the server shuts down instead of holding Shutdown open.
	closing context.Context
	close   context.CancelFunc
}

func New(opts Options) *Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	interval := opts.LiveInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	closing, closeFn := context.WithCancel(context.Background())
	return &Handler{
		closing:      closing,
		close:        closeFn,
		queues:       opts.Queues,
		tasks:        opts.Tasks,
		logger:       logger,
		username:     opts.Username,
		password:     opts.Password,
		liveInterval: interval,
	}
}

// Close ends all open live-update streams. Register it with
// http.Server.RegisterOnShutdown.
func (h *Handler) Close() { h.close() }

// Routes returns the dashboard router, to be mounted at /admin.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(h.basicAuth)
	r.Use(requireHTMXForWrites)

	r.Handle("/static/*", http.StripPrefix("/admin/static/", staticHandler()))

	r.Get("/", h.overview)
	r.Get("/events", h.events)
	r.Post("/queues", h.createQueue)
	r.Post("/queues/{name}/rate-limit", h.updateRateLimit)
	r.Delete("/queues/{name}", h.deleteQueue)

	r.Get("/tasks", h.taskList)
	r.Get("/tasks/{id}", h.taskDetail)
	r.Post("/tasks/{id}/cancel", h.cancelTask)

	r.Get("/dlq", h.dlqList)
	r.Post("/dlq/replay", h.dlqReplay)
	r.Post("/dlq/discard", h.dlqDiscard)

	r.Get("/workers", h.workers)
	return r
}

func staticHandler() http.Handler {
	files := http.FileServerFS(webadmin.Static())
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		files.ServeHTTP(w, r)
	})
}

// basicAuth protects every dashboard route when credentials are configured.
func (h *Handler) basicAuth(next http.Handler) http.Handler {
	if h.username == "" || h.password == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		userOK := subtle.ConstantTimeCompare([]byte(user), []byte(h.username)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(h.password)) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="TaskForge admin", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireHTMXForWrites rejects state-changing requests that were not sent by
// htmx. Browsers cannot attach the HX-Request header to a cross-site form
// post without a CORS preflight, which the dashboard never grants, so this
// blocks cross-site request forgery against the dashboard's actions.
func requireHTMXForWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			if r.Header.Get("HX-Request") != "true" {
				http.Error(w, "dashboard actions must be sent by htmx", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Overview

func (h *Handler) overview(w http.ResponseWriter, r *http.Request) {
	data, err := h.overviewData(r.Context())
	if err != nil {
		h.serverError(w, r, "load overview", err)
		return
	}
	queues, err := h.queues.List(r.Context())
	if err != nil {
		h.serverError(w, r, "list queues", err)
		return
	}
	h.render(w, r, http.StatusOK, views.OverviewPage(data, queues, views.Flash{}))
}

func (h *Handler) overviewData(ctx context.Context) (views.OverviewData, error) {
	queues, err := h.queues.List(ctx)
	if err != nil {
		return views.OverviewData{}, err
	}
	data := views.OverviewData{Queues: make([]model.QueueOverview, 0, len(queues)), GeneratedAt: time.Now().UTC()}
	for _, q := range queues {
		stats, err := h.tasks.Stats(ctx, q.Name)
		if errors.Is(err, model.ErrQueueNotFound) {
			continue // deleted between List and Stats
		}
		if err != nil {
			return views.OverviewData{}, err
		}
		data.Queues = append(data.Queues, model.QueueOverview{Queue: q, Stats: stats})
	}
	workers, err := h.tasks.ActiveWorkers(ctx)
	if err != nil {
		return views.OverviewData{}, err
	}
	data.Workers = workers
	return data, nil
}

func (h *Handler) createQueue(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Invalid form submission."})
		return
	}
	name := strings.TrimSpace(r.PostForm.Get("name"))
	if err := model.ValidateQueueName(name); err != nil {
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: capitalize(err.Error()) + "."})
		return
	}
	maxRetries, err := strconv.Atoi(strings.TrimSpace(r.PostForm.Get("max_retries")))
	if err != nil {
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Max retries must be a whole number."})
		return
	}
	if err := model.ValidateMaxRetries(maxRetries); err != nil {
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: capitalize(err.Error()) + "."})
		return
	}
	rateLimit, msg := parseRateLimit(r.PostForm.Get("rate_limit"))
	if msg != "" {
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: msg})
		return
	}

	_, err = h.queues.Create(r.Context(), model.NewQueue(name, maxRetries, rateLimit))
	switch {
	case errors.Is(err, model.ErrQueueAlreadyExists):
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "A queue named " + name + " already exists."})
	case err != nil:
		h.logger.ErrorContext(r.Context(), "admin create queue", "error", err)
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Could not create the queue. Check the server logs."})
	default:
		h.manageResponse(w, r, views.Flash{Kind: views.FlashSuccess, Message: "Created queue " + name + "."})
	}
}

func (h *Handler) updateRateLimit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := r.ParseForm(); err != nil {
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Invalid form submission."})
		return
	}
	rateLimit, msg := parseRateLimit(r.PostForm.Get("rate_limit"))
	if msg != "" {
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: msg})
		return
	}
	_, err := h.queues.UpdateRateLimit(r.Context(), name, rateLimit)
	switch {
	case errors.Is(err, model.ErrQueueNotFound):
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Queue " + name + " no longer exists."})
	case err != nil:
		h.logger.ErrorContext(r.Context(), "admin update rate limit", "error", err)
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Could not update the rate limit. Check the server logs."})
	case rateLimit == nil:
		h.manageResponse(w, r, views.Flash{Kind: views.FlashSuccess, Message: "Removed the rate limit on " + name + "."})
	default:
		h.manageResponse(w, r, views.Flash{Kind: views.FlashSuccess,
			Message: "Set " + name + " to " + strconv.Itoa(*rateLimit) + " tasks/sec."})
	}
}

func (h *Handler) deleteQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	err := h.queues.Delete(r.Context(), name)
	switch {
	case errors.Is(err, model.ErrQueueNotFound):
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Queue " + name + " no longer exists."})
	case errors.Is(err, model.ErrQueueHasActiveTasks):
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError,
			Message: "Queue " + name + " still has pending, retrying, or executing tasks. Drain it first."})
	case err != nil:
		h.logger.ErrorContext(r.Context(), "admin delete queue", "error", err)
		h.manageResponse(w, r, views.Flash{Kind: views.FlashError, Message: "Could not delete the queue. Check the server logs."})
	default:
		h.manageResponse(w, r, views.Flash{Kind: views.FlashSuccess, Message: "Deleted queue " + name + "."})
	}
}

// manageResponse re-renders the queue management panel with a flash message.
func (h *Handler) manageResponse(w http.ResponseWriter, r *http.Request, flash views.Flash) {
	queues, err := h.queues.List(r.Context())
	if err != nil {
		h.serverError(w, r, "list queues", err)
		return
	}
	h.render(w, r, http.StatusOK, views.ManageQueues(queues, flash))
}

// ---------------------------------------------------------------------------
// Live updates

// events streams re-rendered panels for the requested view as server-sent
// events until the client disconnects.
func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	view := r.URL.Query().Get("view")
	if view != "overview" && view != "workers" {
		http.Error(w, "unknown view", http.StatusBadRequest)
		return
	}

	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return
	}

	ticker := time.NewTicker(h.liveInterval)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(h.closing, cancel)
	defer stop()

	for {
		component, name, err := h.liveComponent(ctx, view)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			h.logger.ErrorContext(ctx, "admin live render", "view", view, "error", err)
			component, name = views.LiveError(), view
		}
		if err := writeSSE(ctx, w, name, component); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *Handler) liveComponent(ctx context.Context, view string) (templ.Component, string, error) {
	switch view {
	case "workers":
		workers, err := h.tasks.ActiveWorkers(ctx)
		if err != nil {
			return nil, "", err
		}
		return views.WorkerRows(workers, time.Now().UTC()), "workers", nil
	default:
		data, err := h.overviewData(ctx)
		if err != nil {
			return nil, "", err
		}
		return views.OverviewLive(data), "overview", nil
	}
}

// ---------------------------------------------------------------------------
// Task inspector

func (h *Handler) taskList(w http.ResponseWriter, r *http.Request) {
	form, filter, flash := parseTaskFilter(r)
	data := views.TaskListData{Form: form, Flash: flash}

	if flash.Message == "" {
		filter.Limit = model.DefaultPageSize + 1
		tasks, err := h.tasks.ListTasks(r.Context(), filter)
		if err != nil {
			h.serverError(w, r, "list tasks", err)
			return
		}
		if len(tasks) > model.DefaultPageSize {
			data.HasNext = true
			tasks = tasks[:model.DefaultPageSize]
		}
		data.Tasks = tasks
	}

	if isFragmentRequest(r, "task-results") {
		h.render(w, r, http.StatusOK, views.TaskResults(data))
		return
	}
	queueNames, err := h.queueNames(r.Context())
	if err != nil {
		h.serverError(w, r, "list queues", err)
		return
	}
	data.QueueNames = queueNames
	h.render(w, r, http.StatusOK, views.TasksPage(data))
}

func (h *Handler) taskDetail(w http.ResponseWriter, r *http.Request) {
	h.renderTaskDetail(w, r, chi.URLParam(r, "id"), views.Flash{})
}

func (h *Handler) renderTaskDetail(w http.ResponseWriter, r *http.Request, taskID string, flash views.Flash) {
	ctx := r.Context()
	data := views.TaskDetailData{TaskID: taskID, Flash: flash}

	task, err := h.tasks.GetTaskByID(ctx, taskID)
	switch {
	case err == nil:
		data.Task = &task
	case !errors.Is(err, model.ErrTaskNotFound):
		h.serverError(w, r, "get task", err)
		return
	}

	if data.Task == nil {
		dead, err := h.tasks.ListDeadLetters(ctx, model.DeadLetterFilter{TaskID: taskID, Limit: 1})
		if err != nil {
			h.serverError(w, r, "get dead-letter task", err)
			return
		}
		if len(dead) == 1 {
			data.Dead = &dead[0]
		}
	}

	events, err := h.tasks.TaskEvents(ctx, taskID)
	if err != nil {
		h.serverError(w, r, "get task events", err)
		return
	}
	data.Events = events

	status := http.StatusOK
	if data.Task == nil && data.Dead == nil && len(events) == 0 {
		status = http.StatusNotFound
	}
	if isFragmentRequest(r, "task-detail") {
		h.render(w, r, status, views.TaskDetailBody(data))
		return
	}
	h.render(w, r, status, views.TaskDetailPage(data))
}

func (h *Handler) cancelTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	if err := r.ParseForm(); err != nil {
		h.renderTaskDetail(w, r, taskID, views.Flash{Kind: views.FlashError, Message: "Invalid form submission."})
		return
	}
	err := h.tasks.Cancel(r.Context(), r.PostForm.Get("queue"), taskID)
	flash := views.Flash{Kind: views.FlashSuccess, Message: "Task cancelled."}
	switch {
	case errors.Is(err, model.ErrTaskNotCancellable):
		flash = views.Flash{Kind: views.FlashError, Message: "Only pending tasks can be cancelled; this task has already moved on."}
	case errors.Is(err, model.ErrTaskNotFound), errors.Is(err, model.ErrQueueNotFound):
		flash = views.Flash{Kind: views.FlashError, Message: "Task not found."}
	case err != nil:
		h.logger.ErrorContext(r.Context(), "admin cancel task", "error", err)
		flash = views.Flash{Kind: views.FlashError, Message: "Could not cancel the task. Check the server logs."}
	}
	h.renderTaskDetail(w, r, taskID, flash)
}

// ---------------------------------------------------------------------------
// Dead-letter browser

func (h *Handler) dlqList(w http.ResponseWriter, r *http.Request) {
	h.renderDLQ(w, r, views.Flash{})
}

func (h *Handler) renderDLQ(w http.ResponseWriter, r *http.Request, flash views.Flash) {
	form, filter, filterFlash := parseDLQFilter(r)
	if flash.Message == "" {
		flash = filterFlash
	}
	data := views.DLQData{Form: form, Flash: flash}

	if filterFlash.Message == "" {
		filter.Limit = model.DefaultPageSize + 1
		tasks, err := h.tasks.ListDeadLetters(r.Context(), filter)
		if err != nil {
			h.serverError(w, r, "list dead-letter tasks", err)
			return
		}
		if len(tasks) > model.DefaultPageSize {
			data.HasNext = true
			tasks = tasks[:model.DefaultPageSize]
		}
		data.Tasks = tasks
	}

	if isFragmentRequest(r, "dlq-results") {
		h.render(w, r, http.StatusOK, views.DLQResults(data))
		return
	}
	queueNames, err := h.queueNames(r.Context())
	if err != nil {
		h.serverError(w, r, "list queues", err)
		return
	}
	data.QueueNames = queueNames
	h.render(w, r, http.StatusOK, views.DLQPage(data))
}

func (h *Handler) dlqReplay(w http.ResponseWriter, r *http.Request) {
	h.dlqBulk(w, r, "replay")
}

func (h *Handler) dlqDiscard(w http.ResponseWriter, r *http.Request) {
	h.dlqBulk(w, r, "discard")
}

// dlqBulk replays or discards every selected dead-letter task. Selections are
// submitted as "queue:id" values in the repeated "selected" field.
func (h *Handler) dlqBulk(w http.ResponseWriter, r *http.Request, action string) {
	if err := r.ParseForm(); err != nil {
		h.renderDLQ(w, r, views.Flash{Kind: views.FlashError, Message: "Invalid form submission."})
		return
	}
	selected := r.PostForm["selected"]
	if len(selected) == 0 {
		h.renderDLQ(w, r, views.Flash{Kind: views.FlashError, Message: "Select at least one task."})
		return
	}

	succeeded, failed := 0, 0
	for _, value := range selected {
		queueName, taskID, ok := strings.Cut(value, ":")
		if !ok {
			failed++
			continue
		}
		var err error
		if action == "replay" {
			_, err = h.tasks.ReplayDeadLetterTask(r.Context(), queueName, taskID)
		} else {
			err = h.tasks.DeleteDeadLetterTask(r.Context(), queueName, taskID)
		}
		if err != nil {
			if !errors.Is(err, model.ErrDeadLetterTaskNotFound) && !errors.Is(err, model.ErrQueueNotFound) {
				h.logger.ErrorContext(r.Context(), "admin dlq "+action, "task_id", taskID, "error", err)
			}
			failed++
			continue
		}
		succeeded++
	}

	verb := map[string]string{"replay": "Replayed", "discard": "Discarded"}[action]
	flash := views.Flash{Kind: views.FlashSuccess, Message: verb + " " + plural(succeeded, "task") + "."}
	if failed > 0 {
		flash.Kind = views.FlashError
		flash.Message += " " + plural(failed, "task") + " could not be " + strings.ToLower(verb) +
			" (already removed or failed; see server logs)."
	}

	// A single-task action from the task detail page re-renders that page.
	if isFragmentRequest(r, "task-detail") && len(selected) == 1 {
		_, taskID, _ := strings.Cut(selected[0], ":")
		h.renderTaskDetail(w, r, taskID, flash)
		return
	}
	h.renderDLQ(w, r, flash)
}

// ---------------------------------------------------------------------------
// Workers

func (h *Handler) workers(w http.ResponseWriter, r *http.Request) {
	workers, err := h.tasks.ActiveWorkers(r.Context())
	if err != nil {
		h.serverError(w, r, "list workers", err)
		return
	}
	h.render(w, r, http.StatusOK, views.WorkersPage(workers, time.Now().UTC()))
}

// ---------------------------------------------------------------------------
// Helpers

func (h *Handler) queueNames(ctx context.Context) ([]string, error) {
	queues, err := h.queues.List(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(queues))
	for _, q := range queues {
		names = append(names, q.Name)
	}
	return names, nil
}

func (h *Handler) render(w http.ResponseWriter, r *http.Request, status int, component templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "HX-Request, HX-Target")
	w.WriteHeader(status)
	if err := component.Render(r.Context(), w); err != nil && r.Context().Err() == nil {
		h.logger.ErrorContext(r.Context(), "admin render", "path", r.URL.Path, "error", err)
	}
}

func (h *Handler) serverError(w http.ResponseWriter, r *http.Request, op string, err error) {
	h.logger.ErrorContext(r.Context(), "admin "+op, "error", err)
	if isHTMX(r) {
		h.render(w, r, http.StatusInternalServerError, views.FlashMessage(views.Flash{
			Kind: views.FlashError, Message: "Something went wrong (" + op + "). Check the server logs.",
		}))
		return
	}
	h.render(w, r, http.StatusInternalServerError, views.ErrorPage("Something went wrong", "Could not "+op+". Check the server logs."))
}

func isHTMX(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

// isFragmentRequest reports whether an htmx request targets the element with
// the given id, in which case only that fragment is rendered. History
// restores and full navigations get the whole page.
func isFragmentRequest(r *http.Request, target string) bool {
	return isHTMX(r) && r.Header.Get("HX-History-Restore-Request") != "true" && r.Header.Get("HX-Target") == target
}

func parseRateLimit(raw string) (*int, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, ""
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return nil, "Rate limit must be a whole number of at least 1, or empty for unlimited."
	}
	return &n, ""
}

// timeLayouts are accepted for filter inputs; datetime-local values carry no
// zone and are interpreted as UTC, matching how the dashboard displays times.
var timeLayouts = []string{"2006-01-02T15:04", "2006-01-02T15:04:05", time.RFC3339}

func parseTimeParam(raw string) (*time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}
	for _, layout := range timeLayouts {
		if t, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			t = t.UTC()
			return &t, true
		}
	}
	return nil, false
}

func parsePage(raw string) int {
	page, err := strconv.Atoi(raw)
	if err != nil || page < 1 {
		return 1
	}
	return page
}

var taskStatuses = map[string]bool{
	string(model.TaskStatusPending): true, string(model.TaskStatusClaimed): true,
	string(model.TaskStatusExecuting): true, string(model.TaskStatusDone): true,
	string(model.TaskStatusRetry): true, string(model.TaskStatusCancelled): true,
}

func parseTaskFilter(r *http.Request) (views.TaskFilterForm, model.TaskFilter, views.Flash) {
	q := r.URL.Query()
	form := views.TaskFilterForm{
		Queue:  strings.TrimSpace(q.Get("queue")),
		Status: strings.TrimSpace(q.Get("status")),
		Worker: strings.TrimSpace(q.Get("worker")),
		TaskID: strings.TrimSpace(q.Get("task_id")),
		From:   strings.TrimSpace(q.Get("from")),
		To:     strings.TrimSpace(q.Get("to")),
		Page:   parsePage(q.Get("page")),
	}
	filter := model.TaskFilter{
		Queue:    form.Queue,
		WorkerID: form.Worker,
		TaskID:   form.TaskID,
		Offset:   (form.Page - 1) * model.DefaultPageSize,
	}
	if form.Status != "" {
		if !taskStatuses[form.Status] {
			return form, filter, views.Flash{Kind: views.FlashError, Message: "Unknown status " + form.Status + "."}
		}
		filter.Status = model.TaskStatus(form.Status)
	}
	from, ok := parseTimeParam(form.From)
	if !ok {
		return form, filter, views.Flash{Kind: views.FlashError, Message: "Could not read the 'created after' time."}
	}
	to, ok := parseTimeParam(form.To)
	if !ok {
		return form, filter, views.Flash{Kind: views.FlashError, Message: "Could not read the 'created before' time."}
	}
	filter.CreatedAfter, filter.CreatedBefore = from, to
	return form, filter, views.Flash{}
}

func parseDLQFilter(r *http.Request) (views.DLQFilterForm, model.DeadLetterFilter, views.Flash) {
	// Bulk actions post the current filters alongside the selection so the
	// refreshed list keeps them.
	values := r.URL.Query()
	if r.Method == http.MethodPost {
		values = r.Form
	}
	form := views.DLQFilterForm{
		Queue: strings.TrimSpace(values.Get("queue")),
		From:  strings.TrimSpace(values.Get("from")),
		To:    strings.TrimSpace(values.Get("to")),
		Page:  parsePage(values.Get("page")),
	}
	filter := model.DeadLetterFilter{Queue: form.Queue, Offset: (form.Page - 1) * model.DefaultPageSize}
	from, ok := parseTimeParam(form.From)
	if !ok {
		return form, filter, views.Flash{Kind: views.FlashError, Message: "Could not read the 'died after' time."}
	}
	to, ok := parseTimeParam(form.To)
	if !ok {
		return form, filter, views.Flash{Kind: views.FlashError, Message: "Could not read the 'died before' time."}
	}
	filter.DiedAfter, filter.DiedBefore = from, to
	return form, filter, views.Flash{}
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
