package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/model"
)

type TaskStore interface {
	Create(ctx context.Context, queueName string, task model.Task) (model.Task, error)
	Get(ctx context.Context, queueName string, taskID string) (model.Task, error)
	Cancel(ctx context.Context, queueName string, taskID string) error
	ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error)
	Heartbeat(ctx context.Context, taskID string, workerID string) error
	Ack(ctx context.Context, taskID string, workerID string, result []byte) error
	Nack(ctx context.Context, taskID string, workerID string, errMessage string) error
	ListDeadLetterTasks(ctx context.Context, queueName string) ([]model.DeadLetterTask, error)
	Stats(ctx context.Context, queueName string) (model.QueueStats, error)
}

type TaskHandler struct {
	store TaskStore
}

type enqueueTaskRequest struct {
	Payload     json.RawMessage `json:"payload"`
	ScheduledAt *time.Time      `json:"scheduled_at"`
	Priority    *int            `json:"priority"`
}

func NewTaskHandler(store TaskStore) *TaskHandler {
	return &TaskHandler{store: store}
}

func (h *TaskHandler) EnqueueTask(w http.ResponseWriter, r *http.Request) {
	var req enqueueTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	payload, err := validateObjectPayload(req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	queueName := chi.URLParam(r, "name")
	scheduledAt := time.Now().UTC()
	if req.ScheduledAt != nil {
		scheduledAt = req.ScheduledAt.UTC()
	}
	priority := 5
	if req.Priority != nil {
		if err := model.ValidatePriority(*req.Priority); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		priority = *req.Priority
	}

	task := model.NewTask("", payload, scheduledAt, priority)
	created, err := h.store.Create(r.Context(), queueName, task)
	if errors.Is(err, model.ErrQueueNotFound) {
		writeError(w, http.StatusNotFound, "queue with name "+queueName+" was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to enqueue task")
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

func (h *TaskHandler) GetTask(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	taskID := chi.URLParam(r, "id")

	task, err := h.store.Get(r.Context(), queueName, taskID)
	if errors.Is(err, model.ErrQueueNotFound) || errors.Is(err, model.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task with id "+taskID+" was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get task")
		return
	}

	writeJSON(w, http.StatusOK, task)
}

func (h *TaskHandler) CancelTask(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	taskID := chi.URLParam(r, "id")

	err := h.store.Cancel(r.Context(), queueName, taskID)
	if errors.Is(err, model.ErrQueueNotFound) || errors.Is(err, model.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task with id "+taskID+" was not found")
		return
	}
	if errors.Is(err, model.ErrTaskNotCancellable) {
		writeError(w, http.StatusConflict, "task "+taskID+" cannot be cancelled because it is not pending")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel task")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func validateObjectPayload(payload json.RawMessage) (json.RawMessage, error) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return nil, errors.New("payload is required and must be a JSON object")
	}
	if !bytes.HasPrefix(payload, []byte("{")) {
		return nil, errors.New("payload must be a JSON object")
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, errors.New("payload must be a valid JSON object")
	}

	return payload, nil
}
