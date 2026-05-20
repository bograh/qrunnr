package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/model"
)

type DLQStore interface {
	ListDeadLetterTasks(ctx context.Context, queueName string) ([]model.DeadLetterTask, error)
	ReplayDeadLetterTask(ctx context.Context, queueName string, taskID string) (model.Task, error)
	DeleteDeadLetterTask(ctx context.Context, queueName string, taskID string) error
}

type DLQHandler struct {
	store DLQStore
}

func NewDLQHandler(store DLQStore) *DLQHandler {
	return &DLQHandler{store: store}
}

func (h *DLQHandler) ListDeadLetterTasks(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	tasks, err := h.store.ListDeadLetterTasks(r.Context(), queueName)
	if errors.Is(err, model.ErrQueueNotFound) {
		writeError(w, http.StatusNotFound, "queue with name "+queueName+" was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list dead-letter tasks")
		return
	}

	writeJSON(w, http.StatusOK, tasks)
}

func (h *DLQHandler) ReplayDeadLetterTask(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	taskID := chi.URLParam(r, "id")

	task, err := h.store.ReplayDeadLetterTask(r.Context(), queueName, taskID)
	if errors.Is(err, model.ErrQueueNotFound) || errors.Is(err, model.ErrDeadLetterTaskNotFound) {
		writeError(w, http.StatusNotFound, "dead-letter task with id "+taskID+" was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to replay dead-letter task")
		return
	}

	writeJSON(w, http.StatusCreated, task)
}

func (h *DLQHandler) DeleteDeadLetterTask(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	taskID := chi.URLParam(r, "id")

	err := h.store.DeleteDeadLetterTask(r.Context(), queueName, taskID)
	if errors.Is(err, model.ErrQueueNotFound) || errors.Is(err, model.ErrDeadLetterTaskNotFound) {
		writeError(w, http.StatusNotFound, "dead-letter task with id "+taskID+" was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete dead-letter task")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
