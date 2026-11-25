package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"qrunnr.dev/internal/model"
)

type QueueStore interface {
	Create(ctx context.Context, queue model.Queue) (model.Queue, error)
	List(ctx context.Context) ([]model.Queue, error)
	GetByName(ctx context.Context, name string) (model.Queue, error)
	UpdateRateLimit(ctx context.Context, name string, rateLimit *int) (model.Queue, error)
	Delete(ctx context.Context, name string) error
}

type QueueHandler struct {
	store QueueStore
}

type createQueueRequest struct {
	Name       string `json:"name"`
	MaxRetries *int   `json:"max_retries"`
	RateLimit  *int   `json:"rate_limit"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type patchQueueRequest struct {
	RateLimit *int `json:"rate_limit"`
}

func NewQueueHandler(store QueueStore) *QueueHandler {
	return &QueueHandler{store: store}
}

func (h *QueueHandler) CreateQueue(w http.ResponseWriter, r *http.Request) {
	var req createQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	if err := model.ValidateQueueName(req.Name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.MaxRetries == nil {
		writeError(w, http.StatusBadRequest, "max_retries is required")
		return
	}

	if err := model.ValidateMaxRetries(*req.MaxRetries); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	queue := model.NewQueue(req.Name, *req.MaxRetries, req.RateLimit)
	created, err := h.store.Create(r.Context(), queue)
	if errors.Is(err, model.ErrQueueAlreadyExists) {
		writeError(w, http.StatusConflict, "queue with name "+req.Name+" already exists")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create queue")
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

func (h *QueueHandler) ListQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := h.store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list queues")
		return
	}

	writeJSON(w, http.StatusOK, queues)
}

func (h *QueueHandler) PatchQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var req patchQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}
	if req.RateLimit != nil && *req.RateLimit < 1 {
		writeError(w, http.StatusBadRequest, "rate_limit must be greater than 0 when set")
		return
	}

	queue, err := h.store.UpdateRateLimit(r.Context(), name, req.RateLimit)
	if errors.Is(err, model.ErrQueueNotFound) {
		writeError(w, http.StatusNotFound, "queue with name "+name+" was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update queue")
		return
	}

	writeJSON(w, http.StatusOK, queue)
}

func (h *QueueHandler) DeleteQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.store.Delete(r.Context(), name); errors.Is(err, model.ErrQueueNotFound) {
		writeError(w, http.StatusNotFound, "queue with name "+name+" was not found")
		return
	} else if errors.Is(err, model.ErrQueueHasActiveTasks) {
		writeError(w, http.StatusConflict, "queue "+name+" has pending, claimed, or executing tasks and must be drained first")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete queue")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
