package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/model"
)

type TaskStore interface {
	Create(ctx context.Context, queueName string, task model.Task) (model.Task, error)
}

type TaskHandler struct {
	store TaskStore
}

type enqueueTaskRequest struct {
	Payload json.RawMessage `json:"payload"`
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
	task := model.NewImmediateTask("", payload)
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

func (h *TaskHandler) GetTask(w http.ResponseWriter, r *http.Request) {}

func (h *TaskHandler) CancelTask(w http.ResponseWriter, r *http.Request) {}

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
