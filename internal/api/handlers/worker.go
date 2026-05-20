package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"taskforge.dev/internal/model"
)

const defaultLeaseDuration = 30 * time.Second

type WorkerStore interface {
	ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error)
	Heartbeat(ctx context.Context, taskID string, workerID string) error
	Ack(ctx context.Context, taskID string, workerID string, result []byte) error
	Nack(ctx context.Context, taskID string, workerID string, errMessage string) error
	ActiveWorkers(ctx context.Context) ([]model.WorkerStatus, error)
}

type WorkerHandler struct {
	store WorkerStore
}

type claimTaskRequest struct {
	WorkerID string `json:"worker_id"`
	Queue    string `json:"queue"`
}

type workerTaskRequest struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
}

type ackTaskRequest struct {
	TaskID   string          `json:"task_id"`
	WorkerID string          `json:"worker_id"`
	Result   json.RawMessage `json:"result"`
}

type nackTaskRequest struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
	Error    string `json:"error"`
}

func NewWorkerHandler(store WorkerStore) *WorkerHandler {
	return &WorkerHandler{store: store}
}

func (h *WorkerHandler) ClaimTask(w http.ResponseWriter, r *http.Request) {
	var req claimTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id is required")
		return
	}

	task, ok, err := h.store.ClaimForWorker(r.Context(), req.WorkerID, req.Queue, defaultLeaseDuration)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to claim task")
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	writeJSON(w, http.StatusOK, task)
}

func (h *WorkerHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	var req workerTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}
	if req.TaskID == "" {
		writeError(w, http.StatusBadRequest, "task_id is required")
		return
	}
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id is required")
		return
	}

	err := h.store.Heartbeat(r.Context(), req.TaskID, req.WorkerID)
	if errors.Is(err, model.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task with id "+req.TaskID+" was not found")
		return
	}
	if errors.Is(err, model.ErrTaskNotOwned) {
		writeError(w, http.StatusForbidden, "task "+req.TaskID+" is not owned by worker "+req.WorkerID)
		return
	}
	if errors.Is(err, model.ErrTaskInvalidStatus) {
		writeError(w, http.StatusConflict, "task "+req.TaskID+" is not executing")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to record heartbeat")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *WorkerHandler) Ack(w http.ResponseWriter, r *http.Request) {
	var req ackTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}
	if req.TaskID == "" {
		writeError(w, http.StatusBadRequest, "task_id is required")
		return
	}
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id is required")
		return
	}

	result := []byte(nil)
	if len(req.Result) > 0 {
		validated, err := validateObjectPayload(req.Result)
		if err != nil {
			writeError(w, http.StatusBadRequest, "result must be a JSON object")
			return
		}
		result = validated
	}

	err := h.store.Ack(r.Context(), req.TaskID, req.WorkerID, result)
	if errors.Is(err, model.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task with id "+req.TaskID+" was not found")
		return
	}
	if errors.Is(err, model.ErrTaskNotOwned) {
		writeError(w, http.StatusForbidden, "task "+req.TaskID+" is not owned by worker "+req.WorkerID)
		return
	}
	if errors.Is(err, model.ErrTaskLeaseExpired) {
		writeError(w, http.StatusConflict, "task "+req.TaskID+" lease expired")
		return
	}
	if errors.Is(err, model.ErrTaskInvalidStatus) {
		writeError(w, http.StatusConflict, "task "+req.TaskID+" is not executing")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to acknowledge task")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *WorkerHandler) Nack(w http.ResponseWriter, r *http.Request) {
	var req nackTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}
	if req.TaskID == "" {
		writeError(w, http.StatusBadRequest, "task_id is required")
		return
	}
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	if req.Error == "" {
		writeError(w, http.StatusBadRequest, "error is required")
		return
	}

	err := h.store.Nack(r.Context(), req.TaskID, req.WorkerID, req.Error)
	if errors.Is(err, model.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task with id "+req.TaskID+" was not found")
		return
	}
	if errors.Is(err, model.ErrTaskNotOwned) {
		writeError(w, http.StatusForbidden, "task "+req.TaskID+" is not owned by worker "+req.WorkerID)
		return
	}
	if errors.Is(err, model.ErrTaskLeaseExpired) {
		writeError(w, http.StatusConflict, "task "+req.TaskID+" lease expired")
		return
	}
	if errors.Is(err, model.ErrTaskInvalidStatus) {
		writeError(w, http.StatusConflict, "task "+req.TaskID+" is not executing")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to reject task")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *WorkerHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := h.store.ActiveWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list workers")
		return
	}

	writeJSON(w, http.StatusOK, workers)
}
