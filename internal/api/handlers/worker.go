package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"taskforge.dev/internal/model"
)

const defaultLeaseDuration = 30 * time.Second

type WorkerStore interface {
	ClaimForWorker(ctx context.Context, workerID string, queueName string, leaseDuration time.Duration) (model.ClaimedTask, bool, error)
}

type WorkerHandler struct {
	store WorkerStore
}

type claimTaskRequest struct {
	WorkerID string `json:"worker_id"`
	Queue    string `json:"queue"`
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

func (h *WorkerHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {}

func (h *WorkerHandler) Ack(w http.ResponseWriter, r *http.Request) {}

func (h *WorkerHandler) Nack(w http.ResponseWriter, r *http.Request) {}
