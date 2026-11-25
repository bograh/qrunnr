package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/model"
)

type StatsStore interface {
	Stats(ctx context.Context, queueName string) (model.QueueStats, error)
}

type StatsHandler struct {
	store StatsStore
}

func NewStatsHandler(store StatsStore) *StatsHandler {
	return &StatsHandler{store: store}
}

func (h *StatsHandler) GetQueueStats(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	stats, err := h.store.Stats(r.Context(), queueName)
	if errors.Is(err, model.ErrQueueNotFound) {
		writeError(w, http.StatusNotFound, "queue with name "+queueName+" was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get queue stats")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}
