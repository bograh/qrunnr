package handlers

import "net/http"

type StatsHandler struct{}

func NewStatsHandler() *StatsHandler {
	return &StatsHandler{}
}

func (h *StatsHandler) GetQueueStats(w http.ResponseWriter, r *http.Request) {}

func (h *StatsHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {}
