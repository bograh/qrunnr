package handlers

import "net/http"

type DLQHandler struct{}

func NewDLQHandler() *DLQHandler {
	return &DLQHandler{}
}

func (h *DLQHandler) ListDeadLetterTasks(w http.ResponseWriter, r *http.Request) {}

func (h *DLQHandler) ReplayDeadLetterTask(w http.ResponseWriter, r *http.Request) {}

func (h *DLQHandler) DeleteDeadLetterTask(w http.ResponseWriter, r *http.Request) {}
