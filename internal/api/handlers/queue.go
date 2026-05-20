package handlers

import "net/http"

type QueueHandler struct{}

func NewQueueHandler() *QueueHandler {
	return &QueueHandler{}
}

func (h *QueueHandler) CreateQueue(w http.ResponseWriter, r *http.Request) {}

func (h *QueueHandler) ListQueues(w http.ResponseWriter, r *http.Request) {}

func (h *QueueHandler) DeleteQueue(w http.ResponseWriter, r *http.Request) {}
