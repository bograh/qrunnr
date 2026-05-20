package handlers

import "net/http"

type WorkerHandler struct{}

func NewWorkerHandler() *WorkerHandler {
	return &WorkerHandler{}
}

func (h *WorkerHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {}

func (h *WorkerHandler) Ack(w http.ResponseWriter, r *http.Request) {}

func (h *WorkerHandler) Nack(w http.ResponseWriter, r *http.Request) {}
