package handlers

import "net/http"

type TaskHandler struct{}

func NewTaskHandler() *TaskHandler {
	return &TaskHandler{}
}

func (h *TaskHandler) EnqueueTask(w http.ResponseWriter, r *http.Request) {}

func (h *TaskHandler) GetTask(w http.ResponseWriter, r *http.Request) {}

func (h *TaskHandler) CancelTask(w http.ResponseWriter, r *http.Request) {}
