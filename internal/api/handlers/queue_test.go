package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"taskforge.dev/internal/model"
	"taskforge.dev/internal/storage/memory"
)

func TestCreateQueue(t *testing.T) {
	store := memory.NewQueueStore()
	handler := NewQueueHandler(store)

	body := `{"name":"email_jobs","max_retries":5,"rate_limit":10}`
	req := httptest.NewRequest(http.MethodPost, "/queues", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.CreateQueue(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	var queue model.Queue
	if err := json.NewDecoder(rec.Body).Decode(&queue); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if queue.Name != "email_jobs" {
		t.Fatalf("expected queue name email_jobs, got %q", queue.Name)
	}
	if queue.MaxRetries != 5 {
		t.Fatalf("expected max retries 5, got %d", queue.MaxRetries)
	}
	if queue.RateLimit == nil || *queue.RateLimit != 10 {
		t.Fatalf("expected rate limit 10, got %v", queue.RateLimit)
	}
	if queue.ID == "" {
		t.Fatal("expected queue ID to be set")
	}

	stored, err := store.GetByName(req.Context(), "email_jobs")
	if err != nil {
		t.Fatalf("expected created queue to be available in store: %v", err)
	}
	if stored.ID != queue.ID {
		t.Fatalf("expected stored queue ID %q, got %q", queue.ID, stored.ID)
	}
}

func TestCreateQueueWithoutRateLimit(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	rec := postQueue(handler, `{"name":"reports","max_retries":0}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, rec.Code, rec.Body.String())
	}

	var queue model.Queue
	if err := json.NewDecoder(rec.Body).Decode(&queue); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if queue.RateLimit != nil {
		t.Fatalf("expected nil rate limit, got %v", queue.RateLimit)
	}
}

func TestCreateQueueDuplicateName(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	first := postQueue(handler, `{"name":"email-jobs","max_retries":3}`)
	if first.Code != http.StatusCreated {
		t.Fatalf("expected initial create status %d, got %d", http.StatusCreated, first.Code)
	}

	second := postQueue(handler, `{"name":"email-jobs","max_retries":3}`)
	if second.Code != http.StatusConflict {
		t.Fatalf("expected duplicate status %d, got %d", http.StatusConflict, second.Code)
	}

	if !strings.Contains(second.Body.String(), "already exists") {
		t.Fatalf("expected descriptive duplicate error, got %s", second.Body.String())
	}
}

func TestCreateQueueRejectsInvalidName(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	tests := []string{
		"",
		"has spaces",
		"has.dot",
		strings.Repeat("a", 65),
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{
				"name":        name,
				"max_retries": 3,
			})
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}

			rec := postQueue(handler, string(body))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestCreateQueueRejectsInvalidMaxRetries(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	tests := []string{
		`{"name":"negative","max_retries":-1}`,
		`{"name":"too_many","max_retries":26}`,
		`{"name":"missing"}`,
	}

	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			rec := postQueue(handler, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestListQueues(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	createFirst := postQueue(handler, `{"name":"reports","max_retries":0}`)
	if createFirst.Code != http.StatusCreated {
		t.Fatalf("expected first create status %d, got %d", http.StatusCreated, createFirst.Code)
	}
	createSecond := postQueue(handler, `{"name":"email_jobs","max_retries":5,"rate_limit":10}`)
	if createSecond.Code != http.StatusCreated {
		t.Fatalf("expected second create status %d, got %d", http.StatusCreated, createSecond.Code)
	}

	rec := getQueues(handler)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var queues []model.Queue
	if err := json.NewDecoder(rec.Body).Decode(&queues); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("expected 2 queues, got %d", len(queues))
	}

	emailQueue := queues[0]
	if emailQueue.ID == "" {
		t.Fatal("expected id to be included")
	}
	if emailQueue.Name != "email_jobs" {
		t.Fatalf("expected name email_jobs, got %q", emailQueue.Name)
	}
	if emailQueue.MaxRetries != 5 {
		t.Fatalf("expected max retries 5, got %d", emailQueue.MaxRetries)
	}
	if emailQueue.RateLimit == nil || *emailQueue.RateLimit != 10 {
		t.Fatalf("expected rate limit 10, got %v", emailQueue.RateLimit)
	}
	if emailQueue.CreatedAt.IsZero() {
		t.Fatal("expected created_at to be included")
	}

	reportsQueue := queues[1]
	if reportsQueue.Name != "reports" {
		t.Fatalf("expected name reports, got %q", reportsQueue.Name)
	}
	if reportsQueue.RateLimit != nil {
		t.Fatalf("expected nil rate limit, got %v", reportsQueue.RateLimit)
	}
}

func TestListQueuesEmpty(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	rec := getQueues(handler)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("expected empty JSON array, got %s", rec.Body.String())
	}
}

func TestDeleteQueue(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	create := postQueue(handler, `{"name":"reports","max_retries":0}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d", http.StatusCreated, create.Code)
	}

	rec := deleteQueue(handler, "reports")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNoContent, rec.Code, rec.Body.String())
	}

	list := getQueues(handler)
	if list.Code != http.StatusOK {
		t.Fatalf("expected list status %d, got %d", http.StatusOK, list.Code)
	}
	if strings.TrimSpace(list.Body.String()) != "[]" {
		t.Fatalf("expected deleted queue to be absent from list, got %s", list.Body.String())
	}
}

func TestDeleteQueueNotFound(t *testing.T) {
	handler := NewQueueHandler(memory.NewQueueStore())

	rec := deleteQueue(handler, "missing")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d: %s", http.StatusNotFound, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "not found") {
		t.Fatalf("expected not found message, got %s", rec.Body.String())
	}
}

func TestDeleteQueueWithActiveTasks(t *testing.T) {
	handler := NewQueueHandler(&conflictQueueStore{})

	rec := deleteQueue(handler, "busy")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d: %s", http.StatusConflict, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "must be drained first") {
		t.Fatalf("expected drain-first message, got %s", rec.Body.String())
	}
}

func postQueue(handler *QueueHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/queues", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.CreateQueue(rec, req)
	return rec
}

func getQueues(handler *QueueHandler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/queues", nil)
	rec := httptest.NewRecorder()
	handler.ListQueues(rec, req)
	return rec
}

func deleteQueue(handler *QueueHandler, name string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, "/queues/"+name, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("name", name)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	rec := httptest.NewRecorder()
	handler.DeleteQueue(rec, req)
	return rec
}

type conflictQueueStore struct{}

func (s *conflictQueueStore) Create(ctx context.Context, queue model.Queue) (model.Queue, error) {
	return model.Queue{}, errors.New("not used")
}

func (s *conflictQueueStore) List(ctx context.Context) ([]model.Queue, error) {
	return nil, errors.New("not used")
}

func (s *conflictQueueStore) GetByName(ctx context.Context, name string) (model.Queue, error) {
	return model.Queue{}, errors.New("not used")
}

func (s *conflictQueueStore) Delete(ctx context.Context, name string) error {
	return model.ErrQueueHasActiveTasks
}
