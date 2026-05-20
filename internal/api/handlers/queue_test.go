package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func postQueue(handler *QueueHandler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/queues", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.CreateQueue(rec, req)
	return rec
}
