package middleware

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecoveryReturnsJSON500AndLogsPanic(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	h := Recovery(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/queues", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
		t.Fatalf("expected JSON error body, got %q", rec.Body.String())
	}
	if !strings.Contains(logs.String(), `"panic":"boom"`) || !strings.Contains(logs.String(), `"stack"`) {
		t.Fatalf("expected panic and stack logged, got %s", logs.String())
	}
}

func TestRecoveryRepanicsAbortHandler(t *testing.T) {
	h := Recovery(slog.New(slog.DiscardHandler))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer func() {
		if rec := recover(); rec != http.ErrAbortHandler {
			t.Fatalf("expected ErrAbortHandler to propagate, got %v", rec)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestLoggerRecordsRequest(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := Logger(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("hi"))
	}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/queues", nil))

	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatalf("expected one JSON log line, got %q", logs.String())
	}
	if entry["method"] != "POST" || entry["path"] != "/queues" || entry["status"] != float64(http.StatusTeapot) || entry["bytes"] != float64(2) {
		t.Fatalf("unexpected log entry: %v", entry)
	}
	if entry["level"] != "INFO" {
		t.Fatalf("expected INFO level, got %v", entry["level"])
	}
}

func TestLoggerQuietPathsAtDebug(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil)) // info level
	h := Logger(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if logs.Len() != 0 {
		t.Fatalf("expected /metrics request not logged at info level, got %s", logs.String())
	}
}
