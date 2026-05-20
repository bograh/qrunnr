package scheduler

import (
	"testing"
	"time"

	"taskforge.dev/internal/storage/memory"
)

func TestNewDefaults(t *testing.T) {
	s := New(memory.NewTaskStore(memory.NewQueueStore()), 0)

	if s.Interval != 15*time.Second {
		t.Fatalf("expected default interval 15s, got %s", s.Interval)
	}
	if s.TimeoutThreshold != 30*time.Second {
		t.Fatalf("expected default timeout threshold 30s, got %s", s.TimeoutThreshold)
	}
}
