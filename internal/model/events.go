package model

import "time"

// TaskEventKind distinguishes status transitions from heartbeats in a task's history.
type TaskEventKind string

const (
	TaskEventTransition TaskEventKind = "transition"
	TaskEventHeartbeat  TaskEventKind = "heartbeat"
	TaskEventReplay     TaskEventKind = "replay"
)

// WatchdogError is recorded as last_error when a task's heartbeat expires.
const WatchdogError = "watchdog: heartbeat expired"

// TaskEvent is one entry in a task's history.
type TaskEvent struct {
	ID         int64         `json:"id"`
	TaskID     string        `json:"task_id"`
	Kind       TaskEventKind `json:"kind"`
	FromStatus TaskStatus    `json:"from_status,omitempty"`
	ToStatus   TaskStatus    `json:"to_status,omitempty"`
	WorkerID   string        `json:"worker_id,omitempty"`
	Message    string        `json:"message,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
}

// Transition describes a task status change. Stores report transitions to an
// Observer after they are durably applied.
type Transition struct {
	TaskID   string
	Queue    string
	WorkerID string
	From     TaskStatus
	To       TaskStatus
	// Duration is the time the task spent in the From status.
	Duration time.Duration
	At       time.Time
}

// Observer receives task transitions. Implementations must be safe for
// concurrent use and must not block.
type Observer interface {
	TaskTransition(Transition)
}

type NopObserver struct{}

func (NopObserver) TaskTransition(Transition) {}

// TaskFilter selects tasks for the admin task inspector.
type TaskFilter struct {
	Queue         string
	Status        TaskStatus
	WorkerID      string
	TaskID        string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Limit         int
	Offset        int
}

// TaskSummary is a task together with the name of its queue.
type TaskSummary struct {
	Task
	QueueName string `json:"queue"`
}

// DeadLetterFilter selects dead-letter tasks across queues.
type DeadLetterFilter struct {
	TaskID     string
	Queue      string
	DiedAfter  *time.Time
	DiedBefore *time.Time
	Limit      int
	Offset     int
}

// QueueOverview is a queue with its current stats.
type QueueOverview struct {
	Queue Queue      `json:"queue"`
	Stats QueueStats `json:"stats"`
}

// StatusCount is the number of tasks in a queue with a given status.
type StatusCount struct {
	Queue  string
	Status TaskStatus
	Count  int
}

const DefaultPageSize = 50

func ClampLimit(limit int) int {
	if limit <= 0 {
		return DefaultPageSize
	}
	if limit > 500 {
		return 500
	}
	return limit
}
