package model

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var ErrNotImplemented = errors.New("not implemented")
var ErrQueueAlreadyExists = errors.New("queue already exists")
var ErrQueueNotFound = errors.New("queue not found")
var ErrQueueHasActiveTasks = errors.New("queue has active tasks")
var ErrTaskNotFound = errors.New("task not found")
var ErrTaskNotCancellable = errors.New("task is not cancellable")
var ErrTaskNotOwned = errors.New("task is not owned by worker")
var ErrTaskInvalidStatus = errors.New("task status does not allow this operation")
var ErrTaskLeaseExpired = errors.New("task lease expired")
var ErrDeadLetterTaskNotFound = errors.New("dead-letter task not found")

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type Queue struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	MaxRetries int       `json:"max_retries"`
	RateLimit  *int      `json:"rate_limit"`
	CreatedAt  time.Time `json:"created_at"`
}

func NewQueue(name string, maxRetries int, rateLimit *int) Queue {
	return Queue{
		ID:         newID(),
		Name:       name,
		MaxRetries: maxRetries,
		RateLimit:  rateLimit,
		CreatedAt:  time.Now().UTC(),
	}
}

func ValidateQueueName(name string) error {
	if !queueNamePattern.MatchString(name) {
		return errors.New("name must be 1-64 characters and contain only letters, numbers, hyphens, and underscores")
	}

	return nil
}

func ValidateMaxRetries(maxRetries int) error {
	if maxRetries < 0 || maxRetries > 25 {
		return errors.New("max_retries must be an integer between 0 and 25")
	}

	return nil
}

func ValidatePriority(priority int) error {
	if priority < 1 || priority > 10 {
		return errors.New("priority must be an integer between 1 and 10")
	}

	return nil
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusClaimed   TaskStatus = "claimed"
	TaskStatusExecuting TaskStatus = "executing"
	TaskStatusDone      TaskStatus = "done"
	TaskStatusRetry     TaskStatus = "retry"
	TaskStatusDead      TaskStatus = "dead"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type Task struct {
	ID          string          `json:"id"`
	QueueID     string          `json:"queue_id"`
	Payload     json.RawMessage `json:"payload"`
	Status      TaskStatus      `json:"status"`
	Priority    int             `json:"priority"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	ClaimedAt   *time.Time      `json:"claimed_at"`
	ClaimedBy   *string         `json:"claimed_by"`
	HeartbeatAt *time.Time      `json:"heartbeat_at"`
	Attempts    int             `json:"attempts"`
	LastError   *string         `json:"last_error"`
	Result      json.RawMessage `json:"result"`
	CreatedAt   time.Time       `json:"created_at"`
}

type ClaimedTask struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Attempts       int             `json:"attempts"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
}

func NewImmediateTask(queueID string, payload json.RawMessage) Task {
	return NewTask(queueID, payload, time.Now().UTC(), 5)
}

func NewTask(queueID string, payload json.RawMessage, scheduledAt time.Time, priority int) Task {
	now := time.Now().UTC()
	return Task{
		ID:          newID(),
		QueueID:     queueID,
		Payload:     payload,
		Status:      TaskStatusPending,
		Priority:    priority,
		ScheduledAt: scheduledAt.UTC(),
		CreatedAt:   now,
	}
}

type DeadLetterTask struct {
	ID         string          `json:"id"`
	QueueID    string          `json:"queue_id"`
	Payload    json.RawMessage `json:"payload"`
	LastError  string          `json:"last_error"`
	Attempts   int             `json:"attempts"`
	DiedAt     time.Time       `json:"died_at"`
	ReplayedAt *time.Time      `json:"replayed_at"`
}

type QueueStats struct {
	Depth                   int `json:"depth"`
	ExecutingCount          int `json:"executing_count"`
	DoneLastMinute          int `json:"done_last_minute"`
	FailedLastMinute        int `json:"failed_last_minute"`
	DLQDepth                int `json:"dlq_depth"`
	OldestPendingAgeSeconds int `json:"oldest_pending_age_seconds"`
}

type WorkerHeartbeat struct {
	WorkerID string
	TaskID   string
	SentAt   time.Time
}
