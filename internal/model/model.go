package model

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrNotImplemented = errors.New("not implemented")

type Queue struct {
	ID         string
	Name       string
	MaxRetries int
	RateLimit  *int
	CreatedAt  time.Time
}

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusClaimed   TaskStatus = "claimed"
	TaskStatusExecuting TaskStatus = "executing"
	TaskStatusDone      TaskStatus = "done"
	TaskStatusRetry     TaskStatus = "retry"
	TaskStatusDead      TaskStatus = "dead"
)

type Task struct {
	ID          string
	QueueID     string
	Payload     json.RawMessage
	Status      TaskStatus
	Priority    int
	ScheduledAt time.Time
	ClaimedAt   *time.Time
	ClaimedBy   *string
	HeartbeatAt *time.Time
	Attempts    int
	LastError   *string
	Result      json.RawMessage
	CreatedAt   time.Time
}

type DeadLetterTask struct {
	ID         string
	QueueID    string
	Payload    json.RawMessage
	LastError  string
	Attempts   int
	DiedAt     time.Time
	ReplayedAt *time.Time
}

type QueueStats struct {
	QueueName     string
	PendingDepth  int
	RetryDepth    int
	Executing     int
	Done          int
	Dead          int
	TasksPerSec   float64
	ErrorRate     float64
	OldestPending *time.Time
}

type WorkerHeartbeat struct {
	WorkerID string
	TaskID   string
	SentAt   time.Time
}
