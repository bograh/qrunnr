// Package ratelimit provides per-queue dispatch rate limiting.
package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Limiter decides whether one more task may be dispatched from a queue whose
// limit is `limit` tasks per second. A limit <= 0 means unlimited.
type Limiter interface {
	Allow(ctx context.Context, queueName string, limit int) (bool, error)
}

// Local is an in-process token bucket limiter. Each queue's bucket holds up to
// `limit` tokens and refills at `limit` tokens per second. It is exact within a
// single process; use the Redis limiter to share limits across API instances.
type Local struct {
	mu      sync.Mutex
	now     func() time.Time
	buckets map[string]bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewLocal() *Local {
	return &Local{now: time.Now, buckets: make(map[string]bucket)}
}

// NewLocalWithClock is used by tests to control time.
func NewLocalWithClock(now func() time.Time) *Local {
	return &Local{now: now, buckets: make(map[string]bucket)}
}

func (l *Local) Allow(_ context.Context, queueName string, limit int) (bool, error) {
	if limit <= 0 {
		return true, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	capacity := float64(limit)
	b, ok := l.buckets[queueName]
	if !ok {
		b = bucket{tokens: capacity, last: now}
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * capacity
		}
		b.last = now
	}
	// A lowered limit takes effect immediately.
	if b.tokens > capacity {
		b.tokens = capacity
	}

	if b.tokens < 1 {
		l.buckets[queueName] = b
		return false, nil
	}
	b.tokens--
	l.buckets[queueName] = b
	return true, nil
}
