package redis

import (
	"context"

	"taskforge.dev/internal/model"
)

type RateLimiter struct{}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{}
}

func (r *RateLimiter) Allow(ctx context.Context, queueName string, limit int) (bool, error) {
	return false, model.ErrNotImplemented
}

func (r *RateLimiter) Reset(ctx context.Context, queueName string) error {
	return model.ErrNotImplemented
}
