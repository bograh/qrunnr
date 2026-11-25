package model

import (
	"crypto/rand"
	"math/big"
	"time"
)

// RetryPolicy computes retry backoff delays using
// min(base * 2^attempt + rand(0, base), max).
type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Hour}
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.BaseDelay <= 0 {
		p.BaseDelay = time.Second
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = time.Hour
	}
	return p
}

// Delay returns the backoff delay for the given attempt number.
func (p RetryPolicy) Delay(attempt int) time.Duration {
	p = p.normalized()
	if attempt < 0 {
		attempt = 0
	}

	delay := p.BaseDelay
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= p.MaxDelay {
			return p.MaxDelay
		}
	}

	delay += jitter(p.BaseDelay)
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

func jitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(base)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}
