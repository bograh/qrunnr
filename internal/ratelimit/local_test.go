package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestLocalTokenBucket(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	l := NewLocalWithClock(func() time.Time { return now })
	ctx := context.Background()

	allow := func() bool {
		ok, err := l.Allow(ctx, "q", 4)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	for i := range 4 {
		if !allow() {
			t.Fatalf("burst token %d denied", i)
		}
	}
	if allow() {
		t.Fatal("expected empty bucket")
	}

	now = now.Add(250 * time.Millisecond) // +1 token at 4/s
	if !allow() {
		t.Fatal("expected one refilled token")
	}
	if allow() {
		t.Fatal("expected bucket empty again")
	}

	now = now.Add(10 * time.Second) // refill is capped at capacity
	for range 4 {
		if !allow() {
			t.Fatal("expected full bucket after idle period")
		}
	}
	if allow() {
		t.Fatal("bucket must not exceed capacity")
	}
}

func TestLocalLoweredLimitAppliesImmediately(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	l := NewLocalWithClock(func() time.Time { return now })
	ctx := context.Background()

	if ok, _ := l.Allow(ctx, "q", 100); !ok {
		t.Fatal("expected allowed")
	}
	// 99 tokens remain at limit 100; dropping the limit to 1 caps them.
	if ok, _ := l.Allow(ctx, "q", 1); !ok {
		t.Fatal("expected one token at the new limit")
	}
	if ok, _ := l.Allow(ctx, "q", 1); ok {
		t.Fatal("expected lowered limit to apply immediately")
	}
}

func TestLocalUnlimitedAndIsolated(t *testing.T) {
	l := NewLocal()
	ctx := context.Background()
	for range 1000 {
		if ok, _ := l.Allow(ctx, "free", 0); !ok {
			t.Fatal("limit 0 must be unlimited")
		}
	}
	if ok, _ := l.Allow(ctx, "a", 1); !ok {
		t.Fatal("expected allowed")
	}
	if ok, _ := l.Allow(ctx, "b", 1); !ok {
		t.Fatal("queues must not share buckets")
	}
}
