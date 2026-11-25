package model

import (
	"testing"
	"time"
)

func TestRetryPolicyDelay(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Hour}
	for attempt, base := range map[int]time.Duration{0: time.Second, 1: 2 * time.Second, 3: 8 * time.Second} {
		for range 50 {
			d := p.Delay(attempt)
			if d < base || d >= base+time.Second {
				t.Fatalf("attempt %d: delay %s outside [%s, %s)", attempt, d, base, base+time.Second)
			}
		}
	}
	if d := p.Delay(30); d != time.Hour {
		t.Fatalf("expected delay capped at max, got %s", d)
	}
}

func TestRetryPolicyJitterVaries(t *testing.T) {
	p := DefaultRetryPolicy()
	seen := map[time.Duration]bool{}
	for range 20 {
		seen[p.Delay(2)] = true
	}
	if len(seen) < 2 {
		t.Fatal("expected jitter to vary delays for the same attempt")
	}
}
