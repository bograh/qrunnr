package redis_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	redisstore "taskforge.dev/internal/storage/redis"
)

func testClient(t *testing.T) *goredis.Client {
	t.Helper()
	url := os.Getenv("TASKFORGE_TEST_REDIS_URL")
	if url == "" {
		t.Skip("TASKFORGE_TEST_REDIS_URL not set")
	}
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := goredis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestRateLimiterBurstAndRefill(t *testing.T) {
	limiter := redisstore.NewRateLimiter(testClient(t))
	ctx := context.Background()
	queue := "burst-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = limiter.Reset(ctx, queue) })

	for i := range 3 {
		ok, err := limiter.Allow(ctx, queue, 3)
		if err != nil || !ok {
			t.Fatalf("token %d: expected allowed, got %v %v", i, ok, err)
		}
	}
	if ok, _ := limiter.Allow(ctx, queue, 3); ok {
		t.Fatal("expected bucket to be empty after a full burst")
	}

	time.Sleep(400 * time.Millisecond) // refills ~1.2 tokens at 3/s
	if ok, _ := limiter.Allow(ctx, queue, 3); !ok {
		t.Fatal("expected a token after refill")
	}
}

func TestRateLimiterUnlimited(t *testing.T) {
	limiter := redisstore.NewRateLimiter(testClient(t))
	ok, err := limiter.Allow(context.Background(), "any", 0)
	if err != nil || !ok {
		t.Fatalf("limit 0 means unlimited: %v %v", ok, err)
	}
}

// Concurrent callers sharing one bucket must never exceed its capacity, as
// several API instances would.
func TestRateLimiterIsAtomicUnderConcurrency(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := "concurrent-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = redisstore.NewRateLimiter(client).Reset(ctx, queue) })

	const limit = 10
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limiter := redisstore.NewRateLimiter(client)
			for range 25 {
				ok, err := limiter.Allow(ctx, queue, limit)
				if err != nil {
					t.Error(err)
					return
				}
				if ok {
					allowed.Add(1)
				}
			}
		}()
	}
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)

	// Capacity plus whatever refilled while the calls ran.
	maxAllowed := int64(limit) + int64(elapsed.Seconds()*limit) + 1
	if got := allowed.Load(); got < limit || got > maxAllowed {
		t.Fatalf("expected between %d and %d allowed in %s, got %d", limit, maxAllowed, elapsed, got)
	}
}
