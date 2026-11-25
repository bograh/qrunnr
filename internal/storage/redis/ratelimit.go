package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// tokenBucketScript atomically refills and consumes from a per-queue token
// bucket. Capacity is `limit` tokens and the refill rate is `limit` tokens per
// second. Redis server time is used so every API instance shares one clock.
//
// KEYS[1] bucket hash key
// ARGV[1] limit (tokens per second)
// Returns 1 when a token was consumed, 0 otherwise.
var tokenBucketScript = goredis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = limit
  ts = now
end

local elapsed = now - ts
if elapsed > 0 then
  tokens = tokens + (elapsed * limit / 1000)
end
if tokens > limit then
  tokens = limit
end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call("HSET", key, "tokens", tostring(tokens), "ts", tostring(now))
redis.call("PEXPIRE", key, 60000)
return allowed
`)

// RateLimiter enforces per-queue dispatch limits with a Redis token bucket,
// shared by every API instance pointing at the same Redis.
type RateLimiter struct {
	client goredis.UniversalClient
}

func NewRateLimiter(client goredis.UniversalClient) *RateLimiter {
	return &RateLimiter{client: client}
}

func (r *RateLimiter) Allow(ctx context.Context, queueName string, limit int) (bool, error) {
	if limit <= 0 {
		return true, nil
	}
	allowed, err := tokenBucketScript.Run(ctx, r.client, []string{bucketKey(queueName)}, limit).Int()
	if err != nil {
		return false, err
	}
	return allowed == 1, nil
}

// Reset clears a queue's bucket, e.g. after the queue is deleted.
func (r *RateLimiter) Reset(ctx context.Context, queueName string) error {
	return r.client.Del(ctx, bucketKey(queueName)).Err()
}

func bucketKey(queueName string) string {
	return "taskforge:ratelimit:" + queueName
}
