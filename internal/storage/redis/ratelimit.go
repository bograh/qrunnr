package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const rateLimitScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local current = tonumber(redis.call("GET", key) or "0")
if current >= limit then
  return 0
end
current = redis.call("INCR", key)
if current == 1 then
  redis.call("PEXPIRE", key, ttl)
end
return 1
`

type RateLimiter struct {
	client *goredis.Client
}

func NewRateLimiter(client *goredis.Client) *RateLimiter {
	return &RateLimiter{client: client}
}

func (r *RateLimiter) Allow(ctx context.Context, queueName string, limit int) (bool, error) {
	if limit <= 0 {
		return true, nil
	}

	key := "taskforge:rate:" + queueName + ":" + time.Now().UTC().Format("20060102150405")
	allowed, err := r.client.Eval(ctx, rateLimitScript, []string{key}, limit, int(time.Second/time.Millisecond)).Int()
	if err != nil {
		return false, err
	}
	return allowed == 1, nil
}

func (r *RateLimiter) Reset(ctx context.Context, queueName string) error {
	return r.client.Del(ctx, "taskforge:rate:"+queueName).Err()
}
