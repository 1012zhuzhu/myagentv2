package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/redis/go-redis/v9"
)

const tokenBucketScript = `
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])

-- 使用 Redis 服务端时间，避免多个 Go 实例的时钟差异
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000
    + math.floor(tonumber(time[2]) / 1000)

local values = redis.call("HMGET", KEYS[1], "tokens", "timestamp")

local tokens = tonumber(values[1]) or capacity
local lastTime = tonumber(values[2]) or now

-- 计算经过时间，并补充令牌
local elapsed = math.max(0, now - lastTime)
tokens = math.min(capacity, tokens + elapsed * rate / 1000)

local allowed = 0

if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

-- 保存当前令牌数和时间
redis.call("HSET", KEYS[1],
    "tokens", tokens,
    "timestamp", now
)

-- 避免长期不访问的限流记录一直占用内存
local ttl = math.ceil(capacity / rate * 2000)
redis.call("PEXPIRE", KEYS[1], ttl)

return allowed
`

type RedisLimiter struct {
	client   *redis.Client
	key      string
	rps      float64
	burst    int
}

func NewRedisLimiter(
	client *redis.Client,
	key string,
	rps float64,
	burst int,
) (*RedisLimiter, error) {
	if client == nil {
		return nil, errors.New("redis client is nil")
	}

	if key == "" || rps <= 0 || burst <= 0 {
		return nil, errors.New("invalid redis limiter configuration")
	}

	if math.IsNaN(rps) || math.IsInf(rps, 0) {
		return nil, errors.New("invalid rps")
	}

	return &RedisLimiter{
		client: client,
		key:    key,
		rps:    rps,
		burst:  burst,
	}, nil
}

func (l *RedisLimiter) Allow(ctx context.Context) (bool, error) {
	if l == nil || l.client == nil {
		return false, errors.New("redis limiter is not initialized")
	}

	result, err := l.client.Eval(
		ctx,
		tokenBucketScript,
		[]string{l.key},
		l.rps,
		l.burst,
	).Int()

	if err != nil {
		return false, fmt.Errorf("redis rate limit failed: %w", err)
	}

	return result == 1, nil
}
