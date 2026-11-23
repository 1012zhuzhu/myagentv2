package ratelimit

import (
	"context"
	"errors"
	"log"
)

type FallbackLimiter struct {
	primary  Limiter // Redis 限流器
	fallback Limiter // 本地限流器
}

func NewFallbackLimiter(primary, fallback Limiter) (*FallbackLimiter, error) {
	if fallback == nil {
		return nil, errors.New("fallback limiter is nil")
	}

	return &FallbackLimiter{
		primary:  primary,
		fallback: fallback,
	}, nil
}

func (l *FallbackLimiter) Allow(ctx context.Context) (bool, error) {
	if l == nil || l.fallback == nil {
		return false, errors.New("fallback limiter is not initialized")
	}

	// Redis 启动时不可用，直接使用本地限流
	if l.primary == nil {
		return l.fallback.Allow(ctx)
	}

	// 优先尝试 Redis
	allowed, err := l.primary.Allow(ctx)
	if err == nil {
		return allowed, nil
	}

	// Redis 出现故障，降级到本地限流
	log.Printf("WARNING: redis limiter failed, using local limiter: %v", err)

	return l.fallback.Allow(ctx)
}