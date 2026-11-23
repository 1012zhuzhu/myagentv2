package ratelimit

import (
	"context"
	"errors"

	"golang.org/x/time/rate"
)

type LocalLimiter struct {
	limiter *rate.Limiter
}

func NewLocalLimiter(rps float64, burst int) (*LocalLimiter, error) {
	if rps <= 0 || burst <= 0 {
		return nil, errors.New("rps and burst must be positive")
	}

	return &LocalLimiter{
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
	}, nil
}

func (l *LocalLimiter) Allow(ctx context.Context) (bool, error) {
	if l == nil || l.limiter == nil {
		return false, errors.New("local limiter is not initialized")
	}

	return l.limiter.Allow(), nil
}