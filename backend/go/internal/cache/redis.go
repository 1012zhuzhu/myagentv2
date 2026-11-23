package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client *redis.Client
}

func NewRedisClient(addr, password string, db int) (*RedisClient,error) {
	client := redis.NewClient(&redis.Options{
		Addr:  			addr,
		Password:  		password,
		DB: 			db,
		DialTimeout:    5 * time.Second,
		ReadTimeout:    3 * time.Second,
		WriteTimeout:   3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect redis failed: %w", err)
	}

	return &RedisClient{
		Client: client,
	},nil
}

func (r *RedisClient) Close() error {
	if r == nil || r.Client == nil {
		return nil
	}

	return r.Client.Close()
}