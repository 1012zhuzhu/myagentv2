package bootstrap

import (
	"database/sql"
	"errors"
	"log"
	"my-agent-v2/backend/go/internal/cache"
	"my-agent-v2/backend/go/internal/consumer"
	"my-agent-v2/backend/go/internal/database"
	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/executor"
	"my-agent-v2/backend/go/internal/publisher"
	"my-agent-v2/backend/go/internal/ratelimit"
	"my-agent-v2/backend/go/internal/repository"
	"my-agent-v2/backend/go/internal/workflow"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type App struct {
	Engine     *workflow.Engine
	DB         *sql.DB
	RunRepo    *repository.RunRepository
	OutboxRepo *repository.OutboxRepository

	RabbitMQ  *publisher.RabbitMQClient
	Publisher *publisher.Publisher

	Consumer        *consumer.Consumer
	ConsumerChannel *amqp.Channel

	Redis        *cache.RedisClient
	RateLimiter ratelimit.Limiter
}

func NewApp(pythonBaseURL string) (*App, error) {
	registry := executor.NewRegistry()

	startExecutor := executor.NewStartExecutor()
	agentExecutor := executor.NewAgentExecutor(pythonBaseURL)
	endExecutor := executor.NewEndExecutor()

	if err := registry.Register(
		domain.NodeTypeStart,
		startExecutor,
	); err != nil {
		return nil, err
	}

	if err := registry.Register(
		domain.NodeTypeAgent,
		agentExecutor,
	); err != nil {
		return nil, err
	}

	if err := registry.Register(
		domain.NodeTypeEnd,
		endExecutor,
	); err != nil {
		return nil, err
	}

	engine := workflow.NewEngine(registry)

	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, err
	}

	dsn := "file:./data/my-agent.db?_pragma=foreign_keys(1)"

	db, err := database.NewDB(dsn)
	if err != nil {
		return nil, err
	}

	if err := database.InitSchema(db); err != nil {
		db.Close()
		return nil, err
	}

	recoveredRuns, err := repository.RecoverInterruptedRuns(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	log.Printf("recovered %d interrupted workflow runs", recoveredRuns)

	runRepo := repository.NewRunRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	redisClient, err := cache.NewRedisClient(
		redisAddr,
		redisPassword,
		0,
	)
	if err != nil {
		log.Printf("WARNING: redis unavailable, running without redis: %v", err)
		redisClient = nil
	}

	closeRedis := func() {
		if redisClient != nil {
			_ = redisClient.Close()
		}
	}

	// 创建本地限流器，作为 Redis 故障时的降级方案
	localLimiter, err := ratelimit.NewLocalLimiter(10, 20)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, err
	}

	// Redis 限流器：Redis 不可用时保持 nil
	var redisLimiter ratelimit.Limiter

	if redisClient != nil {
		redisLimiter, err = ratelimit.NewRedisLimiter(
			redisClient.Client,
			"ratelimit:workflow:run",
			10,
			20,
		)
		if err != nil {
			log.Printf("WARNING: failed to initialize redis limiter, using local limiter: %v", err)
			redisLimiter = nil
		}
	}

	// 组合 Redis 限流器和本地限流器
	rateLimiter, err := ratelimit.NewFallbackLimiter(
		redisLimiter,
		localLimiter,
	)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, err
	}

	amqURL := os.Getenv("AMQP_URL")
	if amqURL == "" {
		amqURL = "amqp://guest:guest@localhost:5672/"
	}

	rabbitMQ, err := publisher.NewRabbitMQClient(amqURL)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, err
	}

	consumerChannel, err := rabbitMQ.NewChannel()
	if err != nil {
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, err
	}

	c := consumer.NewConsumer(
		consumerChannel,
		engine,
		runRepo,
	)

	if err := rabbitMQ.SetupTopology(); err != nil {
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, err
	}

	if err := rabbitMQ.EnableConfirm(); err != nil {
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, err
	}

	p := publisher.NewPublisher(
		outboxRepo, rabbitMQ,
	)

	return &App{
		Engine:          engine,
		DB:              db,
		Redis:           redisClient,
		RateLimiter:     rateLimiter,
		RunRepo:         runRepo,
		OutboxRepo:      outboxRepo,
		RabbitMQ:        rabbitMQ,
		Publisher:       p,
		Consumer:        c,
		ConsumerChannel: consumerChannel,
	}, nil
}

func (app *App) Close() error {
	var closeErr error

	if app.ConsumerChannel != nil {
		if err := app.ConsumerChannel.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	if app.Redis != nil {
		if err := app.Redis.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	if app.RabbitMQ != nil {
		if err := app.RabbitMQ.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	if app.DB != nil {
		if err := app.DB.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
}
