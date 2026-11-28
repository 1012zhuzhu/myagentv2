
package bootstrap

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	"my-agent-v2/backend/go/internal/cache"
	"my-agent-v2/backend/go/internal/cancellation"
	"my-agent-v2/backend/go/internal/consumer"
	"my-agent-v2/backend/go/internal/database"
	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/executor"
	"my-agent-v2/backend/go/internal/publisher"
	"my-agent-v2/backend/go/internal/ratelimit"
	"my-agent-v2/backend/go/internal/repository"
	"my-agent-v2/backend/go/internal/workflow"

	amqp "github.com/rabbitmq/amqp091-go"
)

type App struct {
	Engine        *workflow.Engine
	DB            *sql.DB
	RunRepo       *repository.RunRepository
	EventRepo     *repository.EventRepository
	OutboxRepo    *repository.OutboxRepository
	CancelManager *cancellation.Manager

	RabbitMQ        *publisher.RabbitMQClient
	Publisher       *publisher.Publisher
	Consumer        *consumer.Consumer
	ConsumerChannel *amqp.Channel

	Redis       *cache.RedisClient
	RateLimiter ratelimit.Limiter
}

func NewApp(pythonBaseURL string) (*App, error) {
	if pythonBaseURL == "" {
		return nil, errors.New("python base URL cannot be empty")
	}

	// 1. 创建 Executor Registry
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

	// 2. 初始化 SQLite
	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	dsn := "file:./data/my-agent.db?_pragma=foreign_keys(1)"

	db, err := database.NewDB(dsn)
	if err != nil {
		return nil, fmt.Errorf("initialize database: %w", err)
	}

	if err := database.InitSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize database schema: %w", err)
	}

	// 3. 恢复异常中断的 Run
	recoveredRuns, err := repository.RecoverInterruptedRuns(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("recover interrupted runs: %w", err)
	}

	log.Printf(
		"recovered %d interrupted workflow runs",
		recoveredRuns,
	)

	// 4. 创建 Repository
	runRepo := repository.NewRunRepository(db)
	eventRepo := repository.NewEventRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)

	// 5. 创建 Workflow Engine
	engine := workflow.NewEngine(
		registry,
		eventRepo,
	)

	// 6. 创建共享的 Cancellation Manager
	// Handler 和 Consumer 必须使用同一个实例。
	cancelManager := cancellation.NewManager()

	// 7. 初始化 Redis
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
		log.Printf(
			"WARNING: redis unavailable, running without redis: %v",
			err,
		)
		redisClient = nil
	}

	closeRedis := func() {
		if redisClient != nil {
			_ = redisClient.Close()
		}
	}

	// 8. 创建本地限流器，作为 Redis 故障时的降级方案
	localLimiter, err := ratelimit.NewLocalLimiter(10, 20)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, fmt.Errorf("create local rate limiter: %w", err)
	}

	// 9. Redis 限流器：Redis 不可用时保持 nil
	var redisLimiter ratelimit.Limiter

	if redisClient != nil {
		redisLimiter, err = ratelimit.NewRedisLimiter(
			redisClient.Client,
			"ratelimit:workflow:run",
			10,
			20,
		)
		if err != nil {
			log.Printf(
				"WARNING: failed to initialize redis limiter, using local limiter: %v",
				err,
			)
			redisLimiter = nil
		}
	}

	// 10. 组合 Redis 限流器和本地限流器
	rateLimiter, err := ratelimit.NewFallbackLimiter(
		redisLimiter,
		localLimiter,
	)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, fmt.Errorf("create fallback rate limiter: %w", err)
	}

	// 11. 初始化 RabbitMQ
	amqURL := os.Getenv("AMQP_URL")
	if amqURL == "" {
		amqURL = "amqp://guest:guest@localhost:5672/"
	}

	rabbitMQ, err := publisher.NewRabbitMQClient(amqURL)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, fmt.Errorf("initialize RabbitMQ: %w", err)
	}

	consumerChannel, err := rabbitMQ.NewChannel()
	if err != nil {
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, fmt.Errorf("create consumer channel: %w", err)
	}

	// 12. 创建 Consumer，并注入共享的 Cancellation Manager
	c := consumer.NewConsumer(
		consumerChannel,
		engine,
		runRepo,
		cancelManager,
	)

	// 13. 配置 RabbitMQ 拓扑
	if err := rabbitMQ.SetupTopology(); err != nil {
		consumerChannel.Close()
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, fmt.Errorf("setup RabbitMQ topology: %w", err)
	}

	// 14. 开启 Publisher Confirm
	if err := rabbitMQ.EnableConfirm(); err != nil {
		consumerChannel.Close()
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, fmt.Errorf("enable publisher confirm: %w", err)
	}

	// 15. 创建 Publisher
	p := publisher.NewPublisher(
		outboxRepo,
		rabbitMQ,
	)

	// 16. 返回 App
	return &App{
		Engine:          engine,
		DB:              db,
		RunRepo:         runRepo,
		EventRepo:       eventRepo,
		OutboxRepo:      outboxRepo,
		CancelManager:   cancelManager,
		RabbitMQ:        rabbitMQ,
		Publisher:       p,
		Consumer:        c,
		ConsumerChannel: consumerChannel,
		Redis:           redisClient,
		RateLimiter:     rateLimiter,
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
