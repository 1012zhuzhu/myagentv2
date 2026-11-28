package bootstrap

import (
	"database/sql"
	"errors"
	"log"
	"os"

	"my-agent-v2/backend/go/internal/cache"
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
	Engine     *workflow.Engine
	DB         *sql.DB
	RunRepo    *repository.RunRepository
	EventRepo  *repository.EventRepository
	OutboxRepo *repository.OutboxRepository

	RabbitMQ  *publisher.RabbitMQClient
	Publisher *publisher.Publisher

	Consumer        *consumer.Consumer
	ConsumerChannel *amqp.Channel

	Redis       *cache.RedisClient
	RateLimiter ratelimit.Limiter
}

func NewApp(pythonBaseURL string) (*App, error) {
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

	// 3. 恢复异常中断的 Run
	recoveredRuns, err := repository.RecoverInterruptedRuns(db)
	if err != nil {
		db.Close()
		return nil, err
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
	// Engine 现在可以通过 EventWriter 写入节点事件。
	engine := workflow.NewEngine(
		registry,
		eventRepo,
	)

	// 6. 初始化 Redis
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

	// 7. 创建本地限流器，作为 Redis 故障时的降级方案
	localLimiter, err := ratelimit.NewLocalLimiter(10, 20)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, err
	}

	// 8. Redis 限流器：Redis 不可用时保持 nil
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

	// 9. 组合 Redis 限流器和本地限流器
	rateLimiter, err := ratelimit.NewFallbackLimiter(
		redisLimiter,
		localLimiter,
	)
	if err != nil {
		closeRedis()
		db.Close()
		return nil, err
	}

	// 10. 初始化 RabbitMQ
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

	// 11. 创建 Consumer
	c := consumer.NewConsumer(
		consumerChannel,
		engine,
		runRepo,
	)

	// 12. 配置 RabbitMQ 拓扑
	if err := rabbitMQ.SetupTopology(); err != nil {
		consumerChannel.Close()
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, err
	}

	// 13. 开启 Publisher Confirm
	if err := rabbitMQ.EnableConfirm(); err != nil {
		consumerChannel.Close()
		rabbitMQ.Close()
		closeRedis()
		db.Close()
		return nil, err
	}

	// 14. 创建 Publisher
	p := publisher.NewPublisher(
		outboxRepo,
		rabbitMQ,
	)

	// 15. 返回 App
	return &App{
		Engine:          engine,
		DB:              db,
		Redis:            redisClient,
		RateLimiter:      rateLimiter,
		RunRepo:          runRepo,
		EventRepo:        eventRepo,
		OutboxRepo:       outboxRepo,
		RabbitMQ:         rabbitMQ,
		Publisher:        p,
		Consumer:         c,
		ConsumerChannel:  consumerChannel,
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