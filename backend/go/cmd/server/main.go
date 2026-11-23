
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"my-agent-v2/backend/go/internal/api"
	"my-agent-v2/backend/go/internal/bootstrap"
)

func main() {
	app, err := bootstrap.NewApp("http://localhost:8000")
	if err != nil {
		log.Fatalf("failed to initialize app: %v", err)
	}

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// 启动 Publisher
	publisherCtx, cancelPublisher := context.WithCancel(
		context.Background(),
	)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		app.Publisher.Run(publisherCtx, time.Second)
	}()

	// 启动 Consumer
	consumerCtx, cancelConsumer := context.WithCancel(
		context.Background(),
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := app.Consumer.Run(consumerCtx); err != nil {
			log.Printf("consumer stopped with error: %v", err)
		}
	}()

	// 创建 HTTP Router
	router := api.NewRouter(app)

	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	serverErr := make(chan error, 1)

	go func() {
		log.Println("HTTP server listening on :8080")
		serverErr <- server.ListenAndServe()
	}()

	// 等待退出信号或 HTTP 服务异常
	select {
	case <-signalCtx.Done():
		log.Println("shutdown signal received")

	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server failed: %v", err)
		}
	}

	// 第一阶段：关闭 HTTP 服务
	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown failed: %v", err)
	}

	cancelShutdown()

	// 第二阶段：停止 Publisher
	// 第二阶段：停止后台任务
	cancelPublisher()
	cancelConsumer()
	wg.Wait()

	// 第三阶段：释放 RabbitMQ 和 SQLite
	if err := app.Close(); err != nil {
		log.Printf("failed to close app: %v", err)
	}

	log.Println("application stopped")
}
