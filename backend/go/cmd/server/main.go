
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	// 第一阶段：启动 Publisher。
	publisherCtx, cancelPublisher := context.WithCancel(
		context.Background(),
	)
	defer cancelPublisher()

	publisherDone := make(chan struct{})

	go func() {
		defer close(publisherDone)

		app.Publisher.Run(publisherCtx, time.Second)

		log.Println("publisher stopped")
	}()

	// 第二阶段：启动 Consumer。
	consumerCtx, cancelConsumer := context.WithCancel(
		context.Background(),
	)
	defer cancelConsumer()

	consumerErr := make(chan error, 1)

	go func() {
		err := app.Consumer.Run(consumerCtx)
		consumerErr <- err
	}()

	// 第三阶段：启动 HTTP Server。
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

	// 等待退出信号、HTTP 服务异常或 Consumer 异常退出。
	select {
	case <-signalCtx.Done():
		log.Println("shutdown signal received")

	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server failed: %v", err)
		}

	case err := <-consumerErr:
		if err != nil {
			log.Printf("consumer exited unexpectedly: %v", err)
		} else {
			log.Println("consumer exited unexpectedly without error")
		}
	}

	// 第四阶段：关闭 HTTP 服务，停止接收新的 API 请求。
	log.Println("shutting down HTTP server")

	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	httpShutdownErr := server.Shutdown(shutdownCtx)

	cancelShutdown()

	if httpShutdownErr != nil {
		log.Printf("HTTP shutdown failed: %v", httpShutdownErr)

		// 尽可能关闭 HTTP 监听和连接。
		if err := server.Close(); err != nil {
			log.Printf("HTTP server force close failed: %v", err)
		}
	}

	// 第五阶段：停止 Publisher。
	// 避免继续向 Outbox 发布新的 Workflow 消息。
	log.Println("stopping publisher")

	cancelPublisher()

	select {
	case <-publisherDone:
		log.Println("publisher exited successfully")

	case <-time.After(5 * time.Second):
		log.Println("publisher shutdown timed out")
		log.Println(
			"publisher may still be using shared resources; exiting without closing them",
		)
		return
	}

	// 第六阶段：停止 Consumer 接收新消息。
	log.Println("stopping consumer subscription")

	stopAcceptCtx, cancelStopAccept := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	stopAcceptErr := app.Consumer.StopAccepting(stopAcceptCtx)

	cancelStopAccept()

	if stopAcceptErr != nil {
		log.Printf("stop accepting messages failed: %v", stopAcceptErr)

		// 无法确认订阅已成功取消时，发送停止信号，
		// 防止 Consumer 继续启动新的 Workflow。
		log.Println("forcing consumer stop because subscription shutdown failed")
		app.Consumer.ForceStop()
	}

	// 第七阶段：等待当前 Workflow 完成。
	if stopAcceptErr == nil {
		log.Println("waiting for active workflow to finish")

		consumerWaitCtx, cancelConsumerWait := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)

		waitErr := app.Consumer.Wait(consumerWaitCtx)

		cancelConsumerWait()

		if errors.Is(waitErr, context.DeadlineExceeded) {
			log.Printf("consumer wait timed out: %v", waitErr)

			// 第八阶段：超过宽限时间，尝试取消当前 Workflow。
			log.Println("forcing consumer stop")
			app.Consumer.ForceStop()

			forceWaitCtx, cancelForceWait := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)

			forceWaitErr := app.Consumer.Wait(forceWaitCtx)

			cancelForceWait()

			if errors.Is(forceWaitErr, context.DeadlineExceeded) {
				log.Printf(
					"consumer did not exit after force stop: %v",
					forceWaitErr,
				)
				log.Println(
					"consumer may still be using shared resources; exiting process without closing them",
				)
				return
			}

			if forceWaitErr != nil {
				log.Printf("consumer exited with error: %v", forceWaitErr)
			} else {
				log.Println("consumer exited after force stop")
			}

		} else if waitErr != nil {
			log.Printf("consumer exited with error: %v", waitErr)
		} else {
			log.Println("consumer exited successfully")
		}
	} else {
		// StopAccepting 失败后已经发送 ForceStop，
		// 等待 Consumer 退出。
		forceWaitCtx, cancelForceWait := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		forceWaitErr := app.Consumer.Wait(forceWaitCtx)

		cancelForceWait()

		if errors.Is(forceWaitErr, context.DeadlineExceeded) {
			log.Printf(
				"consumer did not exit after force stop: %v",
				forceWaitErr,
			)
			log.Println(
				"consumer may still be using shared resources; exiting process without closing them",
			)
			return
		}

		if forceWaitErr != nil {
			log.Printf("consumer exited with error: %v", forceWaitErr)
		} else {
			log.Println("consumer exited after force stop")
		}
	}

	// 如果 HTTP Handler 仍未完成，不能安全关闭它们使用的共享资源。
	if httpShutdownErr != nil {
		log.Println(
			"HTTP handlers may still be running; exiting without closing shared resources",
		)
		return
	}

	// 第九阶段：Consumer 和 Publisher 均已退出，
	// HTTP 服务也已关闭，现在可以释放共享资源。
	log.Println("closing application resources")

	if err := app.Close(); err != nil {
		log.Printf("failed to close application resources: %v", err)
	}

	log.Println("application stopped")
}
