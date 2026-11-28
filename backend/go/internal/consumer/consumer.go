
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/repository"
	"my-agent-v2/backend/go/internal/workflow"

	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerTag = "workflow-consumer"

var ErrDeliverySettlement = errors.New(
	"rabbitmq delivery settlement failed",
)

type Consumer struct {
	channel *amqp.Channel
	engine  *workflow.Engine
	runRepo *repository.RunRepository

	started chan struct{}
	done    chan struct{}

	stopping atomic.Bool

	mu           sync.Mutex
	activeCancel context.CancelFunc
	runErr       error

	acceptMu      sync.Mutex
	acceptStopped bool

	stopRequested  chan struct{}
	stopSignalOnce sync.Once
}

func NewConsumer(
	channel *amqp.Channel,
	engine *workflow.Engine,
	runRepo *repository.RunRepository,
) *Consumer {
	return &Consumer{
		channel:       channel,
		engine:        engine,
		runRepo:       runRepo,
		started:       make(chan struct{}),
		done:          make(chan struct{}),
		stopRequested: make(chan struct{}),
	}
}

// logWithRunID 统一 Consumer 日志格式
func logWithRunID(runID string, format string, args ...interface{}) {
	log.Printf(
		"[run_id=%s] %s",
		runID,
		fmt.Sprintf(format, args...),
	)
}

// ack 确认 RabbitMQ 消息
func (c *Consumer) ack(delivery amqp.Delivery) error {
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf(
			"%w: ack message: %v",
			ErrDeliverySettlement,
			err,
		)
	}

	return nil
}

// nack 拒绝 RabbitMQ 消息，可以选择是否重新入队
func (c *Consumer) nack(
	delivery amqp.Delivery,
	requeue bool,
) error {
	if err := delivery.Nack(false, requeue); err != nil {
		return fmt.Errorf(
			"%w: nack message: %v",
			ErrDeliverySettlement,
			err,
		)
	}

	return nil
}

// reject 拒绝无效消息，可以选择是否重新入队
func (c *Consumer) reject(
	delivery amqp.Delivery,
	requeue bool,
) error {
	if err := delivery.Reject(requeue); err != nil {
		return fmt.Errorf(
			"%w: reject message: %v",
			ErrDeliverySettlement,
			err,
		)
	}

	return nil
}

func (c *Consumer) Run(ctx context.Context) (runErr error) {
	defer func() {
		c.mu.Lock()
		c.runErr = runErr
		c.mu.Unlock()

		close(c.done)
	}()

	deliveries, err := c.channel.Consume(
		"workflow.run",
		consumerTag,
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consuming message: %w", err)
	}

	close(c.started)

	// 监听 Consumer 生命周期 Context。
	// Context 取消时，停止接收新消息，但不主动取消正在执行的 Workflow。
	go func() {
		select {
		case <-ctx.Done():
			if err := c.StopAccepting(context.Background()); err != nil {
				log.Printf(
					"stop accepting after context cancellation failed: %v",
					err,
				)
			}

		case <-c.done:
			return
		}
	}()

	log.Println("consumer started, waiting for workflow messages")

	for {
		select {
		case <-c.stopRequested:
			log.Println("consumer received stop request")
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				// Cancel() 可能先关闭 deliveries，
				// 再返回给 StopAccepting。
				// 等待 StopAccepting 完成后再判断关闭原因。
				c.acceptMu.Lock()
				stopped := c.acceptStopped
				c.acceptMu.Unlock()

				if stopped {
					log.Println("rabbitmq consumer stopped gracefully")
					return nil
				}

				return fmt.Errorf(
					"rabbitmq deliveries channel closed unexpectedly",
				)
			}

			// 检查停止状态和登记活动任务必须在同一把锁内。
			// 防止 ForceStop 和 Run 之间出现任务启动竞态。
			c.mu.Lock()

			if c.stopping.Load() {
				c.mu.Unlock()

				log.Println(
					"consumer stopping, leaving delivery unacknowledged",
				)
				return nil
			}

			taskCtx, cancel := context.WithCancel(context.Background())
			c.activeCancel = cancel

			c.mu.Unlock()

			// 串行执行 Workflow。
			if err := c.handleDelivery(taskCtx, delivery); err != nil {
				log.Printf("handle delivery failed: %v", err)

				if errors.Is(err, ErrDeliverySettlement) {
					c.mu.Lock()
					c.activeCancel = nil
					c.mu.Unlock()

					cancel()

					return fmt.Errorf(
						"consumer stopped because message settlement failed: %w",
						err,
					)
				}
			}

			// 当前 Workflow 已经结束，清理活动任务。
			c.mu.Lock()
			c.activeCancel = nil
			c.mu.Unlock()

			cancel()
		}
	}
}

func (c *Consumer) StopAccepting(ctx context.Context) error {
	// 等待 Consumer 启动，或者确认它已经退出。
	select {
	case <-c.started:

	case <-c.done:
		c.mu.Lock()
		err := c.runErr
		c.mu.Unlock()
		return err

	case <-ctx.Done():
		return ctx.Err()
	}

	// 避免 Run 已经退出后仍然操作 Channel。
	select {
	case <-c.done:
		c.mu.Lock()
		err := c.runErr
		c.mu.Unlock()
		return err

	default:
	}

	// 保证多个停止请求不会并发执行 Cancel。
	c.acceptMu.Lock()
	defer c.acceptMu.Unlock()

	if c.acceptStopped {
		return nil
	}

	// 获取 acceptMu 后再次检查，避免等待锁期间 Run 已经退出。
	select {
	case <-c.done:
		c.mu.Lock()
		err := c.runErr
		c.mu.Unlock()
		return err

	default:
	}

	// 与 Run 的任务登记互斥。
	// Cancel 执行期间，Run 不能登记新的 Workflow。
	c.mu.Lock()

	// 先取消 RabbitMQ Consumer。
	if err := c.channel.Cancel(consumerTag, false); err != nil {
		c.mu.Unlock()

		return fmt.Errorf(
			"cancel rabbitmq consumer: %w",
			err,
		)
	}

	// 取消订阅成功后，才正式进入停止状态。
	c.stopping.Store(true)

	// 通知 Run 退出循环。
	c.stopSignalOnce.Do(func() {
		close(c.stopRequested)
	})

	c.mu.Unlock()

	c.acceptStopped = true

	log.Println("consumer stopped accepting new messages")

	return nil
}

func (c *Consumer) ForceStop() {
	c.mu.Lock()

	// 阻止 Run 登记新的 Workflow。
	c.stopping.Store(true)

	// 无论有没有活动任务，都通知 Run 退出。
	c.stopSignalOnce.Do(func() {
		close(c.stopRequested)
	})

	// 获取当前活动 Workflow 的取消函数。
	cancel := c.activeCancel

	c.mu.Unlock()

	if cancel == nil {
		log.Println("force stop: no active workflow")
		return
	}

	// 这里只发送 Context 取消信号，不强制杀死 goroutine。
	cancel()

	log.Println("force stop: cancellation signal sent to active workflow")
}

func (c *Consumer) Wait(ctx context.Context) error {
	select {
	case <-c.done:
		c.mu.Lock()
		err := c.runErr
		c.mu.Unlock()

		return err

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) handleDelivery(
	ctx context.Context,
	delivery amqp.Delivery,
) error {
	// 1. 解析 Outbox
	var outbox domain.Outbox

	if err := json.Unmarshal(delivery.Body, &outbox); err != nil {
		if rejectErr := c.reject(delivery, false); rejectErr != nil {
			return fmt.Errorf(
				"unmarshal outbox failed: %v; %w",
				err,
				rejectErr,
			)
		}

		return fmt.Errorf("unmarshal outbox failed: %w", err)
	}

	runID := outbox.RunID

	logWithRunID(
		runID,
		"received workflow message event_id=%s event_type=%s",
		outbox.EventID,
		outbox.EventType,
	)

	// 2. 校验事件类型
	if outbox.EventType != "workflow.run" {
		if rejectErr := c.reject(delivery, false); rejectErr != nil {
			return fmt.Errorf(
				"unsupported event type: %s; %w",
				outbox.EventType,
				rejectErr,
			)
		}

		return fmt.Errorf("unsupported event type: %s", outbox.EventType)
	}

	// 3. 解析 WorkflowRunPayload
	var payload domain.WorkflowRunPayload

	if err := json.Unmarshal([]byte(outbox.Payload), &payload); err != nil {
		if rejectErr := c.reject(delivery, false); rejectErr != nil {
			return fmt.Errorf(
				"unmarshal workflow payload failed: %v; %w",
				err,
				rejectErr,
			)
		}

		return fmt.Errorf("unmarshal workflow payload failed: %w", err)
	}

	// 4. 校验两层 RunID
	if outbox.RunID == "" || payload.RunID == "" {
		if rejectErr := c.reject(delivery, false); rejectErr != nil {
			return fmt.Errorf(
				"run_id cannot be empty; %w",
				rejectErr,
			)
		}

		return fmt.Errorf("run_id cannot be empty")
	}

	if outbox.RunID != payload.RunID {
		if rejectErr := c.reject(delivery, false); rejectErr != nil {
			return fmt.Errorf(
				"run_id mismatch; %w",
				rejectErr,
			)
		}

		return fmt.Errorf(
			"run_id mismatch: outbox=%s, payload=%s",
			outbox.RunID,
			payload.RunID,
		)
	}

	// 5. 原子领取 Run
	logWithRunID(runID, "attempting to claim run")

	claimed, err := c.runRepo.ClaimPendingRun(runID)
	if err != nil {
		if nackErr := c.nack(delivery, true); nackErr != nil {
			return fmt.Errorf(
				"claim run failed: %v; %w",
				err,
				nackErr,
			)
		}

		return fmt.Errorf("claim run failed: %w", err)
	}

	// 6. 未领取成功：查询 Run 当前状态
	if !claimed {
		run, err := c.runRepo.GetRunByID(runID)
		if err != nil {
			if nackErr := c.nack(delivery, true); nackErr != nil {
				return fmt.Errorf(
					"query run after claim failed: %v; %w",
					err,
					nackErr,
				)
			}

			return fmt.Errorf("query run after claim failed: %w", err)
		}

		switch run.Status {
		case "running", "succeeded", "failed", "cancelled",
			"cancelling", "interrupted":

			logWithRunID(
				runID,
				"claim skipped, current status=%s",
				run.Status,
			)

			return c.ack(delivery)

		case "pending":
			if err := c.nack(delivery, true); err != nil {
				return err
			}

			return fmt.Errorf(
				"run is still pending but claim failed: run_id=%s",
				runID,
			)

		default:
			if err := c.reject(delivery, false); err != nil {
				return err
			}

			return fmt.Errorf(
				"unknown run status: run_id=%s status=%s",
				runID,
				run.Status,
			)
		}
	}

	logWithRunID(runID, "run claimed successfully")

	// 7. 调用 Workflow Engine
	logWithRunID(runID, "workflow execution started")

	result, executeErr := c.engine.Execute(
		ctx,
		payload.Workflow,
		payload.RunID,
		payload.Input,
	)

	// 如果 Workflow 是因为外部取消信号而中断，
	// 不要将它记录为普通业务失败，也不要 ACK 消息。
	if ctx.Err() != nil {
		logWithRunID(
			runID,
			"workflow interrupted, leaving message unacknowledged: %v",
			ctx.Err(),
		)

		return fmt.Errorf(
			"workflow execution interrupted: %w",
			ctx.Err(),
		)
	}

	// 8. 工作流执行失败
	if executeErr != nil {
		logWithRunID(
			runID,
			"workflow execution failed: %v",
			executeErr,
		)

		// 8.1 持久化失败状态和错误信息
		if err := c.runRepo.MarkRunFailed(
			runID,
			executeErr.Error(),
		); err != nil {
			if errors.Is(err, repository.ErrRunStateConflict) {
				run, queryErr := c.runRepo.GetRunByID(runID)

				if queryErr != nil {
					if nackErr := c.nack(delivery, true); nackErr != nil {
						return fmt.Errorf(
							"state conflict: %v; query run failed: %v; %w",
							err,
							queryErr,
							nackErr,
						)
					}

					return fmt.Errorf(
						"state conflict: %v; query run failed: %w",
						err,
						queryErr,
					)
				}

				switch run.Status {
				case "succeeded", "failed", "cancelled":
					logWithRunID(
						runID,
						"run is already in terminal state %s, acknowledging message",
						run.Status,
					)

					return c.ack(delivery)
				}
			}

			if nackErr := c.nack(delivery, true); nackErr != nil {
				return fmt.Errorf(
					"workflow execution failed: %v; save failure info failed: %v; %w",
					executeErr,
					err,
					nackErr,
				)
			}

			return fmt.Errorf(
				"workflow execution failed: %v; save failure info failed: %w",
				executeErr,
				err,
			)
		}

		logWithRunID(runID, "failure status and error saved to database")

		// 8.2 失败状态已经保存，确认 RabbitMQ 消息
		if err := c.ack(delivery); err != nil {
			return err
		}

		logWithRunID(
			runID,
			"failed workflow message acknowledged",
		)

		return nil
	}

	// 9. 工作流执行成功
	logWithRunID(runID, "workflow execution produced a result")

	// 9.1 序列化执行结果
	resultBytes, err := json.Marshal(result)
	if err != nil {
		logWithRunID(
			runID,
			"marshal workflow result failed: %v",
			err,
		)

		if saveErr := c.runRepo.MarkRunFailed(
			runID,
			fmt.Sprintf("marshal workflow result failed: %v", err),
		); saveErr != nil {
			if nackErr := c.nack(delivery, true); nackErr != nil {
				return fmt.Errorf(
					"marshal result failed: %v; save failure status failed: %v; %w",
					err,
					saveErr,
					nackErr,
				)
			}

			return fmt.Errorf(
				"marshal result failed: %v; save failure status failed: %w",
				err,
				saveErr,
			)
		}

		logWithRunID(
			runID,
			"marshal failure saved, run status updated to failed",
		)

		return c.ack(delivery)
	}

	// 9.2 原子保存执行结果并更新 Run 状态
	if err := c.runRepo.CompleteRun(runID, string(resultBytes)); err != nil {
		if errors.Is(err, repository.ErrRunStateConflict) {
			run, queryErr := c.runRepo.GetRunByID(runID)
			if queryErr != nil {
				if nackErr := c.nack(delivery, true); nackErr != nil {
					return fmt.Errorf(
						"query run after completion conflict failed: %v; %w",
						queryErr,
						nackErr,
					)
				}

				return fmt.Errorf(
					"query run after completion conflict failed: %w",
					queryErr,
				)
			}

			switch run.Status {
			case "succeeded", "failed", "cancelled":
				logWithRunID(
					runID,
					"run already reached terminal state, acknowledging message",
				)

				return c.ack(delivery)
			}
		}

		if nackErr := c.nack(delivery, true); nackErr != nil {
			return fmt.Errorf(
				"complete run failed: %v; %w",
				err,
				nackErr,
			)
		}

		return fmt.Errorf("complete run failed: %w", err)
	}

	logWithRunID(
		runID,
		"workflow result saved and run status updated to succeeded",
	)

	// 10. 持久化成功后，确认消息
	if err := c.ack(delivery); err != nil {
		return err
	}

	logWithRunID(
		runID,
		"workflow completed successfully and message acknowledged",
	)

	return nil
}
