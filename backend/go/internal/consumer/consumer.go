
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/repository"
	"my-agent-v2/backend/go/internal/workflow"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	channel *amqp.Channel
	engine  *workflow.Engine
	runRepo *repository.RunRepository
}

func NewConsumer(
	channel *amqp.Channel,
	engine *workflow.Engine,
	runRepo *repository.RunRepository,
) *Consumer {
	return &Consumer{
		channel: channel,
		engine:  engine,
		runRepo: runRepo,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	deliveries, err := c.channel.Consume(
		"workflow.run",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consuming message: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq delivery channel closed")
			}

			if err := c.handleDelivery(ctx, delivery); err != nil {
				log.Printf("handle delivery failed: %v", err)
			}
		}
	}
}

func (c *Consumer) handleDelivery(
	ctx context.Context,
	delivery amqp.Delivery,
) error {

	// 1. 解析 Outbox
	var outbox domain.Outbox

	if err := json.Unmarshal(delivery.Body, &outbox); err != nil {
		if rejectErr := delivery.Reject(false); rejectErr != nil {
			return fmt.Errorf(
				"unmarshal outbox failed: %v; reject failed: %w",
				err,
				rejectErr,
			)
		}

		return fmt.Errorf("unmarshal outbox failed: %w", err)
	}

	// 2. 校验事件类型
	if outbox.EventType != "workflow.run" {
		if rejectErr := delivery.Reject(false); rejectErr != nil {
			return fmt.Errorf(
				"unsupported event type: %s; reject failed: %w",
				outbox.EventType,
				rejectErr,
			)
		}

		return fmt.Errorf("unsupported event type: %s", outbox.EventType)
	}

	// 3. 解析 WorkflowRunPayload
	var payload domain.WorkflowRunPayload

	if err := json.Unmarshal([]byte(outbox.Payload), &payload); err != nil {
		if rejectErr := delivery.Reject(false); rejectErr != nil {
			return fmt.Errorf(
				"unmarshal workflow payload failed: %v; reject failed: %w",
				err,
				rejectErr,
			)
		}

		return fmt.Errorf("unmarshal workflow payload failed: %w", err)
	}

	// 4. 校验两层 RunID
	if outbox.RunID == "" || payload.RunID == "" {
		if rejectErr := delivery.Reject(false); rejectErr != nil {
			return fmt.Errorf(
				"run_id cannot be empty; reject failed: %w",
				rejectErr,
			)
		}

		return fmt.Errorf("run_id cannot be empty")
	}

	if outbox.RunID != payload.RunID {
		if rejectErr := delivery.Reject(false); rejectErr != nil {
			return fmt.Errorf(
				"run_id mismatch; reject failed: %w",
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
	claimed, err := c.runRepo.ClaimPendingRun(outbox.RunID)
	if err != nil {
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf(
				"claim run failed: %v; nack failed: %w",
				err,
				nackErr,
			)
		}

		return fmt.Errorf("claim run failed: %w", err)
	}

	// 6. 未领取成功：重复消息或 Run 已被其他消费者领取
	if !claimed {
		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf("ack duplicate message failed: %w", err)
		}

		log.Printf(
			"run %s already claimed or processed, skip execution",
			outbox.RunID,
		)

		return nil
	}

	// 7. 调用 Workflow Engine
	result, executeErr := c.engine.Execute(
		ctx,
		payload.Workflow,
		payload.RunID,
		payload.Input,
	)

	// 8. 工作流执行失败
	if executeErr != nil {

		// 8.1 持久化失败状态和错误信息
		if err := c.runRepo.MarkRunFailed(
			outbox.RunID,
			executeErr.Error(),
		); err != nil {

			if nackErr := delivery.Nack(false, true); nackErr != nil {
				return fmt.Errorf(
					"workflow execution failed: %v; save failure info failed: %v; nack failed: %w",
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

		// 8.2 失败状态已经保存，确认 RabbitMQ 消息
		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf(
				"ack failed workflow message: %w",
				err,
			)
		}

		// 8.3 业务失败不是消息消费失败
		log.Printf(
			"workflow run %s failed: %v",
			outbox.RunID,
			executeErr,
		)

		return nil
	}

	// 9. 工作流执行成功
	log.Printf(
		"workflow run %s result: %+v",
		outbox.RunID,
		result,
	)

	// 9.1 序列化执行结果
	resultBytes, err := json.Marshal(result)
	if err != nil {
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf(
				"marshal workflow result failed: %v; nack failed: %w",
				err,
				nackErr,
			)
		}

		return fmt.Errorf("marshal workflow result failed: %w", err)
	}

	// 9.2 保存执行结果
	if err := c.runRepo.SaveResult(
		outbox.RunID,
		string(resultBytes),
	); err != nil {

		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf(
				"save workflow result failed: %v; nack failed: %w",
				err,
				nackErr,
			)
		}

		return fmt.Errorf("save workflow result failed: %w", err)
	}

	// 9.3 更新 Run 状态
	if err := c.runRepo.UpdateStatus(
		outbox.RunID,
		"succeeded",
	); err != nil {

		if nackErr := delivery.Nack(false, true); nackErr != nil {
			return fmt.Errorf(
				"update run status failed: %v; nack failed: %w",
				err,
				nackErr,
			)
		}

		return fmt.Errorf("update run status to succeeded failed: %w", err)
	}

	// 10. 持久化成功后，确认消息
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf(
			"ack successful workflow message failed: %w",
			err,
		)
	}

	log.Printf(
		"workflow run %s completed successfully",
		outbox.RunID,
	)

	return nil
}
