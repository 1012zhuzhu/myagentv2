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
	if err != nil{
		return fmt.Errorf("start consuming message %w", err) 
	}

	for {
		select{
		case <- ctx.Done():
			return nil
		
		case delivery, ok := <-deliveries:
			if !ok{
				return fmt.Errorf("rabbitmq delivery channel closed")
			}

			if err := c.handleDelivery(ctx, delivery); err != nil {
				log.Printf("handle delivery failed : %v", err)
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
        delivery.Reject(false)
        return fmt.Errorf("unmarshal outbox failed: %w", err)
    }

    // 2. 校验事件类型
    if outbox.EventType != "workflow.run" {
        delivery.Reject(false)
        return fmt.Errorf("unsupported event type: %s", outbox.EventType)
    }

    // 3. 解析 WorkflowRunPayload
    var payload domain.WorkflowRunPayload

    if err := json.Unmarshal([]byte(outbox.Payload), &payload); err != nil {
        delivery.Reject(false)
        return fmt.Errorf("unmarshal workflow payload failed: %w", err)
    }

    // 4. 校验两层 RunID
    if outbox.RunID == "" || payload.RunID == "" {
        delivery.Reject(false)
        return fmt.Errorf("run_id cannot be empty")
    }

    if outbox.RunID != payload.RunID {
        delivery.Reject(false)
        return fmt.Errorf(
            "run_id mismatch: outbox=%s, payload=%s",
            outbox.RunID,
            payload.RunID,
        )
    }

    // 5. 原子领取 Run
    claimed, err := c.runRepo.ClaimPendingRun(outbox.RunID)
    if err != nil {
        delivery.Nack(false, true)
        return fmt.Errorf("claim run failed: %w", err)
    }

    // 6. 如果没有领取成功，说明它已经不是 pending
    // 可能是重复消息，也可能是其他消费者已经领取
    if !claimed {
        if err := delivery.Ack(false); err != nil {
            return fmt.Errorf("ack duplicate message failed: %w", err)
        }

        log.Printf("run %s already claimed, skip execution", outbox.RunID)
        return nil
    }

    // 7. 调用 Workflow Engine 执行工作流
    _, executeErr := c.engine.Execute(
        ctx,
        payload.Workflow,
        payload.RunID,
        payload.Input,
    )

    // 8. 根据执行结果更新 SQLite 状态
    if executeErr != nil {
        if err := c.runRepo.UpdateStatus(outbox.RunID, "failed"); err != nil {
            delivery.Nack(false, true)
            return fmt.Errorf(
                "workflow execution failed: %v; update run status failed: %w",
                executeErr,
                err,
            )
        }

        if err := delivery.Ack(false); err != nil {
            return fmt.Errorf("ack failed workflow message: %w", err)
        }

        return fmt.Errorf("workflow execution failed: %w", executeErr)
    }

    if err := c.runRepo.UpdateStatus(outbox.RunID, "succeeded"); err != nil {
        delivery.Nack(false, true)
        return fmt.Errorf("update run status to succeeded failed: %w", err)
    }

    // 9. 执行完成且状态持久化后，确认消息
    if err := delivery.Ack(false); err != nil {
        return fmt.Errorf("ack successful workflow message failed: %w", err)
    }

    log.Printf("workflow run %s completed successfully", outbox.RunID)

    return nil
}