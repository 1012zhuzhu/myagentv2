package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"my-agent-v2/backend/go/internal/domain"
	"my-agent-v2/backend/go/internal/repository"
	"time"
)

type Publisher struct {
    outboxRepo *repository.OutboxRepository
    rabbitMQ   *RabbitMQClient
}

func NewPublisher(
    outboxRepo *repository.OutboxRepository,
    rabbitMQ *RabbitMQClient,
) *Publisher {
    return &Publisher{
        outboxRepo: outboxRepo,
        rabbitMQ:   rabbitMQ,
    }
}

func (p *Publisher) PublishPending(ctx context.Context) error {
	
	if err := ctx.Err(); err != nil {
		log.Printf("get pending outboxes failed: %v", err)
		return err
	}

    outboxs, err := p.outboxRepo.GetPendingOutboxes(ctx)
    if err != nil {
        log.Printf("get pending outboxes failed: %v", err)
        return err
    }

	if len(outboxs) == 0 {
		return nil
	}

	log.Printf("pending outboxes loaded count=%d", len(outboxs))

	for _,outbox :=  range outboxs {
		if err := ctx.Err(); err != nil {
            return err
        }

        if err := p.publishOne(ctx, outbox); err != nil {
            return fmt.Errorf(
                "publish outbox %s failed: %w",
                outbox.EventID,
                err,
            )
        }
	}

	return nil
}

func (p *Publisher) publishOne(
    ctx context.Context,
    outbox domain.Outbox,
) error {
    logger := log.New(
        log.Writer(),
        fmt.Sprintf(
            "[event_id=%s run_id=%s event_type=%s] ",
            outbox.EventID,
            outbox.RunID,
            outbox.EventType,
        ),
        log.LstdFlags,
    )

    body, err := json.Marshal(outbox)
    if err != nil {
        logger.Printf("marshal outbox failed: %v", err)
        return err
    }

    logger.Println("publishing outbox")

    err = p.rabbitMQ.PublishWithConfirm(ctx, body)
    if err != nil {
        logger.Printf("rabbitmq publish failed: %v", err)
        return err
    }

    logger.Println("rabbitmq publish confirmed")

    publishedAt := time.Now()

    publishCtx, cancel := context.WithTimeout(
        context.Background(),
        3*time.Second,
    )
    defer cancel()

    err = p.outboxRepo.MarkPublished(
        publishCtx,
        outbox.EventID,
        publishedAt,
    )
    if err != nil {
        logger.Printf("mark outbox published failed: %v", err)
        return fmt.Errorf("mark outbox published failed: %w", err)
    }

    logger.Println("outbox marked as published")

    return nil
}

func (p *Publisher) Run(ctx context.Context, interval time.Duration) {
    if interval <= 0 {
        interval = time.Second
    }

    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        // 发布之前，先检查是否已经收到退出信号
        select {
        case <-ctx.Done():
            log.Println("publisher stopped")
            return
        default:
        }

        if err := p.PublishPending(ctx); err != nil {
        if errors.Is(err, context.Canceled) {
            log.Println("publisher received cancellation")
            return
        }

        log.Printf("publish pending outboxes failed: %v", err)
    }

        select {
        case <-ctx.Done():
            log.Println("publisher stopped")
            return
        case <-ticker.C:
        }
    }
}