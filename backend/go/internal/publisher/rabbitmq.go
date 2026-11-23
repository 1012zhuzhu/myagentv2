package publisher

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewRabbitMQClient(url string) (*RabbitMQClient,error){
	conn, err:= amqp.Dial(url)
	if err != nil {
		return nil,err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil,err
	}
	return &RabbitMQClient{
		conn: conn,
		ch: ch,
	},nil
}

func (r *RabbitMQClient) Close() error {
    var closeErr error

    if r.ch != nil {
        if err := r.ch.Close(); err != nil {
            closeErr = err
        }
    }

    if r.conn != nil {
        if err := r.conn.Close(); err != nil && closeErr == nil {
            closeErr = err
        }
    }

    return closeErr
}

func (r *RabbitMQClient) SetupTopology() error {
	err := r.ch.ExchangeDeclare(
		"workflow.exchange",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	_ , err = r.ch.QueueDeclare(
		"workflow.run",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = r.ch.QueueBind(
		"workflow.run",       // queue
		"workflow.run",       // routing key
		"workflow.exchange",  // exchange
		false,                // noWait
		nil,                  // args
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQClient) EnableConfirm() error {
    return r.ch.Confirm(false)
}

func (r *RabbitMQClient) PublishWithConfirm(
    ctx context.Context,
    body []byte,
) error {
    confirmation, err := r.ch.PublishWithDeferredConfirmWithContext(
        ctx,
        "workflow.exchange",
        "workflow.run",
        false, // mandatory：暂时不启用退回通知
        false, // immediate：RabbitMQ 不支持
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent,
            Body:         body,
        },
    )
    if err != nil {
        return fmt.Errorf("publish message failed: %w", err)
    }

    ack, err := confirmation.WaitContext(ctx)
    if err != nil {
        return fmt.Errorf("wait publisher confirm failed: %w", err)
    }

    if !ack {
        return fmt.Errorf("rabbitmq returned NACK")
    }

    return nil
}

func (r *RabbitMQClient) NewChannel() (*amqp.Channel, error) {
	if r.conn == nil || r.conn.IsClosed() {
		return nil, fmt.Errorf("rabbitmq connection is closed")
	}

	ch, err := r.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create rabbitmq channel failed: %w",err)
	}

	return ch,nil
}