package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

const (
	SimpleQueueTypeDurable SimpleQueueType = iota
	SimpleQueueTypeTransient
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	messages, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %w", err)
	}

	go func() {
		defer ch.Close()
		for msg := range messages {
			var target T
			err := json.Unmarshal(msg.Body, &target)
			if err != nil {
				continue
			}
			ackType := handler(target)
			switch ackType {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("NackRequeue")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("NackDiscard")
			}
		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to create channel: %w", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueTypeDurable,
		queueType == SimpleQueueTypeTransient,
		queueType == SimpleQueueTypeTransient,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue: %w", err)
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue: %w", err)
	}

	return ch, queue, nil
}
