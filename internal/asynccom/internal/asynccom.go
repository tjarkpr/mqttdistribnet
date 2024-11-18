package asynccom_int

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

func MakePubSubClient(
	exchangeName string,
	routingKey string) *PubSubClient {
	return &PubSubClient{
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
	}
}

type PubSubClient struct {
	ExchangeName string
	RoutingKey   string
}

func (psc *PubSubClient) Publish(
	channel *amqp.Channel,
	message *amqp.Publishing,
	ctx *context.Context) error {
	return channel.PublishWithContext(
		*ctx,
		psc.ExchangeName,
		psc.RoutingKey,
		false,
		false,
		*message)
}

func MakePubSubHandler(
	channel *amqp.Channel,
	queueName string,
	ctx *context.Context) (*PubSubHandler, error) {
	receiveQueue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	receiveChannel, err := channel.ConsumeWithContext(
		*ctx,
		receiveQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &PubSubHandler{
		ReceiveChannel: &receiveChannel,
	}, nil
}

type PubSubHandler struct {
	ReceiveChannel *<-chan amqp.Delivery
}

func (psh *PubSubHandler) Subscribe() *<-chan amqp.Delivery { return psh.ReceiveChannel }
