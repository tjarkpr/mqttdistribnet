package amqpcom

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	amqpcomint "mqttdistrib/internal/amqpcom/internal"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
)

func MakeIPublisher[TMessage amqpcommsgs.IMessage](
	channel *amqp091.Channel,
	routingKey string,
	exchangeName string) IPublisher[TMessage] {
	return amqpcomint.MakePublisher[TMessage](channel, routingKey, exchangeName)
}

type IPublisher[TMessage amqpcommsgs.IMessage] interface {
	Publish(request *TMessage) error
}

func MakeIConsumerBuilder[TMessage amqpcommsgs.IMessage](
	channel *amqp091.Channel) IConsumerBuilder[TMessage] {
	return amqpcomint.MakeConsumerBuilder[TMessage](channel)
}

type IConsumerBuilder[TMessage amqpcommsgs.IMessage] interface {
	WithQueueName(name string)
	WithHandlerAction(action func(message TMessage) error)
	BuildAndRun(ctx *context.Context) error
}
