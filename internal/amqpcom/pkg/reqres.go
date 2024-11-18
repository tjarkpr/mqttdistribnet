package amqpcom

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	amqpcomint "mqttdistrib/internal/amqpcom/internal"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
)

func MakeIRequestClient[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage](
	channel *amqp091.Channel,
	routingKey string,
	exchangeName string) (IRequestClient[TResponse, TRequest], amqpcomint.TemporaryQueueName, error) {
	return amqpcomint.MakeRequestClient[TResponse, TRequest](channel, routingKey, exchangeName)
}

type IRequestClient[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage] interface {
	Send(request *TRequest) (*TResponse, error)
	Close() error
}

func MakeIRequestHandlerBuilder[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage](
	channel *amqp091.Channel) IRequestHandlerBuilder[TResponse, TRequest] {
	return amqpcomint.MakeRequestHandlerBuilder[TResponse, TRequest](channel)
}

type IRequestHandlerBuilder[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage] interface {
	WithQueueName(name string)
	WithHandlerAction(action func(request TRequest) (TResponse, error))
	BuildAndRun(ctx *context.Context) error
}
