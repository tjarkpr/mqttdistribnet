package amqpcom

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	amqpcomint "mqttdistrib/internal/amqpcom/internal"
	amqpcomshrd "mqttdistrib/internal/amqpcom/pkg/shrd"
)

func MakeIRequestClient[TResponse amqpcomshrd.IResponse, TRequest amqpcomshrd.IRequest](
	channel *amqp091.Channel,
	routingKey string,
	exchangeName string) (IRequestClient[TResponse, TRequest], amqpcomint.TemporaryQueueName, error) {
	return amqpcomint.MakeRequestClient[TResponse, TRequest](channel, routingKey, exchangeName)
}
type IRequestClient[TResponse amqpcomshrd.IResponse, TRequest amqpcomshrd.IRequest] interface {
	Send(request *TRequest) (*TResponse, error)
	Close() error
}

func MakeIRequestHandlerBuilder[TResponse amqpcomshrd.IResponse, TRequest amqpcomshrd.IRequest](
	channel *amqp091.Channel) IRequestHandlerBuilder[TResponse, TRequest] {
	return amqpcomint.MakeRequestHandlerBuilder[TResponse, TRequest](channel)
}
type IRequestHandlerBuilder[TResponse amqpcomshrd.IResponse, TRequest amqpcomshrd.IRequest] interface {
	WithQueueName(name string)
	WithHandlerAction(action func(request TRequest) (TResponse, error))
	BuildAndRun() (context.CancelFunc, error)
}