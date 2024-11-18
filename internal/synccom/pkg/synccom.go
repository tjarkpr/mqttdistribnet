package synccom

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	synccomint "mqttdistrib/internal/synccom/internal"
)

func MakeIRPCClient(
	channel *amqp.Channel,
	routingKey string,
	exchangeName string) (IRPCClient, synccomint.TemporaryQueueName, error) {
	var err error
	var client IRPCClient
	var queueName synccomint.TemporaryQueueName
	client, queueName, err = synccomint.MakeRPCClient(channel, routingKey, exchangeName)
	if err != nil || client == nil {
		return nil, "", err
	}
	return client, queueName, nil
}

// IRPCClient - AMQP-RPC-Client to Send requests and receive responses.
type IRPCClient interface {
	Send(
		channel *amqp.Channel,
		request *amqp.Publishing,
		ctx *context.Context) (*amqp.Delivery, error)
	Close(channel *amqp.Channel) error
}

func MakeIRPCHandler(
	channel *amqp.Channel,
	queueName string,
	ctx *context.Context) (IRPCHandler, error) {
	var err error
	var handler IRPCHandler
	handler, err = synccomint.MakeRPCHandler(channel, queueName, ctx)
	if err != nil || handler == nil {
		return nil, err
	}
	return handler, nil
}

// IRPCHandler - AMQP-RPC-Handler to Receive requests and ReplyTo the requests with responses.
type IRPCHandler interface {
	Receive() *<-chan amqp.Delivery
	ReplyTo(
		channel *amqp.Channel,
		request *amqp.Delivery,
		response *amqp.Publishing,
		ctx *context.Context) error
}
