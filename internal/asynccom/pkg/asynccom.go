package asynccom

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	asynccomint "github.com/tjarkpr/mqttdistribnet/internal/asynccom/internal"
)

func MakeIPubSubClient(
	exchangeName string,
	routingKey string) IPubSubClient {
	var client IPubSubClient
	client = asynccomint.MakePubSubClient(exchangeName, routingKey)
	return client
}

type IPubSubClient interface {
	Publish(
		channel *amqp.Channel,
		message *amqp.Publishing,
		ctx *context.Context) error
}

func MakeIPubSubHandler(
	channel *amqp.Channel,
	queueName string,
	ctx *context.Context) (IPubSubHandler, error) {
	var handler IPubSubHandler
	var err error
	handler, err = asynccomint.MakePubSubHandler(channel, queueName, ctx)
	return handler, err
}

type IPubSubHandler interface {
	Subscribe() *<-chan amqp.Delivery
}
