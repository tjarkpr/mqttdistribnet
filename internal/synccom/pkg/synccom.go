package synccom

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	synccomint "mqttdistrib/internal/synccom/internal"
)

func MakeIRPCClient(
	channel *amqp.Channel,
	routingKey string,
	exchangeName string) (IRPCClient, synccomint.TemporaryQueueName, error) {
	var err error; var client IRPCClient; var queueName synccomint.TemporaryQueueName
	client, queueName, err = synccomint.MakeRPCClient(channel, routingKey, exchangeName)
	if err != nil || client == nil { return nil, "", err }; return client, queueName, nil
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
	queueName string) (IRPCHandler, error) {
	var err error; var handler IRPCHandler
	handler, err = synccomint.NewRPCHandler(channel, queueName)
	if err != nil || handler == nil { return nil, err }; return handler, nil
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

// FromData - Conversions form amqp091.Delivery to a specified type.
func FromData[TMessage any](
	delivery *amqp.Delivery) (*TMessage, error) {
	msg := new(TMessage); err := json.Unmarshal(delivery.Body, msg)
	if err != nil { return nil, err }; return msg, nil
}
func ToData[TMessage any](
	message *TMessage) (*[]byte, error) {
	data, err := json.Marshal(message)
	return &data, err
}