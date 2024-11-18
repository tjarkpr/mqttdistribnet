package synccom_int

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

type TemporaryQueueName string
type SyncCommunicationError struct { Message string }
func (m *SyncCommunicationError) Error() string { return m.Message }

func MakeRPCClient(
	channel *amqp.Channel,
	routingKey string,
	exchangeName string) (*RPCClient, TemporaryQueueName, error) {
	receiveQueue, err := channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil { return nil, "", err }
	receiveChannel, err := channel.Consume(
		receiveQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil { return nil, "", err }
	return &RPCClient{
		RoutingKey:     routingKey,
		ExchangeName:   exchangeName,
		ReceiveQueue:   &receiveQueue,
		ReceiveChannel: &receiveChannel,
	}, TemporaryQueueName(receiveQueue.Name), nil
}
type RPCClient struct {
	RoutingKey 		string
	ExchangeName  	string
	ReceiveQueue 	*amqp.Queue
	ReceiveChannel 	*<-chan amqp.Delivery
}
func (rc *RPCClient) Send(
	channel *amqp.Channel,
	request *amqp.Publishing,
	ctx *context.Context) (*amqp.Delivery, error) {
	err := channel.PublishWithContext(
		*ctx,
		rc.ExchangeName,
		rc.RoutingKey,
		false,
		false,
		*request)
	if err != nil { return nil, err }
	for delivery := range *rc.ReceiveChannel {
		if request.CorrelationId == delivery.CorrelationId {
			err = delivery.Ack(false)
			if err != nil { return nil, err }
			return &delivery, nil
		}
	}
	return nil, &SyncCommunicationError{Message: "Receive channel was not correlated"}
}
func (rc *RPCClient) Close(channel *amqp.Channel) error {
	_, err := channel.QueueDelete(
		rc.ReceiveQueue.Name,
		false,
		false,
		false)
	if err != nil { return err }; return nil
}

func NewRPCHandler(
	channel *amqp.Channel,
	queueName string) (*RPCHandler, error) {
	receiveQueue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil { return nil, err }
	receiveChannel, err := channel.Consume(
		receiveQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil { return nil, err }
	return &RPCHandler{
		ReceiveChannel: &receiveChannel,
	}, nil
}
type RPCHandler struct { ReceiveChannel *<-chan amqp.Delivery }
func (rh *RPCHandler) Receive() *<-chan amqp.Delivery { return rh.ReceiveChannel }
func (rh *RPCHandler) ReplyTo(
	channel *amqp.Channel,
	request *amqp.Delivery,
	response *amqp.Publishing,
	ctx *context.Context) error {
	err := channel.PublishWithContext(
		*ctx,
		"",
		request.ReplyTo,
		false,
		false,
		*response)
	if err != nil { return err }
	return nil
}