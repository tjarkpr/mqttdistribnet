package amqpcom_int

import (
	"context"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"log"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
	synccom "mqttdistrib/internal/synccom/pkg"
	"time"
)

type TemporaryQueueName string

func MakeRequestClient[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage](
	channel *amqp091.Channel,
	routingKey string,
	exchangeName string) (*RequestClient[TResponse, TRequest], TemporaryQueueName, error) {
	client, queueName, err := synccom.MakeIRPCClient(channel, routingKey, exchangeName)
	if err != nil {
		return nil, "", err
	}
	temporaryQueueName := TemporaryQueueName(queueName)
	return &RequestClient[TResponse, TRequest]{
		Client:             &client,
		TemporaryQueueName: temporaryQueueName,
		Channel:            channel,
	}, temporaryQueueName, nil
}

type RequestClient[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage] struct {
	Channel            *amqp091.Channel
	TemporaryQueueName TemporaryQueueName
	Client             *synccom.IRPCClient
}

func (rc *RequestClient[TResponse, TRequest]) Send(request *TRequest) (*TResponse, error) {
	correlationId := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := ToData[TRequest](request)
	if err != nil {
		return nil, err
	}
	delivery, err := (*rc.Client).Send(rc.Channel,
		&amqp091.Publishing{
			ContentType:   "application/json",
			ReplyTo:       string(rc.TemporaryQueueName),
			CorrelationId: correlationId,
			Body:          *data,
		},
		&ctx)
	if err != nil {
		return nil, err
	}
	response, err := FromData[TResponse](delivery)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (rc *RequestClient[TResponse, TRequest]) Close() error { return (*rc.Client).Close(rc.Channel) }

func MakeRequestHandlerBuilder[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage](
	Channel *amqp091.Channel) *RequestHandlerBuilder[TResponse, TRequest] {
	return &RequestHandlerBuilder[TResponse, TRequest]{
		Channel:   Channel,
		Action:    nil,
		QueueName: "",
	}
}

type RequestHandlerBuilder[TResponse amqpcommsgs.IMessage, TRequest amqpcommsgs.IMessage] struct {
	Channel   *amqp091.Channel
	QueueName string
	Action    func(request TRequest) (TResponse, error)
}

func (builder *RequestHandlerBuilder[TResponse, TRequest]) WithQueueName(name string) {
	builder.QueueName = name
}
func (builder *RequestHandlerBuilder[TResponse, TRequest]) WithHandlerAction(action func(request TRequest) (TResponse, error)) {
	builder.Action = action
}
func (builder *RequestHandlerBuilder[TResponse, TRequest]) BuildAndRun(ctx *context.Context) error {
	if builder.Action == nil || builder.QueueName == "" {
		return &AmqpCommunicationError{Message: "Not initialized."}
	}
	handler, err := synccom.MakeIRPCHandler(builder.Channel, builder.QueueName, ctx)
	if err != nil {
		return err
	}
	go func() {
		for msg := range *handler.Receive() {
			err := msg.Ack(false)
			if err != nil {
				log.Println("Could not acknowledge message.", err)
				continue
			}
			request, err := FromData[TRequest](&msg)
			if err != nil {
				log.Println("Could not build request.", err)
				continue
			}
			response, err := builder.Action(*request)
			if err != nil {
				log.Println("Could not handle request.", err)
				continue
			}
			data, err := ToData[TResponse](&response)
			if err != nil {
				log.Println("Could not marshal response.", err)
				continue
			}
			err = handler.ReplyTo(
				builder.Channel,
				&msg,
				&amqp091.Publishing{
					ContentType:   "application/json",
					CorrelationId: msg.CorrelationId,
					Body:          *data,
				},
				ctx)
			if err != nil {
				log.Println("Could not handle reply.", err)
				continue
			}
		}
	}()
	return nil
}
