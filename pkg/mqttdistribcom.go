package mqttdistribnet

import (
	"context"
	"errors"
	"github.com/google/uuid"
	mqttdistribnetint "mqttdistrib/internal"
	amqpcom "mqttdistrib/internal/amqpcom/pkg"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
	"reflect"
	"time"
)

type IEnvelope interface {
	GetMessageId() uuid.UUID
	GetTimestamp() time.Time
}

type Envelope[TPayload any] struct {
	MessageId uuid.UUID `json:"messageId"`
	Payload TPayload  `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

func (envelope Envelope[TPayload]) GetMessageId() uuid.UUID { return envelope.MessageId }
func (envelope Envelope[TPayload]) GetTimestamp() time.Time { return envelope.Timestamp }
func (envelope Envelope[TPayload]) ToString() string { return amqpcommsgs.ToString(envelope) }

func MakePublisher[TMessage amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager) (amqpcom.IPublisher[TMessage], error) {
	distributor, err := manager.Distributor()
	if err != nil { return nil, err }
	route, err := manager.Route(reflect.TypeFor[TMessage]())
	if err != nil { return nil, err }
	return amqpcom.MakeIPublisher[TMessage](manager.Channel(), route, distributor), nil
}

func MakeConsumer[TMessage amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager,
	action func(TMessage) error,
	ctx *context.Context) error {
	builder := amqpcom.MakeIConsumerBuilder[TMessage](manager.Channel())
	consumer, err := manager.Consumer(reflect.TypeFor[TMessage]())
	if err != nil { return err }
	builder.WithQueueName(consumer)
	builder.WithHandlerAction(action)
	err = builder.BuildAndRun(ctx)
	if err != nil { return err }
	return nil
}

func MakeRequestClient[TRequest amqpcommsgs.IMessage, TResponse amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager) (amqpcom.IRequestClient[TResponse, TRequest], error) {
	distributor, err := manager.Distributor()
	if err != nil { return nil, err }
	route, err := manager.Route(reflect.TypeFor[TRequest]())
	if err != nil { return nil, err }
	client, tq, err := amqpcom.MakeIRequestClient[TResponse, TRequest](manager.Channel(), route, distributor)
	if err != nil { return nil, err }
	if string(tq) == mqttdistribnetint.EmptyString { return nil, errors.New("no temporary queue created") }
	return client, nil
}

func MakeRequestHandler[TRequest amqpcommsgs.IMessage, TResponse amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager,
	action func(request TRequest) (TResponse, error),
	ctx *context.Context) error {
	builder := amqpcom.MakeIRequestHandlerBuilder[TResponse, TRequest](manager.Channel())
	consumer, err := manager.Consumer(reflect.TypeFor[TRequest]())
	if err != nil { return err }
	builder.WithQueueName(consumer)
	builder.WithHandlerAction(action)
	err = builder.BuildAndRun(ctx)
	if err != nil { return err }
	return nil
}