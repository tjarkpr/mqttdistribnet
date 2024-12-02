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
	Payload   TPayload  `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

func (envelope Envelope[TPayload]) GetMessageId() uuid.UUID { return envelope.MessageId }
func (envelope Envelope[TPayload]) GetTimestamp() time.Time { return envelope.Timestamp }
func (envelope Envelope[TPayload]) ToString() string        { return amqpcommsgs.ToString(envelope) }

func MakePublisher[TMessage amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager) (map[string]amqpcom.IPublisher[TMessage], error) {
	distributor, err := manager.Distributor()
	if err != nil {
		return nil, err
	}
	routes, err := manager.Route(reflect.TypeFor[TMessage]())
	if err != nil {
		return nil, err
	}
	results := make(map[string]amqpcom.IPublisher[TMessage])
	for _, route := range routes {
		results[route] = amqpcom.MakeIPublisher[TMessage](manager.Channel(), route, distributor)
	}
	return results, nil
}

func MakeConsumer[TMessage amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager,
	action func(TMessage) error,
	ctx *context.Context) error {
	consumers, err := manager.Consumer(reflect.TypeFor[TMessage]())
	if err != nil {
		return err
	}
	for _, consumer := range consumers {
		builder := amqpcom.MakeIConsumerBuilder[TMessage](manager.Channel())
		builder.WithQueueName(consumer)
		builder.WithHandlerAction(action)
		err = builder.BuildAndRun(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func MakeRequestClient[TRequest amqpcommsgs.IMessage, TResponse amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager) (map[string]amqpcom.IRequestClient[TResponse, TRequest], error) {
	distributor, err := manager.Distributor()
	if err != nil {
		return nil, err
	}
	routes, err := manager.Route(reflect.TypeFor[TRequest]())
	if err != nil {
		return nil, err
	}
	results := make(map[string]amqpcom.IRequestClient[TResponse, TRequest])
	for _, route := range routes {
		client, tq, err := amqpcom.MakeIRequestClient[TResponse, TRequest](manager.Channel(), route, distributor)
		if err != nil {
			return nil, err
		}
		if string(tq) == mqttdistribnetint.EmptyString {
			return results, errors.New("no temporary queue created")
		}
		results[route] = client
	}
	return results, nil
}

func MakeRequestHandler[TRequest amqpcommsgs.IMessage, TResponse amqpcommsgs.IMessage](
	manager IRemoteDistributionNetworkManager,
	action func(request TRequest) (TResponse, error),
	ctx *context.Context) error {
	consumers, err := manager.Consumer(reflect.TypeFor[TRequest]())
	if err != nil {
		return err
	}
	for _, consumer := range consumers {
		builder := amqpcom.MakeIRequestHandlerBuilder[TResponse, TRequest](manager.Channel())
		builder.WithQueueName(consumer)
		builder.WithHandlerAction(action)
		err = builder.BuildAndRun(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
