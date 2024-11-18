package amqpcom_int

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	"log"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
	asynccom "mqttdistrib/internal/asynccom/pkg"
	"time"
)

func MakePublisher[TMessage amqpcommsgs.IMessage](
	channel *amqp091.Channel,
	routingKey string,
	exchangeName string) *Publisher[TMessage] {
	client := asynccom.MakeIPubSubClient(routingKey, exchangeName)
	return &Publisher[TMessage]{
		Client:  &client,
		Channel: channel,
	}
}

type Publisher[TMessage amqpcommsgs.IMessage] struct {
	Channel *amqp091.Channel
	Client  *asynccom.IPubSubClient
}

func (p *Publisher[TMessage]) Publish(message *TMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := ToData[TMessage](message)
	if err != nil {
		return err
	}
	err = (*p.Client).Publish(
		p.Channel,
		&amqp091.Publishing{
			ContentType: "application/json",
			Body:        *data,
		},
		&ctx)
	if err != nil {
		return err
	}
	return nil
}

func MakeConsumerBuilder[TMessage amqpcommsgs.IMessage](
	Channel *amqp091.Channel) *ConsumerBuilder[TMessage] {
	return &ConsumerBuilder[TMessage]{
		Channel:   Channel,
		Action:    nil,
		QueueName: "",
	}
}

type ConsumerBuilder[TMessage amqpcommsgs.IMessage] struct {
	Channel   *amqp091.Channel
	QueueName string
	Action    func(message TMessage) error
}

func (builder *ConsumerBuilder[TMessage]) WithQueueName(name string) {
	builder.QueueName = name
}
func (builder *ConsumerBuilder[TMessage]) WithHandlerAction(action func(message TMessage) error) {
	builder.Action = action
}
func (builder *ConsumerBuilder[TMessage]) BuildAndRun(ctx *context.Context) error {
	if builder.Action == nil || builder.QueueName == "" {
		return &AmqpCommunicationError{Message: "Not initialized."}
	}
	handler, err := asynccom.MakeIPubSubHandler(builder.Channel, builder.QueueName, ctx)
	if err != nil {
		return err
	}
	go func() {
		for msg := range *handler.Subscribe() {
			err := msg.Ack(false)
			if err != nil {
				log.Println("Could not acknowledge message.", err)
				continue
			}
			message, err := FromData[TMessage](&msg)
			if err != nil {
				log.Println("Could not build message.", err)
				continue
			}
			err = builder.Action(*message)
			if err != nil {
				log.Println("Could not handle message.", err)
				continue
			}
		}
	}()
	return nil
}
