package asynccom_tsts

import (
	"context"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	asynccom "mqttdistrib/internal/asynccom/pkg"
	"testing"
)

func TestPubSub(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rabbitmqContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:4-management-alpine",
	)
	if err != nil || rabbitmqContainer == nil {
		t.Error(err)
		return
	}
	url, err := rabbitmqContainer.AmqpURL(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	connection, err := amqp091.Dial(url)
	if err != nil || connection == nil {
		t.Error(err)
		return
	}
	channel, err := connection.Channel()
	if err != nil || channel == nil {
		t.Error(err)
		return
	}
	exchangeName := uuid.New().String()
	routingKey := uuid.New().String()
	client := asynccom.MakeIPubSubClient(exchangeName, routingKey)
	handler, err := asynccom.MakeIPubSubHandler(channel, exchangeName, &ctx)
	if err != nil {
		t.Error(err)
		return
	}
	message := []byte("Test")
	err = client.Publish(
		channel,
		&amqp091.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
		&ctx)
	if err != nil {
		t.Error(err)
		return
	}
	for msg := range *handler.Subscribe() {
		if string(msg.Body) != string(message) {
			t.FailNow()
		}
	}
}
