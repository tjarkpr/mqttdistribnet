package synccom_tsts

import (
	"context"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	synccom "github.com/tjarkpr/mqttdistribnet/internal/synccom/pkg"
	"testing"
)

func TestRequestResponse(t *testing.T) {
	ctx := context.Background()
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
	queueName := uuid.New().String()
	client, temporaryQueueName, err := synccom.MakeIRPCClient(channel, queueName, "")
	if err != nil || temporaryQueueName == "" {
		t.Error(err)
		return
	}
	handler, err := synccom.MakeIRPCHandler(channel, queueName, &ctx)
	if err != nil {
		t.Error(err)
		return
	}
	go func() {
		for msg := range *handler.Receive() {
			err := msg.Ack(false)
			if err != nil {
				t.Error(err)
				return
			}
			err = handler.ReplyTo(
				channel,
				&msg,
				&amqp091.Publishing{
					ContentType:   "text/plain",
					CorrelationId: msg.CorrelationId,
					Body:          msg.Body,
				},
				&ctx)
			if err != nil {
				t.Error(err)
			}
		}
	}()
	correlationId := uuid.New().String()
	message := []byte("Test")
	response, err := client.Send(
		channel,
		&amqp091.Publishing{
			ContentType:   "text/plain",
			CorrelationId: correlationId,
			ReplyTo:       string(temporaryQueueName),
			Body:          message,
		},
		&ctx)
	if err != nil || response == nil {
		t.Error(err)
		return
	}
	if response.CorrelationId != correlationId ||
		string(response.Body) != string(message) {
		t.FailNow()
	}
	err = client.Close(channel)
	if err != nil {
		t.Error(err)
		return
	}
	err = channel.Close()
	if err != nil {
		t.Error(err)
		return
	}
	err = connection.Close()
	if err != nil {
		t.Error(err)
		return
	}
	err = testcontainers.TerminateContainer(rabbitmqContainer)
	if err != nil {
		t.Error(err)
	}
}
