package amqpcom_tsts

import (
	"context"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	amqpcom "mqttdistrib/internal/amqpcom/pkg"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
	"testing"
	"time"
)

type TestMessage struct {
	MessageText string `json:"message_text"`
}

func (m TestMessage) ToString() string { return amqpcommsgs.ToString(m) }

func IntRoutine(ctx context.Context, o chan bool, t *testing.T) {
	rabbitmqContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:4-management-alpine",
	)
	if err != nil || rabbitmqContainer == nil {
		t.Error(err)
		o <- false
		return
	}
	url, err := rabbitmqContainer.AmqpURL(ctx)
	if err != nil {
		t.Error(err)
		o <- false
		return
	}
	connection, err := amqp091.Dial(url)
	if err != nil || connection == nil {
		t.Error(err)
		o <- false
		return
	}
	channel, err := connection.Channel()
	if err != nil || channel == nil {
		t.Error(err)
		o <- false
		return
	}
	pMessage := TestMessage{MessageText: "Hello, World!"}
	queueName := uuid.New().String()
	publisher := amqpcom.MakeIPublisher[TestMessage](channel, queueName, "")
	consumerBuilder := amqpcom.MakeIConsumerBuilder[TestMessage](channel)
	consumerBuilder.WithQueueName(queueName)
	consumerBuilder.WithHandlerAction(func(message TestMessage) error {
		if message.MessageText != pMessage.MessageText {
			t.Error("Unexpected message text")
		}
		return nil
	})
	err = consumerBuilder.BuildAndRun(&ctx)
	if err != nil {
		t.Error(err)
		o <- false
		return
	}
	err = publisher.Publish(&pMessage)
	if err != nil {
		t.Error(err)
	}
	o <- err == nil
}

func TestPubSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out := make(chan bool)
	go IntRoutine(ctx, out, t)
	select {
	case <-ctx.Done():
		t.Error("Timeout")
	case success := <-out:
		if !success {
			t.FailNow()
		}
	}
}
