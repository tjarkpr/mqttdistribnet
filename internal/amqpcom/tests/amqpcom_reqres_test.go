package amqpcom_tsts

import (
	"context"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	amqpcom "github.com/tjarkpr/mqttdistribnet/internal/amqpcom/pkg"
	amqpcommsgs "github.com/tjarkpr/mqttdistribnet/internal/amqpcom/pkg/shrd"
	"testing"
)

type TestRequest struct {
	RequestText string `json:"request_text"`
}

type TestResponse struct {
	ResponseText string `json:"response_text"`
}

func (r TestRequest) ToString() string  { return amqpcommsgs.ToString(r) }
func (r TestResponse) ToString() string { return amqpcommsgs.ToString(r) }

func TestRequestResponse(t *testing.T) {
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
	queueName := uuid.New().String()
	client, temporaryQueueName, err := amqpcom.MakeIRequestClient[TestResponse, TestRequest](channel, queueName, "")
	if err != nil || temporaryQueueName == "" {
		t.Error(err)
		return
	}
	builder := amqpcom.MakeIRequestHandlerBuilder[TestResponse, TestRequest](channel)
	builder.WithQueueName(queueName)
	builder.WithHandlerAction(func(request TestRequest) (TestResponse, error) {
		return TestResponse{ResponseText: request.RequestText}, nil
	})
	err = builder.BuildAndRun(&ctx)
	if err != nil {
		t.Error(err)
		return
	}
	response, err := client.Send(&TestRequest{RequestText: "Test"})
	if err != nil || response == nil {
		t.Error(err)
		return
	}
	if response.ResponseText != "Test" {
		t.FailNow()
	}
	err = client.Close()
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
