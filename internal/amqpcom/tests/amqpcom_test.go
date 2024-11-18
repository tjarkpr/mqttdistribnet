package amqpcom_tsts

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"log"
	amqpcom "mqttdistrib/internal/amqpcom/pkg"
	"testing"
)

type TestRequest struct {
	RequestText string `json:"request_text"`
}

func (r TestRequest) ToString() string {
	str, err := json.Marshal(r)
	if err != nil { log.Panicln("Could not marshal TestRequest"); return ""}
	return string(str)
}

type TestResponse struct {
	ResponseText string `json:"response_text"`
}
func (r TestResponse) ToString() string {
	str, err := json.Marshal(r)
	if err != nil { log.Panicln("Could not marshal TestRequest"); return ""}
	return string(str)
}

func TestRequestResponse(t *testing.T) {
	ctx := context.Background()
	rabbitmqContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:4-management-alpine",
	)
	if err != nil || rabbitmqContainer == nil { t.Error(err); return }
	url, err := rabbitmqContainer.AmqpURL(ctx)
	if err != nil { t.Error(err); return }
	connection, err := amqp091.Dial(url)
	if err != nil || connection == nil { t.Error(err); return }
	channel, err := connection.Channel()
	if err != nil || channel == nil { t.Error(err); return }
	queueName := uuid.New().String()
	client, temporaryQueueName, err := amqpcom.MakeIRequestClient[TestResponse, TestRequest](channel, queueName, "")
	if err != nil || temporaryQueueName == "" { t.Error(err); return }
	builder := amqpcom.MakeIRequestHandlerBuilder[TestResponse, TestRequest](channel)
	builder.WithQueueName(queueName)
	builder.WithHandlerAction(func(request TestRequest) (TestResponse, error) { return TestResponse{ResponseText: request.RequestText}, nil })
	cancel, err := builder.BuildAndRun()
	if err != nil { t.Error(err); return }
	defer cancel()
	response, err := client.Send(&TestRequest{RequestText: "Test"})
	if err != nil || response == nil { t.Error(err); return }
	if response.ResponseText != "Test" { t.FailNow() }
	err = client.Close()
	if err != nil { t.Error(err); return }
	err = channel.Close()
	if err != nil { t.Error(err); return }
	err = connection.Close()
	if err != nil { t.Error(err); return }
	err = testcontainers.TerminateContainer(rabbitmqContainer)
	if err != nil { t.Error(err) }
}