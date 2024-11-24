package mqttdistribnet_tsts

import (
	"context"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	mqttdistribnet "mqttdistrib/pkg"
	"reflect"
	"testing"
	"time"
)

type TestRequest struct {
	TestReqProperty string `json:"testReqProperty"`
}
type TestResponse struct {
	TestResProperty string `json:"testResProperty"`
}

func TestRequestResponse(t *testing.T) {
	ctx := context.Background()
	container, connection, err := ConnectionFromContainer(ctx)
	if err != nil || connection == nil {
		t.Error(err)
		return
	}
	manager, err := mqttdistribnet.MakeIDistributionNetworkManager(connection, "test")
	if err != nil {
		t.Error(err)
		return
	}
	logs, err := manager.Start(&ctx)
	if err != nil {
		t.Error(err)
		return
	}
	go func() {
		for l := range logs {
			t.Log(l)
		}
	}()
	remote, err := mqttdistribnet.MakeIRemoteDistributionNetworkManager(connection, "test")
	if err != nil {
		t.Error(err)
		return
	}
	err = remote.Register(reflect.TypeFor[mqttdistribnet.Envelope[TestRequest]](), "L1.*")
	if err != nil {
		t.Error(err)
		return
	}
	client, err := mqttdistribnet.MakeRequestClient[mqttdistribnet.Envelope[TestRequest], mqttdistribnet.Envelope[TestResponse]](remote)
	if err != nil {
		t.Error(err)
		return
	}
	err = mqttdistribnet.MakeRequestHandler[mqttdistribnet.Envelope[TestRequest], mqttdistribnet.Envelope[TestResponse]](
		remote,
		func(request mqttdistribnet.Envelope[TestRequest]) (mqttdistribnet.Envelope[TestResponse], error) {
			t.Log("Request: " + request.ToString())
			return mqttdistribnet.Envelope[TestResponse]{
				MessageId: uuid.New(),
				Timestamp: time.Now(),
				Payload:   TestResponse{TestResProperty: "Test"},
			}, nil
		}, &ctx)
	if err != nil {
		t.Error(err)
		return
	}
	request := &mqttdistribnet.Envelope[TestRequest]{
		MessageId: uuid.New(),
		Timestamp: time.Now(),
		Payload:   TestRequest{TestReqProperty: "Test"},
	}
	response, err := client.Send(request)
	if response == nil || response.Payload.TestResProperty != request.Payload.TestReqProperty {
		t.FailNow()
	}
	if err != nil {
		t.Error(err)
		return
	}
	err = remote.Close()
	if err != nil {
		t.Error(err)
		return
	}
	err = manager.Close()
	if err != nil {
		t.Error(err)
		return
	}
	err = testcontainers.TerminateContainer(container)
	if err != nil {
		t.Error(err)
	}
}

func ConnectionFromContainer(ctx context.Context) (*rabbitmq.RabbitMQContainer, *amqp091.Connection, error) {
	container, err := rabbitmq.Run(
		ctx,
		"rabbitmq:4-management-alpine")
	if err != nil || container == nil {
		return nil, nil, err
	}
	url, err := container.AmqpURL(ctx)
	if err != nil {
		return nil, nil, err
	}
	connection, err := amqp091.Dial(url)
	if err != nil || connection == nil {
		return nil, nil, err
	}
	return container, connection, nil
}
