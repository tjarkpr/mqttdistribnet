package mqttdistribnet_int

import (
	"errors"
	"github.com/rabbitmq/amqp091-go"
	amqpcom "mqttdistrib/internal/amqpcom/pkg"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
	"reflect"
)

type RemoteDistributionNetworkManager struct {
	Name              string
	ManagementChannel *amqp091.Channel
	RegisterClient    amqpcom.IRequestClient[RegisterResponse, RegisterRequest]
	UnregisterClient  amqpcom.IRequestClient[UnregisterResponse, UnregisterRequest]
	DistributorClient amqpcom.IRequestClient[DistributorResponse, DistributorRequest]
	ConsumerClient    amqpcom.IRequestClient[ConsumerResponse, ConsumerRequest]
	RouteClient       amqpcom.IRequestClient[RouteResponse, RouteRequest]
}

func MakeRemoteDistributionNetworkManager(
	connection *amqp091.Connection,
	name string) (*RemoteDistributionNetworkManager, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}
	registerClient, err := MakeClient[RegisterRequest, RegisterResponse](channel, name)
	if err != nil {
		return nil, err
	}
	unregisterClient, err := MakeClient[UnregisterRequest, UnregisterResponse](channel, name)
	if err != nil {
		return nil, err
	}
	distributorClient, err := MakeClient[DistributorRequest, DistributorResponse](channel, name)
	if err != nil {
		return nil, err
	}
	consumerClient, err := MakeClient[ConsumerRequest, ConsumerResponse](channel, name)
	if err != nil {
		return nil, err
	}
	routeClient, err := MakeClient[RouteRequest, RouteResponse](channel, name)
	if err != nil {
		return nil, err
	}
	return &RemoteDistributionNetworkManager{
		Name:              name,
		ManagementChannel: channel,
		RegisterClient:    registerClient,
		UnregisterClient:  unregisterClient,
		DistributorClient: distributorClient,
		ConsumerClient:    consumerClient,
		RouteClient:       routeClient,
	}, nil
}

func MakeClient[TRequest amqpcommsgs.IMessage, TResponse amqpcommsgs.IMessage](
	channel *amqp091.Channel,
	name string) (amqpcom.IRequestClient[TResponse, TRequest], error) {
	client, tempQueue, err := amqpcom.MakeIRequestClient[TResponse, TRequest](
		channel,
		name+RouteSeparator+reflect.TypeFor[TRequest]().String(),
		"")
	if err != nil {
		return nil, err
	}
	if string(tempQueue) == EmptyString {
		return nil, errors.New("temporary queue is empty")
	}
	return client, nil
}

func (remote *RemoteDistributionNetworkManager) Register(messageType reflect.Type, route string) error {
	response, err := remote.RegisterClient.Send(&RegisterRequest{
		MessageType: messageType.String(),
		Route:       route,
	})
	if err != nil {
		return err
	}
	if !response.Success {
		return errors.New(response.Error)
	}
	return nil
}

func (remote *RemoteDistributionNetworkManager) Unregister(messageType reflect.Type) error {
	response, err := remote.UnregisterClient.Send(&UnregisterRequest{
		MessageType: messageType.String(),
	})
	if err != nil {
		return err
	}
	if !response.Success {
		return errors.New(response.Error)
	}
	return nil
}

func (remote *RemoteDistributionNetworkManager) Distributor() (string, error) {
	response, err := remote.DistributorClient.Send(&DistributorRequest{})
	if err != nil {
		return EmptyString, err
	}
	if response.Distributor == EmptyString {
		return EmptyString, errors.New(response.Error)
	}
	return response.Distributor, nil
}

func (remote *RemoteDistributionNetworkManager) Consumer(messageType reflect.Type) (map[string]string, error) {
	response, err := remote.ConsumerClient.Send(&ConsumerRequest{
		MessageType: messageType.String(),
	})
	if err != nil {
		return make(map[string]string), err
	}
	if len(response.Consumer) == 0 {
		return response.Consumer, errors.New(response.Error)
	}
	return response.Consumer, nil
}

func (remote *RemoteDistributionNetworkManager) Route(messageType reflect.Type) ([]string, error) {
	response, err := remote.RouteClient.Send(&RouteRequest{
		MessageType: messageType.String(),
	})
	if err != nil {
		return []string{}, err
	}
	if len(response.Routes) == 0 {
		return response.Routes, errors.New(response.Error)
	}
	return response.Routes, nil
}

func (remote *RemoteDistributionNetworkManager) Channel() *amqp091.Channel {
	return remote.ManagementChannel
}

func (remote *RemoteDistributionNetworkManager) Close() error {
	err := remote.RegisterClient.Close()
	if err != nil {
		return err
	}
	err = remote.UnregisterClient.Close()
	if err != nil {
		return err
	}
	err = remote.DistributorClient.Close()
	if err != nil {
		return err
	}
	err = remote.ConsumerClient.Close()
	if err != nil {
		return err
	}
	err = remote.RouteClient.Close()
	if err != nil {
		return err
	}
	err = remote.ManagementChannel.Close()
	if err != nil {
		return err
	}
	return nil
}
