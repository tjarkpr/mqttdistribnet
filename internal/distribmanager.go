package mqttdistribnet_int

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	amqpcom "mqttdistrib/internal/amqpcom/pkg"
	amqpcommsgs "mqttdistrib/internal/amqpcom/pkg/shrd"
	"reflect"
)

type DistributionNetworkManager struct {
	Name              string
	MessageToRoute    map[string]string
	ManagementChannel *amqp091.Channel
	Network           IDistributionNetwork
	LoggingChannel    chan string
}

func MakeDistributionNetworkManager(
	connection *amqp091.Connection,
	name string) (*DistributionNetworkManager, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}
	network, err := MakeDistributionNetwork(channel)
	return &DistributionNetworkManager{
		Name:              name,
		MessageToRoute:    make(map[string]string),
		LoggingChannel:    make(chan string),
		ManagementChannel: channel,
		Network:           network,
	}, nil
}

func (manager *DistributionNetworkManager) Start(
	ctx *context.Context) (<-chan string, error) {
	err := Register[RegisterRequest, RegisterResponse](manager,
		func(request RegisterRequest) (RegisterResponse, error) {
			err := manager.Network.Register(manager.ManagementChannel, request.Route)
			if err != nil {
				return RegisterResponse{Success: false, Error: err.Error()}, nil
			}
			manager.MessageToRoute[request.MessageType] = request.Route
			manager.LoggingChannel <- "Register: " + request.MessageType + " @ " + request.Route
			return RegisterResponse{Success: true, Error: EmptyString}, nil
		}, ctx)
	if err != nil {
		return manager.LoggingChannel, err
	}
	err = Register[UnregisterRequest, UnregisterResponse](manager,
		func(request UnregisterRequest) (UnregisterResponse, error) {
			route, ok := manager.MessageToRoute[request.MessageType]
			if !ok {
				return UnregisterResponse{Success: false, Error: "route not found"}, nil
			}
			err := manager.Network.Unregister(manager.ManagementChannel, route)
			if err != nil {
				return UnregisterResponse{Success: false, Error: err.Error()}, nil
			}
			delete(manager.MessageToRoute, request.MessageType)
			manager.LoggingChannel <- "Unregister: " + request.MessageType + " @ " + route
			return UnregisterResponse{Success: true, Error: EmptyString}, nil
		}, ctx)
	if err != nil {
		return manager.LoggingChannel, err
	}
	err = Register[DistributorRequest, DistributorResponse](manager,
		func(request DistributorRequest) (DistributorResponse, error) {
			name, err := manager.Network.GetRootExchangeName()
			if err != nil {
				return DistributorResponse{Distributor: EmptyString, Error: err.Error()}, nil
			}
			manager.LoggingChannel <- "Distributor: " + name
			return DistributorResponse{Distributor: name, Error: EmptyString}, nil
		}, ctx)
	if err != nil {
		return manager.LoggingChannel, err
	}
	err = Register[ConsumerRequest, ConsumerResponse](manager,
		func(request ConsumerRequest) (ConsumerResponse, error) {
			route, ok := manager.MessageToRoute[request.MessageType]
			if !ok {
				return ConsumerResponse{Consumer: EmptyString, Error: "route not found"}, nil
			}
			name, err := manager.Network.GetConsumptionQueueName(route)
			if err != nil {
				return ConsumerResponse{Consumer: EmptyString, Error: err.Error()}, nil
			}
			manager.LoggingChannel <- "Consumer: " + name + " @ " + request.MessageType
			return ConsumerResponse{Consumer: name, Error: EmptyString}, nil
		}, ctx)
	if err != nil {
		return manager.LoggingChannel, err
	}
	err = Register[RouteRequest, RouteResponse](manager,
		func(request RouteRequest) (RouteResponse, error) {
			route, ok := manager.MessageToRoute[request.MessageType]
			if !ok {
				return RouteResponse{Route: EmptyString, Error: "route not found"}, nil
			}
			manager.LoggingChannel <- "Route: " + route + " @ " + request.MessageType
			return RouteResponse{Route: route, Error: EmptyString}, nil
		}, ctx)
	if err != nil {
		return manager.LoggingChannel, err
	}
	return manager.LoggingChannel, nil
}

func (manager *DistributionNetworkManager) Close() error {
	err := manager.Network.Close(manager.ManagementChannel)
	if err != nil {
		return err
	}
	err = manager.ManagementChannel.Close()
	if err != nil {
		return err
	}
	close(manager.LoggingChannel)
	manager.MessageToRoute = map[string]string{}
	return nil
}

func Register[TRequest amqpcommsgs.IMessage, TResponse amqpcommsgs.IMessage](
	manager *DistributionNetworkManager,
	action func(request TRequest) (TResponse, error),
	ctx *context.Context) error {
	builder := amqpcom.MakeIRequestHandlerBuilder[TResponse, TRequest](manager.ManagementChannel)
	builder.WithQueueName(manager.Name + RouteSeparator + reflect.TypeFor[TRequest]().String())
	builder.WithHandlerAction(action)
	err := builder.BuildAndRun(ctx)
	if err != nil {
		return err
	}
	return nil
}

type RegisterRequest struct {
	MessageType string `json:"message_type"`
	Route       string `json:"route"`
}
type UnregisterRequest struct {
	MessageType string `json:"message_type"`
}
type DistributorRequest struct{}
type ConsumerRequest struct {
	MessageType string `json:"message_type"`
}
type RouteRequest struct {
	MessageType string `json:"message_type"`
}
type RegisterResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}
type UnregisterResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}
type DistributorResponse struct {
	Distributor string `json:"distributor"`
	Error       string `json:"error"`
}
type ConsumerResponse struct {
	Consumer string `json:"consumer"`
	Error    string `json:"error"`
}
type RouteResponse struct {
	Route string `json:"route"`
	Error string `json:"error"`
}

func (r RegisterRequest) ToString() string     { return amqpcommsgs.ToString(r) }
func (r UnregisterRequest) ToString() string   { return amqpcommsgs.ToString(r) }
func (r DistributorRequest) ToString() string  { return amqpcommsgs.ToString(r) }
func (r ConsumerRequest) ToString() string     { return amqpcommsgs.ToString(r) }
func (r RouteRequest) ToString() string        { return amqpcommsgs.ToString(r) }
func (r RegisterResponse) ToString() string    { return amqpcommsgs.ToString(r) }
func (r UnregisterResponse) ToString() string  { return amqpcommsgs.ToString(r) }
func (r DistributorResponse) ToString() string { return amqpcommsgs.ToString(r) }
func (r ConsumerResponse) ToString() string    { return amqpcommsgs.ToString(r) }
func (r RouteResponse) ToString() string       { return amqpcommsgs.ToString(r) }
