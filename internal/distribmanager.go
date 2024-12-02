package mqttdistribnet_int

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	amqpcom "github.com/tjarkpr/mqttdistribnet/internal/amqpcom/pkg"
	amqpcommsgs "github.com/tjarkpr/mqttdistribnet/internal/amqpcom/pkg/shrd"
	"reflect"
)

type DistributionNetworkManager struct {
	Name              string
	MessageToRoute    map[string][]string
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
		MessageToRoute:    make(map[string][]string),
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
			if _, ok := manager.MessageToRoute[request.MessageType]; !ok {
				manager.MessageToRoute[request.MessageType] = make([]string, 0)
			}
			manager.MessageToRoute[request.MessageType] = append(manager.MessageToRoute[request.MessageType], request.Route)
			manager.LoggingChannel <- "Register: " + request.MessageType + " @ " + request.Route
			return RegisterResponse{Success: true, Error: EmptyString}, nil
		}, ctx)
	if err != nil {
		return manager.LoggingChannel, err
	}
	err = Register[UnregisterRequest, UnregisterResponse](manager,
		func(request UnregisterRequest) (UnregisterResponse, error) {
			routes, ok := manager.MessageToRoute[request.MessageType]
			if !ok {
				return UnregisterResponse{Success: false, Error: "routes not found"}, nil
			}
			for i, route := range routes {
				err := manager.Network.Unregister(manager.ManagementChannel, route)
				if err != nil {
					return UnregisterResponse{Success: false, Error: err.Error()}, nil
				}
				manager.MessageToRoute[request.MessageType] = append(
					manager.MessageToRoute[request.MessageType][:i],
					manager.MessageToRoute[request.MessageType][i+1:]...)
				manager.LoggingChannel <- "Unregister: " + request.MessageType + " @ " + route
			}
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
			routes, ok := manager.MessageToRoute[request.MessageType]
			if !ok {
				return ConsumerResponse{Consumer: make(map[string]string), Error: "route not found"}, nil
			}
			results := make(map[string]string)
			for _, route := range routes {
				name, err := manager.Network.GetConsumptionQueueName(route)
				if err != nil {
					return ConsumerResponse{Consumer: results, Error: err.Error()}, nil
				}
				manager.LoggingChannel <- "Consumer: " + name + " @ " + request.MessageType
				results[route] = name
			}
			return ConsumerResponse{Consumer: results, Error: EmptyString}, nil
		}, ctx)
	if err != nil {
		return manager.LoggingChannel, err
	}
	err = Register[RouteRequest, RouteResponse](manager,
		func(request RouteRequest) (RouteResponse, error) {
			routes, ok := manager.MessageToRoute[request.MessageType]
			if !ok {
				return RouteResponse{Routes: []string{EmptyString}, Error: "route not found"}, nil
			}
			for _, route := range routes {
				manager.LoggingChannel <- "Routes: " + route + " @ " + request.MessageType
			}
			return RouteResponse{Routes: routes, Error: EmptyString}, nil
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
	manager.MessageToRoute = map[string][]string{}
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
	Consumer map[string]string `json:"consumer"`
	Error    string            `json:"error"`
}
type RouteResponse struct {
	Routes []string `json:"route"`
	Error  string   `json:"error"`
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
