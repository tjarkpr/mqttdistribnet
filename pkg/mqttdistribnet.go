package mqttdistribnet

import (
	"context"
	"github.com/rabbitmq/amqp091-go"
	mqttdistribnetint "mqttdistrib/internal"
	"reflect"
)

type IDistributionNetworkManager interface {
	Start(ctx *context.Context) (<-chan string, error)
	Close() error
}

func MakeIDistributionNetworkManager(
	connection *amqp091.Connection,
	name string) (IDistributionNetworkManager, error) {
	return mqttdistribnetint.MakeDistributionNetworkManager(connection, name)
}

type IRemoteDistributionNetworkManager interface {
	Register(reflect.Type, string) error
	Unregister(reflect.Type) error
	Distributor() (string, error)
	Consumer(reflect.Type) (string, error)
	Route(reflect.Type) (string, error)
	Channel() *amqp091.Channel
	Close() error
}

func MakeIRemoteDistributionNetworkManager(
	connection *amqp091.Connection,
	name string) (IRemoteDistributionNetworkManager, error) {
	return mqttdistribnetint.MakeRemoteDistributionNetworkManager(connection, name)
}