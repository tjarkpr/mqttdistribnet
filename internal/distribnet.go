package mqttdistribnet_int

import (
	"errors"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"strings"
)

const RootDeletedErrorMessage = "root deleted"

type IDistributionNetwork interface {
	Register(channel *amqp091.Channel, routingKey string) error
	Unregister(channel *amqp091.Channel, routingKey string) error
	Close(channel *amqp091.Channel) error
	GetRootExchangeName() (string, error)
	GetConsumptionQueueName(routingKey string) (string, error)
}
type DistributionNetwork struct {
	Queues       map[string]*QueueDistributionNetworkNode
	RootExchange *ExchangeDistributionNetworkNode
}

func MakeDistributionNetwork(channel *amqp091.Channel) (IDistributionNetwork, error) {
	node, err := MakeExchangeDistributionNetworkNode(channel, "", uuid.New().String())
	if err != nil {
		return nil, err
	}
	return &DistributionNetwork{
		Queues:       make(map[string]*QueueDistributionNetworkNode),
		RootExchange: node,
	}, nil
}

func (net *DistributionNetwork) Register(channel *amqp091.Channel, route string) error {
	route = CleanRoute(route)
	if net.RootExchange == nil {
		return errors.New(RootDeletedErrorMessage)
	}
	if route == EmptyString {
		return errors.New("route is empty")
	}
	if _, ok := net.Queues[route]; ok {
		return nil
	}
	pLastNode, remainingRoute := net.RootExchange.FindLastByRoute(route)
	if pLastNode == nil {
		return errors.New("could not register route")
	}
	var lastNode = *pLastNode
	if remainingRoute == EmptyString {
		switch lastNode.(type) {
		case *ExchangeDistributionNetworkNode:
			return errors.New("exchange exists on specified route, please register with '.*' to receive all messages")
		case *QueueDistributionNetworkNode:
			var qNode = lastNode.(*QueueDistributionNetworkNode)
			net.Queues[route] = qNode
			return nil
		default:
			return errors.New("node type unknown")
		}
	}
	switch lastNode.(type) {
	case *ExchangeDistributionNetworkNode:
		var lExNode = lastNode.(*ExchangeDistributionNetworkNode)
		var routeSplits = strings.Split(remainingRoute, RouteSeparator)
		var routeSplitsLastIndex = len(routeSplits) - 1
		for i, routeSplit := range routeSplits {
			if i == routeSplitsLastIndex {
				queueNode, err := MakeQueueDistributionNetworkNode(channel, routeSplit, uuid.New().String())
				if err != nil {
					return err
				}
				var nDistNode IDistributionNetworkNode = queueNode
				err = lExNode.Add(channel, &nDistNode)
				if err != nil {
					return err
				}
				net.Queues[route] = queueNode
			} else {
				nExNode, err := MakeExchangeDistributionNetworkNode(channel, routeSplit, uuid.New().String())
				if err != nil {
					return err
				}
				var nDistNode IDistributionNetworkNode = nExNode
				err = lExNode.Add(channel, &nDistNode)
				if err != nil {
					return err
				}
				lExNode = nExNode
			}
		}
	case *QueueDistributionNetworkNode:
		return errors.New("route ends with a consumption remaining with '" + remainingRoute + "', please unregister the consumption before creating a new path")
	default:
		return errors.New("node type unknown")
	}
	return nil
}
func (net *DistributionNetwork) Unregister(channel *amqp091.Channel, route string) error {
	route = CleanRoute(route)
	if net.RootExchange == nil {
		return errors.New(RootDeletedErrorMessage)
	}
	queue, ok := net.Queues[route]
	if !ok {
		return errors.New("route " + route + " not found")
	}
	nodes, err := queue.Delete(channel)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		delete(net.Queues, (*node).GetFullRoute())
	}
	return nil
}
func (net *DistributionNetwork) Close(channel *amqp091.Channel) error {
	if net.RootExchange == nil {
		return nil
	}
	nodes, err := net.RootExchange.Delete(channel)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		delete(net.Queues, (*node).GetFullRoute())
	}
	net.RootExchange = nil
	return nil
}
func (net *DistributionNetwork) GetRootExchangeName() (string, error) {
	if net.RootExchange == nil {
		return EmptyString, errors.New(RootDeletedErrorMessage)
	}
	return net.RootExchange.ExchangeName, nil
}
func (net *DistributionNetwork) GetConsumptionQueueName(route string) (string, error) {
	route = CleanRoute(route)
	if net.RootExchange == nil {
		return EmptyString, errors.New(RootDeletedErrorMessage)
	}
	if queue, ok := net.Queues[route]; ok {
		return queue.QueueName, nil
	}
	return EmptyString, nil
}

func CleanRoute(route string) string {
	var routeSplits = strings.Split(route, RouteSeparator)
	var r []string
	for _, str := range routeSplits {
		if str != EmptyString {
			r = append(r, str)
		}
	}
	return strings.Join(r, RouteSeparator)
}
