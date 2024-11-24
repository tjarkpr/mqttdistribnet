package mqttdistribnet_int

import (
	"github.com/rabbitmq/amqp091-go"
	"maps"
	"slices"
	"strings"
)

const RouteSeparator string = "."
const EmptyString string = ""
const InvalidNodeTypeErrorMessage = "invalid node type"
const RouteHash = "#"

type IDistributionNetworkNode interface {
	GetRoutingKey() string
	GetFullRoute() string
	GetParent() *ExchangeDistributionNetworkNode
	SetParent(*ExchangeDistributionNetworkNode)
	UnsetParent()
	FindByRoute(string) *IDistributionNetworkNode
	FindLastByRoute(string) (*IDistributionNetworkNode, string)
	Delete(*amqp091.Channel) ([]*IDistributionNetworkNode, error)
	Purge(*amqp091.Channel) error
}
type IExchangeDistributionNetworkNode interface {
	GetChildren() []*IDistributionNetworkNode
	Add(*amqp091.Channel, *IDistributionNetworkNode) error
	Remove(*amqp091.Channel, *IDistributionNetworkNode) error
}
type ExchangeDistributionNetworkNode struct {
	RoutingKey   string
	FullRoute    string
	Parent       *ExchangeDistributionNetworkNode
	Children     map[string]*IDistributionNetworkNode
	ExchangeName string
}
type QueueDistributionNetworkNode struct {
	RoutingKey string
	FullRoute  string
	Parent     *ExchangeDistributionNetworkNode
	QueueName  string
}

func MakeExchangeDistributionNetworkNode(
	channel *amqp091.Channel,
	routingKey string,
	exchangeName string) (*ExchangeDistributionNetworkNode, error) {
	err := channel.ExchangeDeclare(
		exchangeName,
		"topic",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return nil, err
	}
	return &ExchangeDistributionNetworkNode{
		RoutingKey:   routingKey,
		FullRoute:    routingKey,
		Parent:       nil,
		Children:     make(map[string]*IDistributionNetworkNode),
		ExchangeName: exchangeName,
	}, nil
}

func MakeQueueDistributionNetworkNode(
	channel *amqp091.Channel,
	routingKey string,
	queueName string) (*QueueDistributionNetworkNode, error) {
	queue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return nil, err
	}
	return &QueueDistributionNetworkNode{
		RoutingKey: routingKey,
		FullRoute:  routingKey,
		Parent:     nil,
		QueueName:  queue.Name,
	}, nil
}

func (pExchangeNode *ExchangeDistributionNetworkNode) GetRoutingKey() string {
	return pExchangeNode.RoutingKey
}
func (pExchangeNode *ExchangeDistributionNetworkNode) GetFullRoute() string {
	return pExchangeNode.FullRoute
}
func (pExchangeNode *ExchangeDistributionNetworkNode) GetParent() *ExchangeDistributionNetworkNode {
	return pExchangeNode.Parent
}
func (pExchangeNode *ExchangeDistributionNetworkNode) UnsetParent() {
	pExchangeNode.Parent = nil
}
func (pExchangeNode *ExchangeDistributionNetworkNode) SetParent(
	parent *ExchangeDistributionNetworkNode) {
	pExchangeNode.Parent = parent
	if parent.RoutingKey == EmptyString {
		pExchangeNode.FullRoute = pExchangeNode.RoutingKey
	} else {
		pExchangeNode.FullRoute = parent.RoutingKey + RouteSeparator + pExchangeNode.RoutingKey
	}
}
func (pExchangeNode *ExchangeDistributionNetworkNode) GetChildren() []*IDistributionNetworkNode {
	return slices.Collect(maps.Values(pExchangeNode.Children))
}
func (pExchangeNode *ExchangeDistributionNetworkNode) FindByRoute(
	route string) *IDistributionNetworkNode {
	if route == EmptyString {
		return nil
	}
	var routeSplits = strings.Split(route, RouteSeparator)
	if len(routeSplits) == 0 {
		return nil
	}
	child, ok := pExchangeNode.Children[routeSplits[0]]
	if !ok {
		return nil
	}
	var remainingRoute = strings.Join(routeSplits[1:], RouteSeparator)
	if remainingRoute == EmptyString {
		return child
	}
	return (*child).FindByRoute(remainingRoute)
}
func (pExchangeNode *ExchangeDistributionNetworkNode) FindLastByRoute(
	route string) (*IDistributionNetworkNode, string) {
	var node IDistributionNetworkNode = pExchangeNode
	if route == EmptyString {
		return &node, route
	}
	var routeSplits = strings.Split(route, RouteSeparator)
	if len(routeSplits) == 0 {
		return &node, route
	}
	child, ok := pExchangeNode.Children[routeSplits[0]]
	if !ok {
		return &node, route
	}
	var remainingRoute = strings.Join(routeSplits[1:], RouteSeparator)
	if remainingRoute == EmptyString {
		return child, remainingRoute
	}
	return (*child).FindLastByRoute(remainingRoute)
}
func (pExchangeNode *ExchangeDistributionNetworkNode) Add(
	channel *amqp091.Channel, pNode *IDistributionNetworkNode) error {
	var node = *pNode
	var route string
	if pExchangeNode.FullRoute == EmptyString {
		route = node.GetRoutingKey()
	} else {
		route = pExchangeNode.FullRoute + RouteSeparator + node.GetRoutingKey()
	}
	switch node.(type) {
	case *ExchangeDistributionNetworkNode:
		var pExNode = node.(*ExchangeDistributionNetworkNode)
		err := channel.ExchangeBind(
			pExNode.ExchangeName,
			route+RouteSeparator+RouteHash,
			pExchangeNode.ExchangeName,
			false,
			nil)
		if err != nil {
			return err
		}
	case *QueueDistributionNetworkNode:
		var pQNode = node.(*QueueDistributionNetworkNode)
		err := channel.QueueBind(
			pQNode.QueueName,
			route,
			pExchangeNode.ExchangeName,
			false,
			nil)
		if err != nil {
			return err
		}
	default:
		return &DistributionNetworkError{Message: InvalidNodeTypeErrorMessage}
	}
	node.SetParent(pExchangeNode)
	pExchangeNode.Children[node.GetRoutingKey()] = pNode
	return nil
}
func (pExchangeNode *ExchangeDistributionNetworkNode) Remove(
	channel *amqp091.Channel, pNode *IDistributionNetworkNode) error {
	var node = *pNode
	var route string
	if pExchangeNode.FullRoute == EmptyString {
		route = node.GetRoutingKey()
	} else {
		route = pExchangeNode.FullRoute + RouteSeparator + node.GetRoutingKey()
	}
	switch node.(type) {
	case *ExchangeDistributionNetworkNode:
		var pExNode = node.(*ExchangeDistributionNetworkNode)
		err := channel.ExchangeUnbind(
			pExNode.ExchangeName,
			route+RouteSeparator+RouteHash,
			pExchangeNode.ExchangeName,
			false,
			nil)
		if err != nil {
			return err
		}
	case *QueueDistributionNetworkNode:
		var pQNode = node.(*QueueDistributionNetworkNode)
		err := channel.QueueUnbind(
			pQNode.QueueName,
			route,
			pExchangeNode.ExchangeName,
			nil)
		if err != nil {
			return err
		}
	default:
		return &DistributionNetworkError{Message: InvalidNodeTypeErrorMessage}
	}
	node.UnsetParent()
	delete(pExchangeNode.Children, node.GetRoutingKey())
	if len(pExchangeNode.Children) == 0 {
		err := channel.ExchangeDelete(
			pExchangeNode.ExchangeName,
			false,
			false)
		if err != nil {
			return err
		}
	}
	return nil
}
func (pExchangeNode *ExchangeDistributionNetworkNode) Delete(
	channel *amqp091.Channel) ([]*IDistributionNetworkNode, error) {
	var affected []*IDistributionNetworkNode
	for _, node := range pExchangeNode.GetChildren() {
		tAffected, err := (*node).Delete(channel)
		affected = append(affected, tAffected...)
		if err != nil {
			return affected, err
		}
	}
	return affected, nil
}
func (pExchangeNode *ExchangeDistributionNetworkNode) Purge(
	channel *amqp091.Channel) error {
	for _, node := range pExchangeNode.GetChildren() {
		if err := (*node).Purge(channel); err != nil {
			return err
		}
	}
	return nil
}

func (pQueueNode *QueueDistributionNetworkNode) GetRoutingKey() string {
	return pQueueNode.RoutingKey
}
func (pQueueNode *QueueDistributionNetworkNode) GetFullRoute() string {
	return pQueueNode.FullRoute
}
func (pQueueNode *QueueDistributionNetworkNode) GetParent() *ExchangeDistributionNetworkNode {
	return pQueueNode.Parent
}
func (pQueueNode *QueueDistributionNetworkNode) UnsetParent() {
	pQueueNode.Parent = nil
}
func (pQueueNode *QueueDistributionNetworkNode) SetParent(
	parent *ExchangeDistributionNetworkNode) {
	pQueueNode.Parent = parent
	if parent.RoutingKey == EmptyString {
		pQueueNode.FullRoute = pQueueNode.RoutingKey
	} else {
		pQueueNode.FullRoute = parent.RoutingKey + RouteSeparator + pQueueNode.RoutingKey
	}
}
func (pQueueNode *QueueDistributionNetworkNode) FindByRoute(
	route string) *IDistributionNetworkNode {
	if route != EmptyString {
		return nil
	}
	var queueNode IDistributionNetworkNode = pQueueNode
	return &queueNode
}
func (pQueueNode *QueueDistributionNetworkNode) FindLastByRoute(
	route string) (*IDistributionNetworkNode, string) {
	var queueNode IDistributionNetworkNode = pQueueNode
	return &queueNode, route
}
func (pQueueNode *QueueDistributionNetworkNode) Delete(
	channel *amqp091.Channel) ([]*IDistributionNetworkNode, error) {
	var affected []*IDistributionNetworkNode
	var queueNode IDistributionNetworkNode = pQueueNode
	err := pQueueNode.Parent.Remove(channel, &queueNode)
	if err != nil {
		return affected, err
	}
	_, err = channel.QueueDelete(
		pQueueNode.QueueName,
		false,
		false,
		false)
	if err != nil {
		return affected, err
	}
	affected = append(affected, &queueNode)
	return affected, nil
}
func (pQueueNode *QueueDistributionNetworkNode) Purge(
	channel *amqp091.Channel) error {
	_, err := channel.QueuePurge(
		pQueueNode.QueueName,
		false)
	if err != nil {
		return err
	}
	return nil
}
