package mqttdistribnet_int

import (
	"github.com/rabbitmq/amqp091-go"
	"strings"
)

type DistributionNetworkError struct{ Message string }

func (m *DistributionNetworkError) Error() string { return m.Message }

type DistributionNetworkManager struct {
	Connection        *amqp091.Connection
	ManagementChannel *amqp091.Channel
}

type RoutingKey string
type INetworkNode interface {
	GetChild(key RoutingKey) (*INetworkNode, error)
	AddQueue(channel *amqp091.Channel, key RoutingKey, queue *QueueNode) (*INetworkNode, error)
	Delete(channel *amqp091.Channel) error
	Purge(channel *amqp091.Channel) error
	Equals(node *INetworkNode) bool
}

func MakeExchangeNode(
	channel *amqp091.Channel,
	routingKey RoutingKey) (*ExchangeNode, error) {
	err := channel.ExchangeDeclare(
		string(routingKey),
		"topic",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return nil, err
	}
	return &ExchangeNode{
		RoutingKey:   routingKey,
		ExchangeName: string(routingKey),
		Children:     make(map[RoutingKey]*INetworkNode),
	}, nil
}

type ExchangeNode struct {
	RoutingKey   RoutingKey
	ExchangeName string
	Children     map[RoutingKey]*INetworkNode
}

func (e *ExchangeNode) GetChild(key RoutingKey) (*INetworkNode, error) {
	child, ok := e.Children[key]
	if !ok {
		return nil, &DistributionNetworkError{Message: "Child not found"}
	}
	return child, nil
}
func (e *ExchangeNode) AddQueue(channel *amqp091.Channel, key RoutingKey, queue *QueueNode) (*INetworkNode, error) {
	if key == "" {
		return nil, &DistributionNetworkError{Message: "Routing key cannot be empty"}
	}
	routingSplit := strings.Split(string(key), ".")
	remainingRoutingKey := GetRemainingRoutingKey(routingSplit)
	child, err := e.GetChild(RoutingKey(routingSplit[0]))
	if err != nil {
		if remainingRoutingKey == "" {
			if routingSplit[0] != string(queue.RoutingKey) {
				return nil, &DistributionNetworkError{Message: "Routing key does not match queue routing key"}
			}
			err = channel.QueueBind(queue.QueueName, routingSplit[0], e.ExchangeName, false, nil)
			if err != nil {
				return nil, err
			}
			var newNode INetworkNode = queue
			e.Children[RoutingKey(routingSplit[0])] = &newNode
			var castedResult INetworkNode = e
			return &castedResult, nil
		}
		exNode, err := MakeExchangeNode(channel, RoutingKey(routingSplit[0]))
		if err != nil {
			return nil, err
		}
		err = channel.ExchangeBind(exNode.ExchangeName, string(exNode.RoutingKey), e.ExchangeName, false, nil)
		if err != nil {
			return nil, err
		}
		var newNode *INetworkNode
		newNode, err = exNode.AddQueue(channel, RoutingKey(remainingRoutingKey), queue)
		if err != nil {
			return nil, err
		}
		e.Children[RoutingKey(routingSplit[0])] = newNode
		var castedResult INetworkNode = e
		return &castedResult, nil
	}
	var castedQueue INetworkNode = queue
	if remainingRoutingKey == "" {
		if (*child).Equals(&castedQueue) {
			var castedResult INetworkNode = e
			return &castedResult, nil
		}
		err = (*child).Delete(channel)
		if err != nil {
			return nil, err
		}
		switch (*child).(type) {
		case ExchangeNode:
			exNode := (*child).(ExchangeNode)
			err = channel.ExchangeUnbind(exNode.ExchangeName, string(key), e.ExchangeName, false, nil)
		case QueueNode:
			qNode := (*child).(QueueNode)
			err = channel.QueueUnbind(qNode.QueueName, string(key), e.ExchangeName, nil)
		}
		if err != nil {
			return nil, err
		}
		err = channel.QueueBind(queue.QueueName, string(key), e.ExchangeName, false, nil)
		if err != nil {
			return nil, err
		}
		e.Children[key] = &castedQueue
		var castedResult INetworkNode = e
		return &castedResult, nil
	}
	var newNode *INetworkNode
	newNode, err = (*child).AddQueue(channel, RoutingKey(remainingRoutingKey), queue)
	if err != nil {
		return nil, err
	}
	return newNode, nil
}
func GetRemainingRoutingKey(split []string) string {
	if len(split) <= 1 {
		return ""
	}
	return strings.Join(split[1:], ".")
}
func (e *ExchangeNode) Delete(channel *amqp091.Channel) error {
	deletedChildren := make([]RoutingKey, len(e.Children))
	var err error
	for key, child := range e.Children {
		switch (*child).(type) {
		case ExchangeNode:
			exNode := (*child).(ExchangeNode)
			err = channel.ExchangeUnbind(exNode.ExchangeName, string(key), e.ExchangeName, false, nil)
		case QueueNode:
			qNode := (*child).(QueueNode)
			err = channel.QueueUnbind(qNode.QueueName, string(key), e.ExchangeName, nil)
		}
		if err != nil {
			return err
		}
		err = (*child).Delete(channel)
		if err != nil {
			return err
		}
		deletedChildren = append(deletedChildren, key)
	}
	for _, key := range deletedChildren {
		delete(e.Children, key)
	}
	return channel.ExchangeDelete(e.ExchangeName, false, false)
}
func (e *ExchangeNode) Purge(channel *amqp091.Channel) error {
	var err error
	for _, child := range e.Children {
		err = (*child).Purge(channel)
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *ExchangeNode) Equals(node *INetworkNode) bool {
	if node == nil {
		return false
	}
	switch (*node).(type) {
	case ExchangeNode:
		eNode := (*node).(ExchangeNode)
		if e.ExchangeName != eNode.ExchangeName ||
			e.RoutingKey != eNode.RoutingKey ||
			len(e.Children) != len(eNode.Children) {
			return false
		}
		for key, child := range e.Children {
			if !(*child).Equals(eNode.Children[key]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func MakeQueueNode(
	channel *amqp091.Channel,
	exchangeName string,
	routingKey RoutingKey) (*QueueNode, error) {
	queue, err := channel.QueueDeclare(
		"",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return nil, err
	}
	err = channel.QueueBind(
		queue.Name,
		string(routingKey),
		exchangeName,
		false,
		nil)
	if err != nil {
		return nil, err
	}
	return &QueueNode{
		RoutingKey: routingKey,
		QueueName:  queue.Name,
	}, nil
}

type QueueNode struct {
	RoutingKey RoutingKey
	QueueName  string
}

func (q *QueueNode) GetChild(_ RoutingKey) (*INetworkNode, error) {
	return nil, &DistributionNetworkError{Message: "QueueNode has no children"}
}
func (q *QueueNode) AddQueue(channel *amqp091.Channel, key RoutingKey, queue *QueueNode) (*INetworkNode, error) {
	if key == "" {
		return nil, &DistributionNetworkError{Message: "Routing key cannot be empty"}
	}
	var exchangeNode INetworkNode
	exchangeNode, err := MakeExchangeNode(channel, q.RoutingKey)
	if err != nil {
		return nil, err
	}
	return exchangeNode.AddQueue(channel, key, queue)
}
func (q *QueueNode) Delete(channel *amqp091.Channel) error {
	_, err := channel.QueueDelete(q.QueueName, false, false, false)
	return err
}
func (q *QueueNode) Purge(channel *amqp091.Channel) error {
	_, err := channel.QueuePurge(q.QueueName, false)
	return err
}
func (q *QueueNode) Equals(node *INetworkNode) bool {
	if node == nil {
		return false
	}
	switch (*node).(type) {
	case QueueNode:
		qNode := (*node).(QueueNode)
		return q.QueueName == qNode.QueueName && q.RoutingKey == qNode.RoutingKey
	default:
		return false
	}
}
