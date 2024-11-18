package amqpcom_int

import (
	"encoding/json"
	"github.com/rabbitmq/amqp091-go"
)

func FromData[TMessage any](
	delivery *amqp091.Delivery) (*TMessage, error) {
	msg := new(TMessage)
	err := json.Unmarshal(delivery.Body, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func ToData[TMessage any](
	message *TMessage) (*[]byte, error) {
	data, err := json.Marshal(message)
	return &data, err
}
