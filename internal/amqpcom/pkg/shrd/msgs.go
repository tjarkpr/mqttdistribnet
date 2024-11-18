package amqpcom_msgs

import (
	"encoding/json"
	"log"
)

type IMessage interface {
	ToString() string
}

func ToString[TMessage IMessage](msg TMessage) string {
	str, err := json.Marshal(msg)
	if err != nil {
		log.Panicf("Could not marshal message of type %T", &msg)
	}
	return string(str)
}
