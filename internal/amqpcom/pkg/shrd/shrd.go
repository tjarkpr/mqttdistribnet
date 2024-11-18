package amqpcom_shrd

type IRequest interface {
	ToString() string
}

type IResponse interface {
	ToString() string
}