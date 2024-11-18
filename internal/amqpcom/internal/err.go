package amqpcom_int

type AmqpCommunicationError struct{ Message string }

func (m *AmqpCommunicationError) Error() string { return m.Message }
