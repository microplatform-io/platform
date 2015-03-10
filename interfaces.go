package platform

import (
	"io"
)

type Consumer interface {
	io.Closer
	AddHandler(ConsumerHandler)
	ListenAndServe() error
}

type ConsumerFactory interface {
	Create(topic string, handler ConsumerHandler) Consumer
}

type ConsumerHandler interface {
	HandleMessage([]byte) error
}

type ConsumerHandlerFunc func([]byte) error

func (handlerFunc ConsumerHandlerFunc) HandleMessage(p []byte) error {
	return handlerFunc(p)
}

type Publisher interface {
	io.Closer
	Publish(topic string, body []byte) error
}
