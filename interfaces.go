package platform

import (
	"io"
)

type Handler interface {
	HandleRoutedMessage(*RoutedMessage) (*RoutedMessage, error)
}

type HandlerFunc func(*RoutedMessage) (*RoutedMessage, error)

func (handlerFunc HandlerFunc) HandleRoutedMessage(cloudMessage *RoutedMessage) (*RoutedMessage, error) {
	return handlerFunc(cloudMessage)
}

type Subscriber interface {
	Run() error
	Subscribe(topic string, handler ConsumerHandler)
}

type Publisher interface {
	Publish(topic string, body []byte) error
}

type Consumer interface {
	io.Closer
	AddHandler(ConsumerHandler)
	ListenAndServe() error
}

type ConsumerHandler interface {
	HandleMessage([]byte) error
}

type ConsumerHandlerFunc func([]byte) error

func (handlerFunc ConsumerHandlerFunc) HandleMessage(p []byte) error {
	return handlerFunc(p)
}
