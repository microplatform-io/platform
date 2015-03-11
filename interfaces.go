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
	Subscribe(topic string, handler ConsumerHandler) (Subscription, error)
}

type Subscription interface {
	Run() error
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
