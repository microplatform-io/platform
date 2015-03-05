package platform

import (
	"io"
)

type Consumer interface {
	io.Closer
	AddHandler(RoutedMessageHandler)
	ListenAndServe() error
}

type ConsumerFactory interface {
	Create(topic string, handler RoutedMessageHandler) Consumer
}

type RoutedMessageHandler interface {
	HandleMessage(*RoutedMessage) error
}

type RoutedMessageHandlerFunc func(*RoutedMessage) error

func (handlerFunc RoutedMessageHandlerFunc) HandleMessage(cloudMessage *RoutedMessage) error {
	return handlerFunc(cloudMessage)
}

type Publisher interface {
	io.Closer
	Publish(topic string, body []byte) error
}
