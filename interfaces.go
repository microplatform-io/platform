package platform

import (
	"io"
)

type Handler interface {
	Handle(responseSender ResponseSender, request *Request)
}

type HandlerFunc func(responseSender ResponseSender, request *Request)

func (handlerFunc HandlerFunc) Handle(responseSender ResponseSender, request *Request) {
	handlerFunc(responseSender, request)
}

type Subscriber interface {
	Run() error
	Subscribe(topic string, handler ConsumerHandler)
}

type Publisher interface {
	Publish(topic string, body []byte) error
}

type ResponseSender interface {
	Send(response *Request)
}

type Consumer interface {
	io.Closer
	AddHandler(ConsumerHandler)
	ListenAndServe() error
}

type ConsumerHandler interface {
	HandleMessage(body []byte) error
}

type ConsumerHandlerFunc func([]byte) error

func (handlerFunc ConsumerHandlerFunc) HandleMessage(p []byte) error {
	return handlerFunc(p)
}
