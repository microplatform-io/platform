package platform

import (
	"crypto/sha1"
	"fmt"
	"github.com/golang/protobuf/proto"
	"os"
)

var (
	logger       = GetLogger("platform")
	serviceToken = os.Getenv("SERVICE_TOKEN")
)

type Service struct {
	publisher  Publisher
	subscriber Subscriber
}

func (s *Service) AddHandler(method, resource interface{}, handler Handler, concurrency int) {
	s.AddTopicHandler(fmt.Sprintf("%d_%d", method, resource), handler, concurrency)
}

func (s *Service) AddListener(topic string, handler ConsumerHandler, concurrency int) {
	s.subscriber.Subscribe(topic, handler, concurrency)
}

func (s *Service) AddTopicHandler(topic string, handler Handler, concurrency int) {
	logger.Println("> adding topic handler", topic)

	s.subscriber.Subscribe(topic, ConsumerHandlerFunc(func(p []byte) error {
		logger.Printf("> handling %s request", topic)

		request := &RoutedMessage{}
		if err := proto.Unmarshal(p, request); err != nil {
			logger.Println("> failed to decode routed message")

			return nil
		}

		response, err := handler.HandleRoutedMessage(request)
		if err != nil {
			logger.Printf("> failed to handle %s request: %s", topic, err)
			return err
		}

		response.Id = request.Id

		payload, err := Marshal(response)
		if err != nil {
			return nil
		}

		return s.publisher.Publish(request.GetReplyTopic(), payload)
	}), concurrency)
}

func (s *Service) Run() {
	s.subscriber.Run()

	logger.Println("Subscriptions have stopped")
}

func NewService(publisher Publisher, subscriber Subscriber) (*Service, error) {
	return &Service{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func CreateServiceRequestToken(requestPayload []byte) string {
	token := append(requestPayload, ParseUUID(serviceToken)...)
	shaToken := sha1.Sum(token)
	return string(shaToken[:])
}

func IsServiceRequestToken(token string, requestPayload []byte) bool {
	createdToken := CreateServiceRequestToken(requestPayload)
	if token == createdToken {
		return true
	}

	return false
}
