package platform

import (
	"code.google.com/p/goprotobuf/proto"
	"crypto/sha1"
	"fmt"
	"os"
)

var (
	logger       = GetLogger("platform")
	serviceToken = os.Getenv("SERVICE_TOKEN")
)

type Service struct {
	publisher  Publisher
	subscriber Subscriber

	subscriptions []Subscription
}

func (s *Service) AddHandler(method, resource interface{}, handler Handler) {
	s.AddTopicHandler(fmt.Sprintf("%d_%d", method, resource), handler)
}

func (s *Service) AddListener(topic string, handler ConsumerHandler) {
	subscription, err := s.subscriber.Subscribe(topic, handler)
	if err != nil {
		logger.Fatalf("Failed to create to subscriber: %s\n", err)
	}

	s.subscriptions = append(s.subscriptions, subscription)
}

func (s *Service) AddTopicHandler(topic string, handler Handler) {
	logger.Println("> adding topic handler", topic)

	subscription, err := s.subscriber.Subscribe(topic, ConsumerHandlerFunc(func(p []byte) error {
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
	}))
	if err != nil {
		logger.Fatalf("> failed to create a handler: %s", err)
	}

	s.subscriptions = append(s.subscriptions, subscription)
}

func (s *Service) Run() {
	quit := make(chan struct{}, len(s.subscriptions))

	for i := range s.subscriptions {
		go func(i int) {
			logger.Printf("Running subscription: %#v", s.subscriptions[i])
			logger.Printf("Subscription has stopped: %#v : %s", s.subscriptions[i], s.subscriptions[i].Run())

			quit <- struct{}{}
		}(i)
	}

	logger.Println("Serving all topics")
	<-quit
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
