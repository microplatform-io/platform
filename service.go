package platform

import (
	"crypto/sha1"
	"fmt"
	"os"
)

var (
	rabbitUser   = os.Getenv("RABBITMQ_USER")
	rabbitPass   = os.Getenv("RABBITMQ_PASS")
	rabbitAddr   = os.Getenv("RABBITMQ_PORT_5672_TCP_ADDR")
	rabbitPort   = os.Getenv("RABBITMQ_PORT_5672_TCP_PORT")
	rabbitString = fmt.Sprintf("amqp://%s:%s@%s:%s/", rabbitUser, rabbitPass, rabbitAddr, rabbitPort)

	serviceToken = os.Getenv("SERVICE_TOKEN")
)

func GetDefaultPublisher() Publisher {
	conn, err := NewAmqpConnection(rabbitString)
	if err != nil {
		logger.Fatalf("Failed to connect to AMQP: %s\n", err)
	}

	producer, err := NewAmqpProducer(conn, "amq.topic")
	if err != nil {
		logger.Fatalf("Failed to connect to producer: %s\n", err)
	}

	return producer
}

func GetDefaultConsumerFactory(queue string) ConsumerFactory {
	conn, err := NewAmqpConnection(rabbitString)
	if err != nil {
		logger.Fatalf("Failed to connect to AMQP: %s\n", err)
	}

	return &AmqpConsumerFactory{
		conn:  conn,
		queue: queue,
	}
}

type Handler interface {
	HandleRoutedMessage(*RoutedMessage) (*RoutedMessage, error)
}

type HandlerFunc func(*RoutedMessage) (*RoutedMessage, error)

func (handlerFunc HandlerFunc) HandleRoutedMessage(cloudMessage *RoutedMessage) (*RoutedMessage, error) {
	return handlerFunc(cloudMessage)
}

type Service struct {
	publisher       Publisher
	consumerFactory ConsumerFactory

	consumers []Consumer
}

func (s *Service) AddHandler(method, resource interface{}, handler Handler) {
	s.AddTopicHandler(fmt.Sprintf("%d_%d", method, resource), handler)
}

func (s *Service) AddTopicHandler(topic string, handler Handler) {
	logger.Println("> adding topic handler", topic)

	s.consumers = append(s.consumers, s.consumerFactory.Create(topic, RoutedMessageHandlerFunc(func(request *RoutedMessage) error {
		logger.Printf("> handling %s request", topic)

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
	})))
}

func (s *Service) Run() {
	quit := make(chan struct{}, len(s.consumers))

	for i := range s.consumers {
		go func(i int) {
			logger.Printf("Serving consumer: %#v", s.consumers[i])
			logger.Printf("Consumer has stopped: %#v : %s", s.consumers[i], s.consumers[i].ListenAndServe())

			quit <- struct{}{}
		}(i)
	}

	logger.Println("Serving all topics")
	<-quit
	logger.Println("Consumers have stopped")
}

func NewService(publisher Publisher, consumerFactory ConsumerFactory) (*Service, error) {
	return &Service{
		publisher:       publisher,
		consumerFactory: consumerFactory,
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
