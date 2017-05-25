package amqp

import (
	"sync"

	"strconv"

	"github.com/microplatform-io/platform"
)

var MAX_WORKERS = platform.Getenv("MAX_WORKERS", "50")

type subscription struct {
	topic        string
	handler      platform.ConsumerHandler
	closed       bool
	deliveries   chan DeliveryInterface
	totalWorkers int

	mu sync.Mutex
}

func (s *subscription) canHandle(msg DeliveryInterface) bool {
	if s.topic == "" || s.topic == "#" {
		return true
	}

	return s.topic == msg.GetRoutingKey()
}

func (s *subscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		close(s.deliveries)
		s.closed = true
	}

	return nil
}

func (s *subscription) runWorker() {
	s.mu.Lock()
	s.totalWorkers += 1
	s.mu.Unlock()

	for msg := range s.deliveries {
		s.handler.HandleMessage(msg.GetBody())
	}

	s.mu.Lock()
	s.totalWorkers -= 1
	s.mu.Unlock()
}

func newSubscription(topic string, handler platform.ConsumerHandler) *subscription {
	s := &subscription{
		topic:        topic,
		handler:      handler,
		deliveries:   make(chan DeliveryInterface),
		totalWorkers: 0,
	}

	maxWorkers, err := strconv.Atoi(MAX_WORKERS)
	if err != nil {
		maxWorkers = 50
	}

	// TODO: Determine an ideal worker pool
	for i := 0; i < maxWorkers; i++ {
		go s.runWorker()
	}

	return s
}
