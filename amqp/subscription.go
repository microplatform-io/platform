package amqp

import (
	"github.com/microplatform-io/platform"
	"sync"
)

const MAX_WORKERS = 50

type subscription struct {
	topic        string
	handler      platform.ConsumerHandler
	closed       bool
	deliveries   chan DeliveryInterface
	totalWorkers int

	mu sync.Mutex
}

func (s *subscription) canHandle(msg DeliveryInterface) bool {
	if s.topic == "" {
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

	logger.Debugf("[newSubscription] creating '%s' subscription worker pool", topic)

	// TODO: Determine an ideal worker pool
	for i := 0; i < MAX_WORKERS; i++ {
		go s.runWorker()
	}

	return s
}
