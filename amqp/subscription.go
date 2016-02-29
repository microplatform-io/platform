package amqp

import (
	"sync/atomic"

	"github.com/microplatform-io/platform"
	"github.com/streadway/amqp"
)

const MAX_WORKERS = 50

type subscription struct {
	topic        string
	handler      platform.ConsumerHandler
	closed       bool
	deliveries   chan amqp.Delivery
	totalWorkers int32
}

func (s *subscription) canHandle(msg amqp.Delivery) bool {
	if s.topic == "" {
		return true
	}

	return s.topic == msg.RoutingKey
}

func (s *subscription) Close() error {
	if !s.closed {
		close(s.deliveries)
		s.closed = true
	}

	return nil
}

func (s *subscription) runWorker() {
	atomic.AddInt32(&s.totalWorkers, 1)

	for msg := range s.deliveries {
		s.handler.HandleMessage(msg.Body)
		msg.Ack(false)
	}

	atomic.AddInt32(&s.totalWorkers, -1)
}

func newSubscription(topic string, handler platform.ConsumerHandler) *subscription {
	s := &subscription{
		topic:        topic,
		handler:      handler,
		deliveries:   make(chan amqp.Delivery, MAX_WORKERS),
		totalWorkers: 0,
	}

	logger.Printf("[newSubscription] creating '%s' subscription worker pool", topic)

	// TODO: Determine an ideal worker pool
	for i := 0; i < MAX_WORKERS; i++ {
		go s.runWorker()
	}

	return s
}
