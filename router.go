package platform

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Router interface {
	Route(routedMessage *RoutedMessage, expires time.Duration) (*RoutedMessage, error)
}

type StandardRouter struct {
	publisher       Publisher
	consumerFactory ConsumerFactory

	topic string

	pendingRequests map[string]chan *RoutedMessage
	mu              sync.Mutex
}

func (sr *StandardRouter) Route(msg *RoutedMessage, timeout time.Duration) (*RoutedMessage, error) {
	msg.Id = String(CreateUUID())
	msg.ReplyTopic = String(sr.topic)

	logger.Printf("> routing routed message: %s", msg)

	payload, err := Marshal(msg)
	if err != nil {
		return nil, errors.New("MARSHAL ERROR")
	}

	responseChan := make(chan *RoutedMessage, 1)

	sr.mu.Lock()
	sr.pendingRequests[msg.GetId()] = responseChan
	sr.mu.Unlock()

	sr.publisher.Publish(fmt.Sprintf("%d_%d", msg.GetMethod(), msg.GetResource()), payload)

	var response *RoutedMessage

	select {
	case response = <-responseChan:
		// Good to proceed
	case <-time.After(timeout):
		err = errors.New("TIMEOUT")
	}

	sr.mu.Lock()
	delete(sr.pendingRequests, msg.GetId())
	sr.mu.Unlock()

	return response, err
}

func NewStandardRouter(publisher Publisher, consumerFactory ConsumerFactory) Router {
	logger.Printf("> creating a new standard router.")
	logger.Printf("> publisher: %#v", publisher)
	logger.Printf("> consumerFactory: %#v", consumerFactory)

	router := &StandardRouter{
		publisher:       publisher,
		consumerFactory: consumerFactory,
		topic:           "router_" + CreateUUID(),
		pendingRequests: map[string]chan *RoutedMessage{},
	}

	go func() {
		for i := 0; i <= 100; i++ {
			logger.Println("> creating a new consumer...")
			consumer := consumerFactory.Create(router.topic, RoutedMessageHandlerFunc(func(msg *RoutedMessage) error {
				logger.Printf("> receiving message for router: %s", msg)

				router.mu.Lock()
				if replyChan, exists := router.pendingRequests[msg.GetId()]; exists {
					replyChan <- msg
				}
				router.mu.Unlock()

				return nil
			}))

			logger.Println("> consumer created: %#v", consumer)
			consumer.ListenAndServe()

			time.Sleep(time.Duration(i%5) * time.Second)
		}

		panic("Final connection died. Respawning...")
	}()

	logger.Printf("> router has been created: %#v", router)

	return router
}
