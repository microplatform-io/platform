package platform

import (
	"net/url"
	"sync"
	"time"
)

type Router interface {
	Route(request *Request) (chan *Request, chan interface{})
	RouteWithTimeout(request *Request, timeout time.Duration) (chan *Request, chan interface{})
}

type StandardRouter struct {
	publisher  Publisher
	subscriber Subscriber

	topic string

	pendingResponses map[string]chan *Request
	mu               sync.Mutex
}

func (sr *StandardRouter) Route(request *Request) (chan *Request, chan interface{}) {
	if request.Uuid == nil {
		request.Uuid = String("request-" + CreateUUID())
	}

	request.Routing.RouteFrom = append(request.Routing.RouteFrom, &Route{
		Uri: String(sr.topic),
	})

	logger.Printf("[StandardRouter.Route] %s - routing request: %s", request.GetUuid(), request)

	payload, err := Marshal(request)
	if err != nil {
		logger.Printf("[StandardRouter.Route] %s - failed to marshal request payload: %s", request.GetUuid(), err)
	}

	internalResponses := make(chan *Request, 1)
	responses := make(chan *Request, 1)
	streamTimeout := make(chan interface{})

	sr.mu.Lock()
	sr.pendingResponses[request.GetUuid()] = internalResponses
	sr.mu.Unlock()

	go func() {
		for {
			select {
			case response := <-internalResponses:
				responses <- response

			case <-time.After(time.Second):
				streamTimeout <- nil
			}
		}
	}()

	parsedUri, err := url.Parse(request.Routing.RouteTo[0].GetUri())
	if err != nil {
		logger.Fatalf("[StandardRouter.Route] %s - failed to parse route to uri: %s", request.GetUuid(), err)
	}

	if err := sr.publisher.Publish(parsedUri.Scheme+"-"+parsedUri.Path, payload); err != nil {
		logger.Printf("[StandardRouter.Route] %s - failed to publish request to microservices: %s", request.GetUuid(), err)
	}

	return responses, streamTimeout
}

func (sr *StandardRouter) RouteWithTimeout(request *Request, timeout time.Duration) (chan *Request, chan interface{}) {
	responses, streamTimeout := sr.Route(request)

	time.AfterFunc(timeout, func() {
		streamTimeout <- nil
	})

	return responses, streamTimeout
}

func NewStandardRouter(publisher Publisher, subscriber Subscriber) Router {
	logger.Printf("[NewStandardRouter] creating a new standard router.")
	logger.Printf("[NewStandardRouter] using publisher: %#v", publisher)
	logger.Printf("[NewStandardRouter] using subscriber: %#v", subscriber)

	router := &StandardRouter{
		publisher:        publisher,
		subscriber:       subscriber,
		topic:            "router-" + CreateUUID(),
		pendingResponses: map[string]chan *Request{},
	}

	subscriber.Subscribe(router.topic, ConsumerHandlerFunc(func(body []byte) error {
		logger.Println("[StandardRouter.Subscriber] receiving message for router")

		response := &Request{}
		if err := Unmarshal(body, response); err != nil {
			logger.Printf("[StandardRouter.Subscriber] failed to unmarshal response for router: %s", err)

			return err
		}

		logger.Printf("[StandardRouter.Subscriber] received response for router: %s", response)

		router.mu.Lock()
		if responses, exists := router.pendingResponses[response.GetUuid()]; exists {
			select {
			case responses <- response:
				logger.Printf("[StandardRouter.Subscriber] reply chan was available")
			default:
				logger.Printf("[StandardRouter.Subscriber] reply chan was not available")
			}

			if response.GetCompleted() {
				logger.Printf("[StandardRouter.Subscriber] this was the last response, closing and deleting the responses chan")

				delete(router.pendingResponses, response.GetUuid())
				// close(responses)
			}
		}
		router.mu.Unlock()

		logger.Printf("[StandardRouter.Subscriber] completed routing response: %s", response)

		return nil
	}))

	go func() {
		for i := 0; i <= 100; i++ {
			logger.Println("[StandardRouter.Subscriber] running subscriber...")
			if err := subscriber.Run(); err != nil {
				logger.Printf("[StandardRouter.Subscriber] subscriber has exited: %s", err)
			}

			time.Sleep(time.Duration(i%5) * time.Second)
		}

		panic("Final connection died. Respawning...")
	}()

	logger.Printf("[NewStandardRouter] router has been created: %#v", router)

	return router
}
