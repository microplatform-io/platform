package platform

import (
	"net/url"
	"sync"
	"time"
)

type Router interface {
	Route(request *Request) (chan *Request, chan interface{})
	RouteWithTimeout(request *Request, timeout time.Duration) (chan *Request, chan interface{})
	SetHeartbeatTimeout(heartbeatTimeout time.Duration)
}

type StandardRouter struct {
	publisher        Publisher
	subscriber       Subscriber
	heartbeatTimeout time.Duration

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
				// Internal requests shouldn't have to deal with heartbeats from other services
				if IsInternalRequest(request) && response.Routing.RouteTo[0].GetUri() == "resource:///heartbeat" {
					continue
				}

				responses <- response

				if response.GetCompleted() {
					return
				}

			case <-time.After(sr.heartbeatTimeout):
				streamTimeout <- nil

				return
			}
		}
	}()

	parsedUri, err := url.Parse(request.Routing.RouteTo[0].GetUri())
	if err != nil {
		logger.Fatalf("[StandardRouter.Route] %s - Failed to parse route to uri: %s", request.GetUuid(), err)
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

func (sr *StandardRouter) SetHeartbeatTimeout(heartbeatTimeout time.Duration) {
	sr.heartbeatTimeout = heartbeatTimeout
}

func (sr *StandardRouter) subscribe() {
	logger.Printf("[NewStandardRouter] creating a new standard router.")
	logger.Printf("[NewStandardRouter] using publisher: %#v", sr.publisher)
	logger.Printf("[NewStandardRouter] using subscriber: %#v", sr.subscriber)

	consumedWorkCountMutex = &sync.Mutex{}

	sr.subscriber.Subscribe(sr.topic, ConsumerHandlerFunc(func(body []byte) error {
		logger.Println("[StandardRouter.Subscriber] receiving message for router")

		response := &Request{}
		if err := Unmarshal(body, response); err != nil {
			logger.Printf("[StandardRouter.Subscriber] failed to unmarshal response for router: %s", err)

			return err
		}

		logger.Printf("[StandardRouter.Subscriber] received response for router: %s", response)

		sr.mu.Lock()
		if responses, exists := sr.pendingResponses[response.GetUuid()]; exists {
			select {
			case responses <- response:
				logger.Printf("[StandardRouter.Subscriber] reply chan was available")

			case <-time.After(250 * time.Millisecond):
				logger.Printf("[StandardRouter.Subscriber] reply chan was not available")
			}

			if response.GetCompleted() {
				logger.Printf("[StandardRouter.Subscriber] this was the last response, closing and deleting the responses chan")

				delete(sr.pendingResponses, response.GetUuid())
				// close(responses)
			}
		}
		sr.mu.Unlock()

		logger.Printf("[StandardRouter.Subscriber] completed routing response: %s", response)

		return nil
	}))

	go sr.subscriber.Run()

	logger.Printf("[NewStandardRouter] router has been created: %#v", sr)
}

func NewStandardRouter(publisher Publisher, subscriber Subscriber) Router {
	router := &StandardRouter{
		publisher:        publisher,
		subscriber:       subscriber,
		heartbeatTimeout: time.Second,
		topic:            "router-" + CreateUUID(),
		pendingResponses: map[string]chan *Request{},
	}

	router.subscribe()

	return router
}

func NewStandardRouterWithTopic(publisher Publisher, subscriber Subscriber, topic string) Router {
	router := &StandardRouter{
		publisher:        publisher,
		subscriber:       subscriber,
		heartbeatTimeout: time.Second,
		topic:            topic,
		pendingResponses: map[string]chan *Request{},
	}

	router.subscribe()

	return router
}
