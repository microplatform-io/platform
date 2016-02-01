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

	requestUuid := request.GetUuid()
	requestUri := ""
	if len(request.GetRouting().GetRouteTo()) > 0 {
		requestUri = request.GetRouting().GetRouteTo()[0].GetUri()
	}

	logger.Printf("[StandardRouter.Route] %s - %s - routing request: %s", requestUuid, requestUri, request)

	payload, err := Marshal(request)
	if err != nil {
		logger.Printf("[StandardRouter.Route] %s - %s - failed to marshal request payload: %s", requestUuid, requestUri, err)
	}

	internalResponses := make(chan *Request, 1)
	responses := make(chan *Request, 1)
	streamTimeout := make(chan interface{})

	sr.mu.Lock()
	sr.pendingResponses[requestUuid] = internalResponses
	sr.mu.Unlock()

	go func() {
		timer := time.NewTimer(sr.heartbeatTimeout)
		defer timer.Stop()

		for {
			select {
			case response := <-internalResponses:
				responseUri := ""
				if len(response.GetRouting().GetRouteTo()) > 0 {
					responseUri = response.GetRouting().GetRouteTo()[0].GetUri()
				}

				logger.Printf("[StandardRouter.Route] %s - %s - %s - received an internal response", requestUuid, requestUri, responseUri)

				// Internal requests shouldn't have to deal with heartbeats from other services
				if IsInternalRequest(request) && responseUri == "resource:///heartbeat" {
					logger.Printf("[StandardRouter.Route] %s - %s - %s - this was an internal request so we will bypass sending the heartbeat", requestUuid, requestUri, responseUri)
					continue
				}

				select {
				case responses <- response:
					logger.Printf("[StandardRouter.Route] %s - %s - %s - successfully notified client of the response", requestUuid, requestUri, responseUri)
				default:
					logger.Printf("[StandardRouter.Route] %s - %s - %s - failed to notify client of the response", requestUuid, requestUri, responseUri)
				}

				if response.GetCompleted() {
					logger.Printf("[StandardRouter.Route] %s - %s - %s - this was the final response, shutting down the goroutine", requestUuid, requestUri, responseUri)
					return
				}

			case <-timer.C:
				select {
				case streamTimeout <- nil:
					logger.Printf("[StandardRouter.Route] %s - %s - successfully notified client of authentic stream timeout, shutting down the goroutine", requestUuid, requestUri)
				default:
					logger.Printf("[StandardRouter.Route] %s - %s - failed to notify client of authentic stream timeout, shutting down the goroutine", requestUuid, requestUri)
				}

				return
			}

			timer.Reset(sr.heartbeatTimeout)
		}
	}()

	parsedUri, err := url.Parse(requestUri)
	if err != nil {
		logger.Fatalf("[StandardRouter.Route] %s - %s - Failed to parse the request uri: %s", requestUuid, requestUri, err)

		return responses, streamTimeout
	}

	if err := sr.publisher.Publish(parsedUri.Scheme+"-"+parsedUri.Path, payload); err != nil {
		logger.Printf("[StandardRouter.Route] %s - %s - failed to publish request to microservices: %s", requestUuid, requestUri, err)

		return responses, streamTimeout
	}

	return responses, streamTimeout
}

func (sr *StandardRouter) RouteWithTimeout(request *Request, timeout time.Duration) (chan *Request, chan interface{}) {
	responses, streamTimeout := sr.Route(request)

	requestUuid := request.GetUuid()
	requestUri := request.GetRouting().GetRouteTo()[0].GetUri()

	time.AfterFunc(timeout, func() {
		select {
		case streamTimeout <- nil:
			logger.Printf("[StandardRouter.RouteWithTimeout] %s - %s - successfully notified client of artificial stream timeout", requestUuid, requestUri)
		default:
			logger.Printf("[StandardRouter.RouteWithTimeout] %s - %s - failed to notify client of artificial stream timeout", requestUuid, requestUri)
		}
	})

	return responses, streamTimeout
}

func (sr *StandardRouter) SetHeartbeatTimeout(heartbeatTimeout time.Duration) {
	sr.heartbeatTimeout = heartbeatTimeout
}

func (sr *StandardRouter) subscribe() {
	logger.Printf("[NewStandardRouter] creating a new standard router.")

	consumedWorkCountMutex = &sync.Mutex{}

	sr.subscriber.Subscribe(sr.topic, ConsumerHandlerFunc(func(body []byte) error {
		logger.Println("[StandardRouter.Subscriber] receiving message for router")

		response := &Request{}
		if err := Unmarshal(body, response); err != nil {
			logger.Printf("[StandardRouter.Subscriber] failed to unmarshal response for router: %s", err)

			return err
		}

		responseUuid := response.GetUuid()
		responseUri := ""
		if len(response.GetRouting().GetRouteTo()) > 0 {
			responseUri = response.GetRouting().GetRouteTo()[0].GetUri()
		}

		logger.Printf("[StandardRouter.Subscriber] %s - %s - received response for router", responseUuid, responseUri)

		sr.mu.Lock()
		if responses, exists := sr.pendingResponses[response.GetUuid()]; exists {
			select {
			case responses <- response:
				logger.Printf("[StandardRouter.Subscriber] %s - %s - reply chan was available", responseUuid, responseUri)

			default:
				logger.Printf("[StandardRouter.Subscriber] %s - %s - reply chan was not available", responseUuid, responseUri)
			}

			if response.GetCompleted() {
				logger.Printf("[StandardRouter.Subscriber] %s - %s - this was the last response, closing and deleting the responses chan", responseUuid, responseUri)

				delete(sr.pendingResponses, response.GetUuid())
			}
		}
		sr.mu.Unlock()

		logger.Printf("[StandardRouter.Subscriber] %s - %s - completed routing response", responseUuid, responseUri)

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
