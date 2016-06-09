package platform

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

type Router interface {
	Route(request *Request) (chan *Request, chan interface{})
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

func createResponseChanWithError(request *Request, err *Error) chan *Request {
	responses := make(chan *Request, 1)

	errorBytes, _ := Marshal(err)

	responses <- GenerateResponse(request, &Request{
		Routing: RouteToUri("resource:///platform/reply/error"),
		Payload: errorBytes,
	})

	return responses
}

func (sr *StandardRouter) Route(originalRequest *Request) (chan *Request, chan interface{}) {
	request := proto.Clone(originalRequest).(*Request)

	if request.Uuid == nil {
		request.Uuid = String("request-" + CreateUUID())
	}

	requestUUIDSuffix := "::" + strconv.Itoa(int(time.Now().UnixNano()))

	request.Uuid = String(request.GetUuid() + requestUUIDSuffix)

	requestUUID := request.GetUuid()
	requestURI := ""
	if len(request.GetRouting().GetRouteTo()) > 0 {
		requestURI = request.GetRouting().GetRouteTo()[0].GetUri()
	}

	parsedURI, err := url.Parse(requestURI)
	if err != nil {
		logger.Errorf("[StandardRouter.Route] %s - %s - Failed to parse the request uri: %s", requestUUID, requestURI, err)

		return createResponseChanWithError(request, &Error{
			Message: String(fmt.Sprintf("Failed to parse the RouteTo URI: %s", err)),
		}), nil
	}

	if request.Routing != nil {
		request.Routing.RouteFrom = append(request.Routing.RouteFrom, &Route{
			Uri: String(sr.topic),
		})
	}

	logger.Infof("[StandardRouter.Route]          routing %s request", requestURI)
	logger.Printf("[StandardRouter.Route] %s - %s - routing request: %s", requestUUID, requestURI, request)

	payload, err := Marshal(request)
	if err != nil {
		logger.Errorf("[StandardRouter.Route] %s - %s - failed to marshal request payload: %s", requestUUID, requestURI, err)
	}

	internalResponses := make(chan *Request, 5)
	responses := make(chan *Request, 5)
	streamTimeout := make(chan interface{})

	sr.mu.Lock()
	sr.pendingResponses[requestUUID] = internalResponses
	sr.mu.Unlock()

	go func() {
		timer := time.NewTimer(sr.heartbeatTimeout)
		defer timer.Stop()

		for {
			timer.Reset(sr.heartbeatTimeout)

			select {
			case response := <-internalResponses:
				responseUri := ""
				if len(response.GetRouting().GetRouteTo()) > 0 {
					responseUri = response.GetRouting().GetRouteTo()[0].GetUri()
				}

				logger.Printf("[StandardRouter.Route] %s - %s - %s - received an internal response", requestUUID, requestURI, responseUri)

				// Internal requests shouldn't have to deal with heartbeats from other services
				if IsInternalRequest(request) && responseUri == "resource:///heartbeat" {
					logger.Printf("[StandardRouter.Route] %s - %s - %s - this was an internal request so we will bypass sending the heartbeat", requestUUID, requestURI, responseUri)
					continue
				}

				// Remove the request uuid suffix to ensure proper routing on the response
				response.Uuid = String(strings.Replace(response.GetUuid(), requestUUIDSuffix, "", 1))

				select {
				case responses <- response:
					logger.Printf("[StandardRouter.Route] %s - %s - %s - successfully notified client of the response", requestUUID, requestURI, responseUri)
				case <-time.After(5 * time.Second):
					logger.Errorf("[StandardRouter.Route] %s - %s - %s - failed to notify client of the response", requestUUID, requestURI, responseUri)
				}

				if response.GetCompleted() {
					logger.Infof("[StandardRouter.Route]          received response")
					logger.Printf("[StandardRouter.Route] %s - %s - %s - this was the final response, shutting down the goroutine", requestUUID, requestURI, responseUri)
					return
				}

			case <-timer.C:
				close(streamTimeout)

				sr.mu.Lock()
				delete(sr.pendingResponses, requestUUID)
				sr.mu.Unlock()

				return
			}
		}
	}()

	if err := sr.publisher.Publish(parsedURI.Scheme+"-"+parsedURI.Path, payload); err != nil {
		logger.Errorf("[StandardRouter.Route] %s - %s - failed to publish request to microservices: %s", requestUUID, requestURI, err)

		return createResponseChanWithError(request, &Error{
			Message: String(fmt.Sprintf("Failed to publish request to microservices: %s", err)),
		}), nil
	}

	return responses, streamTimeout
}

func (sr *StandardRouter) SetHeartbeatTimeout(heartbeatTimeout time.Duration) {
	sr.heartbeatTimeout = heartbeatTimeout
}

func (sr *StandardRouter) subscribe() {
	logger.Printf("[NewStandardRouter] creating a new standard router.")

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
		} else {
			logger.Errorf("[StandardRouter.Subscriber] %s - %s - pending response channel did not exist, it may have been deleted", responseUuid, responseUri)
		}
		sr.mu.Unlock()

		logger.Printf("[StandardRouter.Subscriber] %s - %s - completed routing response", responseUuid, responseUri)

		return nil
	}))

	sr.subscriber.Run()

	logger.Printf("[NewStandardRouter] router has been created: %#v", sr)
}

func NewStandardRouter(publisher Publisher, subscriber Subscriber) *StandardRouter {
	router := &StandardRouter{
		publisher:        publisher,
		subscriber:       subscriber,
		heartbeatTimeout: time.Second * 2,
		topic:            "router-" + CreateUUID(),
		pendingResponses: map[string]chan *Request{},
	}

	router.subscribe()

	return router
}

func NewStandardRouterWithTopic(publisher Publisher, subscriber Subscriber, topic string) *StandardRouter {
	router := &StandardRouter{
		publisher:        publisher,
		subscriber:       subscriber,
		heartbeatTimeout: time.Second * 2,
		topic:            topic,
		pendingResponses: map[string]chan *Request{},
	}

	router.subscribe()

	return router
}
