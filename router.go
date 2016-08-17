package platform

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

var RequestTimeout = errors.New("Request timed out")

type Router interface {
	Route(request *Request) (*Request, error)
	Stream(request *Request) (chan *Request, chan interface{})

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

	responses <- generateResponse(request, &Request{
		Routing:   RouteToUri("resource:///platform/reply/error"),
		Payload:   errorBytes,
		Completed: Bool(true),
	})

	return responses
}

func (r *StandardRouter) Route(originalRequest *Request) (*Request, error) {
	responses, streamTimeout := r.Stream(originalRequest)

	for {
		select {
		case response := <-responses:
			if response.GetCompleted() {
				return response, nil
			}
		case <-streamTimeout:
			return nil, RequestTimeout
		}
	}

	return nil, RequestTimeout
}

func (r *StandardRouter) Stream(originalRequest *Request) (chan *Request, chan interface{}) {
	logger.Warn("Stream!")
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
		logger.Errorf("[StandardRouter.Stream] %s - %s - Failed to parse the request uri: %s", requestUUID, requestURI, err)

		return createResponseChanWithError(request, &Error{
			Message: String(fmt.Sprintf("Failed to parse the RouteTo URI: %s", err)),
		}), nil
	}

	if request.Routing != nil {
		request.Routing.RouteFrom = append(request.Routing.RouteFrom, &Route{
			Uri: String(r.topic),
		})
	}

	logger.Infof("[StandardRouter.Stream]          routing %s request", requestURI)
	logger.Printf("[StandardRouter.Stream] %s - %s - routing request: %s", requestUUID, requestURI, request)

	payload, err := Marshal(request)
	if err != nil {
		logger.Errorf("[StandardRouter.Stream] %s - %s - failed to marshal request payload: %s", requestUUID, requestURI, err)
	}

	internalResponses := make(chan *Request, 5)
	responses := make(chan *Request, 5)
	streamTimeout := make(chan interface{})

	r.mu.Lock()
	r.pendingResponses[requestUUID] = internalResponses
	r.mu.Unlock()

	logger.Debug("Placing message onto pending responses")

	go func() {
		timer := time.NewTimer(r.heartbeatTimeout)
		defer timer.Stop()

		for {
			timer.Reset(r.heartbeatTimeout)

			select {
			case response := <-internalResponses:
				responseUri := ""
				if len(response.GetRouting().GetRouteTo()) > 0 {
					responseUri = response.GetRouting().GetRouteTo()[0].GetUri()
				}

				logger.Printf("[StandardRouter.Stream] %s - %s - %s - received an internal response", requestUUID, requestURI, responseUri)

				// Internal requests shouldn't have to deal with heartbeats from other services
				if IsInternalRequest(request) && responseUri == "resource:///heartbeat" {
					logger.Printf("[StandardRouter.Stream] %s - %s - %s - this was an internal request so we will bypass sending the heartbeat", requestUUID, requestURI, responseUri)
					continue
				}

				// Remove the request uuid suffix to ensure proper routing on the response
				response.Uuid = String(strings.Replace(response.GetUuid(), requestUUIDSuffix, "", 1))

				select {
				case responses <- response:
					logger.Printf("[StandardRouter.Stream] %s - %s - %s - successfully notified client of the response", requestUUID, requestURI, responseUri)
				case <-time.After(5 * time.Second):
					logger.Errorf("[StandardRouter.Stream] %s - %s - %s - failed to notify client of the response", requestUUID, requestURI, responseUri)
				}

				if response.GetCompleted() {
					logger.Infof("[StandardRouter.Stream]          received response")
					logger.Printf("[StandardRouter.Stream] %s - %s - %s - this was the final response, shutting down the goroutine", requestUUID, requestURI, responseUri)
					return
				}

			case <-timer.C:
				close(streamTimeout)

				r.mu.Lock()
				delete(r.pendingResponses, requestUUID)
				r.mu.Unlock()

				return
			}
		}
	}()

	if err := r.publisher.Publish(parsedURI.Scheme+"-"+parsedURI.Path, payload); err != nil {
		logger.Errorf("[StandardRouter.Stream] %s - %s - failed to publish request to microservices: %s", requestUUID, requestURI, err)

		return createResponseChanWithError(request, &Error{
			Message: String(fmt.Sprintf("Failed to publish request to microservices: %s", err)),
		}), nil
	}

	return responses, streamTimeout
}

func (r *StandardRouter) SetHeartbeatTimeout(heartbeatTimeout time.Duration) {
	r.heartbeatTimeout = heartbeatTimeout
}

func (r *StandardRouter) subscribe() {
	logger.Printf("[NewStandardRouter] creating a new standard router.")

	r.subscriber.Subscribe(r.topic, ConsumerHandlerFunc(func(body []byte) error {
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

		r.mu.Lock()
		if responses, exists := r.pendingResponses[response.GetUuid()]; exists {
			select {
			case responses <- response:
				logger.Printf("[StandardRouter.Subscriber] %s - %s - reply chan was available", responseUuid, responseUri)

			default:
				logger.Printf("[StandardRouter.Subscriber] %s - %s - reply chan was not available", responseUuid, responseUri)
			}

			if response.GetCompleted() {
				logger.Printf("[StandardRouter.Subscriber] %s - %s - this was the last response, closing and deleting the responses chan", responseUuid, responseUri)

				delete(r.pendingResponses, response.GetUuid())
			}
		} else {
			logger.Errorf("[StandardRouter.Subscriber] %s - %s - pending response channel did not exist, it may have been deleted", responseUuid, responseUri)
		}
		r.mu.Unlock()

		logger.Printf("[StandardRouter.Subscriber] %s - %s - completed routing response", responseUuid, responseUri)

		return nil
	}))

	r.subscriber.Run()

	logger.Printf("[NewStandardRouter] router has been created: %#v", r)
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
