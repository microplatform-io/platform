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

type TracingRouter struct {
	parentRouter Router
	tracer       Tracer
	parentTrace  *Trace
}

func (r *TracingRouter) Route(request *Request) (*Request, error) {
	trace := r.tracer.Start(r.parentTrace, request.GetRouting().GetRouteTo()[0].GetUri())
	defer r.tracer.End(trace)

	request.Trace = trace

	return r.parentRouter.Route(request)
}

func (r *TracingRouter) Stream(request *Request) (chan *Request, chan interface{}) {
	trace := r.tracer.Start(r.parentTrace, request.GetRouting().GetRouteTo()[0].GetUri())

	request.Trace = trace

	internalResponses, internalTimeout := r.parentRouter.Stream(request)

	returnedResponses := make(chan *Request)
	returnedTimeout := make(chan interface{})

	go func() {
		defer r.tracer.End(trace)

		for {
			select {
			case response := <-internalResponses:
				returnedResponses <- response

				if response.GetCompleted() {
					return
				}
			case <-internalTimeout:
				close(returnedTimeout)

				return
			}
		}
	}()

	return returnedResponses, returnedTimeout
}

func (r *TracingRouter) SetHeartbeatTimeout(heartbeatTimeout time.Duration) {
	r.parentRouter.SetHeartbeatTimeout(heartbeatTimeout)
}

func NewTracingRouter(parentRouter Router, tracer Tracer, parentTrace *Trace) *TracingRouter {
	return &TracingRouter{
		parentRouter: parentRouter,
		tracer:       tracer,
		parentTrace:  parentTrace,
	}
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
		return createResponseChanWithError(request, &Error{
			Message: String(fmt.Sprintf("Failed to parse the RouteTo URI: %s", err)),
		}), nil
	}

	if request.Routing != nil {
		request.Routing.RouteFrom = append(request.Routing.RouteFrom, &Route{
			Uri: String(r.topic),
		})
	}

	requestBytes, err := Marshal(request)
	if err != nil {
		return createResponseChanWithError(request, &Error{
			Message: String(fmt.Sprintf("Failed to marshal the request: %s", err)),
		}), nil
	}

	internalResponses := make(chan *Request, 5)
	responses := make(chan *Request, 5)
	streamTimeout := make(chan interface{})

	r.mu.Lock()
	r.pendingResponses[requestUUID] = internalResponses
	r.mu.Unlock()

	timer := time.NewTimer(r.heartbeatTimeout * 2)

	go func() {
		defer timer.Stop()

		for {
			select {
			case response := <-internalResponses:
				responseUri := ""
				if len(response.GetRouting().GetRouteTo()) > 0 {
					responseUri = response.GetRouting().GetRouteTo()[0].GetUri()
				}

				// Internal requests shouldn't have to deal with heartbeats from other services
				if IsInternalRequest(request) && responseUri == "resource:///heartbeat" {
					continue
				}

				// Remove the request uuid suffix to ensure proper routing on the response
				response.Uuid = String(strings.Replace(response.GetUuid(), requestUUIDSuffix, "", 1))

				select {
				case responses <- response:
				case <-time.After(5 * time.Second):
					logger.Errorf("[StandardRouter.Stream] %s - %s - %s - failed to notify client of the response", requestUUID, requestURI, responseUri)
				}

				if response.GetCompleted() {
					return
				}

			case <-timer.C:
				close(streamTimeout)

				r.mu.Lock()
				delete(r.pendingResponses, requestUUID)
				r.mu.Unlock()

				return
			}

			timer.Reset(r.heartbeatTimeout)
		}
	}()

	if err := r.publisher.Publish(parsedURI.Scheme+"-"+parsedURI.Path, requestBytes); err != nil {
		return createResponseChanWithError(request, &Error{
			Message: String(fmt.Sprintf("Failed to publish request to microservices: %s", err)),
		}), nil
	}

	timer.Reset(r.heartbeatTimeout)

	return responses, streamTimeout
}

func (r *StandardRouter) SetHeartbeatTimeout(heartbeatTimeout time.Duration) {
	r.heartbeatTimeout = heartbeatTimeout
}

func (r *StandardRouter) subscribe() {
	r.subscriber.Subscribe(r.topic, ConsumerHandlerFunc(func(body []byte) error {
		response := &Request{}
		if err := Unmarshal(body, response); err != nil {
			return err
		}

		responseUuid := response.GetUuid()
		responseUri := ""
		if len(response.GetRouting().GetRouteTo()) > 0 {
			responseUri = response.GetRouting().GetRouteTo()[0].GetUri()
		}

		r.mu.Lock()
		if responses, exists := r.pendingResponses[response.GetUuid()]; exists {
			select {
			case responses <- response:
			default:
				logger.Printf("[StandardRouter.Subscriber] %s - %s - reply chan was not available", responseUuid, responseUri)
			}

			if response.GetCompleted() {
				delete(r.pendingResponses, response.GetUuid())
			}
		} else {
			logger.Errorf("[StandardRouter.Subscriber] %s - %s - pending response channel did not exist, it may have been deleted", responseUuid, responseUri)
		}
		r.mu.Unlock()

		return nil
	}))

	r.subscriber.Run()
}

func NewStandardRouter(publisher Publisher, subscriber Subscriber) *StandardRouter {
	router := &StandardRouter{
		publisher:        publisher,
		subscriber:       subscriber,
		heartbeatTimeout: time.Second * 10,
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
		heartbeatTimeout: time.Second * 10,
		topic:            topic,
		pendingResponses: map[string]chan *Request{},
	}

	router.subscribe()

	return router
}
