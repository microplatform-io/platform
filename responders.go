package platform

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Responder interface {
	Respond(response *Request) error
}

type PublishResponder struct {
	publisher Publisher
}

func (rs *PublishResponder) Respond(response *Request) error {
	destinationRouteIndex := len(response.Routing.RouteTo) - 1
	destinationRoute := response.Routing.RouteTo[destinationRouteIndex]
	response.Routing.RouteTo = response.Routing.RouteTo[:destinationRouteIndex]

	routeTo := ""
	if len(response.GetRouting().GetRouteTo()) > 0 {
		routeTo = response.GetRouting().GetRouteTo()[0].GetUri()
	}

	logger.WithFields(logrus.Fields{
		"request_uuid": response.GetUuid(),
		"trace_uuid":   response.GetTrace().GetUuid(),
		"route_to":     routeTo,
	}).Debug("publishing response")

	responseBytes, err := Marshal(response)
	if err != nil {
		return errors.Wrap(err, "publish responder failed to marshal response")
	}

	// URI may not be valid here, we may need to parse it first for good practice. - Bryan
	if err := rs.publisher.Publish(destinationRoute.GetUri(), responseBytes); err != nil {
		return errors.Wrap(err, "publish responder failed to publish the response")
	}

	return nil
}

func NewPublishResponder(publisher Publisher) *PublishResponder {
	return &PublishResponder{
		publisher: publisher,
	}
}

type TraceResponder struct {
	parent Responder
	tracer Tracer
}

func (r *TraceResponder) Respond(response *Request) error {
	if response.GetCompleted() {
		r.tracer.End(response.Trace)
	}

	return r.parent.Respond(response)
}

func NewTraceResponder(parent Responder, tracer Tracer) *TraceResponder {
	return &TraceResponder{
		parent: parent,
		tracer: tracer,
	}
}

type RequestResponder struct {
	parent    Responder
	request   *Request
	completed bool
	quit      chan bool
	mu        sync.Mutex
}

func (rs *RequestResponder) Respond(response *Request) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.completed {
		return errors.New("request responder has already been completed")
	}

	response = generateResponse(rs.request, response)

	if response.GetCompleted() {
		rs.completed = true
		close(rs.quit)
	}

	return rs.parent.Respond(response)
}

func NewRequestResponder(parent Responder, request *Request) *RequestResponder {
	quit := make(chan bool, 1)

	go func() {
		heartbeatTicker := time.NewTicker(500 * time.Millisecond)
		defer heartbeatTicker.Stop()

		warningTicker := time.NewTicker(60 * time.Second)
		defer warningTicker.Stop()

		for {
			select {
			case <-heartbeatTicker.C:
				parent.Respond(generateResponse(request, &Request{
					Routing: RouteToUri("resource:///heartbeat"),
				}))

			case <-warningTicker.C:
				logger.WithFields(logrus.Fields{
					"request_uuid": request.GetUuid(),
					"trace_uuid":   request.GetTrace().GetUuid(),
				}).Warn("this request has been alive for at least 60 seconds")

			case <-quit:
				return

			}
		}
	}()

	return &RequestResponder{
		parent:  parent,
		request: request,
		quit:    quit,
	}
}
