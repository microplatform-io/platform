package platform

import (
	"sync"
	"time"
)

type Responder interface {
	Respond(response *Request)
}

type PublishResponder struct {
	publisher Publisher
}

func (rs *PublishResponder) Respond(response *Request) {
	if response.GetCompleted() {
		logger.Printf("[PublishResponder.Respond] %s sending FINAL %s", response.GetUuid(), response.Routing.RouteTo[0].GetUri())
	} else {
		logger.Printf("[PublishResponder.Respond] %s sending INTERMEDIARY %s", response.GetUuid(), response.Routing.RouteTo[0].GetUri())
	}

	destinationRouteIndex := len(response.Routing.RouteTo) - 1
	destinationRoute := response.Routing.RouteTo[destinationRouteIndex]
	response.Routing.RouteTo = response.Routing.RouteTo[:destinationRouteIndex]

	body, err := Marshal(response)
	if err != nil {
		logger.Errorf("[PublishResponder.Respond] failed to marshal response: %s", err)
		return
	}

	// URI may not be valid here, we may need to parse it first for good practice. - Bryan
	if err := rs.publisher.Publish(destinationRoute.GetUri(), body); err != nil {
		logger.WithError(err).Warn("Failed to publish a response")
	}

	logger.Infoln("[PublishResponder.Respond] published response successfully")
}

func NewPublishResponder(publisher Publisher) *PublishResponder {
	return &PublishResponder{
		publisher: publisher,
	}
}

type RequestResponder struct {
	parent    Responder
	request   *Request
	completed bool
	quit      chan bool
	mu        sync.Mutex
}

func (rs *RequestResponder) Respond(response *Request) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.completed {
		logger.Printf("[RequestResponder.Respond] %s did not send response due to already been completed", response.GetUuid())
		return
	}

	response = generateResponse(rs.request, response)

	logger.Printf("[RequestResponder.Respond] %s attempting to send response", response.GetUuid())
	if response.GetCompleted() {
		rs.completed = true
		close(rs.quit)
	}

	rs.parent.Respond(response)

	logger.Printf("[RequestResponder.Respond] %s sent response", response.GetUuid())
}

func NewRequestResponder(parent Responder, request *Request) *RequestResponder {
	quit := make(chan bool, 1)

	logger.Println("[NewRequestResponder] creating a new heartbeat courier")

	go func() {
		for {
			select {
			case <-time.After(500 * time.Millisecond):
				parent.Respond(generateResponse(request, &Request{
					Routing: RouteToUri("resource:///heartbeat"),
				}))

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
