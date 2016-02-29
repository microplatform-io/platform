package platform

import (
	"sync"
	"time"
)

type ResponseSender interface {
	Send(response *Request)
}

type StandardResponseSender struct {
	publisher Publisher
	responses chan *Request
}

func (rs *StandardResponseSender) runResponsePublisher() {
	for response := range rs.responses {
		logger.Printf("[Service.Subscriber] publishing response: %s", response)

		destinationRouteIndex := len(response.Routing.RouteTo) - 1
		destinationRoute := response.Routing.RouteTo[destinationRouteIndex]
		response.Routing.RouteTo = response.Routing.RouteTo[:destinationRouteIndex]

		body, err := Marshal(response)
		if err != nil {
			logger.Printf("[Service.Subscriber] failed to marshal response: %s", err)
			continue
		}

		// URI may not be valid here, we may need to parse it first for good practice. - Bryan
		rs.publisher.Publish(destinationRoute.GetUri(), body)

		logger.Println("[Service.Subscriber] published response successfully")
	}
}

func (rs *StandardResponseSender) Send(response *Request) {
	if response.GetCompleted() {
		logger.Printf("[StandardResponseSender] %s sending FINAL %s", response.GetUuid(), response.Routing.RouteTo[0].GetUri())
	} else {
		logger.Printf("[StandardResponseSender] %s sending INTERMEDIARY %s", response.GetUuid(), response.Routing.RouteTo[0].GetUri())
	}

	rs.responses <- response
}

func NewStandardResponseSender(publisher Publisher) *StandardResponseSender {
	rs := &StandardResponseSender{
		publisher: publisher,
		responses: make(chan *Request, 10),
	}

	go rs.runResponsePublisher()

	return rs
}

type RequestHeartbeatResponseSender struct {
	parent    ResponseSender
	completed bool
	quit      chan bool
	mu        sync.Mutex
}

func (rs *RequestHeartbeatResponseSender) Send(response *Request) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.completed {
		logger.Printf("[RequestHeartbeatResponseSender.Send] %s did not send response due to already been completed", response.GetUuid())
		return
	}

	logger.Printf("[RequestHeartbeatResponseSender.Send] %s attempting to send response", response.GetUuid())
	if response.GetCompleted() {
		rs.completed = true
		close(rs.quit)
	}

	rs.parent.Send(response)

	logger.Printf("[RequestHeartbeatResponseSender.Send] %s sent response", response.GetUuid())
}

func NewRequestHeartbeatResponseSender(parent ResponseSender, request *Request) *RequestHeartbeatResponseSender {
	quit := make(chan bool, 1)

	logger.Println("[NewRequestHeartbeatResponseSender] creating a new heartbeat courier")

	go func() {
		for {
			select {
			case <-time.After(500 * time.Millisecond):
				parent.Send(GenerateResponse(request, &Request{
					Routing: RouteToUri("resource:///heartbeat"),
				}))

			case <-quit:
				return

			}
		}
	}()

	return &RequestHeartbeatResponseSender{
		parent: parent,
		quit:   quit,
	}
}
