package platform

import (
	"crypto/sha1"
	"os"
	"time"
)

var (
	logger       = GetLogger("platform")
	serviceToken = os.Getenv("SERVICE_TOKEN")
)

type Courier struct {
	responses chan *Request
}

func (c *Courier) Send(response *Request) {
	c.responses <- response
}

func NewCourier(publisher Publisher) *Courier {
	responses := make(chan *Request, 10)

	go func() {
		for response := range responses {
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
			publisher.Publish(destinationRoute.GetUri(), body)

			logger.Println("[Service.Subscriber] published response successfully")
		}
	}()

	return &Courier{
		responses: responses,
	}
}

type RequestHeartbeatCourier struct {
	parent ResponseSender
	quit   chan bool
}

func (rhc *RequestHeartbeatCourier) Send(response *Request) {
	logger.Printf("[RequestHeartbeatCourier.Send] %s attempting to send response", response.GetUuid())
	if response.GetCompleted() {
		rhc.quit <- true
	}

	rhc.parent.Send(response)

	logger.Printf("[RequestHeartbeatCourier.Send] %s sent response", response.GetUuid())
}

func NewRequestHeartbeatCourier(parent ResponseSender, request *Request) *RequestHeartbeatCourier {
	quit := make(chan bool, 1)

	logger.Println("[NewRequestHeartbeatCourier] creating a new heartbeat courier")

	go func() {
		for {
			select {
			case <-time.After(500 * time.Millisecond):
				logger.Printf("[NewRequestHeartbeatCourier] %s sending heartbeat", request.GetUuid())

				parent.Send(&Request{
					Uuid: request.Uuid,
					Routing: &Routing{
						RouteTo: append([]*Route{&Route{Uri: String("resource:///heartbeat")}}, request.Routing.RouteFrom...),
					},
				})

			case <-quit:
				logger.Printf("[NewRequestHeartbeatCourier] %s handler has sent completed request, exiting heartbeat", request.GetUuid())
				return

			}
		}
	}()

	return &RequestHeartbeatCourier{
		parent: parent,
		quit:   quit,
	}
}

type Service struct {
	publisher  Publisher
	subscriber Subscriber
	courier    *Courier
}

func (s *Service) AddHandler(path string, handler Handler, concurrency int) {
	logger.Println("[Service.AddHandler] adding handler", path)

	s.subscriber.Subscribe("microservice-"+path, ConsumerHandlerFunc(func(p []byte) error {
		logger.Printf("[Service.Subscriber] handling %s request", path)

		request := &Request{}
		if err := Unmarshal(p, request); err != nil {
			logger.Println("[Service.Subscriber] failed to decode request")

			return nil
		}

		handler.Handle(NewRequestHeartbeatCourier(s.courier, request), request)

		return nil
	}), concurrency)
}

func (s *Service) AddListener(topic string, handler ConsumerHandler, concurrency int) {
	s.subscriber.Subscribe(topic, handler, concurrency)
}

func (s *Service) Run() {
	s.subscriber.Run()

	logger.Println("Subscriptions have stopped")
}

func NewService(publisher Publisher, subscriber Subscriber) (*Service, error) {
	return &Service{
		subscriber: subscriber,
		courier:    NewCourier(publisher),
	}, nil
}

func CreateServiceRequestToken(requestPayload []byte) string {
	token := append(requestPayload, ParseUUID(serviceToken)...)
	shaToken := sha1.Sum(token)
	return string(shaToken[:])
}

func IsServiceRequestToken(token string, requestPayload []byte) bool {
	createdToken := CreateServiceRequestToken(requestPayload)
	if token == createdToken {
		return true
	}

	return false
}
