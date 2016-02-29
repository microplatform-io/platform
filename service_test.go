package platform

import (
	"errors"

	"github.com/kr/pretty"
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"
)

type mockCourier struct {
	requests []*Request
}

func (c *mockCourier) Send(request *Request) {
	c.requests = append(c.requests, request)
}

func newMockCourier() *mockCourier {
	return &mockCourier{
		requests: []*Request{},
	}
}

type mockPublish struct {
	topic string
	body  []byte
}

type mockPublisher struct {
	mockPublishes []mockPublish
}

func (p *mockPublisher) Publish(topic string, body []byte) error {
	p.mockPublishes = append(p.mockPublishes, mockPublish{
		topic: topic,
		body:  body,
	})

	return nil
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{
		mockPublishes: []mockPublish{},
	}
}

type mockSubscriber struct {
	topicHandlers map[string][]ConsumerHandler
	totalRunCalls int
}

func (s *mockSubscriber) getTopicTotalHandlers() map[string]int {
	totalHandlers := map[string]int{}

	for topic, handlers := range s.topicHandlers {
		totalHandlers[topic] = len(handlers)
	}

	return totalHandlers
}

func (s *mockSubscriber) Subscribe(topic string, handler ConsumerHandler) {
	s.topicHandlers[topic] = append(s.topicHandlers[topic], handler)
}

func (s *mockSubscriber) Run() {
	s.totalRunCalls += 1
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{
		topicHandlers: make(map[string][]ConsumerHandler),
		totalRunCalls: 0,
	}
}

func TestNewService(t *testing.T) {
	Convey("Creating a service should simply associate the various bootstrapped properties of a service but not publish / subscribe", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		So(service.name, ShouldEqual, "test-service")

		So(mockPublisher.mockPublishes, ShouldResemble, []mockPublish{})
		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{})
	})
}

func TestServiceCanAcceptWork(t *testing.T) {
	Convey("A service should only not be able to accept work when it is closed", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)

		So(service.canAcceptWork(), ShouldBeTrue)
		So(service.Close(), ShouldBeNil)
		So(service.canAcceptWork(), ShouldBeFalse)
	})
}

func TestServiceClose(t *testing.T) {
	Convey("Calling close multiple times should be a safe thing to do", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)
		So(service.closed, ShouldBeFalse)

		So(service.Close(), ShouldBeNil)
		So(service.closed, ShouldBeTrue)

		So(service.Close(), ShouldBeNil)
		So(service.closed, ShouldBeTrue)
	})

	Convey("A service should not be able to close until all if its work is done", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		service.incrementWorkerPendingJobs()
		go func() {
			time.Sleep(100 * time.Millisecond)
			service.decrementWorkerPendingJobs()
		}()

		serviceClosedErr := make(chan error)
		go func() {
			serviceClosedErr <- service.Close()
		}()

		So(service.closed, ShouldBeFalse)

		select {
		case err := <-serviceClosedErr:
			So(err, ShouldBeNil)
		case <-time.After(2 * time.Second):
			t.Error("Service did not shut down in a reasonable amount of time")
		}

		So(service.closed, ShouldBeTrue)
	})
}

func TestServiceHandler(t *testing.T) {
	Convey("Ensure that adding a handler and calling it works properly", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		totalHandlerCalls := 0

		service.AddHandler("testing", HandlerFunc(func(responseSender ResponseSender, request *Request) {
			totalHandlerCalls += 1

			responseSender.Send(&Request{
				Routing:   RouteToUri("resource:///teltech/reply/foobar"),
				Completed: Bool(true),
			})
		}))

		So(mockPublisher.mockPublishes, ShouldResemble, []mockPublish{})
		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"microservice-testing": 1,
		})

		requestBytes, _ := Marshal(&Request{
			Routing: RouteToUri("microservice:///teltech/get/foobar"),
		})

		So(totalHandlerCalls, ShouldEqual, 0)
		So(mockSubscriber.topicHandlers["microservice-testing"][0].HandleMessage(requestBytes), ShouldBeNil)
		So(totalHandlerCalls, ShouldEqual, 1)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 0)

		So(len(mockCourier.requests), ShouldEqual, 1)
		So(mockCourier.requests[0].Routing.RouteTo[0].GetUri(), ShouldEqual, "resource:///teltech/reply/foobar")
	})

	Convey("A handler that panics should return a platform error", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		totalHandlerCalls := 0

		service.AddHandler("testing", HandlerFunc(func(responseSender ResponseSender, request *Request) {
			panic("YAY FAILURE!")
		}))

		So(mockPublisher.mockPublishes, ShouldResemble, []mockPublish{})
		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"microservice-testing": 1,
		})

		So(totalHandlerCalls, ShouldEqual, 0)
		So(mockSubscriber.topicHandlers["microservice-testing"][0].HandleMessage([]byte{}), ShouldBeNil)
		So(totalHandlerCalls, ShouldEqual, 0)

		pretty.Println(mockPublisher.mockPublishes)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)
		So(mockPublisher.mockPublishes[0].topic, ShouldEqual, "panic.handler.testing")

		So(len(mockCourier.requests), ShouldEqual, 1)
		So(mockCourier.requests[0].Routing.RouteTo[0].GetUri(), ShouldEqual, "resource:///platform/reply/error")
	})

	Convey("Synchronously calling a handler after closing the service should not actually call the handler", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		totalHandlerCalls := 0

		service.AddHandler("testing", HandlerFunc(func(responseSender ResponseSender, request *Request) {
			totalHandlerCalls += 1

			responseSender.Send(&Request{
				Routing:   RouteToUri("resource:///teltech/reply/foobar"),
				Completed: Bool(true),
			})
		}))

		So(mockPublisher.mockPublishes, ShouldResemble, []mockPublish{})
		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"microservice-testing": 1,
		})

		So(totalHandlerCalls, ShouldEqual, 0)
		So(mockSubscriber.topicHandlers["microservice-testing"][0].HandleMessage([]byte{}), ShouldBeNil)
		So(totalHandlerCalls, ShouldEqual, 1)

		So(service.Close(), ShouldBeNil)

		So(totalHandlerCalls, ShouldEqual, 1)
		So(mockSubscriber.topicHandlers["microservice-testing"][0].HandleMessage([]byte{}), ShouldResemble, errors.New("no new work can be accepted"))
		So(totalHandlerCalls, ShouldEqual, 1)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 0)

		So(len(mockCourier.requests), ShouldEqual, 1)
		So(mockCourier.requests[0].Routing.RouteTo[0].GetUri(), ShouldEqual, "resource:///teltech/reply/foobar")
	})

	Convey("Asynchronously calling a handler after closing the service should not actually call the handler", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		totalHandlerCalls := 0

		service.AddHandler("testing", HandlerFunc(func(responseSender ResponseSender, request *Request) {
			time.Sleep(750 * time.Millisecond)

			totalHandlerCalls += 1

			responseSender.Send(&Request{
				Routing:   RouteToUri("resource:///teltech/reply/foobar"),
				Completed: Bool(true),
			})
		}))

		So(mockPublisher.mockPublishes, ShouldResemble, []mockPublish{})
		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"microservice-testing": 1,
		})

		firstHandlerEvents := make(chan string)
		closeEvents := make(chan string)
		secondHandlerEvents := make(chan string)

		var firstHandleErr, closeErr, secondHandleErr error

		go func() {
			requestBytes, _ := Marshal(&Request{
				Routing: RouteToUri("microservice:///teltech/get/foobar"),
			})

			firstHandlerEvents <- "started"
			firstHandleErr = mockSubscriber.topicHandlers["microservice-testing"][0].HandleMessage(requestBytes)
			firstHandlerEvents <- "ended"
		}()

		So(<-firstHandlerEvents, ShouldEqual, "started")
		So(firstHandleErr, ShouldBeNil)
		So(totalHandlerCalls, ShouldEqual, 0)

		So(service.workerPendingJobs, ShouldEqual, 1)

		go func() {
			closeEvents <- "started"
			closeErr = service.Close()
			closeEvents <- "ended"
		}()

		So(<-closeEvents, ShouldEqual, "started")
		startedClosingAt := time.Now()
		So(<-closeEvents, ShouldEqual, "ended")
		So(closeErr, ShouldBeNil)

		So(service.workerPendingJobs, ShouldEqual, 0)

		// We need to make sure that the service did wait for "something", which should be the service ending the handler.
		// The validity of this test can be confirmed by shortening the sleep in the handler.
		So(time.Now().Sub(startedClosingAt).Nanoseconds(), ShouldBeGreaterThan, 500*time.Millisecond)

		go func() {
			secondHandlerEvents <- "started"
			secondHandleErr = mockSubscriber.topicHandlers["microservice-testing"][0].HandleMessage([]byte{})
			secondHandlerEvents <- "ended"
		}()

		So(<-secondHandlerEvents, ShouldEqual, "started")
		So(secondHandleErr, ShouldResemble, errors.New("no new work can be accepted"))

		So(service.workerPendingJobs, ShouldEqual, 0)

		So(<-firstHandlerEvents, ShouldEqual, "ended")
		So(totalHandlerCalls, ShouldEqual, 1)
		So(<-secondHandlerEvents, ShouldEqual, "ended")
		So(totalHandlerCalls, ShouldEqual, 1)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 0)

		So(len(mockCourier.requests), ShouldEqual, 2)
		So(mockCourier.requests[0].Routing.RouteTo[0].GetUri(), ShouldEqual, "resource:///heartbeat")
		So(mockCourier.requests[1].Routing.RouteTo[0].GetUri(), ShouldEqual, "resource:///teltech/reply/foobar")
	})
}

func TestServiceListener(t *testing.T) {
	Convey("Ensure that adding a listener and calling it works properly", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		totalListenerCalls := 0

		service.AddListener("testing", ConsumerHandlerFunc(func(body []byte) error {
			totalListenerCalls += 1

			return nil
		}))

		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"testing": 1,
		})

		So(totalListenerCalls, ShouldEqual, 0)
		mockSubscriber.topicHandlers["testing"][0].HandleMessage([]byte{})
		So(totalListenerCalls, ShouldEqual, 1)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 0)
	})

	Convey("A listener that panics should publish an error", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)
		So(service, ShouldNotBeNil)

		totalListenerCalls := 0

		service.AddListener("testing", ConsumerHandlerFunc(func(body []byte) error {
			totalListenerCalls += 1

			panic("YAY FAILURE!")

			return nil
		}))

		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"testing": 1,
		})

		So(totalListenerCalls, ShouldEqual, 0)
		mockSubscriber.topicHandlers["testing"][0].HandleMessage([]byte{})
		So(totalListenerCalls, ShouldEqual, 1)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)
		So(mockPublisher.mockPublishes[0].topic, ShouldEqual, "panic.listener.testing")
	})
}

func TestServiceRun(t *testing.T) {
	Convey("A service's Run call return if the service is successfully closed", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()
		mockCourier := newMockCourier()

		service, err := NewServiceWithResponseSender("test-service", mockPublisher, mockSubscriber, mockCourier)
		So(err, ShouldBeNil)

		go func() {
			time.Sleep(100 * time.Millisecond)
			service.Close()
		}()

		runEnded := make(chan bool)
		go func() {
			service.Run()
			runEnded <- true
		}()

		select {
		case <-runEnded:
			So(true, ShouldBeTrue)
		case <-time.After(1 * time.Second):
			t.Fatal("Service run did not end in a reasonable amount of time")
		}
	})
}
