package platform

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPublishResponder(t *testing.T) {
	Convey("Ensure that the response sender uses the publisher", t, func() {
		mockPublisher := newMockPublisher()

		responder := NewPublishResponder(mockPublisher)

		responder.Respond(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(false),
		})

		// Wait for the response sender's goroutine
		time.Sleep(10 * time.Millisecond)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)

		responder.Respond(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(true),
		})

		// Wait for the response sender's goroutine
		time.Sleep(10 * time.Millisecond)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 2)
	})
}

func TestRequestResponder(t *testing.T) {
	Convey("Ensure that the response sender uses the publisher", t, func() {
		mockPublisher := newMockPublisher()

		requestResponder := NewRequestResponder(NewPublishResponder(mockPublisher), &Request{})

		// Sending a response that is not completed should still produce heartbeats
		requestResponder.Respond(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(false),
		})

		// Let the goroutine send the message
		time.Sleep(10 * time.Millisecond)

		// Just by waiting for at least 500 milliseconds, we should see a heartbeat published
		So(requestResponder.completed, ShouldBeFalse)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)
		time.Sleep(750 * time.Millisecond)
		So(requestResponder.completed, ShouldBeFalse)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 2)

		// By sending a completed response, it should shut down the heartbeats
		requestResponder.Respond(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(true),
		})

		// Let the goroutine send the message
		time.Sleep(10 * time.Millisecond)

		So(requestResponder.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)
		time.Sleep(750 * time.Millisecond)
		So(requestResponder.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)

		// Sending a response after last request was completed should do nothing
		requestResponder.Respond(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(true),
		})

		// Let the goroutine send the message
		time.Sleep(10 * time.Millisecond)

		So(requestResponder.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)
		time.Sleep(750 * time.Millisecond)
		So(requestResponder.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)
	})
}
