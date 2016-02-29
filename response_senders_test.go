package platform

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStandardResponseSender(t *testing.T) {
	Convey("Ensure that the response sender uses the publisher", t, func() {
		mockPublisher := newMockPublisher()

		responseSender := NewStandardResponseSender(mockPublisher)

		responseSender.Send(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(false),
		})

		// Wait for the response sender's goroutine
		time.Sleep(10 * time.Millisecond)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)

		responseSender.Send(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(true),
		})

		// Wait for the response sender's goroutine
		time.Sleep(10 * time.Millisecond)

		So(len(mockPublisher.mockPublishes), ShouldEqual, 2)
	})
}

func TestRequestHeartbeatResponseSender(t *testing.T) {
	Convey("Ensure that the response sender uses the publisher", t, func() {
		mockPublisher := newMockPublisher()

		requestHeartbeatResponseSender := NewRequestHeartbeatResponseSender(NewStandardResponseSender(mockPublisher), &Request{})

		// Sending a response that is not completed should still produce heartbeats
		requestHeartbeatResponseSender.Send(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(false),
		})

		// Let the goroutine send the message
		time.Sleep(10 * time.Millisecond)

		// Just by waiting for at least 500 milliseconds, we should see a heartbeat published
		So(requestHeartbeatResponseSender.completed, ShouldBeFalse)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)
		time.Sleep(750 * time.Millisecond)
		So(requestHeartbeatResponseSender.completed, ShouldBeFalse)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 2)

		// By sending a completed response, it should shut down the heartbeats
		requestHeartbeatResponseSender.Send(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(true),
		})

		// Let the goroutine send the message
		time.Sleep(10 * time.Millisecond)

		So(requestHeartbeatResponseSender.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)
		time.Sleep(750 * time.Millisecond)
		So(requestHeartbeatResponseSender.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)

		// Sending a response after last request was completed should do nothing
		requestHeartbeatResponseSender.Send(&Request{
			Routing:   RouteToUri("resource:///teltech/reply/foobar"),
			Completed: Bool(true),
		})

		// Let the goroutine send the message
		time.Sleep(10 * time.Millisecond)

		So(requestHeartbeatResponseSender.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)
		time.Sleep(750 * time.Millisecond)
		So(requestHeartbeatResponseSender.completed, ShouldBeTrue)
		So(len(mockPublisher.mockPublishes), ShouldEqual, 3)
	})
}
