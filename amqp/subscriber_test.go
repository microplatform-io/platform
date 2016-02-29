package amqp

import (
	"errors"
	"testing"
	"time"

	"github.com/microplatform-io/platform"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"
)

func TestSubscriberClose(t *testing.T) {
	Convey("Closing a subscriber that has no subscriptions should simply mark itself as closed", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(subscriber.closed, ShouldBeFalse)
		So(subscriber.Close(), ShouldBeNil)
		So(subscriber.closed, ShouldBeTrue)

		// Ensure that repetitive calls don't cause issues
		So(subscriber.Close(), ShouldBeNil)
		So(subscriber.closed, ShouldBeTrue)
	})

	Convey("Closing a subscriber with subscriptions should walk through and close all of its subscriptions", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		subscriber.Subscribe("testing-topic-1", platform.ConsumerHandlerFunc(func(body []byte) error {
			return nil
		}))
		subscriber.Subscribe("testing-topic-2", platform.ConsumerHandlerFunc(func(body []byte) error {
			return nil
		}))

		So(subscriber.closed, ShouldBeFalse)
		for i := range subscriber.subscriptions {
			So(subscriber.subscriptions[i].closed, ShouldBeFalse)
		}

		So(subscriber.Close(), ShouldBeNil)

		So(subscriber.closed, ShouldBeTrue)
		for i := range subscriber.subscriptions {
			So(subscriber.subscriptions[i].closed, ShouldBeTrue)
		}
	})
}

func TestSubscriberRun(t *testing.T) {
	Convey("Running a subscriber with a dialer failure should return that error", t, func() {
		mockDialer := newMockDialer()
		mockDialer.dialErr = errors.New("Testing")

		subscriber, err := NewSubscriber(mockDialer, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(subscriber.run(), ShouldEqual, mockDialer.dialErr)
	})

	Convey("Running a subscriber with no subscriptions should dial, declare, and consume, but not bind", t, func() {
		mockDialer := newMockDialer()

		subscriber, err := NewSubscriber(mockDialer, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		var runErr error

		runEnded := make(chan bool)
		go func() {
			runErr = subscriber.run()
			close(runEnded)
		}()

		go func() {
			<-mockDialer.connected
			mockDialer.connection.Close()
		}()

		select {
		case <-runEnded:

		case <-time.After(1000 * time.Millisecond):
			t.Fatal("subscriber did not stop running in a reasonable amount of time")
		}

		So(runErr, ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 1)
		So(mockDialer.connection.channel.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{
			mockQueueDeclare{
				name:       "testing-queue",
				durable:    true,
				autoDelete: false,
				exclusive:  false,
				noWait:     false,
			},
		})
		So(mockDialer.connection.channel.mockQueueBinds, ShouldResemble, []mockQueueBind{})
		So(mockDialer.connection.channel.mockConsumes, ShouldResemble, []mockConsume{
			mockConsume{
				queue:     "testing-queue",
				consumer:  "",
				autoAck:   false,
				exclusive: false,
				noLocal:   false,
				noWait:    true,
			},
		})
	})

	Convey("Running a subscriber should dial, declare, bind, and consume", t, func() {
		mockDialer := newMockDialer()

		subscriber, err := NewSubscriber(mockDialer, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		testingTopic1Called := false
		testingTopic2Called := false
		testingTopic3Called := false

		subscriber.Subscribe("testing-topic-1", platform.ConsumerHandlerFunc(func(body []byte) error {
			testingTopic1Called = true

			return nil
		}))
		subscriber.Subscribe("testing-topic-2", platform.ConsumerHandlerFunc(func(body []byte) error {
			testingTopic2Called = true

			return nil
		}))
		subscriber.Subscribe("testing-topic-3", platform.ConsumerHandlerFunc(func(body []byte) error {
			testingTopic3Called = true

			return nil
		}))

		var runErr error

		runEnded := make(chan bool)
		go func() {
			runErr = subscriber.run()
			close(runEnded)
		}()

		go func() {
			<-mockDialer.connected

			mockDialer.connection.channel.mockDeliveries <- amqp.Delivery{
				RoutingKey: "testing-topic-1",
			}
			mockDialer.connection.channel.mockDeliveries <- amqp.Delivery{
				RoutingKey: "testing-topic-2",
			}

			mockDialer.connection.Close()
		}()

		select {
		case <-runEnded:

		case <-time.After(1000 * time.Millisecond):
			t.Fatal("subscriber did not stop running in a reasonable amount of time")
		}

		So(runErr, ShouldBeNil)

		// Ensure that 2 of the 3 handlers were called
		So(testingTopic1Called, ShouldBeTrue)
		So(testingTopic2Called, ShouldBeTrue)
		So(testingTopic3Called, ShouldBeFalse)

		So(mockDialer.totalDials, ShouldEqual, 1)
		So(mockDialer.connection.channel.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{
			mockQueueDeclare{
				name:       "testing-queue",
				durable:    true,
				autoDelete: false,
				exclusive:  false,
				noWait:     false,
			},
		})
		So(mockDialer.connection.channel.mockQueueBinds, ShouldResemble, []mockQueueBind{
			mockQueueBind{
				name:     "testing-queue",
				key:      "testing-topic-1",
				exchange: "amq.topic",
				noWait:   false,
			},
			mockQueueBind{
				name:     "testing-queue",
				key:      "testing-topic-2",
				exchange: "amq.topic",
				noWait:   false,
			},
			mockQueueBind{
				name:     "testing-queue",
				key:      "testing-topic-3",
				exchange: "amq.topic",
				noWait:   false,
			},
		})
		So(mockDialer.connection.channel.mockConsumes, ShouldResemble, []mockConsume{
			mockConsume{
				queue:     "testing-queue",
				consumer:  "",
				autoAck:   false,
				exclusive: false,
				noLocal:   false,
				noWait:    true,
			},
		})
	})

	Convey("Closing the connection should cause the run to return", t, func() {
		mockDialer := newMockDialer()

		subscriber, err := NewSubscriber(mockDialer, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		var runErr error

		runEnded := make(chan bool)
		go func() {
			runErr = subscriber.run()
			close(runEnded)
		}()

		go func() {
			<-mockDialer.connected
			mockDialer.connection.Close()
		}()

		select {
		case <-runEnded:

		case <-time.After(1000 * time.Millisecond):
			t.Fatal("subscriber did not stop running in a reasonable amount of time")
		}

		So(runErr, ShouldBeNil)
	})

	Convey("Closing the channel should cause the run to return", t, func() {
		mockDialer := newMockDialer()

		subscriber, err := NewSubscriber(mockDialer, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		var runErr error

		runEnded := make(chan bool)
		go func() {
			runErr = subscriber.run()
			close(runEnded)
		}()

		go func() {
			<-mockDialer.connected
			mockDialer.connection.channel.Close()
		}()

		select {
		case <-runEnded:

		case <-time.After(1000 * time.Millisecond):
			t.Fatal("subscriber did not stop running in a reasonable amount of time")
		}

		So(runErr, ShouldBeNil)
	})

	Convey("Closing the subscriber should cause the run to return", t, func() {
		mockDialer := newMockDialer()

		subscriber, err := NewSubscriber(mockDialer, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		var runErr error

		runEnded := make(chan bool)
		go func() {
			runErr = subscriber.run()
			close(runEnded)
		}()

		go func() {
			<-mockDialer.connected
			subscriber.Close()
		}()

		select {
		case <-runEnded:

		case <-time.After(1000 * time.Millisecond):
			t.Fatal("subscriber did not stop running in a reasonable amount of time")
		}

		So(runErr, ShouldEqual, subscriberClosed)
	})
}
