package amqp

import (
	"errors"
	"testing"
	"time"

	"github.com/microplatform-io/platform"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSubscriberQueueBind(t *testing.T) {
	Convey("Binding a subscription on a nil channel interface should return an error", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		bindErr := subscriber.queueBind(nil)
		So(bindErr.Error(), ShouldResemble, "Nil channel interface")
	})

	Convey("Binding a subscriber without any subscriptions should not actually trigger any binds at the channel level", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		ch := &mockChannel{
			mockQueueBinds: []mockQueueBind{},
		}

		So(ch.mockQueueBinds, ShouldResemble, []mockQueueBind{})

		bindErr := subscriber.queueBind(ch)
		So(bindErr, ShouldBeNil)

		So(ch.mockQueueBinds, ShouldResemble, []mockQueueBind{})
	})

	Convey("Binding a subscriber on a channel interface return the error if the channel returns one", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		subscriber.Subscribe("testing-topic", nil)

		ch := &mockChannel{
			mockQueueBinds: []mockQueueBind{},
		}

		ch.errorOnQueueBind(errors.New("FAILED TO BIND"))

		So(ch.mockQueueBinds, ShouldResemble, []mockQueueBind{})

		bindErr := subscriber.queueBind(ch)
		So(bindErr, ShouldResemble, errors.New("FAILED TO BIND"))

		So(ch.mockQueueBinds, ShouldResemble, []mockQueueBind{})
	})

	Convey("Binding a subscriber with a single subscription should trigger a single bind at the channel level", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		subscriber.Subscribe("testing-topic", nil)

		ch := &mockChannel{
			mockQueueBinds: []mockQueueBind{},
		}

		So(ch.mockQueueBinds, ShouldResemble, []mockQueueBind{})

		bindErr := subscriber.queueBind(ch)
		So(bindErr, ShouldBeNil)

		So(ch.mockQueueBinds, ShouldResemble, []mockQueueBind{
			mockQueueBind{
				name:     "testing-queue",
				key:      "testing-topic",
				exchange: "amq.topic",
				noWait:   false,
				args:     nil,
			},
		})
	})
}

func TestSubscriberQueueDeclare(t *testing.T) {
	Convey("Declaring a queue on a nil channel interface should return an error", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		declareErr := subscriber.queueDeclare(nil)
		So(declareErr.Error(), ShouldResemble, "Nil channel interface")
	})

	Convey("Declaring a queue without any flags intentionally set should establish a set of default values for the declare args", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		ch := &mockChannel{
			mockQueueDeclares: []mockQueueDeclare{},
		}

		So(ch.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{})

		declareErr := subscriber.queueDeclare(ch)
		So(declareErr, ShouldBeNil)

		So(ch.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{
			mockQueueDeclare{
				name:       "testing-queue",
				durable:    true,
				autoDelete: false,
				exclusive:  false,
				noWait:     false,
				args:       nil,
			},
		})
	})

	Convey("Setting exclusive to true should make the queue exclusive but not durable", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		subscriber.exclusive = true

		ch := &mockChannel{
			mockQueueDeclares: []mockQueueDeclare{},
		}

		So(ch.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{})

		declareErr := subscriber.queueDeclare(ch)
		So(declareErr, ShouldBeNil)

		So(ch.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{
			mockQueueDeclare{
				name:       "testing-queue",
				durable:    false,
				autoDelete: false,
				exclusive:  true,
				noWait:     false,
				args:       nil,
			},
		})
	})

	Convey("Setting autoDelete to true should set the autoDelete flag to true", t, func() {
		subscriber, err := NewSubscriber(nil, "testing-queue")
		So(subscriber, ShouldNotBeNil)
		So(err, ShouldBeNil)

		subscriber.autoDelete = true

		ch := &mockChannel{
			mockQueueDeclares: []mockQueueDeclare{},
		}

		So(ch.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{})

		declareErr := subscriber.queueDeclare(ch)
		So(declareErr, ShouldBeNil)

		So(ch.mockQueueDeclares, ShouldResemble, []mockQueueDeclare{
			mockQueueDeclare{
				name:       "testing-queue",
				durable:    true,
				autoDelete: true,
				exclusive:  false,
				noWait:     false,
				args:       nil,
			},
		})
	})
}

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
			time.Sleep(10 * time.Millisecond)
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

		testingTopic1Delivery := &mockDelivery{
			RoutingKey: "testing-topic-1",
		}

		testingTopic2Delivery := &mockDelivery{
			RoutingKey: "testing-topic-2",
		}

		badRandomDelivery := &mockDelivery{
			RoutingKey: "unmatched-topic",
		}

		go func() {
			<-mockDialer.connected

			time.Sleep(10 * time.Millisecond)

			mockDialer.connection.channel.mockDeliveries <- testingTopic1Delivery
			mockDialer.connection.channel.mockDeliveries <- testingTopic2Delivery
			mockDialer.connection.channel.mockDeliveries <- badRandomDelivery

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
		So(testingTopic1Delivery.acked, ShouldBeTrue)
		So(testingTopic1Delivery.ackMultiple, ShouldBeFalse)
		So(testingTopic2Delivery.rejected, ShouldBeFalse)

		So(testingTopic2Called, ShouldBeTrue)
		So(testingTopic2Delivery.acked, ShouldBeTrue)
		So(testingTopic2Delivery.ackMultiple, ShouldBeFalse)
		So(testingTopic2Delivery.rejected, ShouldBeFalse)

		So(badRandomDelivery.acked, ShouldBeTrue)
		So(badRandomDelivery.ackMultiple, ShouldBeFalse)
		So(badRandomDelivery.rejected, ShouldBeFalse)

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
			time.Sleep(10 * time.Millisecond)
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
			time.Sleep(10 * time.Millisecond)
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
