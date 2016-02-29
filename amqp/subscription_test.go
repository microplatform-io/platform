package amqp

import (
	"testing"
	"time"

	"github.com/microplatform-io/platform"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"
)

func TestSubscriptionCanHandle(t *testing.T) {
	Convey("A subscription without a topic should be able to handle any delivery", t, func() {
		subscription := &subscription{}

		So(subscription.canHandle(amqp.Delivery{}), ShouldBeTrue)

		So(subscription.canHandle(amqp.Delivery{
			RoutingKey: "whatever",
		}), ShouldBeTrue)
	})

	Convey("A subscription with a matching topic should be able to handle a matching delivery", t, func() {
		subscription := &subscription{
			topic: "success",
		}

		So(subscription.canHandle(amqp.Delivery{
			RoutingKey: "failure",
		}), ShouldBeFalse)

		So(subscription.canHandle(amqp.Delivery{
			RoutingKey: "success",
		}), ShouldBeTrue)
	})
}

func TestSubscriptionRunWorker(t *testing.T) {
	Convey("Running a subscription with a msg chan that immediately closes should return", t, func() {
		subscription := &subscription{
			deliveries: make(chan amqp.Delivery),
		}

		runEnded := make(chan interface{})

		go func() {
			subscription.runWorker()
			close(runEnded)
		}()

		time.Sleep(10 * time.Millisecond)

		So(subscription.totalWorkers, ShouldEqual, 1)

		subscription.Close()

		select {
		case <-runEnded:

		case <-time.After(1 * time.Second):
			t.Error("subscription did not finish running in a reasonable amount of time")
		}

		So(subscription.totalWorkers, ShouldEqual, 0)
	})

	Convey("Running a subscription that receives a message should call the handler", t, func() {
		totalInvocations := 0

		subscription := &subscription{
			handler: platform.ConsumerHandlerFunc(func(msg []byte) error {
				totalInvocations += 1

				return nil
			}),
			deliveries: make(chan amqp.Delivery),
		}

		So(totalInvocations, ShouldEqual, 0)

		go func() {
			subscription.deliveries <- amqp.Delivery{}
			subscription.deliveries <- amqp.Delivery{}
			close(subscription.deliveries)
		}()

		runEnded := make(chan interface{})

		go func() {
			subscription.runWorker()
			close(runEnded)
		}()

		select {
		case <-runEnded:

		case <-time.After(1 * time.Second):
			t.Error("subscription did not finish running in a reasonable amount of time")
		}

		So(totalInvocations, ShouldEqual, 2)
	})
}

func TestNewSubscription(t *testing.T) {
	Convey("A new subscription should run a total of 'MAX_WORKERS' workers", t, func() {
		subscription := newSubscription("testing-topic", platform.ConsumerHandlerFunc(func(body []byte) error {
			return nil
		}))

		So(subscription.totalWorkers, ShouldEqual, 0)

		time.Sleep(time.Millisecond)

		So(subscription.totalWorkers, ShouldEqual, MAX_WORKERS)
	})
}
