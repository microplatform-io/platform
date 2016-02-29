package amqp

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"
)

func TestNewPublisher(t *testing.T) {
	Convey("Creating a new publisher should connect but ensure no publishes occur", t, func() {
		mockDialer := newMockDialer()

		publisher, err := NewPublisher(mockDialer)
		So(publisher, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 0)
	})
}

func TestPublisherPublish(t *testing.T) {
	Convey("Ensure that publishing records properly", t, func() {
		mockDialer := newMockDialer()

		publisher, err := NewPublisher(mockDialer)
		So(publisher, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 0)

		So(publisher.Publish("testing", []byte{}), ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 1)
		So(mockDialer.connection.channel.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				exchange: "amq.topic",
				key:      "testing",
				msg: amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte{},
				},
			},
		})
	})

	Convey("Ensure that publishing reconnects if necessary", t, func() {
		mockDialer := newMockDialer()

		publisher, err := NewPublisher(mockDialer)
		So(publisher, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 0)

		So(publisher.Publish("testing", []byte{}), ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 1)
		So(mockDialer.connection.channel.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				exchange: "amq.topic",
				key:      "testing",
				msg: amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte{},
				},
			},
		})

		firstConnection := mockDialer.connection

		closeErr := mockDialer.connection.Close()
		So(closeErr, ShouldBeNil)

		So(publisher.Publish("testing", []byte{}), ShouldBeNil)

		// Ensure that a new connection was generated
		So(firstConnection, ShouldNotEqual, mockDialer.connection)

		So(mockDialer.totalDials, ShouldEqual, 2)
		So(firstConnection.channel.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				exchange: "amq.topic",
				key:      "testing",
				msg: amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte{},
				},
			},
		})
		So(mockDialer.connection.channel.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				exchange: "amq.topic",
				key:      "testing",
				msg: amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte{},
				},
			},
		})
	})

	Convey("Ensure that publishing republishes if the publish itself returns an error", t, func() {
		mockDialer := newMockDialer()

		publisher, err := NewPublisher(mockDialer)
		So(publisher, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 0)

		go func() {
			<-mockDialer.connected
			mockDialer.connection.channel.errorOnPublish(errors.New("failed to publish..."))
		}()

		So(publisher.Publish("testing", []byte{}), ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 2)
		So(mockDialer.connection.channel.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				exchange: "amq.topic",
				key:      "testing",
				msg: amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte{},
				},
			},
		})
	})

	Convey("Ensure that publishing republishes if the connection dies AND the publish itself returns an error", t, func() {
		mockDialer := newMockDialer()

		publisher, err := NewPublisher(mockDialer)
		So(publisher, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 0)

		go func() {
			<-mockDialer.connected
			mockDialer.connection.Close()
		}()

		time.Sleep(10 * time.Millisecond)

		So(publisher.Publish("testing", []byte{}), ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 1)

		go func() {
			<-mockDialer.connected
			mockDialer.connection.channel.errorOnPublish(errors.New("failed to publish..."))
		}()

		time.Sleep(10 * time.Millisecond)

		So(publisher.Publish("testing", []byte{}), ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 3)
		So(mockDialer.connection.channel.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				exchange: "amq.topic",
				key:      "testing",
				msg: amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte{},
				},
			},
		})
	})

	Convey("Publishes should fail if the exceed 3 attempts", t, func() {
		mockDialer := newMockDialer()

		publisher, err := NewPublisher(mockDialer)
		So(publisher, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(mockDialer.totalDials, ShouldEqual, 0)

		go func() {
			<-mockDialer.connected
			mockDialer.connection.channel.errorOnPublish(errors.New("failed to publish 1..."))

			go func() {
				<-mockDialer.connected
				mockDialer.connection.channel.errorOnPublish(errors.New("failed to publish 2..."))

				go func() {
					<-mockDialer.connected
					mockDialer.connection.channel.errorOnPublish(errors.New("failed to publish 3..."))
				}()
			}()
		}()

		time.Sleep(10 * time.Millisecond)

		So(publisher.Publish("testing", []byte{}), ShouldResemble, errors.New("failed to publish 3..."))

		// The first connect plus every failure triggering a reconnect
		So(mockDialer.totalDials, ShouldEqual, 4)
		So(mockDialer.connection.channel.mockPublishes, ShouldResemble, []mockPublish{})
	})
}
