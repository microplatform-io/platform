package platform

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMultiSubscriber(t *testing.T) {
	Convey("Attempting to run a multi subscriber that has no subscribers should panic", t, func() {
		multiSubscriber := NewMultiSubscriber(nil)
		So(multiSubscriber.Run, ShouldPanic)
	})

	Convey("A multi subscriber should cascade calls to all subscribers", t, func() {
		mockSubscriber1 := newMockSubscriber()
		mockSubscriber2 := newMockSubscriber()

		multiSubscriber := NewMultiSubscriber([]Subscriber{mockSubscriber1, mockSubscriber2})
		So(multiSubscriber, ShouldNotBeNil)
		So(mockSubscriber1.totalRunCalls, ShouldEqual, 0)
		So(mockSubscriber2.totalRunCalls, ShouldEqual, 0)

		multiSubscriber.Run()
		So(mockSubscriber1.totalRunCalls, ShouldEqual, 1)
		So(mockSubscriber2.totalRunCalls, ShouldEqual, 1)

		multiSubscriber.Subscribe("testing", ConsumerHandlerFunc(func(body []byte) error {
			return nil
		}))

		So(mockSubscriber1.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"testing": 1,
		})
		So(mockSubscriber2.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"testing": 1,
		})
	})
}
