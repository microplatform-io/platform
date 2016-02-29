package platform

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMultiPublisher(t *testing.T) {
	Convey("A multi publisher without any publishers should return an error on publish", t, func() {
		multiPublisher := NewMultiPublisher(nil)
		So(multiPublisher.Publish("testing", []byte{}), ShouldResemble, errors.New("No publishers have been declared in the multi publisher"))
	})

	Convey("A multi publisher should round robin publishes", t, func() {
		mockPublisher1 := newMockPublisher()
		mockPublisher2 := newMockPublisher()

		multiPublisher := NewMultiPublisher([]Publisher{mockPublisher1, mockPublisher2})
		So(multiPublisher, ShouldNotBeNil)
		So(multiPublisher.offset, ShouldEqual, 0)
		So(mockPublisher1.mockPublishes, ShouldResemble, []mockPublish{})
		So(mockPublisher2.mockPublishes, ShouldResemble, []mockPublish{})

		So(multiPublisher.Publish("testing-1", []byte{}), ShouldBeNil)
		So(multiPublisher.offset, ShouldEqual, 1)
		So(mockPublisher1.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				topic: "testing-1",
				body:  []byte{},
			},
		})
		So(mockPublisher2.mockPublishes, ShouldResemble, []mockPublish{})

		So(multiPublisher.Publish("testing-2", []byte{}), ShouldBeNil)
		So(multiPublisher.offset, ShouldEqual, 0)
		So(mockPublisher1.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				topic: "testing-1",
				body:  []byte{},
			},
		})
		So(mockPublisher2.mockPublishes, ShouldResemble, []mockPublish{
			mockPublish{
				topic: "testing-2",
				body:  []byte{},
			},
		})
	})
}
