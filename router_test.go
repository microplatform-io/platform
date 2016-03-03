package platform

import (
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func getStandardRouterPendingResponsesMatchingUuidPrefix(router *StandardRouter, requestUuid string) (string, chan *Request) {
	var (
		actualUuid       string
		pendingResponses chan *Request
	)

	for uuid := range router.pendingResponses {
		if strings.HasPrefix(uuid, requestUuid) {
			actualUuid = uuid
			pendingResponses = router.pendingResponses[uuid]
			break
		}
	}

	return actualUuid, pendingResponses
}

func TestNewStandardRouter(t *testing.T) {
	Convey("Routing to an invalid uri should immediately place a platform error on the responses", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()

		router := NewStandardRouterWithTopic(mockPublisher, mockSubscriber, "testing-router")
		router.SetHeartbeatTimeout(10 * time.Millisecond)

		So(router.pendingResponses, ShouldResemble, map[string]chan *Request{})

		responses, timeout := router.Route(&Request{
			Routing: RouteToUri(":///teltech/get/foobar"),
		})

		select {
		case response := <-responses:
			So(response.GetRouting().GetRouteTo()[0].GetUri(), ShouldEqual, "resource:///platform/reply/error")
		case <-timeout:
			t.Error("We were not expecting a timeout")
		}
	})

	Convey("Verify that successful requests clean up pending responses", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()

		requestUuid := CreateUUID()

		router := NewStandardRouterWithTopic(mockPublisher, mockSubscriber, "testing-router")
		router.SetHeartbeatTimeout(10 * time.Millisecond)

		So(router.pendingResponses, ShouldResemble, map[string]chan *Request{})

		responses, timeout := router.Route(&Request{
			Uuid:    String(requestUuid),
			Routing: RouteToUri("microservice:///teltech/get/foobar"),
		})

		// There should be at least one entry in the pending responses
		So(len(router.pendingResponses), ShouldEqual, 1)
		actualUuid, pendingResponses := getStandardRouterPendingResponsesMatchingUuidPrefix(router, requestUuid)
		So(actualUuid, ShouldNotBeEmpty)
		So(pendingResponses, ShouldNotBeNil)

		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"testing-router": 1,
		})
		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)

		responseBytes, _ := Marshal(&Request{
			Uuid:      String(actualUuid),
			Completed: Bool(true),
		})

		mockSubscriber.topicHandlers["testing-router"][0].HandleMessage(responseBytes)

		// By sending a response, we should not get a timeout
		select {
		case <-responses:
		case <-timeout:
			t.Error("We were not expecting a timeout")
		}

		// Now that we've finished the responses, we should have cleared up the map
		So(len(router.pendingResponses), ShouldEqual, 0)
		actualUuid, pendingResponses = getStandardRouterPendingResponsesMatchingUuidPrefix(router, requestUuid)
		So(actualUuid, ShouldBeEmpty)
		So(pendingResponses, ShouldBeNil)
	})

	Convey("Verify that timeouts cleanup the pending responses", t, func() {
		mockPublisher := newMockPublisher()
		mockSubscriber := newMockSubscriber()

		requestUuid := CreateUUID()

		router := NewStandardRouterWithTopic(mockPublisher, mockSubscriber, "testing-router")
		router.SetHeartbeatTimeout(10 * time.Millisecond)

		So(router.pendingResponses, ShouldResemble, map[string]chan *Request{})

		responses, timeout := router.Route(&Request{
			Uuid:    String(requestUuid),
			Routing: RouteToUri("microservice:///teltech/get/foobar"),
		})

		// There should be at least one entry in the pending responses
		So(len(router.pendingResponses), ShouldEqual, 1)
		actualUuid, pendingResponses := getStandardRouterPendingResponsesMatchingUuidPrefix(router, requestUuid)
		So(actualUuid, ShouldNotBeEmpty)
		So(pendingResponses, ShouldNotBeNil)

		So(mockSubscriber.getTopicTotalHandlers(), ShouldResemble, map[string]int{
			"testing-router": 1,
		})
		So(len(mockPublisher.mockPublishes), ShouldEqual, 1)

		// By not sending a response, we should get a timeout
		select {
		case <-responses:
			t.Error("We were not expecting a response")
		case <-timeout:
		}

		// Now that we've timed out, we should have cleared up the map
		So(len(router.pendingResponses), ShouldEqual, 0)
		actualUuid, pendingResponses = getStandardRouterPendingResponsesMatchingUuidPrefix(router, requestUuid)
		So(actualUuid, ShouldBeEmpty)
		So(pendingResponses, ShouldBeNil)
	})
}
