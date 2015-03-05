package middleware //

import (
	"errors"
	"github.com/microplatform-io/platform"
	"github.com/microplatform-io/platform/context"
)

const platformRequestKey = "platform-request"

func HandleRequest(next platform.Handler) platform.Handler {
	return platform.HandlerFunc(func(routedMessage *platform.RoutedMessage) (*platform.RoutedMessage, error) {
		request := &platform.Request{}
		if err := platform.Unmarshal(routedMessage.GetBody(), request); err != nil {
			body, _ := platform.Marshal(&platform.Error{
				Message: platform.String("Failed to unmarshal platform request"),
			})

			return &platform.RoutedMessage{
				Method:   platform.Int32(int32(platform.Method_REPLY)),
				Resource: platform.Int32(int32(platform.Resource_ERROR)),
				Body:     body,
			}, nil
		}

		SetRequestToContext(routedMessage, request)
		return next.HandleRoutedMessage(routedMessage)
	})
}

func SetRequestToContext(routedMessage *platform.RoutedMessage, request *platform.Request) {
	context.Set(routedMessage, platformRequestKey, request)
}

func GetRequestFromContext(routedMessage *platform.RoutedMessage) (*platform.Request, error) {
	r := context.Get(routedMessage, platformRequestKey)
	if r == nil {
		return nil, errors.New("Error from context")
	}

	request, ok := r.(*platform.Request)
	if ok == false {
		return nil, errors.New("non cloud request")
	}

	return request, nil

}
