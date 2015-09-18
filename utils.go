package platform

import (
	"net/url"
	"os"
)

func GenerateResponse(request *Request, response *Request) *Request {
	response.Uuid = request.Uuid

	if response.Routing == nil {
		response.Routing = &Routing{}
	}

	response.Routing.RouteTo = append(response.Routing.RouteTo, request.Routing.RouteFrom...)
	response.Routing.RouteFrom = []*Route{}

	return response
}

func Getenv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

func RouteToSchemeMatches(request *Request, scheme string) bool {
	if request.Routing == nil {
		return false
	}

	if len(request.Routing.RouteTo) <= 0 {
		return false
	}

	targetUri, err := url.Parse(request.Routing.RouteTo[0].GetUri())
	if err != nil {
		return false
	}

	return targetUri.Scheme == scheme
}
