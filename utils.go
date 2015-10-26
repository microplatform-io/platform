package platform

import (
	"net/url"
	"os"
	"strings"
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

func IsInternalRequest(request *Request) bool {
	if request.Routing == nil {
		return false
	}

	if request.Routing.RouteFrom == nil {
		return false
	}

	for _, route := range request.Routing.RouteFrom {
		if strings.HasPrefix(route.GetUri(), "microservice:///") {
			return true
		}
	}

	return false
}

func RouteToUri(uri string) *Routing {
	return &Routing{
		RouteTo: []*Route{
			&Route{
				Uri: String(uri),
			},
		},
	}
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

func incrementConsumedWorkCount() {
	consumedWorkCountMutex.Lock()
	consumedWorkCount += 1
	consumedWorkCountMutex.Unlock()
}

func decrementConsumedWorkCount() {
	consumedWorkCountMutex.Lock()
	consumedWorkCount -= 1
	consumedWorkCountMutex.Unlock()
}

func getConsumerWorkCount() int {
	consumedWorkCountMutex.Lock()
	defer consumedWorkCountMutex.Unlock()
	return consumedWorkCount
}
