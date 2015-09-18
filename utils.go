package platform

import "net/url"

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
