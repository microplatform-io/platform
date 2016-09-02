package platform

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateUuid(t *testing.T) {
	Convey("Creating a uuid should produce a non empty string", t, func() {
		uuid := CreateUUID()
		So(uuid, ShouldNotEqual, "")
	})

	Convey("Multiple uuids should be unique", t, func() {
		uuid1 := CreateUUID()
		So(uuid1, ShouldNotEqual, "")

		uuid2 := CreateUUID()
		So(uuid2, ShouldNotEqual, "")

		So(uuid1, ShouldNotEqual, uuid2)
	})
}

func TestGenerateResponse(t *testing.T) {
	Convey("Generating a response from an empty request object should populate empty routing", t, func() {
		response := generateResponse(&Request{}, &Request{})
		So(response, ShouldResemble, &Request{
			Routing: &Routing{
				RouteTo:   []*Route{},
				RouteFrom: []*Route{},
			},
		})
	})

	Convey("Generating a response from an should place the route from of the request onto the route to of the response", t, func() {
		response := generateResponse(&Request{
			Routing: &Routing{
				RouteFrom: []*Route{
					&Route{
						Uri: String("testing-route-from"),
					},
				},
			},
		}, &Request{})
		So(response, ShouldResemble, &Request{
			Routing: &Routing{
				RouteTo: []*Route{
					&Route{
						Uri: String("testing-route-from"),
					},
				},
				RouteFrom: []*Route{},
			},
		})
	})

	Convey("Generating a response from an should place the route from of the request onto the route to of the response", t, func() {
		response := generateResponse(&Request{
			Routing: &Routing{
				RouteFrom: []*Route{
					&Route{
						Uri: String("testing-route-from"),
					},
				},
			},
		}, &Request{
			Routing: &Routing{
				RouteTo: []*Route{
					&Route{
						Uri: String("testing-route-to"),
					},
				},
			},
		})
		So(response, ShouldResemble, &Request{
			Routing: &Routing{
				RouteTo: []*Route{
					&Route{
						Uri: String("testing-route-to"),
					},
					&Route{
						Uri: String("testing-route-from"),
					},
				},
				RouteFrom: []*Route{},
			},
		})
	})
}

func TestGetenv(t *testing.T) {
	Convey("Getting an environment variable that doesn't exist should resort to the default", t, func() {
		So(Getenv("RANDOM_KEY", "DEFAULT_VALUE"), ShouldEqual, "DEFAULT_VALUE")
	})

	Convey("Getting an environment variable that does exist should resort to the value that was set", t, func() {
		os.Setenv("TEST_GET_ENV", "HELLO")

		So(Getenv("TEST_GET_ENV", "DEFAULT_VALUE"), ShouldEqual, "HELLO")
	})
}

func TestGetLogger(t *testing.T) {
	Convey("Get logger should always return a non nil logger", t, func() {
		logger1 := GetLogger("testing")
		So(logger1, ShouldNotBeNil)
	})
}

func TestGetMyIp(t *testing.T) {
	Convey("Fetching our IP should invoke the handler 3 times", t, func() {
		var totalCalls int32

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&totalCalls, 1)

			// Just buying some time to let all goroutines run
			time.Sleep(10 * time.Millisecond)

			fmt.Fprint(w, "127.0.0.1")
		}))
		defer s.Close()

		ip, err := getMyIp(&http.Client{
			Transport: &http.Transport{
				Proxy: func(req *http.Request) (*url.URL, error) {
					return url.Parse(s.URL)
				},
			},
		}, 1*time.Second)

		So(ip, ShouldEqual, "127.0.0.1")
		So(err, ShouldBeNil)
		So(totalCalls, ShouldEqual, 3)
	})

	Convey("Forcing a timeout should return an error", t, func() {
		var totalCalls int32

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&totalCalls, 1)

			// Just buying some time to let all goroutines run
			time.Sleep(100 * time.Millisecond)

			fmt.Fprint(w, "127.0.0.1")
		}))
		defer s.Close()

		ip, err := getMyIp(&http.Client{
			Transport: &http.Transport{
				Proxy: func(req *http.Request) (*url.URL, error) {
					return url.Parse(s.URL)
				},
			},
		}, 10*time.Millisecond)

		So(ip, ShouldEqual, "")
		So(err, ShouldResemble, errors.New("Timed out trying to fetch ip address."))
		So(totalCalls, ShouldEqual, 3)
	})
}

func TestIsInternalRequest(t *testing.T) {
	Convey("A request with no routing should not be considered internal", t, func() {
		So(IsInternalRequest(&Request{}), ShouldBeFalse)

		So(IsInternalRequest(&Request{
			Routing: &Routing{},
		}), ShouldBeFalse)
	})

	Convey("A request that the client injected 'microservice' into should not be an internal request", t, func() {
		So(IsInternalRequest(&Request{
			Routing: &Routing{
				RouteFrom: []*Route{
					&Route{
						Uri: String("microservice:///shenanigans"),
					},
					&Route{
						Uri: String("client:///shenanigans"),
					},
					&Route{
						Uri: String("router:///shenanigans"),
					},
				},
			},
		}), ShouldBeFalse)
	})

	Convey("A request that the client that properly comes from a microservice according to the last to routes should be considered internal", t, func() {
		So(IsInternalRequest(&Request{
			Routing: &Routing{
				RouteFrom: []*Route{
					&Route{
						Uri: String("microservice:///shenanigans"),
					},
					&Route{
						Uri: String("client:///shenanigans"),
					},
					&Route{
						Uri: String("router:///shenanigans"),
					},
					&Route{
						Uri: String("microservice:///shenanigans"),
					},
				},
			},
		}), ShouldBeTrue)

		So(IsInternalRequest(&Request{
			Routing: &Routing{
				RouteFrom: []*Route{
					&Route{
						Uri: String("microservice:///shenanigans"),
					},
					&Route{
						Uri: String("client:///shenanigans"),
					},
					&Route{
						Uri: String("router:///shenanigans"),
					},
					&Route{
						Uri: String("microservice:///shenanigans"),
					},
					&Route{
						Uri: String("router:///shenanigans"),
					},
				},
			},
		}), ShouldBeTrue)
	})
}

func TestRouteToSchemeMatches(t *testing.T) {
	Convey("Matching against the route to scheme on a request without routing info should return false", t, func() {
		request := &Request{}

		So(RouteToSchemeMatches(request, "microservice"), ShouldBeFalse)
	})

	Convey("Matching against the route to scheme on a request with an empty route to array should return false", t, func() {
		request := &Request{
			Routing: &Routing{
				RouteTo: []*Route{},
			},
		}

		So(RouteToSchemeMatches(request, "microservice"), ShouldBeFalse)
	})

	Convey("Matching against the route to scheme on a request that has an invalid url should return false", t, func() {
		request := &Request{
			Routing: &Routing{
				RouteTo: []*Route{
					&Route{
						Uri: String("://testing"),
					},
				},
			},
		}

		So(RouteToSchemeMatches(request, "microservice"), ShouldBeFalse)
	})

	Convey("Matching against the route to scheme with a matching request should return true", t, func() {
		request := &Request{
			Routing: &Routing{
				RouteTo: []*Route{
					&Route{
						Uri: String("microservice://testing"),
					},
				},
			},
		}

		So(RouteToSchemeMatches(request, "microservice"), ShouldBeTrue)
	})
}
