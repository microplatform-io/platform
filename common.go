package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
)

var (
	cachedIp string

	loggers      = map[string]*logrus.Logger{}
	loggersMutex = sync.Mutex{}

	logger                  = GetLogger("platform")
	PREVENT_PLATFORM_PANICS = Getenv("PLATFORM_PREVENT_PANICS", "1") == "1"
)

func init() {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	default:
		logrus.SetLevel(logrus.DebugLevel)
	}

	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&DefaultFormatter{})
}

func CreateUUID() string {
	return uuid.New()
}

func generateResponse(request *Request, response *Request) *Request {
	response.Uuid = request.Uuid
	response.Trace = request.Trace

	if response.Routing == nil {
		response.Routing = &Routing{}
	}

	if response.Routing.RouteTo == nil {
		response.Routing.RouteTo = []*Route{}
	}

	if request.Routing != nil && request.Routing.RouteFrom != nil {
		response.Routing.RouteTo = append(response.Routing.RouteTo, request.Routing.RouteFrom...)
	}

	response.Routing.RouteFrom = []*Route{}

	return response
}

func Getenv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

// Copied from logrus to prevent field naming conflicts in the JSON formatting
func prefixFieldClashes(data logrus.Fields) {
	if t, ok := data["timestamp"]; ok {
		data["fields.timestamp"] = t
	}

	if m, ok := data["msg"]; ok {
		data["fields.msg"] = m
	}

	if l, ok := data["severity"]; ok {
		data["fields.severity"] = l
	}
}

type DefaultFormatter struct {
}

func (f *DefaultFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	data := make(logrus.Fields, len(entry.Data)+3)
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/Sirupsen/logrus/issues/137
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}
	prefixFieldClashes(data)

	data["timestamp"] = entry.Time.Format("01/02/2006 15:04:05.000")
	data["msg"] = entry.Message
	data["severity"] = entry.Level.String()

	logBytes := []byte{}

	if os.Getenv("LOG_PRETTY_PRINT") == "true" {
		b, err := json.MarshalIndent(data, "", "     ")
		if err != nil {
			return nil, fmt.Errorf("Failed to MarshalIndent fields to JSON: %s", err)
		}

		logBytes = b
	} else {
		buffer := bytes.Buffer{}
		if err := json.NewEncoder(&buffer).Encode(data); err != nil {
			return nil, fmt.Errorf("Failed to marshal fields to JSON, %v", err)
		}

		logBytes = buffer.Bytes()
	}

	return logBytes, nil
}

func GetLogger(prefix string) *logrus.Logger {
	logger := logrus.New()
	if strings.ToLower(os.Getenv("LOG_FORMATTER")) == "text" {
		logger.Formatter = &logrus.TextFormatter{}
	} else {
		logger.Formatter = &DefaultFormatter{}
	}
	logger.Level = logrus.GetLevel()
	logger.Out = os.Stdout

	return logger
}

func getMyIp(client *http.Client, timeout time.Duration) (string, error) {
	urls := []string{"http://ifconfig.me/ip", "http://curlmyip.com", "http://icanhazip.com"}
	respChan := make(chan *http.Response)

	for _, url := range urls {
		go func(url string, responseChan chan *http.Response) {
			res, err := client.Get(url)
			if err == nil {
				responseChan <- res
			}
		}(url, respChan)
	}

	select {
	case res := <-respChan:
		defer res.Body.Close()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return "", err
		}

		cachedIp = strings.Trim(string(body), "\n ")

		return cachedIp, nil
	case <-time.After(timeout):
		return "", errors.New("Timed out trying to fetch ip address.")
	}
}

func GetMyIp() (string, error) {
	if cachedIp != "" {
		return cachedIp, nil
	}

	return getMyIp(http.DefaultClient, 5*time.Second)
}

func IsInternalRequest(request *Request) bool {
	if request.Routing == nil {
		return false
	}

	if request.Routing.RouteFrom == nil {
		return false
	}

	if len(request.Routing.RouteFrom) <= 1 {
		return false
	}

	// If the second to last routing has the microservice:/// prefix, it's internal
	if strings.HasPrefix(request.Routing.RouteFrom[len(request.Routing.RouteFrom)-2].GetUri(), "microservice:///") {
		return true
	}

	// If the last routing has the microservice:/// prefix, it's internal
	if strings.HasPrefix(request.Routing.RouteFrom[len(request.Routing.RouteFrom)-1].GetUri(), "microservice:///") {
		return true
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
