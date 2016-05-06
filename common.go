package platform

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kr/pretty"

	"github.com/Sirupsen/logrus"

	"github.com/pborman/uuid"
)

var (
	cachedIp string

	loggers      = map[string]*Logger{}
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
		logrus.SetLevel(logrus.InfoLevel)
	}

	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&DefaultFormatter{"-----"})
}

func CreateUUID() string {
	return uuid.New()
}

func GenerateResponse(request *Request, response *Request) *Request {
	response.Uuid = request.Uuid

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

type Logger struct {
	*logrus.Logger
}

func (l *Logger) Print(v ...interface{}) {
	l.Debug(v...)
}

func (l *Logger) Printf(format string, v ...interface{}) {
	l.Debugf(format, v...)
}

func (l *Logger) Println(v ...interface{}) {
	l.Debugln(v...)
}

func (l *Logger) PrettyPrint(a ...interface{}) {
	l.Debugln(pretty.Sprint(a...))
}

type DefaultFormatter struct {
	prefix string
}

func (f *DefaultFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	return []byte(fmt.Sprintln("["+f.prefix+"]", entry.Time.Format("01/02/2006 15:04:05.000"), entry.Message)), nil
}

func GetLogger(prefix string) *Logger {
	loggersMutex.Lock()
	defer loggersMutex.Unlock()

	if logger, exists := loggers[prefix]; exists {
		return logger
	}

	logger := logrus.New()
	logger.Formatter = &DefaultFormatter{prefix}
	logger.Level = logrus.GetLevel()
	logger.Out = os.Stdout

	loggers[prefix] = &Logger{logger}

	return loggers[prefix]
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
