package platform

import (
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pborman/uuid"
)

var (
	loggers      = map[string]*log.Logger{}
	loggersMutex = sync.Mutex{}
)

func CreateUUID() string {
	return uuid.New()
}

func ParseUUID(uuidString string) []byte {
	return uuid.Parse(uuidString)
}

func GetLogger(prefix string) *log.Logger {
	loggersMutex.Lock()
	defer loggersMutex.Unlock()

	if logger, exists := loggers[prefix]; exists {
		return logger
	}

	loggers[prefix] = log.New(os.Stdout, "["+prefix+"] ", log.Ldate|log.Ltime|log.Lmicroseconds)

	return loggers[prefix]
}

var cachedIp string

func GetMyIp() (string, error) {
	if cachedIp != "" {
		return cachedIp, nil
	}

	urls := []string{"http://ifconfig.me/ip", "http://curlmyip.com", "http://icanhazip.com"}
	respChan := make(chan *http.Response)

	for _, url := range urls {
		go func(url string, responseChan chan *http.Response) {
			res, err := http.Get(url)
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
	case <-time.After(time.Second * 5):
		return "", errors.New("Timed out trying to fetch ip address.")
	}
}
