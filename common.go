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

	"code.google.com/p/go-uuid/uuid"
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

func GetMyIp() (string, error) {
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
		return strings.Trim(string(body), "\n "), nil
	case <-time.After(time.Second * 5):
		return "", errors.New("Timed out trying to fetch ip address.")
	}
}
