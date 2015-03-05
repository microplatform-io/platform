package platform

import (
	"code.google.com/p/go-uuid/uuid"
	"log"
	"os"
	"sync"
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
