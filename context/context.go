package context

import (
	"github.com/microplatform-io/platform"
	"sync"
)

var (
	mutex   = sync.Mutex{}
	Context = map[*platform.RoutedMessage]map[interface{}]interface{}{}
)

func Set(c *platform.RoutedMessage, key, val interface{}) {
	mutex.Lock()
	if Context[c] == nil {
		Context[c] = make(map[interface{}]interface{})
	}
	Context[c][key] = val
	mutex.Unlock()
}

func Get(c *platform.RoutedMessage, key interface{}) interface{} {
	mutex.Lock()
	if platformMessage := Context[c]; platformMessage != nil {
		value := platformMessage[key]
		mutex.Unlock()
		return value
	}

	mutex.Unlock()
	return nil
}
