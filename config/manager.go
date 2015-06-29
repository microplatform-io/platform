package config

import (
	"errors"
	"github.com/coreos/go-etcd/etcd"
	"os"
	"strings"
)

type ServiceConfig struct {
	User  string
	Pass  string
	Index string
	Addr  string
	Port  string
	Extra map[string]string
}

func (sc *ServiceConfig) Set(key, value string) {
	switch strings.ToUpper(key) {
	case SERVICE_VARIABLE_KEY_USER:
		sc.User = value

	case SERVICE_VARIABLE_KEY_PASS:
		sc.Pass = value

	case SERVICE_VARIABLE_KEY_ADDR:
		sc.Addr = value

	case SERVICE_VARIABLE_KEY_PORT:
		sc.Port = value
	}
}

type ServiceVariableKey struct {
	Name  string
	Index string
	Port  string
	Key   string
}

type ConfigManager interface {
	GetServiceConfigs(serviceName, servicePort string) ([]*ServiceConfig, error)
}

const (
	SERVICE_VARIABLE_KEY_USER = "USER"
	SERVICE_VARIABLE_KEY_PASS = "PASS"
	SERVICE_VARIABLE_KEY_ADDR = "TCP_ADDR"
	SERVICE_VARIABLE_KEY_PORT = "TCP_PORT"
)

var (
	NoServiceConfigs             = errors.New("No service configs could be discovered")
	InvalidServiceVariableFormat = errors.New("Invalid service variable format, must match '{SERVICE}_{INDEX}_PORT_{PORT}_TCP_{TCP_KEY}'")
)

func parseServiceVariableKeyString(serviceVariableKeyString string) (*ServiceVariableKey, error) {
	if len(serviceVariableKeyString) <= 0 {
		return nil, InvalidServiceVariableFormat
	}

	serviceVariableKeyParts := strings.Split(serviceVariableKeyString, "_")

	var (
		serviceIndex       string
		serviceIndexOffset int
	)

	if strings.Contains(serviceVariableKeyString, "_TCP_") {
		switch len(serviceVariableKeyParts) {
		case 5:
			// Service does not have an index, should assume 1
			serviceIndex = "1"
			serviceIndexOffset = 0
		case 6:
			serviceIndex = serviceVariableKeyParts[1]
			serviceIndexOffset = 1
		default:
			return nil, InvalidServiceVariableFormat
		}
	} else if strings.Contains(serviceVariableKeyString, "_USER") || strings.Contains(serviceVariableKeyString, "_PASS") {
		switch len(serviceVariableKeyParts) {
		case 4:
			// Service does not have an index, should assume 1
			serviceIndex = "1"
			serviceIndexOffset = 0
		case 5:
			serviceIndex = serviceVariableKeyParts[1]
			serviceIndexOffset = 1
		default:
			serviceIndex = ""
			serviceIndexOffset = -2
		}
	}

	return &ServiceVariableKey{
		Name:  serviceVariableKeyParts[0],
		Index: serviceIndex,
		Port:  serviceVariableKeyParts[2+serviceIndexOffset],
		Key:   strings.Join(serviceVariableKeyParts[3+serviceIndexOffset:], "_"), // Everything after the port is the key
	}, nil
}

type ArrayConfigManager struct {
	serviceVariableStrings []string
}

func (acm *ArrayConfigManager) GetServiceConfigs(serviceName, servicePort string) ([]*ServiceConfig, error) {
	serviceConfigsMap := map[string]*ServiceConfig{}

	defaultUser := ""
	defaultPass := ""

	for _, serviceVariableString := range acm.serviceVariableStrings {
		keyValueParts := strings.SplitN(serviceVariableString, "=", 2)

		key := keyValueParts[0]
		value := keyValueParts[1]

		serviceVariableKey, err := parseServiceVariableKeyString(key)
		if err != nil {
			continue
		}

		if serviceVariableKey.Index == "" {
			switch serviceVariableKey.Key {
			case SERVICE_VARIABLE_KEY_USER:
				defaultUser = value
			case SERVICE_VARIABLE_KEY_PASS:
				defaultPass = value
			}
		}

		if serviceVariableKey.Name != serviceName {
			continue
		}

		if serviceVariableKey.Port != servicePort {
			continue
		}

		serviceConfig, exists := serviceConfigsMap[serviceVariableKey.Index]
		if !exists {
			serviceConfig = &ServiceConfig{
				Index: serviceVariableKey.Index,
			}
			serviceConfigsMap[serviceVariableKey.Index] = serviceConfig
		}

		serviceConfig.Set(serviceVariableKey.Key, value)
	}

	serviceConfigs := []*ServiceConfig{}
	for _, serviceConfig := range serviceConfigsMap {
		if serviceConfig.User == "" {
			serviceConfig.User = defaultUser
		}

		if serviceConfig.Pass == "" {
			serviceConfig.Pass = defaultPass
		}

		serviceConfigs = append(serviceConfigs, serviceConfig)
	}

	if len(serviceConfigs) <= 0 {
		return nil, NoServiceConfigs
	}

	return serviceConfigs, nil
}

func NewArrayConfigManager(serviceVariableStrings []string) (*ArrayConfigManager, error) {
	return &ArrayConfigManager{
		serviceVariableStrings: serviceVariableStrings,
	}, nil
}

func NewEnvConfigManager() (*ArrayConfigManager, error) {
	return NewArrayConfigManager(os.Environ())
}

func getEtcdBasename(key string) string {
	keyParts := strings.Split(key, "/")

	return keyParts[len(keyParts)-1]
}

type EtcdConfigManager struct {
	*ServiceConfig

	client *etcd.Client
}

func (ecm *EtcdConfigManager) GetServiceConfigs(serviceName, servicePort string) ([]*ServiceConfig, error) {
	response, err := ecm.client.Get(serviceName+"/"+servicePort, false, true)
	if err != nil {
		return nil, err
	}

	defaultUser := ""
	defaultPass := ""

	serviceConfigs := []*ServiceConfig{}

	for _, serviceRootNode := range response.Node.Nodes {
		baseIndex := strings.ToUpper(getEtcdBasename(serviceRootNode.Key))

		switch baseIndex {
		case SERVICE_VARIABLE_KEY_USER:
			defaultUser = serviceRootNode.Value
		case SERVICE_VARIABLE_KEY_PASS:
			defaultPass = serviceRootNode.Value
		default:
			serviceConfig := &ServiceConfig{
				Index: baseIndex,
			}

			for _, serviceAttrNode := range serviceRootNode.Nodes {
				serviceConfig.Set(getEtcdBasename(serviceAttrNode.Key), serviceAttrNode.Value)
			}

			serviceConfigs = append(serviceConfigs, serviceConfig)
		}
	}

	for _, serviceConfig := range serviceConfigs {
		if serviceConfig.User == "" {
			serviceConfig.User = defaultUser
		}

		if serviceConfig.Pass == "" {
			serviceConfig.Pass = defaultPass
		}
	}

	return serviceConfigs, nil
}

func NewEtcdConfigManager(serviceConfig *ServiceConfig) (*EtcdConfigManager, error) {
	if serviceConfig == nil {
		return nil, errors.New("Invalid service config provded")
	}

	client := etcd.NewClient([]string{"http://" + serviceConfig.Addr + ":" + serviceConfig.Port})

	return &EtcdConfigManager{
		ServiceConfig: serviceConfig,
		client:        client,
	}, nil
}
