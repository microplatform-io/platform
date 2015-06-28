package config

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestArrayConfigManager(t *testing.T) {
	Convey("Fetching a service without any environment variables set should return an error", t, func() {
		configManager, err := NewArrayConfigManager([]string{})
		So(err, ShouldBeNil)
		So(configManager, ShouldNotBeNil)

		serviceConfigs, err := configManager.FetchServiceConfigs("RABBITMQ", "5672")
		So(err, ShouldEqual, NoServiceConfigs)
		So(len(serviceConfigs), ShouldEqual, 0)
	})

	Convey("Fetching a service config that has values set should return a service config", t, func() {
		configManager, err := NewArrayConfigManager([]string{
			"RABBITMQ_1_PORT_5672_TCP_ADDR=127.0.0.1",
			"RABBITMQ_1_PORT_5672_TCP_PORT=5672",
		})
		So(err, ShouldBeNil)
		So(configManager, ShouldNotBeNil)

		serviceConfigs, err := configManager.FetchServiceConfigs("RABBITMQ", "5672")
		So(err, ShouldBeNil)
		So(len(serviceConfigs), ShouldEqual, 1)
		So(serviceConfigs, ShouldResemble, []*ServiceConfig{
			&ServiceConfig{
				User:  "",
				Pass:  "",
				Index: "1",
				Addr:  "127.0.0.1",
				Port:  "5672",
			},
		})
	})

	Convey("A service at an unspecified index should assume index 1, and not return duplicates", t, func() {
		configManager, err := NewArrayConfigManager([]string{
			"RABBITMQ_PORT_5672_TCP_ADDR=127.0.0.1",
			"RABBITMQ_PORT_5672_TCP_PORT=5672",
			"RABBITMQ_1_PORT_5672_TCP_ADDR=127.0.0.1",
			"RABBITMQ_1_PORT_5672_TCP_PORT=5672",
		})
		So(err, ShouldBeNil)
		So(configManager, ShouldNotBeNil)

		serviceConfigs, err := configManager.FetchServiceConfigs("RABBITMQ", "5672")
		So(err, ShouldBeNil)
		So(serviceConfigs, ShouldResemble, []*ServiceConfig{
			&ServiceConfig{
				User:  "",
				Pass:  "",
				Index: "1",
				Addr:  "127.0.0.1",
				Port:  "5672",
			},
		})
	})

	Convey("When a user and pass is set at the service index level, it should be included on the service", t, func() {
		configManager, err := NewArrayConfigManager([]string{
			"RABBITMQ_1_PORT_5672_USER=user",
			"RABBITMQ_1_PORT_5672_PASS=pass",
			"RABBITMQ_1_PORT_5672_TCP_ADDR=127.0.0.1",
			"RABBITMQ_1_PORT_5672_TCP_PORT=5672",
		})
		So(err, ShouldBeNil)
		So(configManager, ShouldNotBeNil)

		serviceConfigs, err := configManager.FetchServiceConfigs("RABBITMQ", "5672")
		So(err, ShouldBeNil)
		So(serviceConfigs, ShouldResemble, []*ServiceConfig{
			&ServiceConfig{
				User:  "user",
				Pass:  "pass",
				Index: "1",
				Addr:  "127.0.0.1",
				Port:  "5672",
			},
		})
	})

	Convey("When a user and pass is set at the root level, it should be included on every service", t, func() {
		configManager, err := NewArrayConfigManager([]string{
			"RABBITMQ_USER=user",
			"RABBITMQ_PASS=pass",
			"RABBITMQ_1_PORT_5672_TCP_ADDR=127.0.0.1",
			"RABBITMQ_1_PORT_5672_TCP_PORT=5672",
			"RABBITMQ_2_PORT_5672_TCP_ADDR=127.0.0.1",
			"RABBITMQ_2_PORT_5672_TCP_PORT=5672",
		})
		So(err, ShouldBeNil)
		So(configManager, ShouldNotBeNil)

		serviceConfigs, err := configManager.FetchServiceConfigs("RABBITMQ", "5672")
		So(err, ShouldBeNil)
		So(len(serviceConfigs), ShouldEqual, 2)
		So(serviceConfigs, ShouldResemble, []*ServiceConfig{
			&ServiceConfig{
				User:  "user",
				Pass:  "pass",
				Index: "1",
				Addr:  "127.0.0.1",
				Port:  "5672",
			},
			&ServiceConfig{
				User:  "user",
				Pass:  "pass",
				Index: "2",
				Addr:  "127.0.0.1",
				Port:  "5672",
			},
		})
	})

	Convey("A service at an unspecified index should assume index 1, and should include additional service indices", t, func() {
		configManager, err := NewArrayConfigManager([]string{
			"RABBITMQ_PORT_5672_TCP_ADDR=127.0.0.1",
			"RABBITMQ_PORT_5672_TCP_PORT=5672",
			"RABBITMQ_2_PORT_5672_TCP_ADDR=127.0.0.2",
			"RABBITMQ_2_PORT_5672_TCP_PORT=5672",
		})
		So(err, ShouldBeNil)
		So(configManager, ShouldNotBeNil)

		serviceConfigs, err := configManager.FetchServiceConfigs("RABBITMQ", "5672")
		So(err, ShouldBeNil)
		So(len(serviceConfigs), ShouldEqual, 2)
		So(serviceConfigs, ShouldResemble, []*ServiceConfig{
			&ServiceConfig{
				User:  "",
				Pass:  "",
				Index: "1",
				Addr:  "127.0.0.1",
				Port:  "5672",
			},
			&ServiceConfig{
				User:  "",
				Pass:  "",
				Index: "2",
				Addr:  "127.0.0.2",
				Port:  "5672",
			},
		})
	})
}

func TestParseServiceVariableString(t *testing.T) {
	Convey("Parsing an service variable key should return an error", t, func() {
		serviceVariableKey, err := parseServiceVariableKeyString("")
		So(serviceVariableKey, ShouldBeNil)
		So(err, ShouldEqual, InvalidServiceVariableFormat)
	})

	Convey("Parsing a valid indexed service should produce a valid service variable key", t, func() {
		serviceVariableKey, err := parseServiceVariableKeyString("RABBITMQ_1_PORT_5672_TCP_ADDR")
		So(err, ShouldBeNil)
		So(serviceVariableKey, ShouldNotBeNil)
		So(serviceVariableKey, ShouldResemble, &ServiceVariableKey{
			Name:  "RABBITMQ",
			Index: "1",
			Port:  "5672",
			Key:   "TCP_ADDR",
		})
	})

	Convey("Parsing a valid indexed service should produce a valid service variable key", t, func() {
		serviceVariableKey, err := parseServiceVariableKeyString("RABBITMQ_1_PORT_5672_USER")
		So(err, ShouldBeNil)
		So(serviceVariableKey, ShouldNotBeNil)
		So(serviceVariableKey, ShouldResemble, &ServiceVariableKey{
			Name:  "RABBITMQ",
			Index: "1",
			Port:  "5672",
			Key:   "USER",
		})
	})
}
