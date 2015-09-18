// Code generated by protoc-gen-go.
// source: platform.proto
// DO NOT EDIT!

/*
Package platform is a generated protocol buffer package.

It is generated from these files:
	platform.proto

It has these top-level messages:
	Documentation
	DocumentationList
	Error
	IpAddress
	Request
	Route
	Routing
	RouterConfig
	RouterConfigList
	ServiceRoute
*/
package platform

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type IpAddress_Version int32

const (
	IpAddress_V4 IpAddress_Version = 0
	IpAddress_V6 IpAddress_Version = 1
)

var IpAddress_Version_name = map[int32]string{
	0: "V4",
	1: "V6",
}
var IpAddress_Version_value = map[string]int32{
	"V4": 0,
	"V6": 1,
}

func (x IpAddress_Version) Enum() *IpAddress_Version {
	p := new(IpAddress_Version)
	*p = x
	return p
}
func (x IpAddress_Version) String() string {
	return proto.EnumName(IpAddress_Version_name, int32(x))
}
func (x *IpAddress_Version) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(IpAddress_Version_value, data, "IpAddress_Version")
	if err != nil {
		return err
	}
	*x = IpAddress_Version(value)
	return nil
}

type RouterConfig_RouterType int32

const (
	RouterConfig_ROUTER_TYPE_WEBSOCKET RouterConfig_RouterType = 1
	RouterConfig_ROUTER_TYPE_GRPC      RouterConfig_RouterType = 2
)

var RouterConfig_RouterType_name = map[int32]string{
	1: "ROUTER_TYPE_WEBSOCKET",
	2: "ROUTER_TYPE_GRPC",
}
var RouterConfig_RouterType_value = map[string]int32{
	"ROUTER_TYPE_WEBSOCKET": 1,
	"ROUTER_TYPE_GRPC":      2,
}

func (x RouterConfig_RouterType) Enum() *RouterConfig_RouterType {
	p := new(RouterConfig_RouterType)
	*p = x
	return p
}
func (x RouterConfig_RouterType) String() string {
	return proto.EnumName(RouterConfig_RouterType_name, int32(x))
}
func (x *RouterConfig_RouterType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(RouterConfig_RouterType_value, data, "RouterConfig_RouterType")
	if err != nil {
		return err
	}
	*x = RouterConfig_RouterType(value)
	return nil
}

type RouterConfig_ProtocolType int32

const (
	RouterConfig_PROTOCOL_TYPE_HTTP  RouterConfig_ProtocolType = 1
	RouterConfig_PROTOCOL_TYPE_HTTPS RouterConfig_ProtocolType = 2
)

var RouterConfig_ProtocolType_name = map[int32]string{
	1: "PROTOCOL_TYPE_HTTP",
	2: "PROTOCOL_TYPE_HTTPS",
}
var RouterConfig_ProtocolType_value = map[string]int32{
	"PROTOCOL_TYPE_HTTP":  1,
	"PROTOCOL_TYPE_HTTPS": 2,
}

func (x RouterConfig_ProtocolType) Enum() *RouterConfig_ProtocolType {
	p := new(RouterConfig_ProtocolType)
	*p = x
	return p
}
func (x RouterConfig_ProtocolType) String() string {
	return proto.EnumName(RouterConfig_ProtocolType_name, int32(x))
}
func (x *RouterConfig_ProtocolType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(RouterConfig_ProtocolType_value, data, "RouterConfig_ProtocolType")
	if err != nil {
		return err
	}
	*x = RouterConfig_ProtocolType(value)
	return nil
}

type Documentation struct {
	Description      *string         `protobuf:"bytes,1,opt,name=description" json:"description,omitempty"`
	ServiceRoutes    []*ServiceRoute `protobuf:"bytes,2,rep,name=service_routes" json:"service_routes,omitempty"`
	XXX_unrecognized []byte          `json:"-"`
}

func (m *Documentation) Reset()         { *m = Documentation{} }
func (m *Documentation) String() string { return proto.CompactTextString(m) }
func (*Documentation) ProtoMessage()    {}

func (m *Documentation) GetDescription() string {
	if m != nil && m.Description != nil {
		return *m.Description
	}
	return ""
}

func (m *Documentation) GetServiceRoutes() []*ServiceRoute {
	if m != nil {
		return m.ServiceRoutes
	}
	return nil
}

type DocumentationList struct {
	Documentations   []*Documentation `protobuf:"bytes,1,rep,name=documentations" json:"documentations,omitempty"`
	XXX_unrecognized []byte           `json:"-"`
}

func (m *DocumentationList) Reset()         { *m = DocumentationList{} }
func (m *DocumentationList) String() string { return proto.CompactTextString(m) }
func (*DocumentationList) ProtoMessage()    {}

func (m *DocumentationList) GetDocumentations() []*Documentation {
	if m != nil {
		return m.Documentations
	}
	return nil
}

type Error struct {
	Message          *string `protobuf:"bytes,1,opt,name=message" json:"message,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Error) Reset()         { *m = Error{} }
func (m *Error) String() string { return proto.CompactTextString(m) }
func (*Error) ProtoMessage()    {}

func (m *Error) GetMessage() string {
	if m != nil && m.Message != nil {
		return *m.Message
	}
	return ""
}

type IpAddress struct {
	Address          *string            `protobuf:"bytes,1,opt,name=address" json:"address,omitempty"`
	Version          *IpAddress_Version `protobuf:"varint,2,opt,name=version,enum=platform.IpAddress_Version" json:"version,omitempty"`
	XXX_unrecognized []byte             `json:"-"`
}

func (m *IpAddress) Reset()         { *m = IpAddress{} }
func (m *IpAddress) String() string { return proto.CompactTextString(m) }
func (*IpAddress) ProtoMessage()    {}

func (m *IpAddress) GetAddress() string {
	if m != nil && m.Address != nil {
		return *m.Address
	}
	return ""
}

func (m *IpAddress) GetVersion() IpAddress_Version {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return IpAddress_V4
}

type Request struct {
	Uuid             *string  `protobuf:"bytes,1,opt,name=uuid" json:"uuid,omitempty"`
	Routing          *Routing `protobuf:"bytes,2,opt,name=routing" json:"routing,omitempty"`
	Context          []byte   `protobuf:"bytes,3,opt,name=context" json:"context,omitempty"`
	Payload          []byte   `protobuf:"bytes,4,opt,name=payload" json:"payload,omitempty"`
	Completed        *bool    `protobuf:"varint,5,opt,name=completed" json:"completed,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Request) Reset()         { *m = Request{} }
func (m *Request) String() string { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()    {}

func (m *Request) GetUuid() string {
	if m != nil && m.Uuid != nil {
		return *m.Uuid
	}
	return ""
}

func (m *Request) GetRouting() *Routing {
	if m != nil {
		return m.Routing
	}
	return nil
}

func (m *Request) GetContext() []byte {
	if m != nil {
		return m.Context
	}
	return nil
}

func (m *Request) GetPayload() []byte {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (m *Request) GetCompleted() bool {
	if m != nil && m.Completed != nil {
		return *m.Completed
	}
	return false
}

type Route struct {
	Uri              *string    `protobuf:"bytes,1,opt,name=uri" json:"uri,omitempty"`
	IpAddress        *IpAddress `protobuf:"bytes,2,opt,name=ip_address" json:"ip_address,omitempty"`
	XXX_unrecognized []byte     `json:"-"`
}

func (m *Route) Reset()         { *m = Route{} }
func (m *Route) String() string { return proto.CompactTextString(m) }
func (*Route) ProtoMessage()    {}

func (m *Route) GetUri() string {
	if m != nil && m.Uri != nil {
		return *m.Uri
	}
	return ""
}

func (m *Route) GetIpAddress() *IpAddress {
	if m != nil {
		return m.IpAddress
	}
	return nil
}

type Routing struct {
	RouteTo          []*Route `protobuf:"bytes,1,rep,name=route_to" json:"route_to,omitempty"`
	RouteFrom        []*Route `protobuf:"bytes,2,rep,name=route_from" json:"route_from,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Routing) Reset()         { *m = Routing{} }
func (m *Routing) String() string { return proto.CompactTextString(m) }
func (*Routing) ProtoMessage()    {}

func (m *Routing) GetRouteTo() []*Route {
	if m != nil {
		return m.RouteTo
	}
	return nil
}

func (m *Routing) GetRouteFrom() []*Route {
	if m != nil {
		return m.RouteFrom
	}
	return nil
}

type RouterConfig struct {
	ProtocolType     *RouterConfig_ProtocolType `protobuf:"varint,1,opt,name=protocol_type,enum=platform.RouterConfig_ProtocolType" json:"protocol_type,omitempty"`
	Host             *string                    `protobuf:"bytes,2,opt,name=host" json:"host,omitempty"`
	Port             *string                    `protobuf:"bytes,3,opt,name=port" json:"port,omitempty"`
	RouterType       *RouterConfig_RouterType   `protobuf:"varint,4,opt,name=router_type,enum=platform.RouterConfig_RouterType" json:"router_type,omitempty"`
	XXX_unrecognized []byte                     `json:"-"`
}

func (m *RouterConfig) Reset()         { *m = RouterConfig{} }
func (m *RouterConfig) String() string { return proto.CompactTextString(m) }
func (*RouterConfig) ProtoMessage()    {}

func (m *RouterConfig) GetProtocolType() RouterConfig_ProtocolType {
	if m != nil && m.ProtocolType != nil {
		return *m.ProtocolType
	}
	return RouterConfig_PROTOCOL_TYPE_HTTP
}

func (m *RouterConfig) GetHost() string {
	if m != nil && m.Host != nil {
		return *m.Host
	}
	return ""
}

func (m *RouterConfig) GetPort() string {
	if m != nil && m.Port != nil {
		return *m.Port
	}
	return ""
}

func (m *RouterConfig) GetRouterType() RouterConfig_RouterType {
	if m != nil && m.RouterType != nil {
		return *m.RouterType
	}
	return RouterConfig_ROUTER_TYPE_WEBSOCKET
}

type RouterConfigList struct {
	RouterConfigs    []*RouterConfig `protobuf:"bytes,1,rep,name=router_configs" json:"router_configs,omitempty"`
	XXX_unrecognized []byte          `json:"-"`
}

func (m *RouterConfigList) Reset()         { *m = RouterConfigList{} }
func (m *RouterConfigList) String() string { return proto.CompactTextString(m) }
func (*RouterConfigList) ProtoMessage()    {}

func (m *RouterConfigList) GetRouterConfigs() []*RouterConfig {
	if m != nil {
		return m.RouterConfigs
	}
	return nil
}

type ServiceRoute struct {
	Description      *string  `protobuf:"bytes,1,opt,name=description" json:"description,omitempty"`
	Request          *Route   `protobuf:"bytes,2,opt,name=request" json:"request,omitempty"`
	Responses        []*Route `protobuf:"bytes,3,rep,name=responses" json:"responses,omitempty"`
	IsDeprecated     *bool    `protobuf:"varint,4,opt,name=is_deprecated" json:"is_deprecated,omitempty"`
	Version          *string  `protobuf:"bytes,5,opt,name=version" json:"version,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *ServiceRoute) Reset()         { *m = ServiceRoute{} }
func (m *ServiceRoute) String() string { return proto.CompactTextString(m) }
func (*ServiceRoute) ProtoMessage()    {}

func (m *ServiceRoute) GetDescription() string {
	if m != nil && m.Description != nil {
		return *m.Description
	}
	return ""
}

func (m *ServiceRoute) GetRequest() *Route {
	if m != nil {
		return m.Request
	}
	return nil
}

func (m *ServiceRoute) GetResponses() []*Route {
	if m != nil {
		return m.Responses
	}
	return nil
}

func (m *ServiceRoute) GetIsDeprecated() bool {
	if m != nil && m.IsDeprecated != nil {
		return *m.IsDeprecated
	}
	return false
}

func (m *ServiceRoute) GetVersion() string {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return ""
}

func init() {
	proto.RegisterEnum("platform.IpAddress_Version", IpAddress_Version_name, IpAddress_Version_value)
	proto.RegisterEnum("platform.RouterConfig_RouterType", RouterConfig_RouterType_name, RouterConfig_RouterType_value)
	proto.RegisterEnum("platform.RouterConfig_ProtocolType", RouterConfig_ProtocolType_name, RouterConfig_ProtocolType_value)
}
