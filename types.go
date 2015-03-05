package platform

import (
	"code.google.com/p/goprotobuf/proto"
)

const ORGANIZATION = "platform"

var (
	Bool    = proto.Bool
	Int     = proto.Int
	Int32   = proto.Int32
	Int64   = proto.Int64
	Uint32  = proto.Uint32
	Uint64  = proto.Uint64
	Float32 = proto.Float32
	Float64 = proto.Float64
	String  = proto.String

	Marshal   = proto.Marshal
	Unmarshal = proto.Unmarshal
)

type Message interface {
	Reset()
	String() string
	ProtoMessage()
}

func GenerateRoutedMessage(method Method, resource Resource, message Message) (*RoutedMessage, error) {
	cloudBody, err := Marshal(message)
	if err != nil {
		return nil, err
	}

	return &RoutedMessage{
		Method:   proto.Int32(int32(method)),
		Resource: proto.Int32(int32(resource)),
		Body:     cloudBody,
	}, nil
}
