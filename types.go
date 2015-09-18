package platform

import (
	"github.com/golang/protobuf/proto"
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
