// Code generated by protoc-gen-go. DO NOT EDIT.
// source: helloworld.proto

package helloworld

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// The request message containing the user's name.
type HelloRequest struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *HelloRequest) Reset()         { *m = HelloRequest{} }
func (m *HelloRequest) String() string { return proto.CompactTextString(m) }
func (*HelloRequest) ProtoMessage()    {}
func (*HelloRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_17b8c58d586b62f2, []int{0}
}

func (m *HelloRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HelloRequest.Unmarshal(m, b)
}
func (m *HelloRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HelloRequest.Marshal(b, m, deterministic)
}
func (m *HelloRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HelloRequest.Merge(m, src)
}
func (m *HelloRequest) XXX_Size() int {
	return xxx_messageInfo_HelloRequest.Size(m)
}
func (m *HelloRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_HelloRequest.DiscardUnknown(m)
}

var xxx_messageInfo_HelloRequest proto.InternalMessageInfo

func (m *HelloRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

// The response message containing the greetings
type HelloReply struct {
	Message              string   `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *HelloReply) Reset()         { *m = HelloReply{} }
func (m *HelloReply) String() string { return proto.CompactTextString(m) }
func (*HelloReply) ProtoMessage()    {}
func (*HelloReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_17b8c58d586b62f2, []int{1}
}

func (m *HelloReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HelloReply.Unmarshal(m, b)
}
func (m *HelloReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HelloReply.Marshal(b, m, deterministic)
}
func (m *HelloReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HelloReply.Merge(m, src)
}
func (m *HelloReply) XXX_Size() int {
	return xxx_messageInfo_HelloReply.Size(m)
}
func (m *HelloReply) XXX_DiscardUnknown() {
	xxx_messageInfo_HelloReply.DiscardUnknown(m)
}

var xxx_messageInfo_HelloReply proto.InternalMessageInfo

func (m *HelloReply) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

func init() {
	proto.RegisterType((*HelloRequest)(nil), "helloworld.HelloRequest")
	proto.RegisterType((*HelloReply)(nil), "helloworld.HelloReply")
}

func init() { proto.RegisterFile("helloworld.proto", fileDescriptor_17b8c58d586b62f2) }

var fileDescriptor_17b8c58d586b62f2 = []byte{
	// 203 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0xc8, 0x48, 0xcd, 0xc9,
	0xc9, 0x2f, 0xcf, 0x2f, 0xca, 0x49, 0xd1, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x42, 0x88,
	0x28, 0x29, 0x71, 0xf1, 0x78, 0x80, 0x78, 0x41, 0xa9, 0x85, 0xa5, 0xa9, 0xc5, 0x25, 0x42, 0x42,
	0x5c, 0x2c, 0x79, 0x89, 0xb9, 0xa9, 0x12, 0x8c, 0x0a, 0x8c, 0x1a, 0x9c, 0x41, 0x60, 0xb6, 0x92,
	0x1a, 0x17, 0x17, 0x54, 0x4d, 0x41, 0x4e, 0xa5, 0x90, 0x04, 0x17, 0x7b, 0x6e, 0x6a, 0x71, 0x71,
	0x62, 0x3a, 0x4c, 0x11, 0x8c, 0x6b, 0x74, 0x91, 0x91, 0x8b, 0xdd, 0xbd, 0x28, 0x35, 0xb5, 0x24,
	0xb5, 0x48, 0xc8, 0x81, 0x8b, 0x23, 0x38, 0xb1, 0x12, 0xac, 0x4d, 0x48, 0x42, 0x0f, 0xc9, 0x09,
	0xc8, 0xb6, 0x49, 0x89, 0x61, 0x91, 0x29, 0xc8, 0xa9, 0x54, 0x62, 0x30, 0x60, 0x14, 0x72, 0xe4,
	0xe2, 0x0c, 0x4e, 0xac, 0x0c, 0x4a, 0x2d, 0x48, 0x4d, 0x2c, 0xa1, 0xc8, 0x08, 0xf7, 0xfc, 0xe2,
	0xe2, 0xcc, 0x02, 0xf2, 0x8c, 0x70, 0x32, 0xe0, 0x92, 0xce, 0xcc, 0xd7, 0x4b, 0x2f, 0x2a, 0x48,
	0xd6, 0x4b, 0xad, 0x48, 0xcc, 0x2d, 0xc8, 0x49, 0x2d, 0x46, 0x52, 0xed, 0xc4, 0x0f, 0x56, 0x1e,
	0x0e, 0x62, 0x07, 0x80, 0xc2, 0x36, 0x80, 0x31, 0x89, 0x0d, 0x1c, 0xc8, 0xc6, 0x80, 0x00, 0x00,
	0x00, 0xff, 0xff, 0xee, 0x2a, 0x28, 0x8e, 0x78, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// GreeterClient is the client API for Greeter service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type GreeterClient interface {
	// Sends a greeting
	SayHello(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (Greeter_SayHelloClient, error)
	SayRepeat(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (Greeter_SayRepeatClient, error)
	SayGossip(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (Greeter_SayGossipClient, error)
}

type greeterClient struct {
	cc *grpc.ClientConn
}

func NewGreeterClient(cc *grpc.ClientConn) GreeterClient {
	return &greeterClient{cc}
}

func (c *greeterClient) SayHello(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (Greeter_SayHelloClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Greeter_serviceDesc.Streams[0], "/helloworld.Greeter/SayHello", opts...)
	if err != nil {
		return nil, err
	}
	x := &greeterSayHelloClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Greeter_SayHelloClient interface {
	Recv() (*HelloReply, error)
	grpc.ClientStream
}

type greeterSayHelloClient struct {
	grpc.ClientStream
}

func (x *greeterSayHelloClient) Recv() (*HelloReply, error) {
	m := new(HelloReply)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *greeterClient) SayRepeat(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (Greeter_SayRepeatClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Greeter_serviceDesc.Streams[1], "/helloworld.Greeter/SayRepeat", opts...)
	if err != nil {
		return nil, err
	}
	x := &greeterSayRepeatClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Greeter_SayRepeatClient interface {
	Recv() (*HelloReply, error)
	grpc.ClientStream
}

type greeterSayRepeatClient struct {
	grpc.ClientStream
}

func (x *greeterSayRepeatClient) Recv() (*HelloReply, error) {
	m := new(HelloReply)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *greeterClient) SayGossip(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (Greeter_SayGossipClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Greeter_serviceDesc.Streams[2], "/helloworld.Greeter/SayGossip", opts...)
	if err != nil {
		return nil, err
	}
	x := &greeterSayGossipClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Greeter_SayGossipClient interface {
	Recv() (*HelloReply, error)
	grpc.ClientStream
}

type greeterSayGossipClient struct {
	grpc.ClientStream
}

func (x *greeterSayGossipClient) Recv() (*HelloReply, error) {
	m := new(HelloReply)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// GreeterServer is the server API for Greeter service.
type GreeterServer interface {
	// Sends a greeting
	SayHello(*HelloRequest, Greeter_SayHelloServer) error
	SayRepeat(*HelloRequest, Greeter_SayRepeatServer) error
	SayGossip(*HelloRequest, Greeter_SayGossipServer) error
}

func RegisterGreeterServer(s *grpc.Server, srv GreeterServer) {
	s.RegisterService(&_Greeter_serviceDesc, srv)
}

func _Greeter_SayHello_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(HelloRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(GreeterServer).SayHello(m, &greeterSayHelloServer{stream})
}

type Greeter_SayHelloServer interface {
	Send(*HelloReply) error
	grpc.ServerStream
}

type greeterSayHelloServer struct {
	grpc.ServerStream
}

func (x *greeterSayHelloServer) Send(m *HelloReply) error {
	return x.ServerStream.SendMsg(m)
}

func _Greeter_SayRepeat_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(HelloRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(GreeterServer).SayRepeat(m, &greeterSayRepeatServer{stream})
}

type Greeter_SayRepeatServer interface {
	Send(*HelloReply) error
	grpc.ServerStream
}

type greeterSayRepeatServer struct {
	grpc.ServerStream
}

func (x *greeterSayRepeatServer) Send(m *HelloReply) error {
	return x.ServerStream.SendMsg(m)
}

func _Greeter_SayGossip_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(HelloRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(GreeterServer).SayGossip(m, &greeterSayGossipServer{stream})
}

type Greeter_SayGossipServer interface {
	Send(*HelloReply) error
	grpc.ServerStream
}

type greeterSayGossipServer struct {
	grpc.ServerStream
}

func (x *greeterSayGossipServer) Send(m *HelloReply) error {
	return x.ServerStream.SendMsg(m)
}

var _Greeter_serviceDesc = grpc.ServiceDesc{
	ServiceName: "helloworld.Greeter",
	HandlerType: (*GreeterServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SayHello",
			Handler:       _Greeter_SayHello_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SayRepeat",
			Handler:       _Greeter_SayRepeat_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SayGossip",
			Handler:       _Greeter_SayGossip_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "helloworld.proto",
}
