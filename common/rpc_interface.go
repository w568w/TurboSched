package common

import (
	grpcnetconn "github.com/hashicorp/go-grpc-net-conn"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pb "turbo_sched/common/proto"
)

// RawCommandLine is a JSON-friendly wrapper for the [pb.CommandLine] protobuf struct.
type RawCommandLine struct {
	Program string
	Args    []string
	Env     []string
}

func ToRawCommandLine(cmdLine *pb.CommandLine) RawCommandLine {
	return RawCommandLine{
		Program: cmdLine.Program,
		Args:    cmdLine.Args,
		Env:     cmdLine.Env,
	}
}

func (r *RawCommandLine) ToCommandLine() *pb.CommandLine {
	return &pb.CommandLine{
		Program: r.Program,
		Args:    r.Args,
		Env:     r.Env,
	}
}

func NewGrpcConn(stream grpc.Stream) (*grpcnetconn.Conn, <-chan *pb.SshBytes_SshAttributeUpdate) {
	attributeUpdates := make(chan *pb.SshBytes_SshAttributeUpdate)
	netConn := &grpcnetconn.Conn{
		Stream:   stream,
		Request:  &pb.SshBytes{},
		Response: &pb.SshBytes{},
		Encode: func(msg proto.Message, p []byte) (int, error) {
			msg.(*pb.SshBytes).Data = &pb.SshBytes_RawData{RawData: p}
			return len(p), nil
		},
		Decode: func(msg proto.Message, offset int, p []byte) ([]byte, error) {
			b := msg.(*pb.SshBytes)
			if rawData := b.GetRawData(); rawData != nil {
				copy(p, rawData[offset:])
				return rawData, nil
			} else if attrUpdate := b.GetAttributeUpdate(); attrUpdate != nil {
				attributeUpdates <- attrUpdate
			}
			return nil, nil
		},
	}
	return netConn, attributeUpdates
}
