package common

import pb "turbo_sched/common/proto"

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
