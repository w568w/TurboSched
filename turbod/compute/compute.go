package compute

import (
	"context"
	"fmt"
	"github.com/creack/pty"
	"github.com/google/uuid"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"os/exec"
	"turbo_sched/common"
	pb "turbo_sched/common/proto"
)

type computeSession struct {
	assignInfo *pb.TaskAssignInfo
}

type ComputeInterface struct {
	ControlClient pb.ControllerClient
	connMap       map[string]computeSession
	pb.UnimplementedComputeServer
}

func (c *ComputeInterface) SshTunnel(server pb.Compute_SshTunnelServer) error {
	s := server.Context().Value(Session).(computeSession)

	procExited := make(chan bool)
	netConn, attrUpdateChan := common.NewGrpcConn(server)

	// piping the netConn with the stdin/stdout/stderr of the command
	cmd := exec.Command(s.assignInfo.CommandLine.Program, s.assignInfo.CommandLine.Args...)
	cmd.Env = append(cmd.Env, s.assignInfo.CommandLine.Env...)

	// wait the first window size update
	windowEvent, err := server.Recv()
	if err != nil {
		return err
	}
	window := windowEvent.GetAttributeUpdate().GetWindowSize()
	if window == nil {
		return fmt.Errorf("first message should be about window size")
	}
	f, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: uint16(window.Rows),
		Cols: uint16(window.Columns),
	})
	if err != nil {
		return err
	}
	var eg errgroup.Group
	eg.Go(func() error {
		_, _ = io.Copy(f, netConn)
		return nil
	})
	eg.Go(func() error {
		_, _ = io.Copy(netConn, f)
		return nil
	})
	eg.Go(func() error {
		for {
			select {
			case <-procExited:
				return nil
			case attribute := <-attrUpdateChan:
				if windowSize := attribute.GetWindowSize(); windowSize != nil {
					err := pty.Setsize(f, &pty.Winsize{
						Rows: uint16(windowSize.Rows),
						Cols: uint16(windowSize.Columns),
					})
					if err != nil {
						return err
					}
				}
			}
		}
	})
	err = cmd.Wait()
	if err != nil {
		panic(err)
	}
	procExited <- true
	err = server.Send(&pb.SshBytes{
		Data: &pb.SshBytes_AttributeUpdate{
			AttributeUpdate: &pb.SshBytes_SshAttributeUpdate{
				Update: &pb.SshBytes_SshAttributeUpdate_ExitStatus_{
					ExitStatus: &pb.SshBytes_SshAttributeUpdate_ExitStatus{
						ExitStatus: int32(cmd.ProcessState.ExitCode()),
					},
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	// clean up
	_, err = c.ControlClient.ReportTask(context.Background(), &pb.TaskReportInfo{
		Id: s.assignInfo.Id,
		Event: &pb.TaskReportInfo_Exited{
			Exited: &pb.TaskReportInfo_TaskExited{
				ExitCode: int32(cmd.ProcessState.ExitCode()),
				Output:   nil,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	err = eg.Wait()
	if err != nil {
		panic(err)
	}

	return nil
}

const (
	Session = "session"
)

func NewComputeInterface(controlClient pb.ControllerClient) *ComputeInterface {
	return &ComputeInterface{
		ControlClient: controlClient,
		connMap:       make(map[string]computeSession),
	}
}

func (c *ComputeInterface) AuthIntercept(ctx context.Context) (context.Context, error) {
	token, err := grpcauth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}
	if _, ok := c.connMap[token]; !ok {
		return nil, status.Errorf(codes.Unauthenticated, "Bad authorization token")
	}
	newCtx := context.WithValue(ctx, Session, c.connMap[token])
	return newCtx, nil
}

func (c *ComputeInterface) TaskAssign(ctx context.Context, submission *pb.TaskAssignInfo) (*emptypb.Empty, error) {
	fmt.Println("TaskAssign", submission)

	if submission.Interactive {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, err
		}
		token := id.String()
		c.connMap[token] = computeSession{
			assignInfo: submission,
		}
		_, err = c.ControlClient.ReportTask(context.Background(), &pb.TaskReportInfo{
			Id: submission.Id,
			Event: &pb.TaskReportInfo_ReadyForAttach{
				ReadyForAttach: &pb.TaskReportInfo_TaskReadyForAttach{
					Token: token,
				},
			},
		})
		if err != nil {
			return nil, err
		}
	} else {
		go func() {
			cmd := exec.Command(submission.CommandLine.Program, submission.CommandLine.Args...)
			cmd.Env = append(cmd.Env, submission.CommandLine.Env...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Println("TaskAssign failed:", err)
				return
			}
			_, err = c.ControlClient.ReportTask(context.Background(), &pb.TaskReportInfo{
				Id: submission.Id,
				Event: &pb.TaskReportInfo_Exited{
					Exited: &pb.TaskReportInfo_TaskExited{
						ExitCode: int32(cmd.ProcessState.ExitCode()),
						Output:   output,
					},
				},
			})
			if err != nil {
				panic(err)
			}
		}()
	}
	return &emptypb.Empty{}, nil
}
