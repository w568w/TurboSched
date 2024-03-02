package compute

import (
	"context"
	"fmt"
	"github.com/creack/pty"
	"github.com/google/uuid"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
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

	netConn, attrUpdateChan := common.NewGrpcConn(server)

	// piping the netConn with the stdin/stdout/stderr of the command
	cmd := exec.Command(s.assignInfo.CommandLine.Program, s.assignInfo.CommandLine.Args...)
	cmd.Env = append(cmd.Env, s.assignInfo.CommandLine.Env...)

	// wait the first window size update
	windowEvent, err := server.Recv()
	window := windowEvent.GetAttributeUpdate().GetWindowSize()
	if window == nil {
		return fmt.Errorf("first message should be a window size update")
	}
	println("Window size:", window.Rows, window.Columns)
	f, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: uint16(window.Rows),
		Cols: uint16(window.Columns),
	})
	go func() {
		_, err := io.Copy(f, netConn)
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		for attribute := range attrUpdateChan {
			if windowSize := attribute.GetWindowSize(); windowSize != nil {
				println("New Window size:", windowSize.Rows, windowSize.Columns)
				pty.Setsize(f, &pty.Winsize{
					Rows: uint16(windowSize.Rows),
					Cols: uint16(windowSize.Columns),
				})
			}
		}
	}()
	_, err = io.Copy(netConn, f)
	if err != nil {
		panic(err)
	}
	err = cmd.Wait()
	println("Process exited")
	if err != nil {
		panic(err)
	}

	// clean up
	_, err = c.ControlClient.ReportTask(context.Background(), &pb.TaskReportInfo{
		Id: s.assignInfo.Id,
		Event: &pb.TaskReportInfo_Exited{
			Exited: &pb.TaskReportInfo_TaskExited{
				ExitCode: int32(cmd.ProcessState.ExitCode()),
				Output:   make([]byte, 0),
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return nil
	//rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	//if err != nil {
	//	panic(err)
	//}
	//signer, err := gossh.NewSignerFromKey(rsaKey)
	//if err != nil {
	//	panic(err)
	//}
	//sshServer := &ssh.Server{
	//	Handler: func(s ssh.Session) {
	//		// TODO start the command and connect the stdin/stdout/stderr to the netConn
	//		print("Print the command")
	//		_, _ = io.WriteString(s, fmt.Sprintf("Hello %s\n", s.User()))
	//	},
	//	ConnectionFailedCallback: func(conn net.Conn, err error) {
	//		panic(err)
	//	},
	//	HostSigners: []ssh.Signer{signer},
	//}
	//sshServer.HandleConn(netConn)
	//print("HandleConn")
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
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
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
