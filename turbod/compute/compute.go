package compute

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_net_conn "github.com/hashicorp/go-grpc-net-conn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"os/exec"
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

	fieldFunc := func(msg proto.Message) *[]byte {
		return &msg.(*pb.SshBytes).Data
	}
	netConn := &grpc_net_conn.Conn{
		Stream:   server,
		Request:  &pb.SshBytes{},
		Response: &pb.SshBytes{},
		Encode:   grpc_net_conn.SimpleEncoder(fieldFunc),
		Decode:   grpc_net_conn.SimpleDecoder(fieldFunc),
	}

	// piping the netConn with the stdin/stdout/stderr of the command
	cmd := exec.Command(s.assignInfo.CommandLine.Program, s.assignInfo.CommandLine.Args...)
	cmd.Env = append(cmd.Env, s.assignInfo.CommandLine.Env...)

	pout, _ := cmd.StdoutPipe()
	pin, _ := cmd.StdinPipe()
	perr, _ := cmd.StderrPipe()
	cmd.Start()
	go func() {
		_, err := io.Copy(pin, netConn)
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		_, err := io.Copy(netConn, perr)
		if err != nil {
			panic(err)
		}
	}()
	_, err := io.Copy(netConn, pout)
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
