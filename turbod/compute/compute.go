package compute

import (
	"context"
	"errors"
	"fmt"
	"github.com/creack/pty"
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/google/uuid"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"log/slog"
	"os/exec"
	"turbo_sched/common"
	pb "turbo_sched/common/proto"
)

type computeSession struct {
	assignInfo *pb.TaskAssignInfo
}

type ComputeInterface struct {
	ControlClient pb.ControllerClient
	connMap       common.DualMap[string, uint64, computeSession]
	taskCancel    map[uint64]*common.TaskCanceler
	taskStatus    map[uint64]common.TaskStatus
	reporter      failsafe.Executor[any]
	Glog          *slog.Logger
	pb.UnimplementedComputeServer
}

func (c *ComputeInterface) reportErrorTaskAsync(id *pb.TaskId, err error) failsafe.ExecutionResult[any] {
	return c.reporter.RunAsync(func() error {
		_, err := c.ControlClient.ReportTask(context.Background(), &pb.TaskReportInfo{
			Id: id,
			Event: &pb.TaskReportInfo_Error{
				Error: &pb.TaskReportInfo_TaskError{
					Message: err.Error(),
				},
			},
		})
		return err
	})
}

func (c *ComputeInterface) reportExitedTaskAsync(id *pb.TaskId, exitCode int32, output []byte) failsafe.ExecutionResult[any] {
	return c.reporter.RunAsync(func() error {
		_, err := c.ControlClient.ReportTask(context.Background(), &pb.TaskReportInfo{
			Id: id,
			Event: &pb.TaskReportInfo_Exited{
				Exited: &pb.TaskReportInfo_TaskExited{
					ExitCode: exitCode,
					Output:   output,
				},
			},
		})
		return err
	})
}

func (c *ComputeInterface) SshTunnel(server pb.Compute_SshTunnelServer) error {
	s := server.Context().Value(Session).(computeSession)

	canceler := c.taskCancel[s.assignInfo.Id.Id]
	if canceler.HasCanceled() {
		return context.Canceled
	}

	cxt := canceler.RoutineRegister(server.Context())
	defer canceler.RoutineUnregister()

	procExited := make(chan struct{})
	netConn, attrUpdateChan := common.NewGrpcConn(server)

	// piping the netConn with the stdin/stdout/stderr of the command
	cmd := exec.Command(s.assignInfo.CommandLine.Program, s.assignInfo.CommandLine.Args...)
	cmd.Env = append(cmd.Env, s.assignInfo.CommandLine.Env...)
	cmd.Dir = s.assignInfo.CommandLine.Cwd

	// wait the first window size update
	windowEvent, err := server.Recv()
	if err != nil {
		c.reportErrorTaskAsync(s.assignInfo.Id, err)
		return err
	}
	window := windowEvent.GetAttributeUpdate().GetWindowSize()
	if window == nil {
		err = fmt.Errorf("first message should be about window size")
		c.reportErrorTaskAsync(s.assignInfo.Id, err)
		return err
	}

	// before we start the command, check cancellation again
	if canceler.HasCanceled() {
		return context.Canceled
	}

	c.taskStatus[s.assignInfo.Id.Id] = common.Running

	f, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: uint16(window.Rows),
		Cols: uint16(window.Columns),
	})
	if err != nil {
		c.reportErrorTaskAsync(s.assignInfo.Id, err)
		return err
	}
	var eg errgroup.Group
	eg.Go(func() error {
		_, err = io.Copy(f, netConn)
		return err
	})
	eg.Go(func() error {
		_, err = io.Copy(netConn, f)
		return err
	})
	go func() {
		errCnt := 0
		for {
			select {
			case <-procExited:
				return
			case attribute := <-attrUpdateChan:
				if windowSize := attribute.GetWindowSize(); windowSize != nil {
					err := pty.Setsize(f, &pty.Winsize{
						Rows: uint16(windowSize.Rows),
						Cols: uint16(windowSize.Columns),
					})
					if err != nil {
						errCnt++
						if errCnt == 1 {
							c.Glog.Warn("Failed to set window size", err)
						} else if common.IsPowerOfTwo(errCnt) {
							c.Glog.Warn(fmt.Sprintf("Failed to set window size for %d times", errCnt), err)
						}
					}
				}
			}
		}
	}()

	var execErr error
	go func() {
		execErr = cmd.Wait()
		close(procExited)
	}()

	// wait until the command exits
	alreadySentKilled := false
forloop:
	for {
		if !alreadySentKilled {
			select {
			case <-cxt.Done():
				if err = cmd.Process.Kill(); err != nil {
					c.Glog.Warn("Failed to kill the process", err)
				}
				alreadySentKilled = true
			case _, _ = <-procExited:
				break forloop
			}
		} else {
			_, _ = <-procExited
			break forloop
		}
	}
	c.taskStatus[s.assignInfo.Id.Id] = common.Exited

	if execErr != nil {
		// is there something error, or just the command exited with non-zero code?
		c.Glog.Warn("Command failed", execErr)
		var exitError *exec.ExitError
		if !errors.As(execErr, &exitError) {
			c.reportErrorTaskAsync(s.assignInfo.Id, execErr)
			return execErr
		}
	}
	// tell the client we are done, so that it will close remote connection
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
		c.Glog.Error("Failed to send exit status to client", err)
	}
	// wait for piping to finish
	pipingErr := eg.Wait()
	if pipingErr != nil {
		// the command exited normally, but we failed to pipe the data.
		// it can be that the process exited normally or the connection is closed early.
		// TODO we don't know for now. Just log it.
		c.Glog.Debug("Piping error", pipingErr)
	}
	// tell the controller we are done, if we are not being cancelled
	if !alreadySentKilled {
		result := c.reportExitedTaskAsync(s.assignInfo.Id, int32(cmd.ProcessState.ExitCode()), nil)
		if err := result.Error(); err != nil {
			return err
		}
	}
	return nil
}

const (
	Session = "session"
)

func NewComputeInterface(controlClient pb.ControllerClient, logger *slog.Logger) *ComputeInterface {
	return &ComputeInterface{
		ControlClient: controlClient,
		connMap:       common.NewDualMap[string, uint64, computeSession](),
		taskCancel:    make(map[uint64]*common.TaskCanceler),
		taskStatus:    make(map[uint64]common.TaskStatus),
		reporter:      failsafe.NewExecutor[any](retrypolicy.Builder[any]().WithMaxRetries(3).Build()),
		Glog:          logger,
	}
}

func (c *ComputeInterface) AuthIntercept(ctx context.Context) (context.Context, error) {
	token, err := grpcauth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}

	if val, ok := c.connMap.GetByKey1(token); !ok {
		return nil, status.Errorf(codes.Unauthenticated, "Bad authorization token")
	} else {
		newCtx := context.WithValue(ctx, Session, *val)
		return newCtx, nil
	}
}

func (c *ComputeInterface) TaskAssign(ctx context.Context, submission *pb.TaskAssignInfo) (*emptypb.Empty, error) {
	fmt.Println("TaskAssign", submission)

	c.taskStatus[submission.Id.Id] = common.Submitting
	c.taskCancel[submission.Id.Id] = common.NewTaskCanceler(false)

	canceler := c.taskCancel[submission.Id.Id]
	_ = canceler.RoutineRegister(nil)
	defer canceler.RoutineUnregister()

	if submission.Interactive {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, err
		}
		token := id.String()
		c.connMap.Put(token, submission.Id.Id, computeSession{
			assignInfo: submission,
		})
		c.taskStatus[submission.Id.Id] = common.Attaching
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
		c.taskStatus[submission.Id.Id] = common.Running
		go func() {
			cxt := canceler.RoutineRegister(nil)
			defer canceler.RoutineUnregister()

			cmd := exec.Command(submission.CommandLine.Program, submission.CommandLine.Args...)
			cmd.Env = append(cmd.Env, submission.CommandLine.Env...)
			var procErr error
			var output []byte
			procExited := make(chan struct{})
			go func() {
				output, procErr = cmd.CombinedOutput()
				close(procExited)
			}()
			// wait until the command exits
			alreadySentKilled := false
		forloop:
			for {
				if !alreadySentKilled {
					select {
					case <-cxt.Done():
						if err := cmd.Process.Kill(); err != nil {
							c.Glog.Warn("Failed to kill the process", err)
						}
						alreadySentKilled = true
					case _, _ = <-procExited:
						break forloop
					}
				} else {
					_, _ = <-procExited
					break forloop
				}
			}
			c.taskStatus[submission.Id.Id] = common.Exited
			if procErr != nil {
				fmt.Println("TaskAssign failed:", procErr)
				return
			}
			_, err := c.ControlClient.ReportTask(context.Background(), &pb.TaskReportInfo{
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

func (c *ComputeInterface) TaskCancel(ctx context.Context, id *pb.TaskId) (*emptypb.Empty, error) {
	fmt.Println("TaskCancel", id)
	canceler, ok := c.taskCancel[id.Id]
	if !ok {
		// fixme: canceling an old task?
		return nil, errors.New("task not found")
	}
	if canceler.HasCanceled() {
		return nil, context.Canceled
	}
	ok = canceler.CancelAndWaitAllRoutine()
	if !ok {
		return nil, context.Canceled
	}

	switch c.taskStatus[id.Id] {
	case common.Pending:
		panic("Pending tasks should never be sent to a compute node")
	case common.Running:
		panic("after cancelling, the task cannot be running")
	case common.Attaching:
		c.connMap.DeleteByKey2(id.Id)
		fallthrough
	case common.Submitting:
		delete(c.taskStatus, id.Id)
		delete(c.taskCancel, id.Id)
		fallthrough
	case common.Exited:
	case common.Errored:
	case common.Canceled:
	}
	return &emptypb.Empty{}, nil
}
