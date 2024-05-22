package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/creack/pty"
	"github.com/dixonwille/wlog/v3"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/ross96D/cancelreader"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcMetadata "google.golang.org/grpc/metadata"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"turbo_sched/common"
	pb "turbo_sched/common/proto"
)

const AppName = "turbosched"

var globalConfig common.FileConfig

func ctxWithToken(ctx context.Context, scheme string, token string) context.Context {
	md := grpcMetadata.Pairs("authorization", fmt.Sprintf("%s %v", scheme, token))
	return metautils.NiceMD(md).ToOutgoing(ctx)
}

var Glog wlog.UI

func askYesNo(question string) (bool, error) {
	for {
		input, err := Glog.Ask(question+" (y/n) ", " ")
		if err != nil {
			return false, err
		}
		lowerInput := strings.ToLower(input)
		if lowerInput == "y" {
			return true, nil
		} else if lowerInput == "n" {
			return false, nil
		} else {
			Glog.Error(fmt.Sprintf("Invalid input: %s", input))
		}
	}
}

func main() {
	Glog = common.SetupCLILogger()

	common.SetupConfigNameAndPaths(slog.Default(), AppName, "config")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
	if err := viper.Unmarshal(&globalConfig); err != nil {
		panic(err)
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", globalConfig.Controller.Addr, globalConfig.Controller.Port), opts...)
	if err != nil {
		Glog.Error("cannot connect to controller")
		panic(err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			Glog.Error(fmt.Sprintf("cannot close connection to controller: %v", err))
		}
	}(conn)

	client := pb.NewControllerClient(conn)

	cwd, err := os.Getwd()
	if err != nil {
		Glog.Warn(fmt.Sprintf("cannot get current working directory, using empty string as default: %v", err))
		cwd = ""
	}

	if os.Args[1] == "stop" {
		id, err := strconv.ParseUint(os.Args[2], 10, 64)
		if err != nil {
			Glog.Error("cannot parse the task id")
			panic(err)
		}
		_, err = client.CancelTask(context.Background(), &pb.TaskId{Id: id})
		if err != nil {
			Glog.Error("cannot cancel the task")
			panic(err)
		}
		Glog.Success(fmt.Sprintf("Task %d has been cancelled", id))
		return
	}
	stream, err := client.SubmitNewTaskInteractive(context.Background(), &pb.TaskSubmitInfo{
		CommandLine: &pb.CommandLine{
			Program: os.Args[1],
			Args:    os.Args[2:],
			Env:     os.Environ(),
			Cwd:     cwd,
		},
		DeviceRequirement: 1,
	})
	if err != nil {
		Glog.Error("cannot submit the task")
		panic(err)
	}

	ctrlcChan := make(chan os.Signal, 1)
	signal.Notify(ctrlcChan, os.Interrupt)

forLoop:
	for {
		nextEventChan := make(chan struct {
			*pb.TaskEvent
			error
		}, 1)
		go func() {
			event, err := stream.Recv()
			nextEventChan <- struct {
				*pb.TaskEvent
				error
			}{event, err}
			close(nextEventChan)
		}()
		select {
		case <-ctrlcChan:
			toExit, err := askYesNo("Do you want to cancel the task?")
			if err != nil {
				Glog.Error("error asking for user input")
				panic(err)
			}
			if toExit {

			}
			return
		case eventAndErr := <-nextEventChan:
			event := eventAndErr.TaskEvent
			err := eventAndErr.error
			if errors.Is(err, io.EOF) {
				break forLoop
			}
			if err != nil {
				Glog.Error("error waiting for controller")
				panic(err)
			}
			if readyForAttach := event.GetReadyForAttach(); readyForAttach != nil {
				Glog.Success("Node is ready. Attaching...")
				interactiveTaskMain(readyForAttach)
				break forLoop
			} else if taskId := event.GetObtainedId(); taskId != nil {
				Glog.Success(fmt.Sprintf("Task submitted with id: %d", taskId.Id))
				Glog.Running("Queueing for node allocation...")
			}
		}
	}
}

func interactiveTaskMain(readyForAttach *pb.TaskEvent_ReadyForAttach) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", readyForAttach.ConnInfos[0].Host, readyForAttach.ConnInfos[0].Port), opts...)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			Glog.Error(fmt.Sprintf("cannot close connection to compute node: %v", err))
		}
	}(conn)
	if err != nil {
		Glog.Error("cannot connect to compute node")
		panic(err)
	}

	computeClient := pb.NewComputeClient(conn)

	sshStream, err := computeClient.SshTunnel(ctxWithToken(context.Background(), "bearer", readyForAttach.ConnInfos[0].Token))
	if err != nil {
		Glog.Error("cannot open terminal channel with compute node")
		panic(err)
	}

	netConn, attrUpdateChan := common.NewGrpcConn(sshStream)
	// send window size
	r, c, _ := pty.Getsize(os.Stdin)
	err = sshStream.Send(&pb.SshBytes{
		Data: &pb.SshBytes_AttributeUpdate{
			AttributeUpdate: &pb.SshBytes_SshAttributeUpdate{
				Update: &pb.SshBytes_SshAttributeUpdate_WindowSize_{
					WindowSize: &pb.SshBytes_SshAttributeUpdate_WindowSize{
						Rows:    uint32(r),
						Columns: uint32(c),
					},
				},
			},
		},
	})
	if err != nil {
		Glog.Error("cannot send terminal info to compute node")
		panic(err)
	}

	exitCodeChan := make(chan int32, 1)
	pipingErrChan := make(chan error, 3)
	// Handle pty size.
	sizeChangedSig := make(chan os.Signal, 1)
	signal.Notify(sizeChangedSig, syscall.SIGWINCH)
	go func() {
		for {
			select {
			case attribute := <-attrUpdateChan:
				if exitStatus := attribute.GetExitStatus(); exitStatus != nil {
					exitCodeChan <- exitStatus.ExitStatus
					return
				}
			case <-sizeChangedSig:
				r, c, _ := pty.Getsize(os.Stdin)
				err = sshStream.Send(&pb.SshBytes{
					Data: &pb.SshBytes_AttributeUpdate{
						AttributeUpdate: &pb.SshBytes_SshAttributeUpdate{
							Update: &pb.SshBytes_SshAttributeUpdate_WindowSize_{
								WindowSize: &pb.SshBytes_SshAttributeUpdate_WindowSize{
									Rows:    uint32(r),
									Columns: uint32(c),
								},
							},
						},
					},
				})
				if err != nil {
					Glog.Warn(fmt.Sprintf("cannot send terminal info to compute node: %v", err))
				}
			}
		}
	}()

	// piping the netConn with the stdin/stdout/stderr
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		Glog.Warn(fmt.Sprintf("cannot set up terminal into raw mode: %v", err))
	}
	defer func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }()
	var eg errgroup.Group
	cancelableStdin, err := cancelreader.NewReader(os.Stdin)
	if err != nil {
		panic(err)
	}
	eg.Go(func() error {
		_, err := io.Copy(netConn, cancelableStdin)
		pipingErrChan <- err
		return err
	})
	eg.Go(func() error {
		_, err := io.Copy(os.Stdout, netConn)
		pipingErrChan <- err
		return err
	})

	// wait for error occurred or exit status received
	for {
		select {
		case exitCode := <-exitCodeChan: // if receiving exit status, break the loop
			Glog.Info(fmt.Sprintf("Task exited with code %d", exitCode))
			// cancel the piping stdin
			cancelableStdin.Cancel()
			// close the sshStream
			err = netConn.Close()
			if err != nil {
				Glog.Warn(fmt.Sprintf("cannot close the channel: %v", err))
			}
			// wait for the io.Copy to finish
			if err := eg.Wait(); !errors.Is(err, cancelreader.ErrCanceled) && !errors.Is(err, io.EOF) {
				panic(err)
			}
			return
		case err = <-pipingErrChan: // if receiving error, only break the loop if it is abnormal
			if !errors.Is(err, cancelreader.ErrCanceled) && !errors.Is(err, io.EOF) {
				panic(err)
			}
		}
	}
}
