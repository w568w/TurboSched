package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/creack/pty"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/ross96D/cancelreader"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcMetadata "google.golang.org/grpc/metadata"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
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

func main() {
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
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", globalConfig.Controller.Addr, globalConfig.Controller.Port), opts...)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewControllerClient(conn)

	stream, err := client.SubmitNewTaskInteractive(context.Background(), &pb.TaskSubmitInfo{
		CommandLine: &pb.CommandLine{
			Program: "btop",
		},
		DeviceRequirement: 1,
	})
	if err != nil {
		panic(err)
	}

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Panicf("%v.SubmitNewTaskInteractive(_) = _, %v", client, err)
		}
		log.Println(event)
		if readyForAttach := event.GetReadyForAttach(); readyForAttach != nil {
			interactiveTaskMain(readyForAttach)
		}
	}
}

func interactiveTaskMain(readyForAttach *pb.TaskEvent_ReadyForAttach) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", readyForAttach.ConnInfos[0].Host, readyForAttach.ConnInfos[0].Port), opts...)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	computeClient := pb.NewComputeClient(conn)

	sshStream, err := computeClient.SshTunnel(ctxWithToken(context.Background(), "bearer", readyForAttach.ConnInfos[0].Token))
	if err != nil {
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
		panic(err)
	}
	println("Window size sent")

	exitCodeChan := make(chan int32, 1)
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
					// TODO: log error to console
					println("error sending window size")
				}
			}
		}
	}()

	// piping the netConn with the stdin/stdout/stderr
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	defer func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }()
	var eg errgroup.Group
	cancelableStdin, err := cancelreader.NewReader(os.Stdin)
	if err != nil {
		panic(err)
	}
	eg.Go(func() error {
		_, err := io.Copy(netConn, cancelableStdin)
		return err
	})
	eg.Go(func() error {
		_, err := io.Copy(os.Stdout, netConn)
		return err
	})

	// wait for the command to exit
	exitCode := <-exitCodeChan
	println("Command exited with code", exitCode)
	// cancel the piping stdin
	cancelableStdin.Cancel()
	// close the sshStream
	netConn.Close()
	// wait for the io.Copy to finish
	if err := eg.Wait(); !errors.Is(err, cancelreader.ErrCanceled) && !errors.Is(err, io.EOF) {
		panic(err)
	}
}
