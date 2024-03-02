package main

import (
	"context"
	"fmt"
	"github.com/creack/pty"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/spf13/viper"
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

const APP_NAME = "turbosched"

var globalConfig common.FileConfig

func ctxWithToken(ctx context.Context, scheme string, token string) context.Context {
	md := grpcMetadata.Pairs("authorization", fmt.Sprintf("%s %v", scheme, token))
	return metautils.NiceMD(md).ToOutgoing(ctx)
}

func main() {
	common.SetupConfigNameAndPaths(slog.Default(), APP_NAME, "config")
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
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%v.SubmitNewTaskInteractive(_) = _, %v", client, err)
		}
		log.Println(event)
		if readyForAttach := event.GetReadyForAttach(); readyForAttach != nil {
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

			netConn, _ := common.NewGrpcConn(sshStream)

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

			// Handle pty size.
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, syscall.SIGWINCH)
			go func() {
				for range ch {
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
				}
			}()

			// piping the netConn with the stdin/stdout/stderr
			oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
			if err != nil {
				panic(err)
			}
			defer func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }()
			go func() {
				_, err := io.Copy(netConn, os.Stdin)
				if err != nil {
					panic(err)
				}
			}()
			_, err = io.Copy(os.Stdout, netConn)
			if err != nil {
				panic(err)
			}

			//sshClientConfig := ssh.ClientConfig{
			//	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			//}
			//con, n, r, err := ssh.NewClientConn(netConn, fmt.Sprintf("%s:%d", readyForAttach.ConnInfos[0].Host, readyForAttach.ConnInfos[0].Port), &sshClientConfig)
			//if err != nil {
			//	panic(err)
			//}
			//sshClient := ssh.NewClient(con, n, r)
			//session, err := sshClient.NewSession()
			//if err != nil {
			//	panic(err)
			//}
			//defer session.Close()
			//
			//p, err := session.StdoutPipe()
			//if err != nil {
			//	panic(err)
			//}
			//output, err := io.ReadAll(p)
			//if err != nil {
			//	panic(err)
			//}
			//fmt.Println(string(output))
		}
	}

}
