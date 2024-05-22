package main

import (
	"context"
	"fmt"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
	"net"
	"os"
	"turbo_sched/common"
	pb "turbo_sched/common/proto"
	"turbo_sched/turbod/compute"
	"turbo_sched/turbod/controller"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const AppName = "turbosched"

var Glog *slog.Logger

var globalConfig common.FileConfig

type CommandLineArgs struct {
	RunController bool
	RunCompute    bool
}

var args CommandLineArgs
var hostName string

func main() {
	Glog = common.SetupLogger()

	flag.BoolVarP(&args.RunController, "control", "c", false, "Run as controller")
	flag.BoolVarP(&args.RunCompute, "compute", "m", false, "Run as compute node")
	flag.Parse()

	var err error
	hostName, err = os.Hostname()
	if err != nil {
		panic(err)
	}

	common.SetupConfigNameAndPaths(Glog, AppName, "config")

	if err = viper.ReadInConfig(); err != nil {
		panic(err)
	}
	if err = viper.Unmarshal(&globalConfig); err != nil {
		panic(err)
	}

	// automatic mode detection
	if args.RunCompute && args.RunController {
		panic("Cannot run as both controller and compute node. You need to start two turbod processes if you want to do that.")
	}
	if !args.RunCompute && !args.RunController {
		Glog.Info("No mode specified. Try to detect automatically.")
		if globalConfig.Controller.HostName == hostName {
			args.RunController = true
			Glog.Info("Detected controller mode.")
		} else {
			args.RunCompute = true
			Glog.Info("Detected compute mode.")
		}
	}

	if args.RunController {
		// warn user if host name is not the same as in config
		if globalConfig.Controller.HostName != hostName {
			Glog.Warn(fmt.Sprintf("In config, controller host name is %s, but detected host name is %s. This may cause problems.", globalConfig.Controller.HostName, hostName))
		}
		controlMain()
	} else if args.RunCompute {
		computeMain()
	}
}

func controlMain() {
	Glog.Info("control")
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	err = db.AutoMigrate(&common.DeviceModel{}, &common.NodeModel{}, &common.TaskModel{})
	if err != nil {
		panic(err)
	}

	controllerInterface := controller.NewControlInterface(db, Glog)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", globalConfig.Controller.Port))
	if err != nil {
		panic(err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterControllerServer(grpcServer, controllerInterface)
	err = grpcServer.Serve(l)
	if err != nil {
		panic(err)
	}
}

func probeGPUDevices() []*pb.NodeInfo_DeviceInfo {
	devices := make([]*pb.NodeInfo_DeviceInfo, 0)
	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		panic(fmt.Sprintf("Unable to initialize NVML: %v", nvml.ErrorString(ret)))
	}
	defer func() {
		ret := nvml.Shutdown()
		if ret != nvml.SUCCESS {
			Glog.Error(fmt.Sprintf("Unable to shutdown NVML: %v", nvml.ErrorString(ret)))
		}
	}()

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		panic(fmt.Sprintf("Unable to get device count: %v", nvml.ErrorString(ret)))
	}

	for i := 0; i < count; i++ {
		device, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			panic(fmt.Sprintf("Unable to get handle of device at index %d: %v", i, nvml.ErrorString(ret)))
		}

		uuid, ret := device.GetUUID()
		if ret != nvml.SUCCESS {
			panic(fmt.Sprintf("Unable to get UUID of device at index %d: %v", i, nvml.ErrorString(ret)))
		}

		devices = append(devices, &pb.NodeInfo_DeviceInfo{
			LocalId: uint32(i),
			Uuid:    uuid,
			Status:  common.Idle,
		})
	}

	return devices
}

func computeMain() {
	Glog.Info("compute")

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", globalConfig.Controller.Addr, globalConfig.Controller.Port), opts...)
	if err != nil {
		panic(err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)

	client := pb.NewControllerClient(conn)

	computeInterface := compute.NewComputeInterface(client, Glog)

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	svrOpts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpcauth.StreamServerInterceptor(computeInterface.AuthIntercept)),
	}
	grpcServer := grpc.NewServer(svrOpts...)
	pb.RegisterComputeServer(grpcServer, computeInterface)
	var eg errgroup.Group
	eg.Go(func() error {
		return grpcServer.Serve(l)
	})

	Glog.Info(fmt.Sprintf("RPC server started at %s", l.Addr().String()))
	// parse port from address
	tcpAddr, err := net.ResolveTCPAddr(l.Addr().Network(), l.Addr().String())
	if err != nil {
		panic(err)
	}
	listenPort := tcpAddr.Port
	_, err = client.CheckInNode(context.Background(), &pb.NodeInfo{
		HostName: hostName,
		Port:     uint32(listenPort),
		Devices:  probeGPUDevices(),
	})
	if err != nil {
		panic(err)
	}
	Glog.Info("Node checked in with controller")
	if err = eg.Wait(); err != nil {
		panic(err)
	}
}
