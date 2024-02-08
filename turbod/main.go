package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"
	"turbo_sched/common"
	"turbo_sched/turbod/compute"
	"turbo_sched/turbod/controller"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/server"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/util/wait"

	flag "github.com/spf13/pflag"
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

	var err error = nil
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
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})

	err := db.AutoMigrate(&common.DeviceModel{}, &common.NodeModel{}, &common.TaskModel{})
	if err != nil {
		panic(err)
	}

	controllerInterface := controller.NewControlInterface(db, Glog)

	rpcServer := server.NewServer()
	err = rpcServer.RegisterName("controller", controllerInterface, "")
	if err != nil {
		panic(err)
	}
	err = rpcServer.Serve("tcp", fmt.Sprintf(":%d", globalConfig.Controller.Port))
	if err != nil {
		panic(err)
	}
}

func probeGPUDevices() []common.DeviceInfo {
	devices := make([]common.DeviceInfo, 0)
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

		devices = append(devices, common.DeviceInfo{
			LocalId: uint32(i),
			Uuid:    uuid,
			Status:  common.Idle,
		})
	}

	return devices
}

func computeMain() {
	Glog.Info("compute")

	d, err := client.NewPeer2PeerDiscovery(fmt.Sprintf("tcp@%s",
		net.JoinHostPort(globalConfig.Controller.Addr, fmt.Sprintf("%d", globalConfig.Controller.Port))), "")
	if err != nil {
		panic(err)
	}
	defer d.Close()

	xclient := client.NewXClient("controller", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer func(xclient client.XClient) {
		err := xclient.Close()
		if err != nil {
			panic(err)
		}
	}(xclient)

	computeInterface := compute.ComputeInterface{ControlClient: xclient}

	rpcServer := server.NewServer()
	err = rpcServer.RegisterName("compute", &computeInterface, "")
	if err != nil {
		panic(err)
	}

	var eg errgroup.Group
	eg.Go(func() error {
		// automatically choose a port
		return rpcServer.Serve("tcp", ":0")
	})
	// wait for server to come up
	ctx := context.Background()
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		return rpcServer.Address() != nil, nil
	})
	if err != nil {
		// must be timeout
		Glog.Error(fmt.Sprintf("Timeout when waiting for RPC server to start: %v", err))
		os.Exit(1)
	}
	Glog.Info(fmt.Sprintf("RPC server started at %s", rpcServer.Address().String()))
	// parse port from address
	tcpAddr, err := net.ResolveTCPAddr(rpcServer.Address().Network(), rpcServer.Address().String())
	if err != nil {
		panic(err)
	}
	listenPort := tcpAddr.Port

	if err = xclient.Call(context.Background(), "CheckInNode", common.NodeInfo{
		HostName: hostName,
		Port:     uint16(listenPort),
		Devices:  probeGPUDevices(),
	}, &common.VOID); err != nil {
		panic(err)
	}

	if err = eg.Wait(); err != nil {
		panic(err)
	}
}
