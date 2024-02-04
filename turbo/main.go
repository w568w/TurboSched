package main

import (
	"context"
	"fmt"
	"log/slog"
	"turbo_sched/common"

	"github.com/smallnest/rpcx/client"
	"github.com/spf13/viper"
)

const APP_NAME = "turbosched"

var globalConfig common.FileConfig

func main() {
	common.SetupConfigNameAndPaths(slog.Default(), APP_NAME, "config")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
	if err := viper.Unmarshal(&globalConfig); err != nil {
		panic(err)
	}

	d, err := client.NewPeer2PeerDiscovery(fmt.Sprintf("tcp@%s:%d", globalConfig.Controller.Addr, globalConfig.Controller.Port), "")
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

	var reply uint64
	err = xclient.Call(context.Background(), "SubmitNewTask", common.TaskSubmitInfo{
		CommandLine: common.CommandLine{
			Program: "ls",
		},
		DeviceRequirements: 1,
	}, &reply)
	if err != nil {
		panic(err)
	}

	fmt.Println("Task ID:", reply)
}
