package controller

import (
	"context"
	"fmt"
	"net"
	"sync"
	"turbo_sched/common"

	"github.com/smallnest/rpcx/server"
	"gorm.io/gorm"
)

type ControlInterface struct {
	Database   *gorm.DB
	taskPool   *common.TaskPool
	devicePool *common.DevicePool
}

func NewControlInterface(db *gorm.DB) *ControlInterface {
	accessLock := sync.Mutex{}
	schedCond := sync.NewCond(&accessLock)

	go func() {
		for {
			schedCond.L.Lock()
			schedCond.Wait()
			fmt.Println("schedCond triggered")
			schedCond.L.Unlock()
		}
	}()

	return &ControlInterface{
		Database:   db,
		taskPool:   common.NewTaskPool(db, schedCond),
		devicePool: common.NewDevicePool(db, schedCond),
	}
}

func (c *ControlInterface) CheckInNode(ctx context.Context, nodeInfo *common.NodeInfo, reply *bool) error {
	conn := ctx.Value(server.RemoteConnContextKey).(net.Conn)
	// prepare node info
	node := common.NodeModel{
		Name:   nodeInfo.HostName,
		Addr:   conn.RemoteAddr().String(),
		Port:   nodeInfo.Port,
		Status: common.Online,
	}

	// prepare device info
	deviceModels := make([]*common.DeviceModel, 0, len(nodeInfo.Devices))
	deviceUuids := make([]string, 0, len(nodeInfo.Devices))
	for _, device := range nodeInfo.Devices {
		deviceModel := common.DeviceModel{
			LocalId:       device.LocalId,
			Uuid:          device.Uuid,
			NodeModelName: nodeInfo.HostName,
			Status:        device.Status,
		}
		deviceModels = append(deviceModels, &deviceModel)
		deviceUuids = append(deviceUuids, device.Uuid)
	}

	fmt.Println("CheckInNode", nodeInfo.HostName, nodeInfo.Port, nodeInfo.Devices)

	defer func() {
		*reply = true
	}()
	return c.Database.Transaction(func(tx *gorm.DB) error {
		tx.Save(&node)
		// remove old devices which Uuid is not in Devices
		tx.Where("node_model_name = ? AND uuid NOT IN ?", nodeInfo.HostName, deviceUuids).Delete(&common.DeviceModel{})
		tx.Save(&deviceModels)
		return nil
	})
}

func (c *ControlInterface) SubmitNewTask(ctx context.Context, taskInfo *common.TaskSumbitInfo, taskId *uint64) error {
	task := common.TaskModel{
		CommandLine:        taskInfo.CommandLine,
		DeviceRequirements: taskInfo.DeviceRequirements,
		Status:             common.Pending,
	}
	id, err := c.taskPool.Put(&task)
	if err != nil {
		return err
	}

	*taskId = id
	return nil
}
