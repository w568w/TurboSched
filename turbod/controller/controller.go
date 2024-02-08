package controller

import (
	"context"
	"errors"
	"fmt"
	"github.com/smallnest/rpcx/client"
	"gorm.io/datatypes"
	"log/slog"
	"net"
	"sync"
	"turbo_sched/common"

	"github.com/smallnest/rpcx/server"
	"gorm.io/gorm"
)

type ControlInterface struct {
	Database    *gorm.DB
	taskPool    *common.TaskPool
	devicePool  *common.DevicePool
	connections map[string]client.XClient
	Glog        *slog.Logger
}

func NewControlInterface(db *gorm.DB, logger *slog.Logger) *ControlInterface {
	accessLock := sync.Mutex{}
	schedCond := sync.NewCond(&accessLock)

	c := &ControlInterface{
		Database:    db,
		taskPool:    common.NewTaskPool(db, schedCond),
		devicePool:  common.NewDevicePool(db, schedCond),
		connections: make(map[string]client.XClient),
		Glog:        logger,
	}
	go func() {
		for {
			schedCond.L.Lock()
			schedCond.Wait()
			fmt.Println("schedCond triggered")
			var nextTask common.TaskModel
			result := c.Database.Where(&common.TaskModel{Status: common.Pending}, "Status").Take(&nextTask)
			if result.Error != nil {
				schedCond.L.Unlock()
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					fmt.Println("No task to schedule")
					continue
				}
				panic(result.Error)
			}
			var nodeWithAvailDevices []common.NodeModel
			result = c.Database.Model(&common.NodeModel{}).
				Preload("Devices", c.Database.Where(&common.DeviceModel{Status: common.Idle}, "Status")).
				Find(&nodeWithAvailDevices)
			if result.Error != nil {
				schedCond.L.Unlock()
				panic(result.Error)
			}

			// find [nextTask.DeviceRequirements] devices on the same node from nodeWithAvailDevices
			var pickNode *common.NodeModel
			var pickDevice []*common.DeviceModel
			for _, node := range nodeWithAvailDevices {
				if len(node.Devices) >= int(nextTask.DeviceRequirements) {
					pickNode = &node
					for i := 0; i < int(nextTask.DeviceRequirements); i++ {
						pickDevice = append(pickDevice, &node.Devices[i])
					}
				}
			}

			if pickNode == nil {
				fmt.Println("No enough devices for task", nextTask.ID)
				schedCond.L.Unlock()
				continue
			}

			// set pickDevice to busy and task to submitting
			err := c.Database.Transaction(func(tx *gorm.DB) error {
				for _, device := range pickDevice {
					tx.Model(device).Select("Status").Updates(common.DeviceModel{
						Status: common.DeviceStatus(nextTask.ID),
					})
				}
				tx.Model(&nextTask).Select("Status").Updates(common.TaskModel{
					Status: common.Submitting,
				})
				return nil
			})
			if err != nil {
				schedCond.L.Unlock()
				panic(err)
			}

			schedCond.L.Unlock()

			go c.executeTask(&nextTask, pickNode, pickDevice)
		}
	}()

	return c
}

func (c *ControlInterface) CheckInNode(ctx context.Context, nodeInfo *common.NodeInfo, reply *common.Void) error {
	conn := ctx.Value(server.RemoteConnContextKey).(net.Conn)
	// prepare node info
	tcpAddr, err := net.ResolveTCPAddr(conn.RemoteAddr().Network(), conn.RemoteAddr().String())
	if err != nil {
		panic(err)
	}
	node := common.NodeModel{
		Name:   nodeInfo.HostName,
		Addr:   tcpAddr.IP.String(),
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
		*reply = common.VOID
	}()
	return c.Database.Transaction(func(tx *gorm.DB) error {
		tx.Save(&node)
		// remove old devices which Uuid is not in Devices
		tx.Where("node_model_name = ? AND uuid NOT IN ?", nodeInfo.HostName, deviceUuids).Delete(&common.DeviceModel{})
		tx.Save(&deviceModels)
		return nil
	})
}

func (c *ControlInterface) SubmitNewTask(ctx context.Context, taskInfo *common.TaskSubmitInfo, taskId *uint64) error {
	task := common.TaskModel{
		CommandLine:        datatypes.NewJSONType(taskInfo.CommandLine),
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

func (c *ControlInterface) ReportTask(ctx context.Context, taskInfo *common.TaskReportInfo, reply *common.Void) error {
	task := common.TaskModel{
		ID:     taskInfo.ID,
		Status: common.Exited,
	}
	c.Glog.Info(fmt.Sprintf("Task %d exited with code %d:\n%s", taskInfo.ID, taskInfo.ExitCode, taskInfo.Output))
	result := c.Database.Model(&task).Select("Status").Updates(task)
	if result.Error != nil {
		return result.Error
	}

	// set device to idle
	result = c.Database.Model(&common.DeviceModel{}).
		Where(&common.DeviceModel{Status: common.DeviceStatus(taskInfo.ID)}, "Status").
		Update("Status", common.Idle)
	if result.Error != nil {
		return result.Error
	}
	*reply = common.VOID
	return nil
}

func (c *ControlInterface) executeTask(task *common.TaskModel, node *common.NodeModel, deviceModels []*common.DeviceModel) {
	fmt.Println("executeTask", task.ID, node.Name, deviceModels)

	if c.connections[node.Name] == nil {
		d, err := client.NewPeer2PeerDiscovery(fmt.Sprintf("tcp@%s",
			net.JoinHostPort(node.Addr, fmt.Sprintf("%d", node.Port))), "")
		if err != nil {
			panic(err)
		}
		xclient := client.NewXClient("compute", client.Failtry, client.RandomSelect, d, client.DefaultOption)
		c.connections[node.Name] = xclient
	}

	xclient := c.connections[node.Name]

	err := xclient.Call(context.Background(), "TaskAssign", common.TaskAssignInfo{
		ID:          task.ID,
		CommandLine: task.CommandLine.Data(),
		DeviceUuids: common.Map(deviceModels, func(device *common.DeviceModel) string {
			return device.Uuid
		}),
	}, &common.VOID)
	if err != nil {
		panic(err)
	}

	result := c.Database.Model(&task).Select("Status").Updates(common.TaskModel{
		Status: common.Running,
	})
	if result.Error != nil {
		panic(result.Error)
	}
}
