package controller

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"
	"gorm.io/datatypes"
	"log/slog"
	"net"
	"sync"
	"turbo_sched/common"
	pb "turbo_sched/common/proto"

	"gorm.io/gorm"
)

type ControlInterface struct {
	Database    *gorm.DB
	taskPool    *common.TaskPool
	devicePool  *common.DevicePool
	taskSub     *common.EventBus[uint64]
	connections map[string]pb.ComputeClient
	Glog        *slog.Logger
	pb.UnimplementedControllerServer
}

// TaskEvent is sent to subscribers through taskSub.
type TaskEvent struct {
	NewStatus common.TaskStatus
	// ExtraData carries extra data for the event.
	// - Attaching: the [pb.TaskEvent_ReadyForAttach] for attaching;
	ExtraData any
}

func NewControlInterface(db *gorm.DB, logger *slog.Logger) *ControlInterface {
	accessLock := sync.Mutex{}
	schedCond := sync.NewCond(&accessLock)

	c := &ControlInterface{
		Database:    db,
		taskPool:    common.NewTaskPool(db, schedCond),
		devicePool:  common.NewDevicePool(db, schedCond),
		taskSub:     common.NewEventBus[uint64](),
		connections: make(map[string]pb.ComputeClient),
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
						Status: int64(nextTask.ID),
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

			c.taskSub.Publish(nextTask.ID, TaskEvent{
				NewStatus: common.Submitting,
			})
			go c.executeTask(&nextTask, pickNode, pickDevice)
		}
	}()

	return c
}

func (c *ControlInterface) submitNewTask(taskInfo *pb.TaskSubmitInfo, interactive bool) (uint64, error) {
	task := common.TaskModel{
		CommandLine:        datatypes.NewJSONType(common.ToRawCommandLine(taskInfo.CommandLine)),
		DeviceRequirements: taskInfo.DeviceRequirement,
		Status:             common.Pending,
		Interactive:        interactive,
	}
	id, err := c.taskPool.Put(&task)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (c *ControlInterface) executeTask(task *common.TaskModel, node *common.NodeModel, deviceModels []*common.DeviceModel) {
	fmt.Println("executeTask", task.ID, node.Name, deviceModels)

	if c.connections[node.Name] == nil {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		conn, err := grpc.Dial(net.JoinHostPort(node.Addr, fmt.Sprintf("%d", node.Port)), opts...)
		if err != nil {
			panic(err)
		}
		computeClient := pb.NewComputeClient(conn)
		c.connections[node.Name] = computeClient
	}

	realClient := c.connections[node.Name]

	cmdLine := task.CommandLine.Data()
	_, err := realClient.TaskAssign(context.Background(), &pb.TaskAssignInfo{
		Id:          &pb.TaskId{Id: task.ID},
		CommandLine: cmdLine.ToCommandLine(),
		DeviceUuids: common.Map(deviceModels, func(device *common.DeviceModel) string {
			return device.Uuid
		}),
		Interactive: task.Interactive,
	})
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

func (c *ControlInterface) CheckInNode(ctx context.Context, nodeInfo *pb.NodeInfo) (*emptypb.Empty, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, errors.New("failed to get peer")
	}
	// prepare node info
	tcpAddr, err := net.ResolveTCPAddr(p.Addr.Network(), p.Addr.String())
	if err != nil {
		panic(err)
	}
	node := common.NodeModel{
		Name:   nodeInfo.HostName,
		Addr:   tcpAddr.IP.String(),
		Port:   uint16(nodeInfo.Port),
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

	err = c.Database.Transaction(func(tx *gorm.DB) error {
		tx.Save(&node)
		// remove old devices which Uuid is not in Devices
		tx.Where("node_model_name = ? AND uuid NOT IN ?", nodeInfo.HostName, deviceUuids).Delete(&common.DeviceModel{})
		tx.Save(&deviceModels)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (c *ControlInterface) ReportTask(ctx context.Context, info *pb.TaskReportInfo) (*emptypb.Empty, error) {
	if exitedStatus := info.GetExited(); exitedStatus != nil {
		task := common.TaskModel{
			ID:     info.Id.Id,
			Status: common.Exited,
		}
		c.Glog.Info(fmt.Sprintf("Task %d exited with code %d:\n%s", info.Id.Id, exitedStatus.ExitCode, exitedStatus.Output))
		result := c.Database.Model(&task).Select("Status").Updates(task)
		if result.Error != nil {
			return nil, result.Error
		}

		// set device to idle
		result = c.Database.Model(&common.DeviceModel{}).
			Where(&common.DeviceModel{Status: int64(info.Id.Id)}, "Status").
			Update("Status", common.Idle)
		if result.Error != nil {
			return nil, result.Error
		}
	} else if readyForAttach := info.GetReadyForAttach(); readyForAttach != nil {
		task := common.TaskModel{
			ID:     info.Id.Id,
			Status: common.Attaching,
		}
		result := c.Database.Model(&task).Select("Status").Updates(task)
		if result.Error != nil {
			return nil, result.Error
		}

		// get connection info
		// TODO change for multi-node scheduling
		var deviceModels []common.DeviceModel
		result = c.Database.Where(&common.DeviceModel{Status: int64(info.Id.Id)}, "Status").Find(&deviceModels)
		if result.Error != nil {
			return nil, result.Error
		}
		node := common.NodeModel{Name: deviceModels[0].NodeModelName}
		result = c.Database.First(&node)
		if result.Error != nil {
			return nil, result.Error
		}

		c.taskSub.Publish(info.Id.Id, TaskEvent{
			NewStatus: common.Attaching,
			ExtraData: &pb.TaskEvent_ReadyForAttach{
				Id: info.Id,
				ConnInfos: []*pb.TaskEvent_ConnInfo{
					{
						Host:  node.Addr,
						Port:  uint32(node.Port),
						Token: readyForAttach.Token,
					},
				},
			},
		})
	} else {
		return nil, errors.New("unknown report type")
	}
	return &emptypb.Empty{}, nil
}

func (c *ControlInterface) SubmitNewTask(ctx context.Context, taskInfo *pb.TaskSubmitInfo) (*pb.TaskId, error) {
	id, err := c.submitNewTask(taskInfo, false)
	if err != nil {
		return nil, err
	}
	return &pb.TaskId{Id: id}, nil
}

func (c *ControlInterface) SubmitNewTaskInteractive(taskInfo *pb.TaskSubmitInfo, stream pb.Controller_SubmitNewTaskInteractiveServer) error {
	id, err := c.submitNewTask(taskInfo, true)
	if err != nil {
		return err
	}

	err = stream.Send(&pb.TaskEvent{Event: &pb.TaskEvent_ObtainedId{ObtainedId: &pb.TaskId{Id: id}}})
	if err != nil {
		return err
	}
	notification := c.taskSub.Subscribe(id, 0)
	defer c.taskSub.Unsubscribe(id, notification)
	for {
		select {
		case event, ok := <-notification:
			if !ok {
				return nil
			}
			newStatus := event.(TaskEvent).NewStatus
			if newStatus == common.Attaching {
				attachment := event.(TaskEvent).ExtraData.(*pb.TaskEvent_ReadyForAttach)
				err = stream.Send(&pb.TaskEvent{
					Event: &pb.TaskEvent_ReadyForAttach_{
						ReadyForAttach: attachment,
					},
				})
			}
			// fixme: enumerate all possible status
			if newStatus >= common.Attaching {
				return nil
			}
		}
	}
}
