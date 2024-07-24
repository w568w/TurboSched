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
	taskSub     *common.EventBus[uint64] // TODO: replace with a stated channel
	taskCancel  map[uint64]*common.TaskCanceler
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
		taskCancel:  make(map[uint64]*common.TaskCanceler),
		connections: make(map[string]pb.ComputeClient),
		Glog:        logger,
	}
	go func() {
		for {
			schedCond.L.Lock()
			schedCond.Wait()
			c.Glog.Debug("schedCond triggered")
			var nextTask common.TaskModel
			result := c.Database.Where(&common.TaskModel{Status: common.Pending}, "Status").Take(&nextTask)
			if result.Error != nil {
				schedCond.L.Unlock()
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					c.Glog.Debug("No task to schedule")
					continue
				}
				// panic if the database has something wrong
				panic(result.Error)
			}
			canceler := c.taskCancel[nextTask.ID]
			if canceler.HasCanceled() {
				schedCond.L.Unlock()
				continue
			}

			canceler.RoutineRegister(nil)

			var nodeWithAvailDevices []common.NodeModel
			result = c.Database.Model(&common.NodeModel{}).
				Preload("Devices", c.Database.Where(&common.DeviceModel{Status: common.Idle}, "Status")).
				Find(&nodeWithAvailDevices)
			if result.Error != nil {
				canceler.RoutineUnregister()
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
				c.Glog.Info("No enough devices for task", nextTask.ID)
				canceler.RoutineUnregister()
				schedCond.L.Unlock()
				continue
			}

			// set pickDevice to busy and task to submitting
			err := c.Database.Transaction(func(tx *gorm.DB) error {
				for _, device := range pickDevice {
					err := tx.Model(device).Select("Status").Updates(common.DeviceModel{
						Status: int64(nextTask.ID),
					}).Error
					if err != nil {
						return err
					}
				}
				err := tx.Model(&nextTask).Select("Status").Updates(common.TaskModel{
					Status: common.Submitting,
				}).Error
				return err
			})
			if err != nil {
				canceler.RoutineUnregister()
				schedCond.L.Unlock()
				panic(err)
			}

			canceler.RoutineUnregister()
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
		return 0, common.WrapError(common.UCodeDatabase, "failed to put task into pool", err, false)
	}
	c.taskCancel[id] = common.NewTaskCanceler(false)

	return id, nil
}

func (c *ControlInterface) getComputeClient(cxt context.Context, node *common.NodeModel) (pb.ComputeClient, error) {
	if c.connections[node.Name] == nil {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		conn, err := grpc.DialContext(cxt, net.JoinHostPort(node.Addr, fmt.Sprintf("%d", node.Port)), opts...)
		if err != nil {
			return nil, err
		}
		computeClient := pb.NewComputeClient(conn)
		c.connections[node.Name] = computeClient
	}
	return c.connections[node.Name], nil
}

func (c *ControlInterface) markNodeOffline(db *gorm.DB, node *common.NodeModel, err error) error {
	c.Glog.Warn(fmt.Sprintf("Node %s is offline, err: %s", node.Name, err.Error()))
	return db.Model(node).Select("Status").Updates(common.NodeModel{
		Status: common.Offline,
	}).Error
}

//type NodeOfflineError struct {
//	Err error
//}
//
//func (e NodeOfflineError) Error() string {
//	return fmt.Sprintf("node offline: %s", e.Err.Error())
//}

func (c *ControlInterface) executeTask(task *common.TaskModel, node *common.NodeModel, deviceModels []*common.DeviceModel) error {
	c.Glog.Debug(fmt.Sprintf("Executing task %d on node %s with devices %v", task.ID, node.Name, deviceModels))

	canceler := c.taskCancel[task.ID]
	if canceler.HasCanceled() {
		return context.Canceled
	}
	cxt := canceler.RoutineRegister(nil)
	defer canceler.RoutineUnregister()

	realClient, err := c.getComputeClient(cxt, node)
	if err != nil {
		// TODO when error happens, we should tell the client that we have some trouble
		return common.WrapError(common.UCodeDialNode, "failed to dial compute client", err, false)
	}

	cmdLine := task.CommandLine.Data()
	_, err = realClient.TaskAssign(cxt, &pb.TaskAssignInfo{
		Id:          &pb.TaskId{Id: task.ID},
		CommandLine: cmdLine.ToCommandLine(),
		DeviceUuids: common.Map(deviceModels, func(device *common.DeviceModel) string {
			return device.Uuid
		}),
		Interactive: task.Interactive,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		dbErr := c.Database.Transaction(func(tx *gorm.DB) error {
			err := c.markNodeOffline(tx, node, err)
			if err != nil {
				return err
			}
			// return the device to the pool
			// set device to idle
			err = tx.Model(&common.DeviceModel{}).
				Where(&common.DeviceModel{Status: int64(task.ID)}, "Status").
				Update("Status", common.Idle).Error
			if err != nil {
				return err
			}
			// return the task to the pool
			err = c.taskPool.Action(func() (bool, error) {
				return true, c.Database.Model(task).Select("Status").Updates(common.TaskModel{
					Status: common.Pending,
				}).Error
			})
			return err
		})
		if dbErr != nil {
			return common.WrapError(common.UCodeDatabase, "failed to rollback task", dbErr, false)
		}

		return common.WrapError(common.UCodeAssignTask, "failed to assign task to node", err, false)
	}

	result := c.Database.Model(&task).Select("Status").Updates(common.TaskModel{
		Status: common.Running,
	})
	if result.Error != nil {
		return common.WrapError(common.UCodeDatabase, "failed to update task status to running", result.Error, false)
	}
	return nil
}

func (c *ControlInterface) CheckInNode(ctx context.Context, nodeInfo *pb.NodeInfo) (*emptypb.Empty, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, common.NewError(common.UCodeInvalidPeerInfo, "no peer info in context", false)
	}
	// prepare node info
	tcpAddr, err := net.ResolveTCPAddr(p.Addr.Network(), p.Addr.String())
	if err != nil {
		return nil, common.WrapError(common.UCodeInvalidPeerInfo, "failed to resolve peer TCP address", err, false)
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

	c.Glog.Info(fmt.Sprintf("New node %s:%d with %d devices", nodeInfo.HostName, nodeInfo.Port, len(nodeInfo.Devices)))

	err = c.Database.Transaction(func(tx *gorm.DB) error {
		err := tx.Save(&node).Error
		if err != nil {
			return err
		}
		// remove old devices which Uuid is not in Devices
		err = tx.Where("node_model_name = ? AND uuid NOT IN ?", nodeInfo.HostName, deviceUuids).Delete(&common.DeviceModel{}).Error
		if err != nil {
			return err
		}
		err = tx.Save(&deviceModels).Error
		return err
	})
	if err != nil {
		return nil, common.WrapError(common.UCodeDatabase, "failed to save node and devices", err, false)
	}
	return &emptypb.Empty{}, nil
}

func (c *ControlInterface) ReportTask(ctx context.Context, info *pb.TaskReportInfo) (*emptypb.Empty, error) {
	canceler, ok := c.taskCancel[info.Id.Id]
	if !ok {
		return nil, common.NewError(common.UCodeTaskNotFound, "task not found", false)
	}
	exitedStatus := info.GetExited()
	err := info.GetError()
	// only cancel if the task is not exiting or errored. otherwise, we can just let it go
	if canceler.HasCanceled() && exitedStatus == nil && err == nil {
		return nil, context.Canceled
	}
	_ = canceler.RoutineRegister(ctx)
	defer canceler.RoutineUnregister()
	if exitedStatus != nil {
		task := common.TaskModel{
			ID:     info.Id.Id,
			Status: common.Exited,
		}
		c.Glog.Info(fmt.Sprintf("Task %d exited with code %d:\n%s", info.Id.Id, exitedStatus.ExitCode, exitedStatus.Output))
		err := c.Database.Transaction(func(tx *gorm.DB) error {
			result := tx.Model(&task).Select("Status").Updates(task)
			if result.Error != nil {
				return result.Error
			}
			// set device to idle
			result = tx.Model(&common.DeviceModel{}).
				Where(&common.DeviceModel{Status: int64(info.Id.Id)}, "Status").
				Update("Status", common.Idle)
			return result.Error
		})
		if err != nil {
			return nil, common.WrapError(common.UCodeDatabase, "failed to update task status to exited", err, false)
		}
	} else if readyForAttach := info.GetReadyForAttach(); readyForAttach != nil {
		task := common.TaskModel{
			ID:     info.Id.Id,
			Status: common.Attaching,
		}

		var node common.NodeModel
		err := c.Database.Transaction(func(tx *gorm.DB) error {
			result := tx.Model(&task).Select("Status").Updates(task)
			if result.Error != nil {
				return result.Error
			}
			// get all devices assigned to the task
			var deviceModels []common.DeviceModel
			result = tx.Where(&common.DeviceModel{Status: int64(info.Id.Id)}, "Status").Find(&deviceModels)
			if result.Error != nil {
				return result.Error
			}

			// get node with the devices
			// TODO change for multi-node scheduling
			node.Name = deviceModels[0].NodeModelName
			result = tx.First(&node)
			return result.Error
		})
		if err != nil {
			return nil, common.WrapError(common.UCodeDatabase, "failed to update task status to attaching", err, false)
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
	} else if err != nil {
		c.Glog.Error(fmt.Sprintf("Task %d errored: %s", info.Id.Id, err.Message))
		task := common.TaskModel{
			ID:           info.Id.Id,
			Status:       common.Errored,
			ErrorMessage: err.Message,
		}
		err := c.Database.Transaction(func(tx *gorm.DB) error {
			result := tx.Model(&task).Select("Status", "ErrorMessage").Updates(task)
			if result.Error != nil {
				return result.Error
			}

			// set device to idle
			result = tx.Model(&common.DeviceModel{}).
				Where(&common.DeviceModel{Status: int64(info.Id.Id)}, "Status").
				Update("Status", common.Idle)
			return result.Error
		})
		if err != nil {
			return nil, common.WrapError(common.UCodeDatabase, "failed to update task status to errored", err, false)
		}

		c.taskSub.Publish(info.Id.Id, TaskEvent{
			NewStatus: common.Errored,
			ExtraData: err,
		})
	} else {
		return nil, common.NewError(common.UCodeMalformedVariant, "unrecognized variant in task report", false)
	}
	return &emptypb.Empty{}, nil
}

func (c *ControlInterface) SubmitNewTask(ctx context.Context, taskInfo *pb.TaskSubmitInfo) (*pb.TaskId, error) {
	id, wrappedErr := c.submitNewTask(taskInfo, false)
	if wrappedErr != nil {
		return nil, wrappedErr
	}
	return &pb.TaskId{Id: id}, nil
}

func (c *ControlInterface) SubmitNewTaskInteractive(taskInfo *pb.TaskSubmitInfo, stream pb.Controller_SubmitNewTaskInteractiveServer) error {
	id, wrappedErr := c.submitNewTask(taskInfo, true)
	if wrappedErr != nil {
		return wrappedErr
	}

	err := stream.Send(&pb.TaskEvent{Event: &pb.TaskEvent_ObtainedId{ObtainedId: &pb.TaskId{Id: id}}})
	if err != nil {
		// TODO when unable to contact with client, we should cancel the task
		return common.WrapError(common.UCodeStreamError, "failed to send task id to client", err, false)
	}
	notification := c.taskSub.Subscribe(id, 0)
	defer c.taskSub.Unsubscribe(id, notification)
	canceler := c.taskCancel[id]
	if canceler.HasCanceled() {
		return context.Canceled
	}
	cxt := canceler.RoutineRegister(nil)
	defer canceler.RoutineUnregister()
	for {
		select {
		case event, ok := <-notification:
			if !ok {
				// the task status channel is closed. Stop the stream
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
				if err != nil {
					// maybe client has disconnected?
					c.Glog.Error(fmt.Sprintf("Failed to send ready for attach event to client: %s", err.Error()))
					return common.WrapError(common.UCodeStreamError, "failed to send ready for attach event to client", err, false)
				}
			}
			// fixme: enumerate all possible status
			if newStatus >= common.Attaching {
				return nil
			}
		case <-cxt.Done():
			return cxt.Err() // usually context.Canceled
		}
	}
}

func (c *ControlInterface) CancelTask(ctx context.Context, id *pb.TaskId) (*emptypb.Empty, error) {
	canceler, ok := c.taskCancel[id.Id]
	if !ok {
		// fixme: canceling an old task?
		return nil, common.NewError(common.UCodeTaskNotFound, "task not found", false)
	}
	if canceler.HasCanceled() {
		return nil, common.NewError(common.UCodeWrongTaskState, "task already canceled", false)
	}
	ok = canceler.CancelAndWaitAllRoutine()
	if !ok {
		return nil, common.NewError(common.UCodeWrongTaskState, "task already canceled", false)
	}

	// clean up resources according to the task status
	var task = common.TaskModel{ID: id.Id}
	err := c.Database.First(&task).Error
	if err != nil {
		return nil, common.WrapError(common.UCodeDatabase, "task is in taskCancel list but not found in database. This is a bug: does anyone modify the database directly?", err, false)
	}

	removeAndFreeDevice := func() error {
		return c.Database.Transaction(func(tx *gorm.DB) error {
			// set device to idle
			err := tx.Model(&common.DeviceModel{}).
				Where(&common.DeviceModel{Status: int64(id.Id)}, "Status").
				Update("Status", common.Idle).Error
			if err != nil {
				return err
			}
			// set task to canceled
			task.Status = common.Canceled
			err = tx.Model(&task).Select("Status").Updates(task).Error
			if err != nil {
				return err
			}

			c.taskSub.Publish(task.ID, TaskEvent{
				NewStatus: common.Canceled,
				ExtraData: nil,
			})
			return nil
		})
	}
	switch task.Status {
	case common.Pending:
		if err = removeAndFreeDevice(); err != nil {
			return nil, common.WrapError(common.UCodeDatabase, "failed to cancel the pending task", err, false)
		}
	case common.Submitting:
		if err = removeAndFreeDevice(); err != nil {
			return nil, common.WrapError(common.UCodeDatabase, "failed to cancel the submitting task", err, false)
		}
	case common.Attaching:
		fallthrough
	case common.Running:
		// TODO change for multi-node scheduling
		var deviceModels []common.DeviceModel
		result := c.Database.Where(&common.DeviceModel{Status: int64(id.Id)}, "Status").Find(&deviceModels)
		if result.Error != nil {
			return nil, common.WrapError(common.UCodeDatabase, "failed to get devices for task", result.Error, false)
		}
		node := common.NodeModel{Name: deviceModels[0].NodeModelName}
		result = c.Database.First(&node)
		realClient, err := c.getComputeClient(ctx, &node)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				return nil, common.WrapError(common.UCodeDialNode, "failed to dial compute client to cancel task", err, false)
			}
		}
		// tell the node to terminate the task
		if _, err = realClient.TaskCancel(ctx, id); err != nil {
			if !errors.Is(err, context.Canceled) {
				// fixme UCodeAssignTask is not suitable here
				return nil, common.WrapError(common.UCodeAssignTask, "failed to cancel task running on node", err, false)
			}
		}
		if err = removeAndFreeDevice(); err != nil {
			panic(err)
		}
	case common.Exited:
		return nil, common.NewError(common.UCodeWrongTaskState, "task already exited", false)
	case common.Errored:
		return nil, common.NewError(common.UCodeWrongTaskState, "task already errored", false)
	case common.Canceled:
		return nil, common.NewError(common.UCodeWrongTaskState, "task already canceled", false)
	}
	return &emptypb.Empty{}, nil
}
