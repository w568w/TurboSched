package common

import (
	"gorm.io/datatypes"
	"math"
)

const (
	Idle  int64 = math.MaxInt64 - 1
	Error int64 = math.MaxInt64
)

type DeviceModel struct {
	LocalId       uint32
	Uuid          string `gorm:"primaryKey"`
	NodeModelName string // foreign key
	Status        int64
	Tasks         []*TaskModel `gorm:"many2many:device_task;"`
}

//go:generate stringer -type=NodeStatus
type NodeStatus uint8

const (
	Offline NodeStatus = iota
	Online
)

type NodeModel struct {
	Name    string `gorm:"primaryKey"`
	Addr    string
	Port    uint16
	Status  NodeStatus
	Devices []DeviceModel
}

type TaskStatus uint8

const (
	// the task is waiting in the queue
	Pending TaskStatus = iota
	// the task is being submitted to the node(s)
	Submitting
	// the task has been submitted and waiting for attaching
	// (only for interactive tasks)
	Attaching
	// the task is running on the node(s)
	Running
	// the task has exited
	Exited
)

type TaskModel struct {
	ID                 uint64 `gorm:"primaryKey"`
	CommandLine        datatypes.JSONType[RawCommandLine]
	Status             TaskStatus
	DeviceRequirements uint32
	DeviceModels       []*DeviceModel `gorm:"many2many:device_task;"`
	Interactive        bool
}

// impl Task for TaskModel
func (t *TaskModel) GetId() uint64 {
	return t.ID
}

func (t *TaskModel) AsDBPtr() any {
	return t
}
