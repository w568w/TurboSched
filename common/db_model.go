package common

import (
	"gorm.io/datatypes"
	"math"
)

//go:generate stringer -type=DeviceStatus
type DeviceStatus int64

const (
	Idle  DeviceStatus = math.MaxInt64 - 1
	Error DeviceStatus = math.MaxInt64
)

type DeviceModel struct {
	LocalId       uint32
	Uuid          string `gorm:"primaryKey"`
	NodeModelName string // foreign key
	Status        DeviceStatus
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
	// the task is running on the node(s)
	Running
	// the task has exited
	Exited
)

type TaskModel struct {
	ID                 uint64 `gorm:"primaryKey"`
	CommandLine        datatypes.JSONType[CommandLine]
	Status             TaskStatus
	DeviceRequirements uint32
	DeviceModels       []*DeviceModel `gorm:"many2many:device_task;"`
}

// impl Task for TaskModel
func (t *TaskModel) GetId() uint64 {
	return t.ID
}

func (t *TaskModel) AsDBPtr() any {
	return t
}

type CommandLine struct {
	Program string
	// Args is the list of command-line arguments.
	// You should NOT contain the program name in the args.
	Args []string
	Env  []string
}
