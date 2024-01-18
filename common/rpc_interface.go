package common

type NodeInfo struct {
	HostName string
	Port     uint16
	Devices  []DeviceInfo
}

type DeviceInfo struct {
	LocalId uint32
	Uuid    string
	Status  DeviceStatus
}

type TaskAssignInfo struct {
	ID          uint64
	CommandLine string
	DeviceUuids []string
}

type TaskSumbitInfo struct {
	CommandLine        string
	DeviceRequirements uint32
}
