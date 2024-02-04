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
	CommandLine CommandLine
	DeviceUuids []string
}

type TaskSubmitInfo struct {
	CommandLine        CommandLine
	DeviceRequirements uint32
}

type TaskReportInfo struct {
	ID       uint64
	Output   []byte
	ExitCode int
}
