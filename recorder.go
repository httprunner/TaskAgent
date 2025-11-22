package pool

import (
	"context"
	"time"
)

// DeviceInfoUpdate captures snapshot metadata for a device.
type DeviceInfoUpdate struct {
	DeviceSerial string
	Status       string
	OSType       string
	OSVersion    string
	IsRoot       string
	AgentVersion string
	ProviderUUID string
	LastError    string
	LastSeenAt   time.Time
	RunningTask  string
	PendingTasks []string
}

// DeviceRecorder receives callbacks from the pool to persist device/job state.
type DeviceRecorder interface {
	UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error
}
