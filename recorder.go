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
	AgentVersion string
	LastError    string
	LastSeenAt   time.Time
}

// DeviceJobRecord describes one dispatch batch for a device.
type DeviceJobRecord struct {
	JobID         string
	DeviceSerial  string
	App           string
	State         string
	AssignedTasks []string
	RunningTask   string
	StartAt       time.Time
}

// DeviceJobUpdate describes terminal state for a dispatched job.
type DeviceJobUpdate struct {
	State          string
	RunningTask    string
	EndAt          *time.Time
	ElapsedSeconds *int64
	ErrorMessage   string
}

// DeviceRecorder receives callbacks from the pool to persist device/job state.
type DeviceRecorder interface {
	UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error
	CreateJob(ctx context.Context, rec *DeviceJobRecord) error
	UpdateJob(ctx context.Context, jobID string, upd *DeviceJobUpdate) error
}
