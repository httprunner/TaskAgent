package devrecorder

import (
	"context"
	"time"

	pool "github.com/httprunner/TaskAgent"
)

// DeviceRecorder captures device + job state to an external store (e.g., Feishu bitable).
type DeviceRecorder interface {
	UpsertDevices(ctx context.Context, devices []pool.DeviceInfoUpdate) error
	CreateJob(ctx context.Context, rec *pool.DeviceJobRecord) error
	UpdateJob(ctx context.Context, jobID string, upd *pool.DeviceJobUpdate) error
}

// NoopRecorder is the default implementation when recording is disabled.
type NoopRecorder struct{}

func (NoopRecorder) UpsertDevices(ctx context.Context, devices []pool.DeviceInfoUpdate) error {
	return nil
}
func (NoopRecorder) CreateJob(ctx context.Context, rec *pool.DeviceJobRecord) error { return nil }
func (NoopRecorder) UpdateJob(ctx context.Context, jobID string, upd *pool.DeviceJobUpdate) error {
	return nil
}

// DeviceInfoUpdate constructor helper for callers.
func NewDeviceInfo(serial, status, osType, osVersion, agentVersion, lastError string, lastSeen time.Time) pool.DeviceInfoUpdate {
	return pool.DeviceInfoUpdate{
		DeviceSerial: serial,
		Status:       status,
		OSType:       osType,
		OSVersion:    osVersion,
		AgentVersion: agentVersion,
		LastError:    lastError,
		LastSeenAt:   lastSeen,
	}
}
