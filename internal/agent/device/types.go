package device

import (
	"context"
	"time"
)

// Provider 返回当前可用设备序列号列表。
type Provider interface {
	ListDevices(ctx context.Context) ([]string, error)
}

// Recorder 负责将设备信息同步到外部存储（Feishu/SQLite）。
type Recorder interface {
	UpsertDevices(ctx context.Context, devices []InfoUpdate) error
}

// InfoUpdate 描述需要上报的设备状态。
type InfoUpdate struct {
	DeviceSerial string
	Status       string
	OSType       string
	OSVersion    string
	IsRoot       string
	ProviderUUID string
	AgentVersion string
	LastError    string
	LastSeenAt   time.Time
	RunningTask  string
	PendingTasks []string
}
