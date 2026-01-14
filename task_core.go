package taskagent

import (
	"context"
	"time"
)

// Task 表示单个待执行的任务，包含设备调度需要的最小信息。
type Task struct {
	ID           string
	Payload      any
	DeviceSerial string
	ResultStatus string
	Logs         string
	// LastScreenShotPath stores the local screenshot file path for this task.
	// It can be used by TaskAgent to upload the screenshot to a Feishu attachment field.
	LastScreenShotPath string
}

// TaskNotifier exposes task lifecycle callbacks for job runners.
type TaskNotifier interface {
	OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error
	OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error
}

// TaskManager 定义任务来源需要实现的能力。
type TaskManager interface {
	FetchAvailableTasks(ctx context.Context, limit int, filters []TaskFetchFilter) ([]*Task, error)
	OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error
	OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error
	OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error
	OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error
}

type TaskFetchFilter struct {
	Scene  string
	Status string
	Date   string
}

// JobRequest bundles the execution details for a device.
type JobRequest struct {
	DeviceSerial string
	Tasks        []*Task
	Notifier     TaskNotifier
}

// JobRunner executes tasks on a concrete device.
type JobRunner interface {
	RunJob(ctx context.Context, req JobRequest) error
}

// DeviceProvider 返回当前可用设备序列号列表。
type DeviceProvider interface {
	ListDevices(ctx context.Context) ([]string, error)
}

// DeviceStateProvider optionally returns the raw device states from the provider.
// When implemented, DevicePoolAgent will use it to avoid scheduling offline/unauthorized devices.
type DeviceStateProvider interface {
	ListDevicesWithState(ctx context.Context) (map[string]string, error)
}

// DispatchPlanner optionally customizes how tasks are assigned to idle devices.
// When nil, DevicePoolAgent falls back to its built-in round-robin strategy.
type DispatchPlanner interface {
	PlanDispatch(ctx context.Context, idleDevices []string, tasks []*Task) ([]DispatchAssignment, error)
}

// DispatchAssignment describes a per-device task assignment produced by a DispatchPlanner.
type DispatchAssignment struct {
	DeviceSerial string
	Tasks        []*Task
}

// DeviceRecorder 负责将设备信息同步到外部存储（Feishu/SQLite）。
type DeviceRecorder interface {
	UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error
}

// DeviceInfoUpdate 描述需要上报的设备状态。
type DeviceInfoUpdate struct {
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

// deviceStatus 描述设备在调度中的状态。
type deviceStatus string

const (
	deviceStatusIdle       deviceStatus = "idle"
	deviceStatusDispatched deviceStatus = "dispatched"
	deviceStatusRunning    deviceStatus = "running"
)

// deviceMeta 保存设备的静态信息。
type deviceMeta struct {
	OSType       string
	OSVersion    string
	IsRoot       string
	ProviderUUID string
}
