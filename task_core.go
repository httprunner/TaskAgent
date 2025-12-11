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
}

// TaskLifecycle 暴露任务生命周期回调。
type TaskLifecycle struct {
	OnTaskStarted func(task *Task)
	OnTaskResult  func(task *Task, err error)
}

// TaskManager 定义任务来源需要实现的能力。
type TaskManager interface {
	FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error)
	OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error
	OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error
}

// TaskStartNotifier 描述任务开始时的回调。
type TaskStartNotifier interface {
	OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error
}

// TaskResultNotifier 描述任务完成后的回调。
type TaskResultNotifier interface {
	OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error
}

// JobRequest bundles the execution details for a device.
type JobRequest struct {
	DeviceSerial string
	Tasks        []*Task
	Lifecycle    *TaskLifecycle
}

// JobRunner executes tasks on a concrete device.
type JobRunner interface {
	RunJob(ctx context.Context, req JobRequest) error
}

// DeviceProvider 返回当前可用设备序列号列表。
type DeviceProvider interface {
	ListDevices(ctx context.Context) ([]string, error)
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
	deviceStatusIdle    deviceStatus = "idle"
	deviceStatusRunning deviceStatus = "running"
)

// deviceMeta 保存设备的静态信息。
type deviceMeta struct {
	OSType       string
	OSVersion    string
	IsRoot       string
	ProviderUUID string
}
