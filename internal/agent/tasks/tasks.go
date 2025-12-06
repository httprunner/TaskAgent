package tasks

import "context"

// Task 表示单个待执行的任务，包含设备调度需要的最小信息。
type Task struct {
	ID           string
	Payload      any
	DeviceSerial string
	ResultStatus string
}

// Source 定义任务来源需要实现的能力。
type Source interface {
	FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error)
	OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error
	OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error
}

// StartNotifier 描述任务开始时的回调。
type StartNotifier interface {
	OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error
}

// ResultNotifier 描述任务完成后的回调。
type ResultNotifier interface {
	OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error
}
