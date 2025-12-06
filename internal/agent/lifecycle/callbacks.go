package lifecycle

import "github.com/httprunner/TaskAgent/internal/agent/tasks"

// Callbacks 聚合任务执行过程中的本地回调。
type Callbacks struct {
	OnTaskStarted func(task *tasks.Task)
	OnTaskResult  func(task *tasks.Task, err error)
}
