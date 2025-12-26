package taskagent

import (
	"context"
	"strings"

	"github.com/pkg/errors"
)

// MultiJobRunner executes search and single-url tasks on a single device sequentially.
type MultiJobRunner struct {
	SearchRunner    JobRunner
	SingleURLRunner JobRunner
}

func (r *MultiJobRunner) RunJob(ctx context.Context, req JobRequest) error {
	if r == nil {
		return errors.New("multi job runner is nil")
	}
	if len(req.Tasks) == 0 {
		return errors.New("job request missing tasks")
	}
	if strings.TrimSpace(req.DeviceSerial) == "" {
		return errors.New("device serial is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	searchTasks := make([]*Task, 0, len(req.Tasks))
	singleURLTasks := make([]*Task, 0, len(req.Tasks))
	firstScene := ""

	for _, task := range req.Tasks {
		if task == nil {
			continue
		}
		scene := ""
		if ft, ok := task.Payload.(*FeishuTask); ok && ft != nil {
			scene = strings.TrimSpace(ft.Scene)
		}
		if firstScene == "" && scene != "" {
			firstScene = scene
		}
		if scene == SceneSingleURLCapture {
			singleURLTasks = append(singleURLTasks, task)
			continue
		}
		searchTasks = append(searchTasks, task)
	}

	run := func(runner JobRunner, tasks []*Task) error {
		if runner == nil || len(tasks) == 0 {
			return nil
		}
		return runner.RunJob(ctx, JobRequest{
			DeviceSerial: req.DeviceSerial,
			Tasks:        tasks,
			Notifier:     req.Notifier,
		})
	}

	if firstScene == SceneSingleURLCapture {
		if err := run(r.SingleURLRunner, singleURLTasks); err != nil {
			return err
		}
		return run(r.SearchRunner, searchTasks)
	}

	if err := run(r.SearchRunner, searchTasks); err != nil {
		return err
	}
	return run(r.SingleURLRunner, singleURLTasks)
}
