package taskagent

import (
	"context"
	"strings"

	"github.com/pkg/errors"
)

// MultiDispatchPlanner assigns tasks to idle devices with separate per-device caps:
// - SearchMaxTasksPerDevice applies to non-single-url tasks.
// - SingleURLMaxTasksPerDevice applies to SceneSingleURLCapture tasks.
//
// Devices are preferred 50/50 for single-url vs search by index parity, but may
// still receive mixed tasks as a fallback when one queue is short.
type MultiDispatchPlanner struct {
	SearchMaxTasksPerDevice    int
	SingleURLMaxTasksPerDevice int
}

func (p *MultiDispatchPlanner) PlanDispatch(ctx context.Context, idleDevices []string, tasks []*Task) ([]DispatchAssignment, error) {
	if p == nil {
		return nil, errors.New("dispatch planner is nil")
	}
	searchCap := p.SearchMaxTasksPerDevice
	singleCap := p.SingleURLMaxTasksPerDevice
	if searchCap <= 0 {
		searchCap = 1
	}
	if singleCap <= 0 {
		singleCap = 1
	}
	totalCap := singleCap
	if searchCap > totalCap {
		totalCap = searchCap
	}

	idleSet := make(map[string]struct{}, len(idleDevices))
	idle := make([]string, 0, len(idleDevices))
	for _, serial := range idleDevices {
		serial = strings.TrimSpace(serial)
		if serial == "" {
			continue
		}
		if _, ok := idleSet[serial]; ok {
			continue
		}
		idleSet[serial] = struct{}{}
		idle = append(idle, serial)
	}
	if len(idle) == 0 || len(tasks) == 0 {
		return nil, nil
	}

	targeted := make(map[string][]*Task)
	searchGeneral := make([]*Task, 0, len(tasks))
	singleGeneral := make([]*Task, 0, len(tasks))

	for _, task := range tasks {
		if task == nil {
			continue
		}
		target := strings.TrimSpace(task.DeviceSerial)
		if target != "" {
			if _, ok := idleSet[target]; ok {
				targeted[target] = append(targeted[target], task)
			}
			continue
		}
		if isSingleURLTask(task) {
			singleGeneral = append(singleGeneral, task)
			continue
		}
		searchGeneral = append(searchGeneral, task)
	}

	type deviceQuota struct {
		search int
		single int
		total  int
	}

	pop := func(queue []*Task, idx *int) *Task {
		for *idx < len(queue) {
			task := queue[*idx]
			*idx++
			if task != nil {
				return task
			}
		}
		return nil
	}

	appendTask := func(out []*Task, quota *deviceQuota, task *Task) ([]*Task, bool) {
		if task == nil || quota == nil {
			return out, false
		}
		if quota.total >= totalCap {
			return out, false
		}
		if isSingleURLTask(task) {
			if quota.single >= singleCap {
				return out, false
			}
			quota.single++
			quota.total++
			return append(out, task), true
		}
		if quota.search >= searchCap {
			return out, false
		}
		quota.search++
		quota.total++
		return append(out, task), true
	}

	searchIdx := 0
	singleIdx := 0
	assignments := make([]DispatchAssignment, 0, len(idle))

	for i, serial := range idle {
		quota := &deviceQuota{}
		out := make([]*Task, 0, totalCap)

		if list := targeted[serial]; len(list) > 0 {
			for _, task := range list {
				var ok bool
				out, ok = appendTask(out, quota, task)
				if !ok && quota.total >= totalCap {
					break
				}
			}
		}

		preferSingle := (i%2 == 0)
		fillFromQueues := func(primarySingle bool) {
			for quota.total < totalCap {
				var candidate *Task
				if primarySingle {
					candidate = pop(singleGeneral, &singleIdx)
					if candidate != nil {
						if updated, ok := appendTask(out, quota, candidate); ok {
							out = updated
							continue
						}
					}
					candidate = pop(searchGeneral, &searchIdx)
					if candidate != nil {
						if updated, ok := appendTask(out, quota, candidate); ok {
							out = updated
							continue
						}
					}
				} else {
					candidate = pop(searchGeneral, &searchIdx)
					if candidate != nil {
						if updated, ok := appendTask(out, quota, candidate); ok {
							out = updated
							continue
						}
					}
					candidate = pop(singleGeneral, &singleIdx)
					if candidate != nil {
						if updated, ok := appendTask(out, quota, candidate); ok {
							out = updated
							continue
						}
					}
				}
				break
			}
		}

		fillFromQueues(preferSingle)

		if len(out) > 0 {
			assignments = append(assignments, DispatchAssignment{
				DeviceSerial: serial,
				Tasks:        out,
			})
		}
	}

	return assignments, nil
}

func isSingleURLTask(task *Task) bool {
	if task == nil {
		return false
	}
	ft, ok := task.Payload.(*FeishuTask)
	if !ok || ft == nil {
		return false
	}
	return strings.TrimSpace(ft.Scene) == SceneSingleURLCapture
}
