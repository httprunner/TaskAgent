package taskagent

import (
	"context"
	"strings"

	"github.com/rs/zerolog/log"
)

// MultiTaskManager multiplexes task fetching and lifecycle callbacks across two task managers:
// - Search: handles non-single-url tasks.
// - SingleURL: handles SceneSingleURLCapture tasks.
type MultiTaskManager struct {
	Search              TaskManager
	SingleURL           TaskManager
	SearchFetchLimit    int
	SingleURLFetchLimit int
}

func (m *MultiTaskManager) FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error) {
	if m == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if limit <= 0 {
		return nil, nil
	}

	searchLimit, singleLimit := splitMultiFetchLimits(limit, m.SearchFetchLimit, m.SingleURLFetchLimit)
	var searchTasks []*Task
	var singleURLTasks []*Task

	if searchLimit > 0 && m.Search != nil {
		tasks, err := m.Search.FetchAvailableTasks(ctx, app, searchLimit)
		if err != nil {
			log.Warn().Err(err).Int("limit", searchLimit).Msg("multi: fetch search tasks failed")
		} else {
			searchTasks = tasks
		}
	}
	if singleLimit > 0 && m.SingleURL != nil {
		tasks, err := m.SingleURL.FetchAvailableTasks(ctx, app, singleLimit)
		if err != nil {
			log.Warn().Err(err).Int("limit", singleLimit).Msg("multi: fetch single url tasks failed")
		} else {
			singleURLTasks = tasks
		}
	}

	remaining := limit - (len(searchTasks) + len(singleURLTasks))
	if remaining > 0 {
		seen := make(map[string]struct{}, len(searchTasks)+len(singleURLTasks))
		markSeen := func(tasks []*Task) {
			for _, t := range tasks {
				if t == nil {
					continue
				}
				id := strings.TrimSpace(t.ID)
				if id == "" {
					continue
				}
				seen[id] = struct{}{}
			}
		}
		markSeen(searchTasks)
		markSeen(singleURLTasks)

		appendUnique := func(dst []*Task, extra []*Task, max int) []*Task {
			for _, t := range extra {
				if t == nil {
					continue
				}
				if max > 0 && len(dst) >= max {
					break
				}
				id := strings.TrimSpace(t.ID)
				if id != "" {
					if _, ok := seen[id]; ok {
						continue
					}
					seen[id] = struct{}{}
				}
				dst = append(dst, t)
			}
			return dst
		}

		// If one side under-fetched (no tasks available), top up from the other side
		// to avoid leaving idle devices unused.
		if len(singleURLTasks) < singleLimit && m.Search != nil {
			top := remaining
			if m.SearchFetchLimit > 0 {
				if left := m.SearchFetchLimit - len(searchTasks); left < top {
					top = left
				}
			}
			if top > 0 {
				if extra, err := m.Search.FetchAvailableTasks(ctx, app, top); err != nil {
					log.Warn().Err(err).Int("limit", top).Msg("multi: top up search tasks failed")
				} else {
					searchTasks = appendUnique(searchTasks, extra, 0)
				}
			}
		}
		remaining = limit - (len(searchTasks) + len(singleURLTasks))
		if remaining > 0 && len(searchTasks) < searchLimit && m.SingleURL != nil {
			top := remaining
			if m.SingleURLFetchLimit > 0 {
				if left := m.SingleURLFetchLimit - len(singleURLTasks); left < top {
					top = left
				}
			}
			if top > 0 {
				if extra, err := m.SingleURL.FetchAvailableTasks(ctx, app, top); err != nil {
					log.Warn().Err(err).Int("limit", top).Msg("multi: top up single url tasks failed")
				} else {
					singleURLTasks = appendUnique(singleURLTasks, extra, 0)
				}
			}
		}
	}

	combined := make([]*Task, 0, len(searchTasks)+len(singleURLTasks))
	combined = append(combined, searchTasks...)
	combined = append(combined, singleURLTasks...)

	if len(combined) > limit {
		combined = combined[:limit]
	}
	return combined, nil
}

func (m *MultiTaskManager) OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error {
	search, single := splitTasksByScene(tasks)
	if len(search) > 0 && m.Search != nil {
		if err := m.Search.OnTasksDispatched(ctx, deviceSerial, search); err != nil {
			return err
		}
	}
	if len(single) > 0 && m.SingleURL != nil {
		if err := m.SingleURL.OnTasksDispatched(ctx, deviceSerial, single); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiTaskManager) OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error {
	if isSingleURLScene(task) {
		if m.SingleURL != nil {
			return m.SingleURL.OnTaskStarted(ctx, deviceSerial, task)
		}
		return nil
	}
	if m.Search != nil {
		return m.Search.OnTaskStarted(ctx, deviceSerial, task)
	}
	return nil
}

func (m *MultiTaskManager) OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error {
	if isSingleURLScene(task) {
		if m.SingleURL != nil {
			return m.SingleURL.OnTaskResult(ctx, deviceSerial, task, runErr)
		}
		return nil
	}
	if m.Search != nil {
		return m.Search.OnTaskResult(ctx, deviceSerial, task, runErr)
	}
	return nil
}

func (m *MultiTaskManager) OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error {
	search, single := splitTasksByScene(tasks)
	if len(search) > 0 && m.Search != nil {
		if err := m.Search.OnTasksCompleted(ctx, deviceSerial, search, jobErr); err != nil {
			return err
		}
	}
	if len(single) > 0 && m.SingleURL != nil {
		// Single URL tasks may override status updates in OnTaskResult.
		// If the job fails before all tasks can report results, mark remaining tasks failed.
		if jobErr != nil {
			for _, task := range single {
				if task == nil {
					continue
				}
				if err := m.SingleURL.OnTaskResult(ctx, deviceSerial, task, jobErr); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func splitMultiFetchLimits(total, searchCap, singleCap int) (int, int) {
	if total <= 0 {
		return 0, 0
	}
	search := total / 2
	single := total - search

	if searchCap > 0 && search > searchCap {
		search = searchCap
	}
	if singleCap > 0 && single > singleCap {
		single = singleCap
	}

	remaining := total - (search + single)
	if remaining <= 0 {
		return search, single
	}

	if singleCap <= 0 || single < singleCap {
		add := remaining
		if singleCap > 0 && single+add > singleCap {
			add = singleCap - single
		}
		if add > 0 {
			single += add
			remaining -= add
		}
	}
	if remaining <= 0 {
		return search, single
	}
	if searchCap <= 0 || search < searchCap {
		add := remaining
		if searchCap > 0 && search+add > searchCap {
			add = searchCap - search
		}
		if add > 0 {
			search += add
		}
	}
	return search, single
}

func splitTasksByScene(tasks []*Task) (search []*Task, singleURL []*Task) {
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if isSingleURLScene(task) {
			singleURL = append(singleURL, task)
			continue
		}
		search = append(search, task)
	}
	return search, singleURL
}

func isSingleURLScene(task *Task) bool {
	if task == nil {
		return false
	}
	ft, ok := task.Payload.(*FeishuTask)
	if !ok || ft == nil {
		return false
	}
	return strings.TrimSpace(ft.Scene) == SceneSingleURLCapture
}
