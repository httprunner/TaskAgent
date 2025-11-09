package pool

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	feishusvc "github.com/httprunner/TaskAgent/feishu"
)

// FeishuTaskConfig describes how to pull/update tasks from Feishu bitable.
type FeishuTaskConfig struct {
	BitableURL     string
	App            string
	DispatchStatus string
	SuccessStatus  string
	FailureStatus  string
}

type feishuTargetTaskManager struct {
	client         *feishusvc.Client
	bitableURL     string
	app            string
	dispatchStatus string
	successStatus  string
	failureStatus  string
	clock          func() time.Time
}

func newFeishuTaskManager(cfg *FeishuTaskConfig) (TaskManager, error) {
	if cfg == nil {
		return nil, errors.New("feishu task config is nil")
	}
	if strings.TrimSpace(cfg.BitableURL) == "" {
		return nil, errors.New("feishu task config missing bitable url")
	}
	if strings.TrimSpace(cfg.App) == "" {
		return nil, errors.New("feishu task config missing app")
	}
	client, err := feishusvc.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &feishuTargetTaskManager{
		client:         client,
		bitableURL:     cfg.BitableURL,
		app:            cfg.App,
		dispatchStatus: cfg.DispatchStatus,
		successStatus:  cfg.SuccessStatus,
		failureStatus:  cfg.FailureStatus,
	}, nil
}

func (m *feishuTargetTaskManager) FetchAvailableTasks(ctx context.Context, maxTasks int) ([]*Task, error) {
	tasks, err := fetchTodayPendingFeishuTasks(ctx, m.client, m.bitableURL, m.app, maxTasks, m.now())
	if err != nil {
		return nil, err
	}
	result := make([]*Task, 0, len(tasks))
	for _, t := range tasks {
		result = append(result, &Task{ID: strconv.FormatInt(t.TaskID, 10), Payload: t})
	}
	return result, nil
}

func (m *feishuTargetTaskManager) OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error {
	if strings.TrimSpace(m.dispatchStatus) == "" {
		return nil
	}
	return m.updateStatuses(ctx, tasks, m.dispatchStatus)
}

func (m *feishuTargetTaskManager) OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error {
	status := m.successStatus
	if jobErr != nil {
		status = m.failureStatus
	}
	if strings.TrimSpace(status) == "" {
		return nil
	}
	return m.updateStatuses(ctx, tasks, status)
}

func (m *feishuTargetTaskManager) updateStatuses(ctx context.Context, tasks []*Task, status string) error {
	feishuTasks, err := extractFeishuTasks(tasks)
	if err != nil {
		return err
	}
	return updateFeishuTaskStatuses(ctx, feishuTasks, status)
}

func (m *feishuTargetTaskManager) now() time.Time {
	if m.clock != nil {
		return m.clock()
	}
	return time.Now()
}

// FeishuTask represents a pending capture job fetched from Feishu bitable.
type FeishuTask struct {
	TaskID int64
	Params string

	source *feishuTaskSource
}

type feishuTaskSource struct {
	client targetTableClient
	table  *feishusvc.TargetTable
}

type targetTableClient interface {
	FetchTargetTableWithOptions(ctx context.Context, rawURL string, override *feishusvc.TargetFields, opts *feishusvc.TargetQueryOptions) (*feishusvc.TargetTable, error)
	UpdateTargetStatus(ctx context.Context, table *feishusvc.TargetTable, taskID int64, newStatus string) error
}

const (
	feishuTaskFilterLayout = "2006-01-02 15:04:05"
	maxFeishuTasksPerApp   = 5
)

func fetchTodayPendingFeishuTasks(ctx context.Context, client targetTableClient, bitableURL, app string, limit int, now time.Time) ([]*FeishuTask, error) {
	if client == nil {
		return nil, errors.New("feishu: client is nil")
	}
	if strings.TrimSpace(bitableURL) == "" {
		return nil, errors.New("feishu: bitable url is empty")
	}
	if limit <= 0 {
		limit = maxFeishuTasksPerApp
	}

	loc := now.Location()
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	dayEnd := dayStart.Add(24 * time.Hour)
	fields := feishusvc.DefaultTargetFields

	statusPriority := []string{"", "failed"}
	tasks := make([]*FeishuTask, 0, limit)

	for _, status := range statusPriority {
		if len(tasks) >= limit {
			break
		}
		filter := buildFeishuFilterExpression(fields, app, dayStart, dayEnd, status)
		need := limit - len(tasks)
		subset, err := fetchFeishuTasksWithFilter(ctx, client, bitableURL, filter, need)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, subset...)
	}

	return tasks, nil
}

func fetchFeishuTasksWithFilter(ctx context.Context, client targetTableClient, bitableURL, filter string, limit int) ([]*FeishuTask, error) {
	opts := &feishusvc.TargetQueryOptions{
		Filter: strings.TrimSpace(filter),
		Limit:  limit,
	}
	table, err := client.FetchTargetTableWithOptions(ctx, bitableURL, nil, opts)
	if err != nil {
		return nil, err
	}
	if table == nil || len(table.Rows) == 0 {
		return nil, nil
	}

	source := &feishuTaskSource{client: client, table: table}
	tasks := make([]*FeishuTask, 0, len(table.Rows))
	for _, row := range table.Rows {
		params := strings.TrimSpace(row.Params)
		if params == "" || row.TaskID == 0 {
			continue
		}
		tasks = append(tasks, &FeishuTask{
			TaskID: row.TaskID,
			Params: params,
			source: source,
		})
		if limit > 0 && len(tasks) >= limit {
			break
		}
	}
	return tasks, nil
}

func buildFeishuFilterExpression(fields feishusvc.TargetFields, app string, start, end time.Time, status string) string {
	var clauses []string

	if field := strings.TrimSpace(fields.App); field != "" && strings.TrimSpace(app) != "" {
		clauses = append(clauses, fmt.Sprintf("%s = \"%s\"", bitableFieldRef(field), escapeBitableFilterValue(app)))
	}
	if field := strings.TrimSpace(fields.Datetime); field != "" {
		startStr := start.Format(feishuTaskFilterLayout)
		endStr := end.Format(feishuTaskFilterLayout)
		clauses = append(clauses,
			fmt.Sprintf("%s >= \"%s\"", bitableFieldRef(field), startStr),
			fmt.Sprintf("%s < \"%s\"", bitableFieldRef(field), endStr),
		)
	}
	if statusClause := buildFeishuStatusClause(fields, status); statusClause != "" {
		clauses = append(clauses, statusClause)
	}

	switch len(clauses) {
	case 0:
		return ""
	case 1:
		return clauses[0]
	default:
		return fmt.Sprintf("AND(%s)", strings.Join(clauses, ", "))
	}
}

func buildFeishuStatusClause(fields feishusvc.TargetFields, status string) string {
	field := strings.TrimSpace(fields.Status)
	if field == "" {
		return ""
	}
	ref := bitableFieldRef(field)
	if strings.TrimSpace(status) == "" {
		return fmt.Sprintf("OR(ISBLANK(%s), %s = \"\")", ref, ref)
	}
	return fmt.Sprintf("%s = \"%s\"", ref, escapeBitableFilterValue(status))
}

func updateFeishuTaskStatuses(ctx context.Context, tasks []*FeishuTask, status string) error {
	if len(tasks) == 0 {
		return nil
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("feishu: status cannot be empty")
	}

	grouped := make(map[*feishuTaskSource][]*FeishuTask, len(tasks))
	for _, task := range tasks {
		if task == nil || task.source == nil || task.source.client == nil || task.source.table == nil {
			return errors.New("feishu: target task missing source context")
		}
		grouped[task.source] = append(grouped[task.source], task)
	}

	var errs []string
	for source, subset := range grouped {
		for _, task := range subset {
			if err := source.client.UpdateTargetStatus(ctx, source.table, task.TaskID, status); err != nil {
				errs = append(errs, fmt.Sprintf("task %d: %v", task.TaskID, err))
			}
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func extractFeishuTasks(tasks []*Task) ([]*FeishuTask, error) {
	result := make([]*FeishuTask, 0, len(tasks))
	for _, task := range tasks {
		ft, ok := task.Payload.(*FeishuTask)
		if !ok || ft == nil {
			return nil, errors.New("invalid feishu target task payload")
		}
		result = append(result, ft)
	}
	return result, nil
}

func bitableFieldRef(name string) string {
	return fmt.Sprintf("CurrentValue.[%s]", name)
}

func escapeBitableFilterValue(value string) string {
	return strings.ReplaceAll(value, "\"", "\\\"")
}
