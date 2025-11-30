package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// NewFeishuTaskClient constructs a reusable client for fetching and updating Feishu tasks.
func NewFeishuTaskClient(bitableURL string) (*FeishuTaskClient, error) {
	if strings.TrimSpace(bitableURL) == "" {
		return nil, errors.New("feishu task config missing bitable url")
	}

	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	mirror, err := storage.NewTaskMirror()
	if err != nil {
		return nil, err
	}
	return &FeishuTaskClient{
		client:     client,
		bitableURL: bitableURL,
		mirror:     mirror,
	}, nil
}

type FeishuTaskClient struct {
	client     *feishu.Client
	bitableURL string
	clock      func() time.Time
	mirror     *storage.TaskMirror
}

func (c *FeishuTaskClient) FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error) {
	feishuTasks, err := fetchTodayPendingFeishuTasks(ctx, c.client, c.bitableURL, app, limit)
	if err != nil {
		return nil, err
	}
	if err := c.syncTaskMirror(feishuTasks); err != nil {
		return nil, err
	}
	result := make([]*Task, 0, len(feishuTasks))
	for _, t := range feishuTasks {
		result = append(result, &Task{ID: strconv.FormatInt(t.TaskID, 10), Payload: t, DeviceSerial: strings.TrimSpace(t.DeviceSerial)})
	}
	return result, nil
}

func (c *FeishuTaskClient) FetchPendingTasks(ctx context.Context, app string, limit int) ([]*FeishuTask, error) {
	if c == nil {
		return nil, errors.New("feishu: task client is nil")
	}
	tasks, err := fetchTodayPendingFeishuTasks(ctx, c.client, c.bitableURL, app, limit)
	if err != nil {
		return nil, err
	}
	if err := c.syncTaskMirror(tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (c *FeishuTaskClient) UpdateTaskStatuses(ctx context.Context, tasks []*FeishuTask, status string) error {
	if c == nil {
		return errors.New("feishu: task client is nil")
	}
	if err := updateFeishuTaskStatuses(ctx, tasks, status, "", c.statusUpdateMeta(status)); err != nil {
		return err
	}
	trimmedStatus := strings.TrimSpace(status)
	for _, task := range tasks {
		if task == nil {
			continue
		}
		task.Status = trimmedStatus
	}
	return c.syncTaskMirror(tasks)
}

func (c *FeishuTaskClient) OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error {
	feishuTasks, err := c.updateStatuses(ctx, tasks, feishu.StatusDispatched, deviceSerial)
	if err != nil {
		return err
	}
	if err := c.syncTaskMirror(feishuTasks); err != nil {
		return err
	}
	log.Info().
		Str("device_serial", strings.TrimSpace(deviceSerial)).
		Int("task_count", len(tasks)).
		Str("status", feishu.StatusDispatched).
		Msg("feishu tasks marked as dispatched")
	return nil
}

func (c *FeishuTaskClient) OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error {
	status := feishu.StatusSuccess
	if jobErr != nil {
		status = feishu.StatusFailed
	}
	if strings.TrimSpace(status) == "" {
		return nil
	}
	feishuTasks, err := c.updateStatuses(ctx, tasks, status, deviceSerial)
	if err != nil {
		return err
	}
	if err := c.syncTaskMirror(feishuTasks); err != nil {
		return err
	}
	resultStatus := strings.TrimSpace(status)
	log.Info().
		Str("device_serial", strings.TrimSpace(deviceSerial)).
		Int("task_count", len(tasks)).
		Str("status", resultStatus).
		Bool("job_error", jobErr != nil).
		Msg("feishu tasks completion status updated")
	return nil
}

// OnTaskStarted marks a single task as running once execution begins.
func (c *FeishuTaskClient) OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error {
	if c == nil || task == nil {
		return nil
	}
	feishuTasks, err := extractFeishuTasks([]*Task{task})
	if err != nil {
		return err
	}
	if len(feishuTasks) == 0 {
		return nil
	}
	return c.UpdateTaskStatuses(ctx, feishuTasks, feishu.StatusRunning)
}

// OnTaskResult updates task status immediately when a task finishes.
func (c *FeishuTaskClient) OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error {
	if c == nil || task == nil {
		return nil
	}
	status := feishu.StatusSuccess
	if runErr != nil {
		status = feishu.StatusFailed
	}
	feishuTasks, err := extractFeishuTasks([]*Task{task})
	if err != nil {
		return err
	}
	if len(feishuTasks) == 0 {
		return nil
	}
	return c.UpdateTaskStatuses(ctx, feishuTasks, status)
}

func (c *FeishuTaskClient) updateStatuses(ctx context.Context, tasks []*Task, status, deviceSerial string) ([]*FeishuTask, error) {
	feishuTasks, err := extractFeishuTasks(tasks)
	if err != nil {
		return nil, errors.Wrap(err, "extract feishu tasks failed")
	}
	if len(feishuTasks) == 0 {
		return nil, nil
	}
	selected := filterTasksForStatusOverride(feishuTasks, status)
	if len(selected) == 0 {
		log.Debug().
			Str("status", strings.TrimSpace(status)).
			Msg("skip feishu status override: all tasks already finalized")
		return feishuTasks, nil
	}
	if err := updateFeishuTaskStatuses(ctx, selected, status, deviceSerial, c.statusUpdateMeta(status)); err != nil {
		return nil, err
	}
	trimmedStatus := strings.TrimSpace(status)
	normalizedSerial := strings.TrimSpace(deviceSerial)
	for _, task := range selected {
		if task == nil {
			continue
		}
		task.Status = trimmedStatus
		if normalizedSerial != "" {
			task.DispatchedDevice = normalizedSerial
		}
	}
	return feishuTasks, nil
}

// TaskExtraUpdate describes an extra-field update for a Feishu task.
type TaskExtraUpdate struct {
	Task  *FeishuTask
	Extra string
}

// UpdateTaskExtras updates the Extra column for the provided tasks.
func (c *FeishuTaskClient) UpdateTaskExtras(ctx context.Context, updates []TaskExtraUpdate) error {
	if c == nil {
		return errors.New("feishu: task client is nil")
	}
	if len(updates) == 0 {
		return nil
	}
	grouped := make(map[*feishuTaskSource][]TaskExtraUpdate, len(updates))
	for _, upd := range updates {
		if upd.Task == nil || upd.Task.source == nil || upd.Task.source.client == nil || upd.Task.source.table == nil {
			return errors.New("feishu: task missing source context for extra update")
		}
		grouped[upd.Task.source] = append(grouped[upd.Task.source], upd)
	}

	var errs []string
	touched := make([]*FeishuTask, 0, len(updates))
	for source, subset := range grouped {
		extraField := strings.TrimSpace(source.table.Fields.Extra)
		if extraField == "" {
			return errors.New("feishu: extra field is not configured in task table")
		}
		for _, upd := range subset {
			fields := map[string]any{
				extraField: upd.Extra,
			}
			if err := source.client.UpdateTaskFields(ctx, source.table, upd.Task.TaskID, fields); err != nil {
				errs = append(errs, fmt.Sprintf("task %d: %v", upd.Task.TaskID, err))
				continue
			}
			upd.Task.Extra = upd.Extra
			touched = append(touched, upd.Task)
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	if len(touched) == 0 {
		return nil
	}
	return c.syncTaskMirror(touched)
}

// UpdateTaskWebhooks updates the webhook column for the provided tasks.
func (c *FeishuTaskClient) UpdateTaskWebhooks(ctx context.Context, tasks []*FeishuTask, webhookStatus string) error {
	if c == nil {
		return errors.New("feishu: task client is nil")
	}
	if len(tasks) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(webhookStatus)
	if trimmed == "" {
		return errors.New("feishu: webhook status is empty")
	}
	grouped := make(map[*feishuTaskSource][]*FeishuTask, len(tasks))
	for _, task := range tasks {
		if task == nil || task.source == nil || task.source.client == nil || task.source.table == nil {
			return errors.New("feishu: task missing source context for webhook update")
		}
		grouped[task.source] = append(grouped[task.source], task)
	}
	var errs []string
	touched := make([]*FeishuTask, 0, len(tasks))
	for source, subset := range grouped {
		field := strings.TrimSpace(source.table.Fields.Webhook)
		if field == "" {
			return errors.New("feishu: webhook field is not configured in task table")
		}
		payload := map[string]any{field: trimmed}
		for _, task := range subset {
			if err := source.client.UpdateTaskFields(ctx, source.table, task.TaskID, payload); err != nil {
				errs = append(errs, fmt.Sprintf("task %d: %v", task.TaskID, err))
				continue
			}
			task.Webhook = trimmed
			touched = append(touched, task)
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	if len(touched) == 0 {
		return nil
	}
	return c.syncTaskMirror(touched)
}

func (c *FeishuTaskClient) statusUpdateMeta(status string) *taskStatusMeta {
	if c == nil {
		return nil
	}
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		return nil
	}
	if trimmed == feishu.StatusDispatched {
		ts := c.now()
		return &taskStatusMeta{dispatchedAt: &ts}
	}
	if trimmed == feishu.StatusSuccess || trimmed == feishu.StatusFailed {
		ts := c.now()
		return &taskStatusMeta{completedAt: &ts}
	}
	return nil
}

func (c *FeishuTaskClient) syncTaskMirror(tasks []*FeishuTask) error {
	if c == nil || c.mirror == nil || len(tasks) == 0 {
		return nil
	}
	rows := make([]*storage.TaskStatus, 0, len(tasks))
	for _, task := range tasks {
		if converted := toStorageTaskStatus(task); converted != nil {
			rows = append(rows, converted)
		}
	}
	if len(rows) == 0 {
		return nil
	}
	return c.mirror.UpsertTasks(rows)
}

func (c *FeishuTaskClient) now() time.Time {
	if c.clock != nil {
		return c.clock()
	}
	return time.Now()
}

// FeishuTask represents a pending capture job fetched from Feishu bitable.
type FeishuTask struct {
	TaskID           int64
	Params           string
	App              string
	Scene            string
	Status           string
	OriginalStatus   string
	Webhook          string
	UserID           string
	UserName         string
	Extra            string
	DeviceSerial     string
	DispatchedDevice string
	StartAt          *time.Time
	StartAtRaw       string
	EndAt            *time.Time
	EndAtRaw         string
	Datetime         *time.Time
	DatetimeRaw      string
	DispatchedAt     *time.Time
	DispatchedAtRaw  string
	ElapsedSeconds   int64
	TargetCount      int
	TaskRef          *Task `json:"-"`

	source *feishuTaskSource
}

func toStorageTaskStatus(task *FeishuTask) *storage.TaskStatus {
	if task == nil || task.TaskID == 0 {
		return nil
	}
	return &storage.TaskStatus{
		TaskID:           task.TaskID,
		Params:           task.Params,
		App:              task.App,
		Scene:            task.Scene,
		StartAt:          task.StartAt,
		StartAtRaw:       task.StartAtRaw,
		EndAt:            task.EndAt,
		EndAtRaw:         task.EndAtRaw,
		Datetime:         task.Datetime,
		DatetimeRaw:      task.DatetimeRaw,
		Status:           task.Status,
		Webhook:          task.Webhook,
		UserID:           task.UserID,
		UserName:         task.UserName,
		Extra:            task.Extra,
		DeviceSerial:     task.DeviceSerial,
		DispatchedDevice: task.DispatchedDevice,
		DispatchedAt:     task.DispatchedAt,
		DispatchedAtRaw:  task.DispatchedAtRaw,
		ElapsedSeconds:   task.ElapsedSeconds,
	}
}

type feishuTaskSource struct {
	client targetTableClient
	table  *feishu.TaskTable
}

type targetTableClient interface {
	FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishu.TaskFields, opts *feishu.TaskQueryOptions) (*feishu.TaskTable, error)
	UpdateTaskStatus(ctx context.Context, table *feishu.TaskTable, taskID int64, newStatus string) error
	UpdateTaskFields(ctx context.Context, table *feishu.TaskTable, taskID int64, fields map[string]any) error
}

const (
	maxFeishuTasksPerApp    = 5
	SceneGeneralSearch      = "综合页搜索"
	SceneProfileSearch      = "个人页搜索"
	SceneCollection         = "合集视频采集"
	SceneAnchorCapture      = "视频锚点采集"
	SceneVideoScreenCapture = "视频录屏采集"
)

func fetchTodayPendingFeishuTasks(ctx context.Context, client targetTableClient, bitableURL, app string, limit int) ([]*FeishuTask, error) {
	if client == nil {
		return nil, errors.New("feishu: client is nil")
	}
	if strings.TrimSpace(bitableURL) == "" {
		return nil, errors.New("feishu: bitable url is empty")
	}
	if limit <= 0 {
		limit = maxFeishuTasksPerApp
	}

	fields := feishu.DefaultTaskFields
	result := make([]*FeishuTask, 0, limit)
	seen := make(map[int64]struct{}, limit)

	priorityCombos := []struct {
		scene  string
		status string
	}{
		{scene: SceneVideoScreenCapture, status: feishu.StatusPending},
		{scene: SceneVideoScreenCapture, status: feishu.StatusFailed},
		{scene: SceneGeneralSearch, status: feishu.StatusPending},
		{scene: SceneProfileSearch, status: feishu.StatusPending},
		{scene: SceneCollection, status: feishu.StatusPending},
		{scene: SceneAnchorCapture, status: feishu.StatusPending},
		{scene: SceneGeneralSearch, status: feishu.StatusFailed},
		{scene: SceneProfileSearch, status: feishu.StatusFailed},
		{scene: SceneCollection, status: feishu.StatusFailed},
		{scene: SceneAnchorCapture, status: feishu.StatusFailed},
	}

	appendAndMaybeReturn := func(batch []*FeishuTask) {
		result = appendUniqueFeishuTasks(result, batch, limit, seen)
	}

	for _, combo := range priorityCombos {
		if limit > 0 && len(result) >= limit {
			break
		}
		remaining := limit
		if remaining > 0 {
			remaining = limit - len(result)
			if remaining <= 0 {
				break
			}
		}
		batch, err := fetchFeishuTasksWithStrategy(ctx, client, bitableURL, fields, app, []string{combo.status}, remaining, combo.scene)
		if err != nil {
			log.Warn().Err(err).Str("scene", combo.scene).Msg("fetch feishu tasks failed for scene; skipping")
			continue
		}
		appendAndMaybeReturn(batch)
	}

	if len(result) > limit && limit > 0 {
		result = result[:limit]
	}
	if len(result) > 1 {
		sort.Slice(result, func(i, j int) bool {
			if result[i] == nil {
				return false
			}
			if result[j] == nil {
				return true
			}
			return result[i].TaskID < result[j].TaskID
		})
	}
	return result, nil
}

func fetchFeishuTasksWithStrategy(ctx context.Context, client targetTableClient, bitableURL string, fields feishu.TaskFields, app string, statuses []string, limit int, scene string) ([]*FeishuTask, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	fetchLimit := limit
	if fetchLimit <= 0 {
		fetchLimit = maxFeishuTasksPerApp
	}

	filter := buildFeishuFilterInfo(fields, app, statuses, scene)
	log.Debug().
		Str("app", app).
		Strs("statuses", statuses).
		Str("scene", strings.TrimSpace(scene)).
		Int("fetch_limit", fetchLimit).
		Str("filter", formatFilterForLog(filter)).
		Msg("fetching feishu tasks from bitable")
	subset, err := fetchFeishuTasksWithFilter(ctx, client, bitableURL, filter, fetchLimit)
	if err != nil {
		return nil, err
	}
	if len(subset) > 0 {
		log.Info().
			Str("app", app).
			Strs("statuses", statuses).
			Int("batch_limit", limit).
			Int("fetch_limit", fetchLimit).
			Int("selected", len(subset)).
			Interface("tasks", summarizeFeishuTasks(subset)).
			Msg("feishu tasks selected after filtering")
	}
	if limit > 0 && len(subset) > limit {
		log.Info().
			Int("batch_limit", limit).
			Int("aggregated", len(subset)).
			Msg("feishu tasks aggregated over limit; trimming to cap")
		subset = subset[:limit]
	}
	return subset, nil
}

func fetchFeishuTasksWithFilter(ctx context.Context, client targetTableClient, bitableURL string, filter *feishu.FilterInfo, limit int) ([]*FeishuTask, error) {
	opts := &feishu.TaskQueryOptions{
		Filter: filter,
		Limit:  limit,
	}
	table, err := client.FetchTaskTableWithOptions(ctx, bitableURL, nil, opts)
	if err != nil {
		return nil, errors.Wrap(err, "fetch task table with options failed")
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
			TaskID:           row.TaskID,
			Params:           params,
			App:              row.App,
			Scene:            row.Scene,
			Status:           row.Status,
			OriginalStatus:   row.Status,
			Webhook:          row.Webhook,
			UserID:           row.UserID,
			UserName:         row.UserName,
			Extra:            row.Extra,
			DeviceSerial:     row.DeviceSerial,
			DispatchedDevice: row.DispatchedDevice,
			StartAt:          row.StartAt,
			StartAtRaw:       row.StartAtRaw,
			EndAt:            row.EndAt,
			EndAtRaw:         row.EndAtRaw,
			Datetime:         row.Datetime,
			DatetimeRaw:      row.DatetimeRaw,
			DispatchedAt:     row.DispatchedAt,
			DispatchedAtRaw:  row.DispatchedAtRaw,
			ElapsedSeconds:   row.ElapsedSeconds,
			source:           source,
		})
		if limit > 0 && len(tasks) >= limit {
			break
		}
	}
	return tasks, nil
}

func buildFeishuFilterInfo(fields feishu.TaskFields, app string, statuses []string, scene string) *feishu.FilterInfo {
	filter := feishu.NewFilterInfo("and")
	if field := strings.TrimSpace(fields.App); field != "" && strings.TrimSpace(app) != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(field, "is", strings.TrimSpace(app)))
	}
	if field := strings.TrimSpace(fields.Scene); field != "" && strings.TrimSpace(scene) != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(field, "is", strings.TrimSpace(scene)))
	}
	if field := strings.TrimSpace(fields.Datetime); field != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(field, "is", "Today"))
	}
	if statusChild := buildFeishuStatusGroup(fields, statuses); statusChild != nil {
		filter.Children = append(filter.Children, statusChild)
	}
	if len(filter.Conditions) == 0 && len(filter.Children) == 0 {
		return nil
	}
	return filter
}

func buildFeishuStatusGroup(fields feishu.TaskFields, statuses []string) *feishu.ChildrenFilter {
	field := strings.TrimSpace(fields.Status)
	if field == "" || len(statuses) == 0 {
		return nil
	}
	conds := make([]*feishu.Condition, 0, len(statuses)+1)
	seen := make(map[string]struct{})
	blankAdded := false
	for _, status := range statuses {
		trimmed := strings.TrimSpace(status)
		if trimmed == "" {
			if !blankAdded {
				conds = append(conds, feishu.NewCondition(field, "isEmpty"))
				conds = append(conds, feishu.NewCondition(field, "is", ""))
				blankAdded = true
			}
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		conds = append(conds, feishu.NewCondition(field, "is", trimmed))
		seen[trimmed] = struct{}{}
	}
	if len(conds) == 0 {
		return nil
	}
	return feishu.NewChildrenFilter("or", conds...)
}

func formatFilterForLog(filter *feishu.FilterInfo) string {
	if filter == nil {
		return ""
	}
	raw, err := json.Marshal(filter)
	if err != nil {
		return ""
	}
	return string(raw)
}

func summarizeFeishuTasks(tasks []*FeishuTask) []map[string]any {
	const tsLayout = "2006-01-02 15:04:05"
	result := make([]map[string]any, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		entry := map[string]any{
			"task_id":   task.TaskID,
			"params":    task.Params,
			"scene":     task.Scene,
			"status":    task.Status,
			"webhook":   task.Webhook,
			"user_id":   task.UserID,
			"user_name": task.UserName,
			"extra":     task.Extra,
		}
		if task.Datetime != nil {
			entry["datetime"] = task.Datetime.In(task.Datetime.Location()).Format(tsLayout)
		} else {
			entry["datetime"] = ""
		}
		result = append(result, entry)
	}
	return result
}

type taskStatusMeta struct {
	dispatchedAt *time.Time
	completedAt  *time.Time
}

func updateFeishuTaskStatuses(ctx context.Context, tasks []*FeishuTask, status string, deviceSerial string, meta *taskStatusMeta) error {
	if len(tasks) == 0 {
		return nil
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("feishu: status cannot be empty")
	}
	deviceSerial = strings.TrimSpace(deviceSerial)

	grouped := make(map[*feishuTaskSource][]*FeishuTask, len(tasks))
	for _, task := range tasks {
		if task == nil || task.source == nil || task.source.client == nil || task.source.table == nil {
			return errors.New("feishu: target task missing source context")
		}
		grouped[task.source] = append(grouped[task.source], task)
	}

	var errs []string
	var dispatchedAtValue *time.Time
	var dispatchedAtMillis int64
	var dispatchedAtRaw string
	var hasDispatchedAt bool
	if meta != nil && meta.dispatchedAt != nil && !meta.dispatchedAt.IsZero() {
		dispatchedAtValue = meta.dispatchedAt
		dispatchedAtMillis = meta.dispatchedAt.UTC().UnixMilli()
		dispatchedAtRaw = strconv.FormatInt(dispatchedAtMillis, 10)
		hasDispatchedAt = true
	}
	var completedAtValue *time.Time
	var completedAtMillis int64
	var completedAtRaw string
	if meta != nil && meta.completedAt != nil && !meta.completedAt.IsZero() {
		completed := meta.completedAt
		completedAtValue = completed
		completedAtMillis = completedAtValue.UTC().UnixMilli()
		completedAtRaw = strconv.FormatInt(completedAtMillis, 10)
	}

	for source, subset := range grouped {
		statusField := strings.TrimSpace(source.table.Fields.Status)
		if statusField == "" {
			return errors.New("feishu: status field is not configured in task table")
		}
		dispatchedField := strings.TrimSpace(source.table.Fields.DispatchedDevice)
		updateSerial := deviceSerial != "" && dispatchedField != ""
		if deviceSerial != "" && dispatchedField == "" {
			log.Warn().Msg("feishu task table missing DispatchedDevice column; skip binding device serial")
		}
		dispatchedTimeField := strings.TrimSpace(source.table.Fields.DispatchedAt)
		elapsedField := strings.TrimSpace(source.table.Fields.ElapsedSeconds)
		startField := strings.TrimSpace(source.table.Fields.StartAt)
		endField := strings.TrimSpace(source.table.Fields.EndAt)

		for _, task := range subset {
			fields := map[string]any{
				statusField: status,
			}
			if updateSerial {
				fields[dispatchedField] = deviceSerial
			}
			if dispatchedTimeField != "" && hasDispatchedAt {
				fields[dispatchedTimeField] = dispatchedAtMillis
				task.DispatchedAt = dispatchedAtValue
				task.DispatchedAtRaw = dispatchedAtRaw
			}
			if startField != "" && hasDispatchedAt {
				fields[startField] = dispatchedAtMillis
				task.StartAt = dispatchedAtValue
				task.StartAtRaw = dispatchedAtRaw
			}
			if endField != "" && completedAtValue != nil {
				fields[endField] = completedAtMillis
				task.EndAt = completedAtValue
				task.EndAtRaw = completedAtRaw
			}
			if elapsedField != "" && completedAtValue != nil {
				if secs, ok := elapsedSecondsForTask(task, *completedAtValue); ok {
					fields[elapsedField] = secs
					task.ElapsedSeconds = secs
				}
			}
			if err := source.client.UpdateTaskFields(ctx, source.table, task.TaskID, fields); err != nil {
				errs = append(errs, fmt.Sprintf("task %d: %v", task.TaskID, err))
			}
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func filterTasksForStatusOverride(tasks []*FeishuTask, newStatus string) []*FeishuTask {
	if len(tasks) == 0 {
		return nil
	}
	desired := strings.TrimSpace(newStatus)
	result := make([]*FeishuTask, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		current := strings.TrimSpace(task.Status)
		if shouldSkipFeishuStatusOverride(current, desired) {
			log.Debug().
				Int64("task_id", task.TaskID).
				Str("current", current).
				Str("desired", desired).
				Msg("feishu task already in terminal state; skip status override")
			continue
		}
		result = append(result, task)
	}
	return result
}

func shouldSkipFeishuStatusOverride(currentStatus, desiredStatus string) bool {
	current := strings.TrimSpace(currentStatus)
	desired := strings.TrimSpace(desiredStatus)
	if desired == "" {
		return true
	}
	return current == desired
}

func elapsedSecondsForTask(task *FeishuTask, completedAt time.Time) (int64, bool) {
	if task == nil || task.DispatchedAt == nil || completedAt.IsZero() {
		return 0, false
	}
	if completedAt.Before(*task.DispatchedAt) {
		return 0, true
	}
	secs := int64(completedAt.Sub(*task.DispatchedAt) / time.Second)
	if secs < 0 {
		secs = 0
	}
	return secs, true
}

func extractFeishuTasks(tasks []*Task) ([]*FeishuTask, error) {
	result := make([]*FeishuTask, 0, len(tasks))
	for _, task := range tasks {
		ft, ok := task.Payload.(*FeishuTask)
		if !ok || ft == nil {
			return nil, errors.New("invalid feishu target task payload")
		}
		ft.TaskRef = task
		result = append(result, ft)
	}
	return result, nil
}

func appendUniqueFeishuTasks(dst []*FeishuTask, src []*FeishuTask, limit int, seen map[int64]struct{}) []*FeishuTask {
	if len(src) == 0 {
		return dst
	}
	for _, task := range src {
		if task == nil {
			continue
		}
		if _, ok := seen[task.TaskID]; ok {
			continue
		}
		seen[task.TaskID] = struct{}{}
		dst = append(dst, task)
		if limit > 0 && len(dst) >= limit {
			break
		}
	}
	return dst
}
