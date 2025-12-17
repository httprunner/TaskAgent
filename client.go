package taskagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/httprunner/TaskAgent/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// FeishuTaskClientOptions customizes Feishu task fetching behavior.
type FeishuTaskClientOptions struct {
	AllowedScenes []string
}

// NewFeishuTaskClient constructs a reusable client for fetching and updating Feishu tasks.
func NewFeishuTaskClient(bitableURL string) (*FeishuTaskClient, error) {
	return NewFeishuTaskClientWithOptions(bitableURL, FeishuTaskClientOptions{})
}

// NewFeishuTaskClientWithOptions constructs a client with advanced options.
func NewFeishuTaskClientWithOptions(bitableURL string, opts FeishuTaskClientOptions) (*FeishuTaskClient, error) {
	if strings.TrimSpace(bitableURL) == "" {
		return nil, errors.New("feishusdk task config missing bitable url")
	}

	client, err := feishusdk.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	mirror, err := storage.NewTaskMirror()
	if err != nil {
		return nil, err
	}
	return &FeishuTaskClient{
		client:        client,
		bitableURL:    bitableURL,
		mirror:        mirror,
		allowedScenes: normalizeAllowedScenes(opts.AllowedScenes),
	}, nil
}

type FeishuTaskClient struct {
	client        *feishusdk.Client
	bitableURL    string
	clock         func() time.Time
	mirror        *storage.TaskMirror
	allowedScenes map[string]struct{}
}

func buildSceneSet(scenes []string) map[string]struct{} {
	if len(scenes) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(scenes))
	for _, scene := range scenes {
		trimmed := strings.TrimSpace(scene)
		if trimmed == "" {
			continue
		}
		set[trimmed] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}

func normalizeAllowedScenes(scenes []string) map[string]struct{} {
	if len(scenes) == 0 {
		return buildSceneSet(defaultDeviceScenes)
	}
	return buildSceneSet(scenes)
}

func (c *FeishuTaskClient) FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error) {
	feishuTasks, err := fetchTodayPendingFeishuTasks(ctx, c.client, c.bitableURL, app, limit, c.allowedScenes)
	if err != nil {
		return nil, err
	}
	if err := c.syncTaskMirror(feishuTasks); err != nil {
		return nil, err
	}
	result := make([]*Task, 0, len(feishuTasks))
	for _, t := range feishuTasks {
		result = append(result, &Task{
			ID:           strconv.FormatInt(t.TaskID, 10),
			Payload:      t,
			DeviceSerial: strings.TrimSpace(t.DeviceSerial),
		})
	}
	return result, nil
}

func (c *FeishuTaskClient) UpdateTaskStatuses(ctx context.Context, tasks []*FeishuTask, status string) error {
	return c.updateTaskStatuses(ctx, tasks, status, nil)
}

func (c *FeishuTaskClient) updateTaskStatuses(ctx context.Context, tasks []*FeishuTask, status string, extraMeta *TaskStatusMeta) error {
	if c == nil {
		return errors.New("feishusdk: task client is nil")
	}
	meta := mergeTaskStatusMeta(extraMeta, c.statusUpdateMeta(status))
	if err := UpdateFeishuTaskStatuses(ctx, tasks, status, "", meta); err != nil {
		return err
	}
	trimmedStatus := strings.TrimSpace(status)
	for _, task := range tasks {
		if task == nil {
			continue
		}
		task.Status = trimmedStatus
		if meta != nil && strings.TrimSpace(meta.Logs) != "" && strings.TrimSpace(task.Logs) == "" {
			task.Logs = strings.TrimSpace(meta.Logs)
		}
	}
	return c.syncTaskMirror(tasks)
}

func (c *FeishuTaskClient) OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error {
	feishuTasks, err := c.updateStatuses(ctx, tasks, feishusdk.StatusDispatched, deviceSerial)
	if err != nil {
		return err
	}
	if err := c.syncTaskMirror(feishuTasks); err != nil {
		return err
	}
	log.Info().
		Str("device_serial", strings.TrimSpace(deviceSerial)).
		Int("task_count", len(tasks)).
		Str("status", feishusdk.StatusDispatched).
		Msg("feishusdk tasks marked as dispatched")
	return nil
}

func (c *FeishuTaskClient) OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error {
	status := feishusdk.StatusSuccess
	if jobErr != nil {
		status = feishusdk.StatusFailed
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
		Msg("feishusdk tasks completion status updated")
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
	logsPath := strings.TrimSpace(task.Logs)
	if logsPath == "" {
		for _, ft := range feishuTasks {
			if ft == nil {
				continue
			}
			if trimmed := strings.TrimSpace(ft.Logs); trimmed != "" {
				logsPath = trimmed
				break
			}
		}
	}
	var meta *TaskStatusMeta
	if logsPath != "" {
		meta = &TaskStatusMeta{Logs: logsPath}
		for _, ft := range feishuTasks {
			if ft == nil {
				continue
			}
			if strings.TrimSpace(ft.Logs) == "" {
				ft.Logs = logsPath
			}
		}
	}
	return c.updateTaskStatuses(ctx, feishuTasks, feishusdk.StatusRunning, meta)
}

// OnTaskResult updates task status immediately when a task finishes.
func (c *FeishuTaskClient) OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error {
	if c == nil || task == nil {
		return nil
	}
	status := feishusdk.StatusSuccess
	if runErr != nil {
		status = feishusdk.StatusFailed
	}
	feishuTasks, err := extractFeishuTasks([]*Task{task})
	if err != nil {
		return err
	}
	if len(feishuTasks) == 0 {
		return nil
	}
	if status == feishusdk.StatusSuccess {
		// Only apply ItemsCollected-based overrides when TASK_MIN_ITEMS_COLLECTED
		// is explicitly configured. If the env is unset, keep success as-is.
		rawMin := env.String("TASK_MIN_ITEMS_COLLECTED", "")
		trimmed := strings.TrimSpace(rawMin)
		if trimmed != "" {
			minItems, err := strconv.Atoi(trimmed)
			if err != nil || minItems <= 0 {
				minItems = 0
			}
			if minItems <= 0 {
				return c.updateTaskStatuses(ctx, feishuTasks, status, nil)
			}
			threshold := int64(minItems)
			for _, ft := range feishuTasks {
				if ft == nil {
					continue
				}
				scene := strings.TrimSpace(ft.Scene)
				if scene != SceneGeneralSearch && scene != SceneProfileSearch && scene != SceneCollection {
					continue
				}
				if ft.ItemsCollected < threshold {
					log.Warn().
						Int64("task_id", ft.TaskID).
						Str("scene", scene).
						Int64("items_collected", ft.ItemsCollected).
						Int("min_items_collected", minItems).
						Msg("override task status to failed due to low items collected")
					status = feishusdk.StatusFailed
					break
				}
			}
		}
	}
	return c.updateTaskStatuses(ctx, feishuTasks, status, nil)
}

func (c *FeishuTaskClient) updateStatuses(ctx context.Context, tasks []*Task, status, deviceSerial string) ([]*FeishuTask, error) {
	feishuTasks, err := extractFeishuTasks(tasks)
	if err != nil {
		return nil, errors.Wrap(err, "extract feishusdk tasks failed")
	}
	if len(feishuTasks) == 0 {
		return nil, nil
	}
	selected := filterTasksForStatusOverride(feishuTasks, status)
	if len(selected) == 0 {
		log.Debug().
			Str("status", strings.TrimSpace(status)).
			Msg("skip feishusdk status override: all tasks already finalized")
		return feishuTasks, nil
	}
	if err := UpdateFeishuTaskStatuses(ctx, selected, status, deviceSerial, c.statusUpdateMeta(status)); err != nil {
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
		return errors.New("feishusdk: task client is nil")
	}
	if len(updates) == 0 {
		return nil
	}
	grouped := make(map[*feishuTaskSource][]TaskExtraUpdate, len(updates))
	for _, upd := range updates {
		if upd.Task == nil || upd.Task.source == nil || upd.Task.source.client == nil || upd.Task.source.table == nil {
			return errors.New("feishusdk: task missing source context for extra update")
		}
		grouped[upd.Task.source] = append(grouped[upd.Task.source], upd)
	}

	var errs []string
	touched := make([]*FeishuTask, 0, len(updates))
	for source, subset := range grouped {
		extraField := strings.TrimSpace(source.table.Fields.Extra)
		if extraField == "" {
			return errors.New("feishusdk: extra field is not configured in task table")
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
		return errors.New("feishusdk: task client is nil")
	}
	if err := UpdateFeishuTaskWebhooks(ctx, tasks, webhookStatus); err != nil {
		return err
	}
	return c.syncTaskMirror(tasks)
}

func (c *FeishuTaskClient) statusUpdateMeta(status string) *TaskStatusMeta {
	if c == nil {
		return nil
	}
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		return nil
	}
	if trimmed == feishusdk.StatusDispatched {
		ts := c.now()
		return &TaskStatusMeta{DispatchedAt: &ts}
	}
	if trimmed == feishusdk.StatusSuccess || trimmed == feishusdk.StatusFailed {
		ts := c.now()
		return &TaskStatusMeta{CompletedAt: &ts}
	}
	return nil
}

func mergeTaskStatusMeta(primary, fallback *TaskStatusMeta) *TaskStatusMeta {
	if primary == nil {
		return fallback
	}
	if fallback == nil {
		return primary
	}
	result := *primary
	if result.DispatchedAt == nil {
		result.DispatchedAt = fallback.DispatchedAt
	}
	if result.CompletedAt == nil {
		result.CompletedAt = fallback.CompletedAt
	}
	if strings.TrimSpace(result.Logs) == "" {
		result.Logs = fallback.Logs
	}
	return &result
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
	ItemID           string
	BookID           string
	URL              string
	App              string
	Scene            string
	Status           string
	OriginalStatus   string
	Webhook          string
	UserID           string
	UserName         string
	Extra            string
	Logs             string
	GroupID          string
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
	ItemsCollected   int64
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
		ItemID:           task.ItemID,
		BookID:           task.BookID,
		URL:              task.URL,
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
		Logs:             task.Logs,
		DeviceSerial:     task.DeviceSerial,
		DispatchedDevice: task.DispatchedDevice,
		DispatchedAt:     task.DispatchedAt,
		DispatchedAtRaw:  task.DispatchedAtRaw,
		ElapsedSeconds:   task.ElapsedSeconds,
		ItemsCollected:   task.ItemsCollected,
	}
}

type feishuTaskSource struct {
	client TargetTableClient
	table  *feishusdk.TaskTable
}

type TargetTableClient interface {
	FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusdk.TaskFields, opts *feishusdk.TaskQueryOptions) (*feishusdk.TaskTable, error)
	UpdateTaskStatus(ctx context.Context, table *feishusdk.TaskTable, taskID int64, newStatus string) error
	UpdateTaskFields(ctx context.Context, table *feishusdk.TaskTable, taskID int64, fields map[string]any) error
}

const (
	maxFeishuTasksPerApp    = 5
	SceneGeneralSearch      = "综合页搜索"
	SceneProfileSearch      = "个人页搜索"
	SceneCollection         = "合集视频采集"
	SceneAnchorCapture      = "视频锚点采集"
	SceneVideoScreenCapture = "视频录屏采集"
	SceneSingleURLCapture   = "单个链接采集"
)

var defaultDeviceScenes = []string{
	SceneVideoScreenCapture,
	SceneGeneralSearch,
	SceneProfileSearch,
	SceneCollection,
	SceneAnchorCapture,
}

func fetchTodayPendingFeishuTasks(ctx context.Context, client TargetTableClient, bitableURL, app string, limit int, allowedScenes map[string]struct{}) ([]*FeishuTask, error) {
	if client == nil {
		return nil, errors.New("feishusdk: client is nil")
	}
	if strings.TrimSpace(bitableURL) == "" {
		return nil, errors.New("feishusdk: bitable url is empty")
	}
	if limit <= 0 {
		limit = maxFeishuTasksPerApp
	}

	fields := feishusdk.DefaultTaskFields
	result := make([]*FeishuTask, 0, limit)
	seen := make(map[int64]struct{}, limit)

	priorityCombos := []struct {
		scene  string
		status string
	}{
		{scene: SceneVideoScreenCapture, status: feishusdk.StatusPending},
		{scene: SceneVideoScreenCapture, status: feishusdk.StatusFailed},
		{scene: SceneSingleURLCapture, status: feishusdk.StatusPending},
		{scene: SceneSingleURLCapture, status: feishusdk.StatusFailed},
		// failed
		{scene: SceneProfileSearch, status: feishusdk.StatusFailed},
		{scene: SceneCollection, status: feishusdk.StatusFailed},
		{scene: SceneAnchorCapture, status: feishusdk.StatusFailed},
		{scene: SceneGeneralSearch, status: feishusdk.StatusFailed},
		// pending
		{scene: SceneProfileSearch, status: feishusdk.StatusPending},
		{scene: SceneCollection, status: feishusdk.StatusPending},
		{scene: SceneAnchorCapture, status: feishusdk.StatusPending},
		{scene: SceneGeneralSearch, status: feishusdk.StatusPending},
	}

	appendAndMaybeReturn := func(batch []*FeishuTask) {
		result = AppendUniqueFeishuTasks(result, batch, limit, seen)
	}

	for _, combo := range priorityCombos {
		if len(allowedScenes) > 0 {
			if _, ok := allowedScenes[combo.scene]; !ok {
				continue
			}
		}
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
		batch, err := FetchFeishuTasksWithStrategy(ctx, client, bitableURL, fields, app, []string{combo.status}, remaining, combo.scene)
		if err != nil {
			log.Warn().Err(err).Str("scene", combo.scene).Msg("fetch feishusdk tasks failed for scene; skipping")
			continue
		}
		appendAndMaybeReturn(batch)
	}

	if len(result) > limit && limit > 0 {
		result = result[:limit]
	}
	return result, nil
}

func FetchFeishuTasksWithStrategy(ctx context.Context, client TargetTableClient, bitableURL string, fields feishusdk.TaskFields, app string, statuses []string, limit int, scene string) ([]*FeishuTask, error) {
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
		Msg("fetching feishusdk tasks from bitable")
	subset, err := FetchFeishuTasksWithFilter(ctx, client, bitableURL, filter, fetchLimit)
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
			Msg("feishusdk tasks selected after filtering")
	}
	if limit > 0 && len(subset) > limit {
		log.Info().
			Int("batch_limit", limit).
			Int("aggregated", len(subset)).
			Msg("feishusdk tasks aggregated over limit; trimming to cap")
		subset = subset[:limit]
	}
	return subset, nil
}

func FetchFeishuTasksWithFilter(ctx context.Context, client TargetTableClient, bitableURL string, filter *feishusdk.FilterInfo, limit int) ([]*FeishuTask, error) {
	opts := &feishusdk.TaskQueryOptions{
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
		itemID := strings.TrimSpace(row.ItemID)
		bookID := strings.TrimSpace(row.BookID)
		url := strings.TrimSpace(row.URL)
		userID := strings.TrimSpace(row.UserID)
		userName := strings.TrimSpace(row.UserName)
		if row.TaskID == 0 {
			continue
		}
		if params == "" && itemID == "" && bookID == "" && url == "" && userID == "" && userName == "" {
			continue
		}
		tasks = append(tasks, &FeishuTask{
			TaskID:           row.TaskID,
			Params:           params,
			ItemID:           itemID,
			BookID:           bookID,
			URL:              url,
			UserID:           userID,
			UserName:         userName,
			GroupID:          strings.TrimSpace(row.GroupID),
			App:              row.App,
			Scene:            row.Scene,
			Status:           row.Status,
			OriginalStatus:   row.Status,
			Webhook:          row.Webhook,
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

func buildFeishuFilterInfo(fields feishusdk.TaskFields, app string, statuses []string, scene string) *feishusdk.FilterInfo {
	baseSpecs := buildFeishuBaseConditionSpecs(fields, app, scene)
	statusChildren := buildFeishuStatusChildren(fields, statuses, baseSpecs)
	if len(statusChildren) > 0 {
		filter := feishusdk.NewFilterInfo("or")
		filter.Children = append(filter.Children, statusChildren...)
		return filter
	}
	baseConds := appendConditionsFromSpecs(nil, baseSpecs)
	if len(baseConds) == 0 {
		return nil
	}
	filter := feishusdk.NewFilterInfo("and")
	filter.Conditions = append(filter.Conditions, baseConds...)
	return filter
}

func buildFeishuStatusChildren(fields feishusdk.TaskFields, statuses []string, baseSpecs []*feishuConditionSpec) []*feishusdk.ChildrenFilter {
	statusField := strings.TrimSpace(fields.Status)
	if statusField == "" || len(statuses) == 0 {
		return nil
	}
	children := make([]*feishusdk.ChildrenFilter, 0, len(statuses))
	seen := make(map[string]struct{}, len(statuses))
	for _, status := range statuses {
		trimmed := strings.TrimSpace(status)
		if trimmed == "" {
			children = append(children, newStatusChild(baseSpecs, newFeishuConditionSpec(statusField, "isEmpty")))
			children = append(children, newStatusChild(baseSpecs, newFeishuConditionSpec(statusField, "is", "")))
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		children = append(children, newStatusChild(baseSpecs, newFeishuConditionSpec(statusField, "is", trimmed)))
	}
	result := make([]*feishusdk.ChildrenFilter, 0, len(children))
	for _, child := range children {
		if child == nil || len(child.Conditions) == 0 {
			continue
		}
		result = append(result, child)
	}
	return result
}

func newStatusChild(baseSpecs []*feishuConditionSpec, statusSpec *feishuConditionSpec) *feishusdk.ChildrenFilter {
	if statusSpec == nil {
		return nil
	}
	child := feishusdk.NewChildrenFilter("and")
	child.Conditions = appendConditionsFromSpecs(child.Conditions, baseSpecs)
	child.Conditions = appendConditionsFromSpecs(child.Conditions, []*feishuConditionSpec{statusSpec})
	if len(child.Conditions) == 0 {
		return nil
	}
	return child
}

type feishuConditionSpec struct {
	field    string
	operator string
	values   []string
}

func newFeishuConditionSpec(field, operator string, values ...string) *feishuConditionSpec {
	trimmedField := strings.TrimSpace(field)
	if trimmedField == "" {
		return nil
	}
	op := strings.TrimSpace(operator)
	if op == "" {
		op = "is"
	}
	trimmedValues := make([]string, 0, len(values))
	for _, val := range values {
		trimmedValues = append(trimmedValues, strings.TrimSpace(val))
	}
	return &feishuConditionSpec{field: trimmedField, operator: op, values: trimmedValues}
}

func (s *feishuConditionSpec) build() *feishusdk.Condition {
	if s == nil {
		return nil
	}
	if len(s.values) == 0 {
		return feishusdk.NewCondition(s.field, s.operator)
	}
	return feishusdk.NewCondition(s.field, s.operator, s.values...)
}

func appendConditionsFromSpecs(dst []*feishusdk.Condition, specs []*feishuConditionSpec) []*feishusdk.Condition {
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		if cond := spec.build(); cond != nil {
			dst = append(dst, cond)
		}
	}
	return dst
}

func buildFeishuBaseConditionSpecs(fields feishusdk.TaskFields, app, scene string) []*feishuConditionSpec {
	specs := make([]*feishuConditionSpec, 0, 3)
	if field := strings.TrimSpace(fields.App); field != "" && strings.TrimSpace(app) != "" {
		specs = append(specs, newFeishuConditionSpec(field, "is", strings.TrimSpace(app)))
	}
	if field := strings.TrimSpace(fields.Scene); field != "" && strings.TrimSpace(scene) != "" {
		specs = append(specs, newFeishuConditionSpec(field, "is", strings.TrimSpace(scene)))
	}
	if field := strings.TrimSpace(fields.Datetime); field != "" {
		specs = append(specs, newFeishuConditionSpec(field, "is", "Today"))
	}
	return specs
}

func formatFilterForLog(filter *feishusdk.FilterInfo) string {
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

type TaskStatusMeta struct {
	DispatchedAt *time.Time
	CompletedAt  *time.Time
	Logs         string
}

func UpdateFeishuTaskStatuses(ctx context.Context, tasks []*FeishuTask, status string, deviceSerial string, meta *TaskStatusMeta) error {
	if len(tasks) == 0 {
		return nil
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("feishusdk: status cannot be empty")
	}
	deviceSerial = strings.TrimSpace(deviceSerial)

	grouped := make(map[*feishuTaskSource][]*FeishuTask, len(tasks))
	for _, task := range tasks {
		if task == nil || task.source == nil || task.source.client == nil || task.source.table == nil {
			return errors.New("feishusdk: target task missing source context")
		}
		grouped[task.source] = append(grouped[task.source], task)
	}

	var errs []string
	var dispatchedAtValue *time.Time
	var dispatchedAtMillis int64
	var dispatchedAtRaw string
	var hasDispatchedAt bool
	if meta != nil && meta.DispatchedAt != nil && !meta.DispatchedAt.IsZero() {
		dispatchedAtValue = meta.DispatchedAt
		dispatchedAtMillis = meta.DispatchedAt.UTC().UnixMilli()
		dispatchedAtRaw = strconv.FormatInt(dispatchedAtMillis, 10)
		hasDispatchedAt = true
	}
	var completedAtValue *time.Time
	var completedAtMillis int64
	var completedAtRaw string
	if meta != nil && meta.CompletedAt != nil && !meta.CompletedAt.IsZero() {
		completed := meta.CompletedAt
		completedAtValue = completed
		completedAtMillis = completedAtValue.UTC().UnixMilli()
		completedAtRaw = strconv.FormatInt(completedAtMillis, 10)
	}

	for source, subset := range grouped {
		statusField := strings.TrimSpace(source.table.Fields.Status)
		if statusField == "" {
			return errors.New("feishusdk: status field is not configured in task table")
		}
		dispatchedField := strings.TrimSpace(source.table.Fields.DispatchedDevice)
		updateSerial := deviceSerial != "" && dispatchedField != ""
		if deviceSerial != "" && dispatchedField == "" {
			log.Warn().Msg("feishusdk task table missing DispatchedDevice column; skip binding device serial")
		}
		dispatchedTimeField := strings.TrimSpace(source.table.Fields.DispatchedAt)
		elapsedField := strings.TrimSpace(source.table.Fields.ElapsedSeconds)
		itemsCollectedField := strings.TrimSpace(source.table.Fields.ItemsCollected)
		startField := strings.TrimSpace(source.table.Fields.StartAt)
		endField := strings.TrimSpace(source.table.Fields.EndAt)
		logsField := strings.TrimSpace(source.table.Fields.Logs)
		lastScreenshotField := strings.TrimSpace(source.table.Fields.LastScreenShot)
		type bitableMediaUploader interface {
			UploadBitableMedia(ctx context.Context, appToken string, fileName string, content []byte, asImage bool) (string, error)
		}
		uploader, canUploadScreenshot := source.client.(bitableMediaUploader)
		var logsValue string
		if meta != nil {
			logsValue = strings.TrimSpace(meta.Logs)
		}

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
			if itemsCollectedField != "" && task.ItemsCollected > 0 {
				fields[itemsCollectedField] = task.ItemsCollected
			}
			if logsField != "" && logsValue != "" {
				fields[logsField] = logsValue
				task.Logs = logsValue
			}
			if err := source.client.UpdateTaskFields(ctx, source.table, task.TaskID, fields); err != nil {
				errs = append(errs, fmt.Sprintf("task %d: %v", task.TaskID, err))
				continue
			}

			if lastScreenshotField == "" || !canUploadScreenshot || task == nil || task.TaskRef == nil {
				continue
			}
			path := strings.TrimSpace(task.TaskRef.LastScreenShotPath)
			if path == "" {
				continue
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				log.Warn().Err(readErr).Str("path", path).Msg("read last screenshot failed; skip reporting")
				continue
			}
			content, ext := compressImageIfNeeded(raw)
			fileName := fmt.Sprintf("last_screenshot_task_%d%s", task.TaskID, ext)
			fileToken, uploadErr := uploader.UploadBitableMedia(ctx, source.table.Ref.AppToken, fileName, content, true)
			if uploadErr != nil {
				log.Warn().Err(uploadErr).Str("path", path).Msg("upload last screenshot failed; skip reporting")
				continue
			}
			if strings.TrimSpace(fileToken) == "" {
				continue
			}
			if err := source.client.UpdateTaskFields(ctx, source.table, task.TaskID, map[string]any{
				lastScreenshotField: []map[string]any{
					{
						"file_token": strings.TrimSpace(fileToken),
						"name":       filepath.Base(fileName),
					},
				},
			}); err != nil {
				log.Warn().Err(err).
					Int64("task_id", task.TaskID).
					Msg("update LastScreenShot field failed; skip reporting")
			}
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func compressImageIfNeeded(raw []byte) ([]byte, string) {
	const (
		defaultThreshold = 200 * 1024
		defaultQuality   = 75
	)
	if len(raw) == 0 {
		return raw, ".png"
	}

	threshold := defaultThreshold
	if v := strings.TrimSpace(os.Getenv("SCREENSHOT_COMPRESS_MIN_BYTES")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed >= 0 {
			threshold = parsed
		}
	}
	if len(raw) < threshold {
		return raw, ".png"
	}

	quality := defaultQuality
	if v := strings.TrimSpace(os.Getenv("SCREENSHOT_JPEG_QUALITY")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			if parsed < 30 {
				parsed = 30
			}
			if parsed > 95 {
				parsed = 95
			}
			quality = parsed
		}
	}

	img, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil || img == nil {
		return raw, ".png"
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return raw, ".png"
	}
	if buf.Len() == 0 || buf.Len() >= len(raw) {
		return raw, ".png"
	}
	return buf.Bytes(), ".jpg"
}

func UpdateFeishuTaskWebhooks(ctx context.Context, tasks []*FeishuTask, webhookStatus string) error {
	if len(tasks) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(webhookStatus)
	if trimmed == "" {
		return errors.New("feishusdk: webhook status is empty")
	}
	grouped := make(map[*feishuTaskSource][]*FeishuTask, len(tasks))
	for _, task := range tasks {
		if task == nil || task.source == nil || task.source.client == nil || task.source.table == nil {
			return errors.New("feishusdk: task missing source context for webhook update")
		}
		grouped[task.source] = append(grouped[task.source], task)
	}
	var errs []string
	for source, subset := range grouped {
		field := strings.TrimSpace(source.table.Fields.Webhook)
		if field == "" {
			return errors.New("feishusdk: webhook field is not configured in task table")
		}
		payload := map[string]any{field: trimmed}
		for _, task := range subset {
			if err := source.client.UpdateTaskFields(ctx, source.table, task.TaskID, payload); err != nil {
				errs = append(errs, fmt.Sprintf("task %d: %v", task.TaskID, err))
				continue
			}
			task.Webhook = trimmed
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
				Msg("feishusdk task already in terminal state; skip status override")
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
			return nil, errors.New("invalid feishusdk target task payload")
		}
		ft.TaskRef = task
		if logs := strings.TrimSpace(task.Logs); logs != "" {
			ft.Logs = logs
		}
		result = append(result, ft)
	}
	return result, nil
}

func AppendUniqueFeishuTasks(dst []*FeishuTask, src []*FeishuTask, limit int, seen map[int64]struct{}) []*FeishuTask {
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

var ErrMissingTaskSource = errors.New("feishusdk: task missing source context")

// TaskSourceContext exposes the raw table + client backing a task.
func TaskSourceContext(task *FeishuTask) (TargetTableClient, *feishusdk.TaskTable, error) {
	if task == nil || task.source == nil || task.source.client == nil || task.source.table == nil {
		return nil, nil, ErrMissingTaskSource
	}
	return task.source.client, task.source.table, nil
}

// UpdateTaskFields updates arbitrary columns for a single task using its bound source context.
func UpdateTaskFields(ctx context.Context, task *FeishuTask, fields map[string]any) error {
	if len(fields) == 0 {
		return errors.New("feishusdk: no fields to update")
	}
	client, table, err := TaskSourceContext(task)
	if err != nil {
		return err
	}
	return client.UpdateTaskFields(ctx, table, task.TaskID, fields)
}
