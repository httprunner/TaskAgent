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

	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/httprunner/TaskAgent/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// FeishuTaskClientOptions customizes Feishu task fetching behavior.
type FeishuTaskClientOptions struct {
	NodeIndex int
	NodeTotal int
}

const (
	TaskDateToday     = "Today"
	TaskDateYesterday = "Yesterday"
	// TaskDateAny means no date constraint.
	TaskDateAny = "Any"
)

// NewFeishuTaskClient constructs a reusable client for fetching and updating Feishu tasks.
func NewFeishuTaskClient(bitableURL, app string) (*FeishuTaskClient, error) {
	return NewFeishuTaskClientWithOptions(bitableURL, app, FeishuTaskClientOptions{})
}

// NewFeishuTaskClientWithOptions constructs a client with advanced options.
func NewFeishuTaskClientWithOptions(bitableURL, app string, opts FeishuTaskClientOptions) (*FeishuTaskClient, error) {
	if strings.TrimSpace(bitableURL) == "" {
		return nil, errors.New("feishusdk task config missing bitable url")
	}
	app = strings.TrimSpace(app)
	if app == "" {
		return nil, errors.New("feishusdk task config missing app")
	}

	client, err := feishusdk.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	mirror, err := storage.NewTaskMirror()
	if err != nil {
		return nil, err
	}
	nodeIndex, nodeTotal := NormalizeShardConfig(opts.NodeIndex, opts.NodeTotal)
	return &FeishuTaskClient{
		client:     client,
		bitableURL: bitableURL,
		app:        app,
		mirror:     mirror,
		nodeIndex:  nodeIndex,
		nodeTotal:  nodeTotal,
	}, nil
}

type FeishuTaskClient struct {
	client     *feishusdk.Client
	bitableURL string
	app        string
	clock      func() time.Time
	mirror     *storage.TaskMirror
	nodeIndex  int
	nodeTotal  int
}

// ShardConfig returns the shard config and whether sharding is enabled.
func (c *FeishuTaskClient) ShardConfig() (int, int, bool) {
	if c == nil || c.nodeTotal <= 1 {
		return 0, 1, false
	}
	return c.nodeIndex, c.nodeTotal, true
}

func (c *FeishuTaskClient) FetchAvailableTasks(ctx context.Context, limit int, filters []TaskFetchFilter) ([]*Task, error) {
	fetchLimit := limit
	if c != nil && c.nodeTotal > 1 && limit > 0 {
		fetchLimit = limit * c.nodeTotal
		if fetchLimit < limit {
			fetchLimit = limit
		}
	}
	if len(filters) == 0 {
		return nil, errors.New("task fetch filters are required")
	}
	fields := feishusdk.DefaultTaskFields
	feishuTasks := make([]*FeishuTask, 0, fetchLimit)
	seen := make(map[int64]struct{}, fetchLimit)

	for _, filter := range filters {
		if fetchLimit > 0 && len(feishuTasks) >= fetchLimit {
			break
		}
		remaining := fetchLimit
		if remaining > 0 {
			remaining = fetchLimit - len(feishuTasks)
			if remaining <= 0 {
				break
			}
		}
		filterApp := strings.TrimSpace(filter.App)
		if filterApp == "" {
			return nil, errors.New("task fetch filter missing app")
		}
		if strings.TrimSpace(c.app) != filterApp {
			return nil, errors.New("task fetch filter app mismatch")
		}
		filterQuery := filter
		filterQuery.FeishuTaskQueryOptions = FeishuTaskQueryOptions{
			IgnoreView: true,
			Limit:      remaining,
		}
		batch, _, err := FetchFeishuTasks(ctx, c.client, c.bitableURL, fields, filterQuery)
		if err != nil {
			log.Warn().Err(err).Str("scene", strings.TrimSpace(filter.Scene)).Msg("fetch feishusdk tasks failed for scene; skipping")
			continue
		}
		feishuTasks = AppendUniqueFeishuTasks(feishuTasks, batch, fetchLimit, seen)
	}
	if len(feishuTasks) > fetchLimit && fetchLimit > 0 {
		feishuTasks = feishuTasks[:fetchLimit]
	}
	if len(feishuTasks) > 0 {
		now := time.Now()
		ready := make([]*FeishuTask, 0, len(feishuTasks))
		toError := make([]*FeishuTask, 0)

		for _, t := range feishuTasks {
			if t == nil {
				continue
			}
			status := strings.TrimSpace(t.Status)
			switch status {
			case feishusdk.StatusFailed:
				if t.RetryCount > maxFailedRetryCount {
					toError = append(toError, t)
					continue
				}
				if !backoffRetryReady(t, now, feishusdk.StatusFailed) {
					continue
				}
			case feishusdk.StatusError:
				if t.RetryCount > maxErrorRetryCount {
					continue
				}
				if !backoffRetryReady(t, now, feishusdk.StatusError) {
					continue
				}
			}
			ready = append(ready, t)
		}

		if len(toError) > 0 {
			if err := UpdateFeishuTaskStatuses(ctx, toError, feishusdk.StatusError, "", nil); err != nil {
				log.Error().
					Err(err).
					Int("task_count", len(toError)).
					Msg("feishusdk: mark tasks as error due to RetryCount limit failed")
			} else {
				ids := make([]int64, 0, len(toError))
				for _, t := range toError {
					if t != nil && t.TaskID > 0 {
						ids = append(ids, t.TaskID)
					}
				}
				log.Info().
					Int("task_count", len(ids)).
					Interface("task_ids", ids).
					Int("max_retries", maxFailedRetryCount).
					Msg("feishusdk: tasks exceeded retry limit and were marked error")
			}
		}
		feishuTasks = ready
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
	if c != nil && c.nodeTotal > 1 {
		return FilterTasksByShard(result, c.nodeIndex, c.nodeTotal, limit), nil
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
	meta := &TaskStatusMeta{}
	if logsPath != "" {
		meta.Logs = logsPath
		for _, ft := range feishuTasks {
			if ft == nil {
				continue
			}
			if strings.TrimSpace(ft.Logs) == "" {
				ft.Logs = logsPath
			}
		}
	}

	// For failed/error tasks, increment RetryCount when they start running again.
	for _, ft := range feishuTasks {
		if ft == nil {
			continue
		}
		original := strings.TrimSpace(ft.OriginalStatus)
		if original != feishusdk.StatusFailed && original != feishusdk.StatusError {
			continue
		}
		next := ft.RetryCount + 1
		meta.RetryCount = &next
		break
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
	err := c.mirror.UpsertTasks(rows)
	if err != nil {
		log.Error().Err(err).Msg("mirror sync feishusdk tasks to sqlite failed")
		return err
	}
	return nil
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
	BizTaskID        string
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
	RetryCount       int64
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
		BizTaskID:        task.BizTaskID,
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
		RetryCount:       task.RetryCount,
	}
}

type feishuTaskSource struct {
	client TaskTableClient
	table  *feishusdk.TaskTable
}

type TaskTableClient interface {
	FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusdk.TaskFields, opts *feishusdk.QueryOptions) (*feishusdk.TaskTable, error)
	UpdateTaskStatus(ctx context.Context, table *feishusdk.TaskTable, taskID int64, newStatus string) error
	UpdateTaskFields(ctx context.Context, table *feishusdk.TaskTable, taskID int64, fields map[string]any) error
}

// FeishuFetchPageInfo describes paging metadata for a single fetch call.
type FeishuFetchPageInfo struct {
	HasMore       bool
	NextPageToken string
	Pages         int
}

type bitableMediaUploader interface {
	UploadBitableMedia(ctx context.Context, appToken string, fileName string, content []byte, asImage bool) (string, error)
}

type bitableAttachmentFetcher interface {
	BatchGetBitableRows(ctx context.Context, ref feishusdk.BitableRef, recordIDs []string, userIDType string) ([]feishusdk.BitableRow, error)
}

const (
	maxFeishuTasksPerApp    = 5
	SceneGeneralSearch      = "综合页搜索"
	SceneProfileSearch      = "个人页搜索"
	SceneCollection         = "合集视频采集"
	SceneAnchorCapture      = "视频锚点采集"
	SceneVideoScreenCapture = "视频录屏采集"
	SceneSingleURLCapture   = "单个链接采集"

	// Retry policies for failed/error tasks.
	// Failed retries: allow RetryCount 0/1, with 5m/10m backoff. RetryCount >=2 -> error.
	// Error retries: allow RetryCount 2/3, with 5m backoff. RetryCount >=4 -> skip.
	maxFailedRetryCount = 1
	maxErrorRetryCount  = 3
)

var failedRetryBackoff = map[int64]time.Duration{
	0: 5 * time.Minute,
	1: 10 * time.Minute,
}

var errorRetryBackoff = map[int64]time.Duration{
	2: 5 * time.Minute,
	3: 5 * time.Minute,
}

func FetchFeishuTasks(
	ctx context.Context,
	client TaskTableClient,
	bitableURL string,
	fields feishusdk.TaskFields,
	filter TaskFetchFilter,
) ([]*FeishuTask, FeishuFetchPageInfo, error) {
	if client == nil {
		return nil, FeishuFetchPageInfo{}, errors.New("feishusdk: client is nil")
	}
	if strings.TrimSpace(bitableURL) == "" {
		return nil, FeishuFetchPageInfo{}, errors.New("feishusdk: bitable url is empty")
	}
	queryOpts := filter.FeishuTaskQueryOptions
	app := strings.TrimSpace(filter.App)
	if app == "" {
		return nil, FeishuFetchPageInfo{}, errors.New("feishusdk: fetch filter missing app")
	}
	scene := strings.TrimSpace(filter.Scene)
	if scene == "" {
		return nil, FeishuFetchPageInfo{}, errors.New("feishusdk: fetch filter missing scene")
	}
	status := strings.TrimSpace(filter.Status)
	if status == "" {
		status = feishusdk.StatusPending
	}
	datePreset := strings.TrimSpace(filter.Date)
	if datePreset == "" {
		datePreset = TaskDateToday
	}
	fetchLimit := queryOpts.Limit
	if fetchLimit <= 0 {
		fetchLimit = maxFeishuTasksPerApp
	}
	filterInfo := buildFeishuFilterInfo(fields, app, status, scene, datePreset)
	if filterInfo != nil {
		filterQuery := feishusdk.QueryOptions{
			ViewID:     strings.TrimSpace(queryOpts.ViewID),
			IgnoreView: queryOpts.IgnoreView,
			PageToken:  strings.TrimSpace(queryOpts.PageToken),
			MaxPages:   queryOpts.MaxPages,
			Limit:      fetchLimit,
		}
		filterInfo.QueryOptions = &filterQuery
	}
	table, err := client.FetchTaskTableWithOptions(ctx, bitableURL, nil, buildQueryOptions(filterInfo))
	if err != nil {
		return nil, FeishuFetchPageInfo{}, errors.Wrap(err, "fetch task table with options failed")
	}
	tasks, pageInfo := decodeFeishuTasksFromTable(table, client, fetchLimit)
	if queryOpts.Limit > 0 && len(tasks) > queryOpts.Limit {
		log.Info().
			Int("batch_limit", queryOpts.Limit).
			Int("aggregated", len(tasks)).
			Msg("feishusdk tasks aggregated over limit; trimming to cap")
		tasks = tasks[:queryOpts.Limit]
	}
	if len(tasks) > 0 {
		log.Info().
			Str("app", app).
			Str("status", status).
			Int("batch_limit", queryOpts.Limit).
			Int("fetch_limit", fetchLimit).
			Bool("ignore_view", queryOpts.IgnoreView).
			Str("next_page_token", strings.TrimSpace(pageInfo.NextPageToken)).
			Bool("has_more", pageInfo.HasMore).
			Int("pages", pageInfo.Pages).
			Int("selected", len(tasks)).
			Interface("tasks", summarizeFeishuTasks(tasks)).
			Msg("feishusdk tasks selected after filtering")
	}
	return tasks, pageInfo, nil
}

func buildQueryOptions(filter *feishusdk.FilterInfo) *feishusdk.QueryOptions {
	limit := 0
	if filter != nil && filter.QueryOptions != nil {
		limit = filter.QueryOptions.Limit
	}
	opts := &feishusdk.QueryOptions{
		Filter: filter,
		Limit:  limit,
	}
	if filter != nil && filter.QueryOptions != nil {
		queryOpts := filter.QueryOptions
		if strings.TrimSpace(queryOpts.ViewID) != "" {
			opts.ViewID = strings.TrimSpace(queryOpts.ViewID)
		}
		if queryOpts.IgnoreView {
			opts.IgnoreView = true
		}
		if strings.TrimSpace(queryOpts.PageToken) != "" {
			opts.PageToken = strings.TrimSpace(queryOpts.PageToken)
		}
		if queryOpts.MaxPages > 0 {
			opts.MaxPages = queryOpts.MaxPages
		}
	}
	return opts
}

func decodeFeishuTasksFromTable(table *feishusdk.TaskTable, client TaskTableClient, limit int) ([]*FeishuTask, FeishuFetchPageInfo) {
	if table == nil || len(table.Rows) == 0 {
		if table == nil {
			return nil, FeishuFetchPageInfo{}
		}
		return nil, FeishuFetchPageInfo{HasMore: table.HasMore, NextPageToken: strings.TrimSpace(table.NextPageToken), Pages: table.Pages}
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
			BizTaskID:        row.BizTaskID,
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
			Logs:             row.Logs,
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
			ItemsCollected:   row.ItemsCollected,
			RetryCount:       row.RetryCount,
			source:           source,
		})
		if limit > 0 && len(tasks) >= limit {
			break
		}
	}
	pageInfo := FeishuFetchPageInfo{
		HasMore:       table.HasMore,
		NextPageToken: strings.TrimSpace(table.NextPageToken),
		Pages:         table.Pages,
	}
	return tasks, pageInfo
}

func buildFeishuFilterInfo(fields feishusdk.TaskFields, app string, status string, scene, datePreset string) *feishusdk.FilterInfo {
	baseSpecs := buildFeishuBaseConditionSpecs(fields, app, scene, datePreset)
	baseConds := appendConditionsFromSpecs(nil, baseSpecs)
	statusField := strings.TrimSpace(fields.Status)
	trimmedStatus := strings.TrimSpace(status)
	if statusField != "" && trimmedStatus != "" {
		baseConds = appendConditionsFromSpecs(baseConds, []*feishuConditionSpec{
			newFeishuConditionSpec(statusField, "is", trimmedStatus),
		})
	}
	if len(baseConds) == 0 {
		return nil
	}
	filter := feishusdk.NewFilterInfo("and")
	filter.Conditions = append(filter.Conditions, baseConds...)
	return filter
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

func buildFeishuBaseConditionSpecs(fields feishusdk.TaskFields, app, scene, datePreset string) []*feishuConditionSpec {
	specs := make([]*feishuConditionSpec, 0, 3)
	if field := strings.TrimSpace(fields.App); field != "" && strings.TrimSpace(app) != "" {
		specs = append(specs, newFeishuConditionSpec(field, "is", strings.TrimSpace(app)))
	}
	if field := strings.TrimSpace(fields.Scene); field != "" && strings.TrimSpace(scene) != "" {
		specs = append(specs, newFeishuConditionSpec(field, "is", strings.TrimSpace(scene)))
	}
	if field := strings.TrimSpace(fields.Date); field != "" {
		preset := strings.TrimSpace(datePreset)
		if strings.EqualFold(preset, TaskDateAny) {
			// Skip date constraint for explicit TaskID queries.
			preset = ""
		} else if preset == "" {
			preset = TaskDateToday
		}
		if preset != "" {
			specs = append(specs, newFeishuConditionSpec(field, "is", preset))
		}
	}
	return specs
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
	RetryCount   *int64
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
		extraField := strings.TrimSpace(source.table.Fields.Extra)
		dispatchedField := strings.TrimSpace(source.table.Fields.DispatchedDevice)
		if deviceSerial != "" && dispatchedField == "" {
			log.Warn().Msg("feishusdk task table missing DispatchedDevice column; skip binding device serial")
		}
		dispatchedTimeField := strings.TrimSpace(source.table.Fields.DispatchedAt)
		elapsedField := strings.TrimSpace(source.table.Fields.ElapsedSeconds)
		itemsCollectedField := strings.TrimSpace(source.table.Fields.ItemsCollected)
		startField := strings.TrimSpace(source.table.Fields.StartAt)
		endField := strings.TrimSpace(source.table.Fields.EndAt)
		logsField := strings.TrimSpace(source.table.Fields.Logs)
		retryField := strings.TrimSpace(source.table.Fields.RetryCount)
		lastScreenshotField := strings.TrimSpace(source.table.Fields.LastScreenShot)
		var logsValue string
		if meta != nil {
			logsValue = strings.TrimSpace(meta.Logs)
		}

		updateSerial := strings.TrimSpace(deviceSerial) != "" && dispatchedField != ""
		trimmedStatus := strings.TrimSpace(status)
		updatedTasks := make([]*FeishuTask, 0, len(subset))
		updates := make([]feishusdk.TaskFieldUpdate, 0, len(subset))
		for _, task := range subset {
			if task == nil {
				continue
			}
			fields := map[string]any{
				statusField: status,
			}
			if trimmedStatus == feishusdk.StatusSuccess && extraField != "" {
				if extra := strings.TrimSpace(task.Extra); extra != "" && hasFeedURL(extra) {
					fields[extraField] = extra
				}
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
			if itemsCollectedField != "" {
				// Always persist ItemsCollected so Bitables can distinguish
				// between "no items collected" (0) and "not reported" (empty).
				fields[itemsCollectedField] = task.ItemsCollected
			}
			if logsField != "" && logsValue != "" {
				fields[logsField] = logsValue
				task.Logs = logsValue
			}
			if meta != nil && meta.RetryCount != nil && retryField != "" {
				fields[retryField] = *meta.RetryCount
				task.RetryCount = *meta.RetryCount
			}
			updates = append(updates, feishusdk.TaskFieldUpdate{
				TaskID: task.TaskID,
				Fields: fields,
			})
			updatedTasks = append(updatedTasks, task)
		}

		if len(updates) == 0 {
			continue
		}

		type taskFieldsBatchUpdater interface {
			BatchUpdateTaskFields(ctx context.Context, table *feishusdk.TaskTable, updates []feishusdk.TaskFieldUpdate) error
		}

		var updateErr error
		if batcher, ok := source.client.(taskFieldsBatchUpdater); ok && len(updates) > 1 {
			updateErr = batcher.BatchUpdateTaskFields(ctx, source.table, updates)
			if updateErr != nil {
				log.Warn().
					Err(updateErr).
					Int("task_count", len(updates)).
					Msg("feishusdk: batch update failed; falling back to per-task updates")
			}
		}
		if updateErr != nil || len(updates) == 1 {
			// Fall back to per-task updates so partial failures don't stall the scheduler.
			updatedTasks = updatedTasks[:0]
			for _, upd := range updates {
				if err := source.client.UpdateTaskFields(ctx, source.table, upd.TaskID, upd.Fields); err != nil {
					errs = append(errs, fmt.Sprintf("task %d: %v", upd.TaskID, err))
					continue
				}
				for _, task := range subset {
					if task != nil && task.TaskID == upd.TaskID {
						updatedTasks = append(updatedTasks, task)
						break
					}
				}
			}
		}

		if lastScreenshotField == "" || len(updatedTasks) == 0 {
			continue
		}

		handleTaskScreenshotsForSource(ctx, source, updatedTasks, lastScreenshotField)
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func readAndUploadScreenshot(
	ctx context.Context,
	uploader bitableMediaUploader,
	appToken string,
	taskID int64,
	path string,
) (string, string, bool) {
	raw, readErr := os.ReadFile(path)
	if readErr != nil {
		log.Warn().Err(readErr).Str("path", path).Msg("read last screenshot failed; skip reporting")
		return "", "", false
	}
	content, ext := compressImageIfNeeded(raw)
	fileName := fmt.Sprintf("last_screenshot_task_%d%s", taskID, ext)
	fileToken, uploadErr := uploader.UploadBitableMedia(ctx, appToken, fileName, content, true)
	if uploadErr != nil {
		log.Warn().Err(uploadErr).Str("path", path).Msg("upload last screenshot failed; skip reporting")
		return "", "", false
	}
	token := strings.TrimSpace(fileToken)
	if token == "" {
		return "", "", false
	}
	return token, fileName, true
}

func hasFeedURL(extra string) bool {
	trimmed := strings.TrimSpace(extra)
	if trimmed == "" {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return false
	}
	v, ok := payload["cdn_url"]
	if !ok || v == nil {
		return false
	}
	s, ok := v.(string)
	if !ok {
		return false
	}
	return strings.TrimSpace(s) != ""
}

func lookupTaskRecordID(table *feishusdk.TaskTable, taskID int64) string {
	if table == nil || taskID <= 0 {
		return ""
	}
	if recordID, ok := table.RecordIDByTaskID(taskID); ok && strings.TrimSpace(recordID) != "" {
		return strings.TrimSpace(recordID)
	}
	for _, row := range table.Rows {
		if row.TaskID == taskID {
			return strings.TrimSpace(row.RecordID)
		}
	}
	return ""
}

func normalizeAttachmentList(value any) []map[string]any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []map[string]any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if item == nil {
				continue
			}
			token := item["file_token"]
			if strings.TrimSpace(fmt.Sprint(token)) == "" {
				continue
			}
			out = append(out, item)
		}
		return out
	case []interface{}:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok || m == nil {
				continue
			}
			token := m["file_token"]
			tokenStr := strings.TrimSpace(fmt.Sprint(token))
			if tokenStr == "" {
				continue
			}
			name := ""
			if rawName, ok := m["name"]; ok {
				name = fmt.Sprint(rawName)
			}
			out = append(out, map[string]any{
				"file_token": tokenStr,
				"name":       name,
			})
		}
		return out
	default:
		return nil
	}
}

func handleTaskScreenshotsForSource(
	ctx context.Context,
	source *feishuTaskSource,
	tasks []*FeishuTask,
	lastScreenshotField string,
) {
	if source == nil || source.client == nil || source.table == nil || len(tasks) == 0 {
		return
	}

	uploader, ok := source.client.(bitableMediaUploader)
	if !ok {
		return
	}
	fetcher, _ := source.client.(bitableAttachmentFetcher)

	// First-run tasks: upload and overwrite; retries: fetch existing attachments then append.
	for _, task := range tasks {
		if task == nil || task.TaskRef == nil {
			continue
		}
		path := strings.TrimSpace(task.TaskRef.LastScreenShotPath)
		if path == "" {
			continue
		}

		token, fileName, ok := readAndUploadScreenshot(ctx, uploader, source.table.Ref.AppToken, task.TaskID, path)
		if !ok {
			continue
		}

		if task.RetryCount <= 0 {
			if err := source.client.UpdateTaskFields(ctx, source.table, task.TaskID, map[string]any{
				lastScreenshotField: []map[string]any{
					{
						"file_token": token,
						"name":       filepath.Base(fileName),
					},
				},
			}); err != nil {
				log.Warn().Err(err).
					Int64("task_id", task.TaskID).
					Msg("update LastScreenShot field failed; skip reporting")
			}
			continue
		}

		// Retry: optionally fetch existing attachments for this task, then append.
		var attachments []map[string]any
		if fetcher != nil {
			recordID := lookupTaskRecordID(source.table, task.TaskID)
			if recordID == "" {
				log.Warn().Int64("task_id", task.TaskID).
					Msg("record id not found for task; upload screenshot without appending")
			} else {
				rows, err := fetcher.BatchGetBitableRows(ctx, source.table.Ref, []string{recordID}, "")
				if err != nil {
					log.Warn().Err(err).
						Int64("task_id", task.TaskID).
						Msg("fetch existing screenshot attachments failed; fallback to overwrite mode")
				} else if len(rows) > 0 {
					raw := rows[0].Fields[lastScreenshotField]
					attachments = normalizeAttachmentList(raw)
				}
			}
		}
		attachments = append(attachments, map[string]any{
			"file_token": token,
			"name":       filepath.Base(fileName),
		})
		if err := source.client.UpdateTaskFields(ctx, source.table, task.TaskID, map[string]any{
			lastScreenshotField: attachments,
		}); err != nil {
			log.Warn().Err(err).
				Int64("task_id", task.TaskID).
				Msg("update LastScreenShot field failed; skip reporting")
		}
	}
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

func backoffRetryReady(task *FeishuTask, now time.Time, status string) bool {
	if task == nil {
		return false
	}
	delay := retryBackoffDelay(strings.TrimSpace(status), task.RetryCount)
	if delay <= 0 {
		return true
	}
	endAt, ok := resolveTaskEndAt(task)
	if !ok {
		return true
	}
	return now.Sub(endAt) >= delay
}

func retryBackoffDelay(status string, retryCount int64) time.Duration {
	switch strings.TrimSpace(status) {
	case feishusdk.StatusFailed:
		return failedRetryBackoff[retryCount]
	case feishusdk.StatusError:
		return errorRetryBackoff[retryCount]
	default:
		return 0
	}
}

func resolveTaskEndAt(task *FeishuTask) (time.Time, bool) {
	if task == nil {
		return time.Time{}, false
	}
	if task.EndAt != nil && !task.EndAt.IsZero() {
		return *task.EndAt, true
	}
	raw := strings.TrimSpace(task.EndAtRaw)
	if raw == "" {
		return time.Time{}, false
	}
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.UnixMilli(ms), true
	}
	if ts, err := time.Parse(time.RFC3339, raw); err == nil {
		return ts, true
	}
	return time.Time{}, false
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
func TaskSourceContext(task *FeishuTask) (TaskTableClient, *feishusdk.TaskTable, error) {
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
