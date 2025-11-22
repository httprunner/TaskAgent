package pool

import (
	"context"
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
	mirror, err := storage.NewTargetMirror()
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
	mirror     *storage.TargetMirror
}

func (c *FeishuTaskClient) FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error) {
	feishuTasks, err := fetchTodayPendingFeishuTasks(ctx, c.client, c.bitableURL, app, limit, c.now())
	if err != nil {
		return nil, err
	}
	if err := c.syncTargetMirror(feishuTasks); err != nil {
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
	tasks, err := fetchTodayPendingFeishuTasks(ctx, c.client, c.bitableURL, app, limit, c.now())
	if err != nil {
		return nil, err
	}
	if err := c.syncTargetMirror(tasks); err != nil {
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
	return c.syncTargetMirror(tasks)
}

func (c *FeishuTaskClient) OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error {
	feishuTasks, err := c.updateStatuses(ctx, tasks, feishu.StatusDispatched, deviceSerial)
	if err != nil {
		return err
	}
	if err := c.syncTargetMirror(feishuTasks); err != nil {
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
	if err := c.syncTargetMirror(feishuTasks); err != nil {
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
			return errors.New("feishu: extra field is not configured in target table")
		}
		for _, upd := range subset {
			fields := map[string]any{
				extraField: upd.Extra,
			}
			if err := source.client.UpdateTargetFields(ctx, source.table, upd.Task.TaskID, fields); err != nil {
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
	return c.syncTargetMirror(touched)
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
			return errors.New("feishu: webhook field is not configured in target table")
		}
		payload := map[string]any{field: trimmed}
		for _, task := range subset {
			if err := source.client.UpdateTargetFields(ctx, source.table, task.TaskID, payload); err != nil {
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
	return c.syncTargetMirror(touched)
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

func (c *FeishuTaskClient) syncTargetMirror(tasks []*FeishuTask) error {
	if c == nil || c.mirror == nil || len(tasks) == 0 {
		return nil
	}
	rows := make([]*storage.TargetTask, 0, len(tasks))
	for _, task := range tasks {
		if converted := toStorageTargetTask(task); converted != nil {
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
	TaskID            int64
	Params            string
	App               string
	Scene             string
	Status            string
	OriginalStatus    string
	Webhook           string
	UserID            string
	UserName          string
	Extra             string
	DeviceSerial      string
	DispatchedDevice  string
	StartAt           *time.Time
	StartAtRaw        string
	EndAt             *time.Time
	EndAtRaw          string
	Datetime          *time.Time
	DatetimeRaw       string
	DispatchedTime    *time.Time
	DispatchedTimeRaw string
	ElapsedSeconds    int64
	TargetCount       int

	source *feishuTaskSource
}

func toStorageTargetTask(task *FeishuTask) *storage.TargetTask {
	if task == nil || task.TaskID == 0 {
		return nil
	}
	return &storage.TargetTask{
		TaskID:            task.TaskID,
		Params:            task.Params,
		App:               task.App,
		Scene:             task.Scene,
		StartAt:           task.StartAt,
		StartAtRaw:        task.StartAtRaw,
		EndAt:             task.EndAt,
		EndAtRaw:          task.EndAtRaw,
		Datetime:          task.Datetime,
		DatetimeRaw:       task.DatetimeRaw,
		Status:            task.Status,
		Webhook:           task.Webhook,
		UserID:            task.UserID,
		UserName:          task.UserName,
		Extra:             task.Extra,
		DeviceSerial:      task.DeviceSerial,
		DispatchedDevice:  task.DispatchedDevice,
		DispatchedTime:    task.DispatchedTime,
		DispatchedTimeRaw: task.DispatchedTimeRaw,
		ElapsedSeconds:    task.ElapsedSeconds,
	}
}

type feishuTaskSource struct {
	client targetTableClient
	table  *feishu.TargetTable
}

type targetTableClient interface {
	FetchTargetTableWithOptions(ctx context.Context, rawURL string, override *feishu.TargetFields, opts *feishu.TargetQueryOptions) (*feishu.TargetTable, error)
	UpdateTargetStatus(ctx context.Context, table *feishu.TargetTable, taskID int64, newStatus string) error
	UpdateTargetFields(ctx context.Context, table *feishu.TargetTable, taskID int64, fields map[string]any) error
}

const (
	feishuTaskFilterLayout  = "2006-01-02 15:04:05"
	maxFeishuTasksPerApp    = 5
	fallbackFetchMultiplier = 5
	maxFeishuFallbackFetch  = 100
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
	fields := feishu.DefaultTargetFields

	statusPriority := []string{feishu.StatusPending, feishu.StatusFailed}

	result := make([]*FeishuTask, 0, limit)
	seen := make(map[int64]struct{}, limit)

	withDatetime, err := fetchFeishuTasksWithStrategy(ctx, client, bitableURL, fields, app, dayStart, dayEnd, now, statusPriority, limit, true)
	if err != nil {
		return nil, err
	}
	result = appendUniqueFeishuTasks(result, withDatetime, limit, seen)
	if len(result) >= limit {
		return result[:limit], nil
	}

	withoutDatetime, err := fetchFeishuTasksWithStrategy(ctx, client, bitableURL, fields, app, dayStart, dayEnd, now, statusPriority, limit, false)
	if err != nil {
		return nil, err
	}
	result = appendUniqueFeishuTasks(result, withoutDatetime, limit, seen)
	if len(result) > limit {
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

func fetchFeishuTasksWithStrategy(ctx context.Context, client targetTableClient, bitableURL string, fields feishu.TargetFields, app string, dayStart, dayEnd, now time.Time, statuses []string, limit int, includeDatetime bool) ([]*FeishuTask, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	fetchLimit := limit
	if fetchLimit <= 0 {
		fetchLimit = maxFeishuTasksPerApp
	}
	if !includeDatetime {
		fetchLimit = fetchLimit * fallbackFetchMultiplier
		if fetchLimit < limit {
			fetchLimit = limit
		}
		if fetchLimit > maxFeishuFallbackFetch {
			fetchLimit = maxFeishuFallbackFetch
		}
	}
	purpose := "with_datetime"
	if !includeDatetime {
		purpose = "without_datetime"
	}
	filter := buildFeishuFilterExpression(fields, app, dayStart, dayEnd, statuses, includeDatetime)
	log.Debug().
		Str("app", app).
		Str("purpose", purpose).
		Bool("include_datetime", includeDatetime).
		Strs("statuses", statuses).
		Int("fetch_limit", fetchLimit).
		Str("filter", filter).
		Msg("fetching feishu tasks from bitable")
	subset, err := fetchFeishuTasksWithFilter(ctx, client, bitableURL, filter, fetchLimit)
	if err != nil {
		return nil, err
	}
	subset = filterFeishuTasksByDate(subset, dayStart, dayEnd, now)
	if len(subset) > 0 {
		sortFeishuTasksByStatusPriority(subset, statuses)
		log.Info().
			Str("app", app).
			Str("purpose", purpose).
			Bool("include_datetime", includeDatetime).
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

func fetchFeishuTasksWithFilter(ctx context.Context, client targetTableClient, bitableURL, filter string, limit int) ([]*FeishuTask, error) {
	opts := &feishu.TargetQueryOptions{
		Filter: strings.TrimSpace(filter),
		Limit:  limit,
	}
	table, err := client.FetchTargetTableWithOptions(ctx, bitableURL, nil, opts)
	if err != nil {
		return nil, errors.Wrap(err, "fetch target table with options failed")
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
			TaskID:            row.TaskID,
			Params:            params,
			App:               row.App,
			Scene:             row.Scene,
			Status:            row.Status,
			OriginalStatus:    row.Status,
			Webhook:           row.Webhook,
			UserID:            row.UserID,
			UserName:          row.UserName,
			Extra:             row.Extra,
			DeviceSerial:      row.DeviceSerial,
			DispatchedDevice:  row.DispatchedDevice,
			StartAt:           row.StartAt,
			StartAtRaw:        row.StartAtRaw,
			EndAt:             row.EndAt,
			EndAtRaw:          row.EndAtRaw,
			Datetime:          row.Datetime,
			DatetimeRaw:       row.DatetimeRaw,
			DispatchedTime:    row.DispatchedTime,
			DispatchedTimeRaw: row.DispatchedTimeRaw,
			ElapsedSeconds:    row.ElapsedSeconds,
			source:            source,
		})
		if limit > 0 && len(tasks) >= limit {
			break
		}
	}
	return tasks, nil
}

func filterFeishuTasksByDate(tasks []*FeishuTask, start, end, now time.Time) []*FeishuTask {
	if len(tasks) == 0 || start.IsZero() || end.IsZero() || now.IsZero() {
		return nil
	}
	inWindow := make([]*FeishuTask, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if task.Datetime == nil {
			inWindow = append(inWindow, task)
			continue
		}
		dt := task.Datetime
		if dt.Before(start) || !dt.Before(end) {
			continue
		}
		if dt.After(now) {
			continue
		}
		inWindow = append(inWindow, task)
	}
	return inWindow
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

func sortFeishuTasksByStatusPriority(tasks []*FeishuTask, statuses []string) {
	if len(tasks) <= 1 || len(statuses) == 0 {
		return
	}
	priority := make(map[string]int, len(statuses))
	for idx, status := range statuses {
		priority[strings.TrimSpace(status)] = idx
	}
	defaultPriority := len(statuses)
	sort.SliceStable(tasks, func(i, j int) bool {
		pi, iok := priority[strings.TrimSpace(tasks[i].Status)]
		pj, jok := priority[strings.TrimSpace(tasks[j].Status)]
		if !iok {
			pi = defaultPriority
		}
		if !jok {
			pj = defaultPriority
		}
		if pi == pj {
			return tasks[i].TaskID < tasks[j].TaskID
		}
		return pi < pj
	})
}

func buildFeishuFilterExpression(fields feishu.TargetFields, app string, start, end time.Time, statuses []string, includeDatetime bool) string {
	var clauses []string

	if field := strings.TrimSpace(fields.App); field != "" && strings.TrimSpace(app) != "" {
		clauses = append(clauses, fmt.Sprintf("%s = \"%s\"", bitableFieldRef(field), escapeBitableFilterValue(app)))
	}
	if includeDatetime && !start.IsZero() && !end.IsZero() {
		if field := strings.TrimSpace(fields.Datetime); field != "" {
			startStr := start.Format(feishuTaskFilterLayout)
			endStr := end.Format(feishuTaskFilterLayout)
			clauses = append(clauses,
				fmt.Sprintf("%s >= \"%s\"", bitableFieldRef(field), startStr),
				fmt.Sprintf("%s < \"%s\"", bitableFieldRef(field), endStr),
			)
		}
	}
	if statusClause := buildFeishuStatusClause(fields, statuses); statusClause != "" {
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

func buildFeishuStatusClause(fields feishu.TargetFields, statuses []string) string {
	field := strings.TrimSpace(fields.Status)
	if field == "" || len(statuses) == 0 {
		return ""
	}
	ref := bitableFieldRef(field)
	clauses := make([]string, 0, len(statuses)+1)
	seen := make(map[string]struct{})
	for _, status := range statuses {
		trimmed := strings.TrimSpace(status)
		if trimmed == "" {
			blankClause := fmt.Sprintf("ISBLANK(%s)", ref)
			if _, ok := seen[blankClause]; !ok {
				clauses = append(clauses, blankClause)
				seen[blankClause] = struct{}{}
			}
			eqBlank := fmt.Sprintf("%s = \"\"", ref)
			if _, ok := seen[eqBlank]; !ok {
				clauses = append(clauses, eqBlank)
				seen[eqBlank] = struct{}{}
			}
			continue
		}
		clause := fmt.Sprintf("%s = \"%s\"", ref, escapeBitableFilterValue(trimmed))
		if _, ok := seen[clause]; !ok {
			clauses = append(clauses, clause)
			seen[clause] = struct{}{}
		}
	}
	if len(clauses) == 0 {
		return ""
	}
	if len(clauses) == 1 {
		return clauses[0]
	}
	return fmt.Sprintf("OR(%s)", strings.Join(clauses, ", "))
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
			return errors.New("feishu: status field is not configured in target table")
		}
		dispatchedField := strings.TrimSpace(source.table.Fields.DispatchedDevice)
		updateSerial := deviceSerial != "" && dispatchedField != ""
		if deviceSerial != "" && dispatchedField == "" {
			log.Warn().Msg("feishu target table missing DispatchedDevice column; skip binding device serial")
		}
		dispatchedTimeField := strings.TrimSpace(source.table.Fields.DispatchedTime)
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
				task.DispatchedTime = dispatchedAtValue
				task.DispatchedTimeRaw = dispatchedAtRaw
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
			if err := source.client.UpdateTargetFields(ctx, source.table, task.TaskID, fields); err != nil {
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
	if task == nil || task.DispatchedTime == nil || completedAt.IsZero() {
		return 0, false
	}
	if completedAt.Before(*task.DispatchedTime) {
		return 0, true
	}
	secs := int64(completedAt.Sub(*task.DispatchedTime) / time.Second)
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
