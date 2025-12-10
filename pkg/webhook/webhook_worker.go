package webhook

import (
	"context"
	stdErrors "errors"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	taskagent "github.com/httprunner/TaskAgent"
)

// WebhookWorkerConfig captures the knobs for the standalone webhook worker.
type WebhookWorkerConfig struct {
	TaskBitableURL    string
	SummaryWebhookURL string
	App               string
	PollInterval      time.Duration
	BatchLimit        int
	GroupCooldown     time.Duration
}

// WebhookWorker periodically scans Feishu tasks for pending/failed webhook rows
// and retries the synchronization independently from the search workflow.
type WebhookWorker struct {
	client        *taskagent.FeishuClient
	bitableURL    string
	webhookURL    string
	app           string
	pollInterval  time.Duration
	batchLimit    int
	groupCooldown map[string]time.Time
	cooldownDur   time.Duration
	taskFailures  map[int64]int
}

const maxWebhookFailures = 3

type webhookOutcome struct {
	successIDs []int64
	failedIDs  []int64
	errorIDs   []int64
}

func (w *WebhookWorker) shouldSkipGroup(groupID string) bool {
	if strings.TrimSpace(groupID) == "" || w == nil {
		return false
	}
	expiry, ok := w.groupCooldown[groupID]
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		delete(w.groupCooldown, groupID)
		return false
	}
	return true
}

func (w *WebhookWorker) markGroupCooldown(groupID, reason string) {
	if strings.TrimSpace(groupID) == "" || w == nil || w.cooldownDur <= 0 {
		return
	}
	w.groupCooldown[groupID] = time.Now().Add(w.cooldownDur)
	log.Debug().
		Str("group_id", groupID).
		Dur("cooldown", w.cooldownDur).
		Str("reason", reason).
		Msg("group added to webhook cooldown queue")
}

func (w *WebhookWorker) clearGroupCooldown(groupID string) {
	if strings.TrimSpace(groupID) == "" || w == nil {
		return
	}
	delete(w.groupCooldown, groupID)
}

var webhookCandidatePriority = []struct {
	scene        string
	webhookState string
}{
	{scene: taskagent.SceneVideoScreenCapture, webhookState: taskagent.WebhookPending},
	{scene: taskagent.SceneVideoScreenCapture, webhookState: taskagent.WebhookFailed},
	{scene: taskagent.SceneProfileSearch, webhookState: taskagent.WebhookPending},
	{scene: taskagent.SceneProfileSearch, webhookState: taskagent.WebhookFailed},
}

// NewWebhookWorker builds a webhook worker from the provided configuration.
func NewWebhookWorker(cfg WebhookWorkerConfig) (*WebhookWorker, error) {
	trimmedURL := strings.TrimSpace(cfg.TaskBitableURL)
	if trimmedURL == "" {
		return nil, errors.New("task bitable url is required for webhook worker")
	}
	webhookURL := strings.TrimSpace(cfg.SummaryWebhookURL)
	if webhookURL == "" {
		return nil, errors.New("summary webhook url is required for webhook worker")
	}
	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "init Feishu client for webhook worker failed")
	}
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = time.Minute
	}
	batch := cfg.BatchLimit
	if batch <= 0 {
		batch = 20
	}
	cooldown := cfg.GroupCooldown
	if cooldown <= 0 {
		cooldown = 2 * time.Minute
	}
	return &WebhookWorker{
		client:        client,
		bitableURL:    trimmedURL,
		webhookURL:    webhookURL,
		app:           strings.TrimSpace(cfg.App),
		pollInterval:  interval,
		batchLimit:    batch,
		groupCooldown: make(map[string]time.Time),
		cooldownDur:   cooldown,
		taskFailures:  make(map[int64]int),
	}, nil
}

// Run blocks until ctx is done while periodically retrying webhook deliveries.
func (w *WebhookWorker) Run(ctx context.Context) error {
	if w == nil {
		return errors.New("webhook worker is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	log.Info().
		Str("bitable", w.bitableURL).
		Str("app", w.app).
		Dur("poll_interval", w.pollInterval).
		Int("batch_limit", w.batchLimit).
		Msg("webhook worker started")

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("webhook worker stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := w.processOnce(ctx); err != nil {
				log.Error().Err(err).Msg("webhook worker scan failed")
			}
		}
	}
}

func (w *WebhookWorker) processOnce(ctx context.Context) error {
	table, err := w.fetchWebhookCandidates(ctx)
	if err != nil {
		return err
	}
	if table == nil || len(table.Rows) == 0 {
		log.Debug().Msg("webhook worker found no pending tasks")
		return nil
	}
	log.Info().
		Int("rows", len(table.Rows)).
		Int("invalid_rows", len(table.Invalid)).
		Msg("webhook worker fetched candidates")

	var successIDs []int64
	var failedIDs []int64
	var errorIDs []int64
	var errs []string
	processedGroups := make(map[string]bool)

	for _, row := range table.Rows {
		if !eligibleForWebhook(row) {
			continue
		}
		outcome, err := w.processRow(ctx, table, row, processedGroups)
		if len(outcome.successIDs) > 0 {
			successIDs = append(successIDs, outcome.successIDs...)
		}
		if len(outcome.failedIDs) > 0 {
			failedIDs = append(failedIDs, outcome.failedIDs...)
		}
		if len(outcome.errorIDs) > 0 {
			errorIDs = append(errorIDs, outcome.errorIDs...)
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(successIDs)+len(failedIDs)+len(errorIDs) > 0 {
		log.Info().
			Int("success", len(successIDs)).
			Int("failed", len(failedIDs)).
			Int("error", len(errorIDs)).
			Int("groups_processed", len(processedGroups)).
			Msg("webhook worker iteration completed")
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (w *WebhookWorker) processRow(ctx context.Context, table *taskagent.FeishuTaskTable, row taskagent.FeishuTaskRow, processedGroups map[string]bool) (webhookOutcome, error) {
	var outcome webhookOutcome
	params := strings.TrimSpace(row.Params)
	scene := strings.TrimSpace(row.Scene)
	groupID := strings.TrimSpace(row.GroupID)
	app := strings.TrimSpace(row.App)

	if app == "" {
		log.Warn().
			Str("scene", scene).
			Str("group_id", groupID).
			Int64("task_id", row.TaskID).
			Msg("skip task due to empty app")
		if groupID != "" {
			processedGroups[groupID] = true
		}
		return outcome, nil
	}
	if scene == "" {
		log.Warn().
			Str("group_id", groupID).
			Int64("task_id", row.TaskID).
			Msg("skip task due to empty scene")
		return outcome, nil
	}

	switch {
	case scene == taskagent.SceneProfileSearch && groupID != "":
		return w.handleGroupRow(ctx, table, row, app, processedGroups)
	case scene == taskagent.SceneVideoScreenCapture:
		return w.handleVideoCaptureRow(ctx, table, row, app)
	default:
		if params == "" {
			log.Warn().
				Str("scene", scene).
				Int64("task_id", row.TaskID).
				Msg("skip task due to empty params")
			return outcome, nil
		}
		return w.handleSingleRow(ctx, table, row, app, params)
	}
}

func (w *WebhookWorker) handleGroupRow(ctx context.Context, table *taskagent.FeishuTaskTable, row taskagent.FeishuTaskRow, app string, processedGroups map[string]bool) (webhookOutcome, error) {
	var outcome webhookOutcome
	groupID := strings.TrimSpace(row.GroupID)
	if processedGroups[groupID] {
		return outcome, nil
	}

	groupTasks, err := w.fetchTasksByGroupID(ctx, groupID)
	if err != nil {
		return outcome, fmt.Errorf("fetch group %s tasks failed: %w", groupID, err)
	}
	readyTasks := filterTasksWithStatus(groupTasks)
	if len(readyTasks) == 0 {
		log.Debug().
			Str("scene", row.Scene).
			Str("group_id", groupID).
			Int("total", len(groupTasks)).
			Msg("group not ready: no tasks with status")
		w.markGroupCooldown(groupID, "missing_status")
		return outcome, nil
	}

	referenceDay, ok := normalizeTaskDay(row.Datetime)
	if !ok {
		log.Warn().
			Str("scene", row.Scene).
			Str("group_id", groupID).
			Int64("task_id", row.TaskID).
			Msg("group task missing datetime, skip webhook readiness")
		w.markGroupCooldown(groupID, "missing_datetime")
		return outcome, nil
	}
	dayTasks := filterTasksByDate(readyTasks, referenceDay)
	if len(dayTasks) == 0 {
		log.Debug().
			Str("scene", row.Scene).
			Str("group_id", groupID).
			Time("reference_day", referenceDay).
			Int("total", len(groupTasks)).
			Int("with_status", len(readyTasks)).
			Msg("group not ready: no tasks match reference day")
		w.markGroupCooldown(groupID, "date_mismatch")
		return outcome, nil
	}
	if !allGroupTasksSuccess(dayTasks) {
		log.Debug().
			Str("scene", row.Scene).
			Str("group_id", groupID).
			Time("reference_day", referenceDay).
			Int("total", len(groupTasks)).
			Int("with_status", len(readyTasks)).
			Int("evaluated", len(dayTasks)).
			Int("success", countSuccessTasks(dayTasks)).
			Msg("group day not ready, waiting for success statuses")
		w.markGroupCooldown(groupID, "waiting_success")
		return outcome, nil
	}

	processedGroups[groupID] = true
	taskIDs := extractTaskIDs(dayTasks)
	if err := w.sendGroupWebhook(ctx, app, groupID, dayTasks); err != nil {
		log.Error().Err(err).
			Str("scene", row.Scene).
			Str("group_id", groupID).
			Str("app", app).
			Int("task_count", len(dayTasks)).
			Msg("group webhook delivery failed")
		failureOutcome, updErr := w.applyWebhookFailure(ctx, table, taskIDs, groupID)
		if updErr != nil {
			return failureOutcome, updErr
		}
		return failureOutcome, nil
	}

	log.Info().
		Str("scene", row.Scene).
		Str("group_id", groupID).
		Str("app", app).
		Int("task_count", len(dayTasks)).
		Msg("group webhook delivered")
	outcome.successIDs = append(outcome.successIDs, taskIDs...)
	w.resetFailureCounts(taskIDs)
	w.clearGroupCooldown(groupID)
	return outcome, w.updateWebhookState(ctx, table, taskIDs, taskagent.WebhookSuccess)
}

func normalizeTaskDay(ts *time.Time) (time.Time, bool) {
	if ts == nil {
		return time.Time{}, false
	}
	local := ts.In(time.Local)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, local.Location())
	return day, true
}

func filterTasksByDate(tasks []taskagent.FeishuTaskRow, referenceDay time.Time) []taskagent.FeishuTaskRow {
	if len(tasks) == 0 {
		return nil
	}
	ref := referenceDay.In(time.Local)
	filtered := make([]taskagent.FeishuTaskRow, 0, len(tasks))
	for _, t := range tasks {
		if t.Datetime == nil {
			continue
		}
		currDay, ok := normalizeTaskDay(t.Datetime)
		if !ok {
			continue
		}
		if currDay.Year() == ref.Year() && currDay.YearDay() == ref.YearDay() {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

func filterRowsByDay(tasks []taskagent.FeishuTaskRow, day time.Time) []taskagent.FeishuTaskRow {
	normalized := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, day.Location())
	return filterTasksByDate(tasks, normalized)
}

func (w *WebhookWorker) handleVideoCaptureRow(ctx context.Context, table *taskagent.FeishuTaskTable, row taskagent.FeishuTaskRow, app string) (webhookOutcome, error) {
	var outcome webhookOutcome
	itemID := strings.TrimSpace(row.ItemID)
	groupID := strings.TrimSpace(row.GroupID)
	if itemID == "" {
		log.Warn().
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Msg("video capture task missing item id, marking as error")
		outcome.errorIDs = append(outcome.errorIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, taskagent.WebhookError)
	}

	opts := Options{
		App:             app,
		Params:          strings.TrimSpace(row.Params),
		UserID:          strings.TrimSpace(row.UserID),
		UserName:        strings.TrimSpace(row.UserName),
		Scene:           row.Scene,
		GroupID:         groupID,
		ItemID:          itemID,
		SkipDramaLookup: true,
		RecordLimit:     1,
		PreferLatest:    true,
	}

	sendErr := w.sendSummary(ctx, opts)
	switch {
	case sendErr == nil:
		log.Info().
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("group_id", groupID).
			Str("app", app).
			Str("item_id", itemID).
			Msg("webhook delivered")
		outcome.successIDs = append(outcome.successIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, taskagent.WebhookSuccess)
	case stdErrors.Is(sendErr, ErrNoCaptureRecords):
		log.Warn().
			Err(sendErr).
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("group_id", groupID).
			Str("app", app).
			Str("item_id", itemID).
			Msg("no capture records for video capture task, marking as failed")
		outcome.failedIDs = append(outcome.failedIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, taskagent.WebhookFailed)
	default:
		log.Error().
			Err(sendErr).
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("group_id", groupID).
			Str("app", app).
			Str("item_id", itemID).
			Msg("webhook for video capture task failed")
		outcome.errorIDs = append(outcome.errorIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, taskagent.WebhookError)
	}
}

func (w *WebhookWorker) handleSingleRow(ctx context.Context, table *taskagent.FeishuTaskTable, row taskagent.FeishuTaskRow, app, params string) (webhookOutcome, error) {
	var outcome webhookOutcome
	opts := Options{
		App:    app,
		Params: params,
		UserID: strings.TrimSpace(row.UserID),
	}
	if strings.TrimSpace(row.Scene) == taskagent.SceneProfileSearch {
		opts.Scene = taskagent.SceneProfileSearch
		opts.GroupID = strings.TrimSpace(row.GroupID)
	}

	sendErr := w.sendSummary(ctx, opts)
	switch {
	case sendErr == nil:
		log.Info().
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("group_id", strings.TrimSpace(row.GroupID)).
			Str("app", app).
			Str("params", params).
			Msg("webhook delivered")
		outcome.successIDs = append(outcome.successIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, taskagent.WebhookSuccess)
	case stdErrors.Is(sendErr, ErrNoCaptureRecords):
		log.Warn().
			Err(sendErr).
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("group_id", strings.TrimSpace(row.GroupID)).
			Str("app", app).
			Str("params", params).
			Msg("no capture records for single task, marking as failed")
		outcome.failedIDs = append(outcome.failedIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, taskagent.WebhookFailed)
	default:
		log.Error().
			Err(sendErr).
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("group_id", strings.TrimSpace(row.GroupID)).
			Str("app", app).
			Str("params", params).
			Msg("webhook for single task failed")
		outcome.errorIDs = append(outcome.errorIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, taskagent.WebhookError)
	}
}

func (w *WebhookWorker) fetchTasksByGroupID(ctx context.Context, groupID string) ([]taskagent.FeishuTaskRow, error) {
	fields := taskagent.DefaultTaskFields()
	filter := taskagent.NewFeishuFilterInfo("and")
	if grp := strings.TrimSpace(fields.GroupID); grp != "" && strings.TrimSpace(groupID) != "" {
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(grp, "is", strings.TrimSpace(groupID)))
	}
	if emptyFilter(filter) {
		return nil, nil
	}
	opts := &taskagent.FeishuTaskQueryOptions{
		Filter: filter,
		Limit:  200,
	}
	table, err := w.client.FetchTaskTableWithOptions(ctx, w.bitableURL, nil, opts)
	if err != nil {
		return nil, errors.Wrap(err, "fetch group tasks failed")
	}
	if table == nil {
		return nil, nil
	}
	if len(table.Invalid) > 0 {
		log.Warn().
			Int("invalid_rows", len(table.Invalid)).
			Str("sample_error", table.Invalid[0].Err.Error()).
			Msg("webhook worker skipped invalid group tasks")
	}
	return table.Rows, nil
}

func filterTasksWithStatus(tasks []taskagent.FeishuTaskRow) []taskagent.FeishuTaskRow {
	if len(tasks) == 0 {
		return nil
	}
	result := make([]taskagent.FeishuTaskRow, 0, len(tasks))
	for _, t := range tasks {
		status := strings.TrimSpace(t.Status)
		webhook := strings.TrimSpace(t.Webhook)
		if status == "" || webhook == "" {
			continue
		}
		if status == taskagent.StatusError {
			continue
		}
		result = append(result, t)
	}
	return result
}

func (w *WebhookWorker) applyWebhookFailure(ctx context.Context, table *taskagent.FeishuTaskTable, taskIDs []int64, groupID string) (webhookOutcome, error) {
	var outcome webhookOutcome
	failed, errored := w.partitionFailureStates(taskIDs, groupID)
	if len(failed) > 0 {
		if err := w.updateWebhookState(ctx, table, failed, taskagent.WebhookFailed); err != nil {
			return outcome, err
		}
		log.Warn().
			Int("failed_count", len(failed)).
			Str("group_id", groupID).
			Msg("webhook worker marked tasks as failed")
		outcome.failedIDs = append(outcome.failedIDs, failed...)
	}
	if len(errored) > 0 {
		if err := w.updateWebhookState(ctx, table, errored, taskagent.WebhookError); err != nil {
			return outcome, err
		}
		log.Warn().
			Int("error_count", len(errored)).
			Str("group_id", groupID).
			Msg("webhook worker marked tasks as error")
		outcome.errorIDs = append(outcome.errorIDs, errored...)
	}
	return outcome, nil
}

func (w *WebhookWorker) partitionFailureStates(taskIDs []int64, groupID string) (failedIDs, errorIDs []int64) {
	if len(taskIDs) == 0 {
		return nil, nil
	}
	if w.taskFailures == nil {
		w.taskFailures = make(map[int64]int)
	}
	for _, id := range taskIDs {
		if id == 0 {
			continue
		}
		w.taskFailures[id]++
		if w.taskFailures[id] >= maxWebhookFailures {
			errorIDs = append(errorIDs, id)
			log.Warn().
				Int64("task_id", id).
				Str("group_id", groupID).
				Int("failures", w.taskFailures[id]).
				Msg("webhook delivery upgraded to error after repeated failures")
		} else {
			failedIDs = append(failedIDs, id)
		}
	}
	return failedIDs, errorIDs
}

func (w *WebhookWorker) resetFailureCounts(taskIDs []int64) {
	if len(taskIDs) == 0 || w == nil || w.taskFailures == nil {
		return
	}
	for _, id := range taskIDs {
		delete(w.taskFailures, id)
	}
}

func allGroupTasksSuccess(tasks []taskagent.FeishuTaskRow) bool {
	if len(tasks) == 0 {
		return false
	}
	seenEvaluated := false
	for _, t := range tasks {
		status := strings.TrimSpace(t.Status)
		if status == "" {
			continue
		}
		seenEvaluated = true
		if status != taskagent.StatusSuccess {
			return false
		}
	}
	return seenEvaluated
}

func countSuccessTasks(tasks []taskagent.FeishuTaskRow) int {
	count := 0
	for _, t := range tasks {
		if strings.TrimSpace(t.Status) == taskagent.StatusSuccess {
			count++
		}
	}
	return count
}

func extractTaskIDs(tasks []taskagent.FeishuTaskRow) []int64 {
	if len(tasks) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(tasks))
	for _, t := range tasks {
		if t.TaskID != 0 {
			ids = append(ids, t.TaskID)
		}
	}
	return ids
}

// sendGroupWebhook sends a webhook for a group of tasks, aggregating results from all scenes.
func (w *WebhookWorker) sendGroupWebhook(ctx context.Context, app, groupID string, tasks []taskagent.FeishuTaskRow) error {
	if len(tasks) == 0 {
		return errors.New("no tasks in group")
	}

	var params, userID, userName string
	for _, t := range tasks {
		if params == "" {
			params = strings.TrimSpace(t.Params)
		}
		if userID == "" {
			userID = strings.TrimSpace(t.UserID)
		}
		if userName == "" {
			userName = strings.TrimSpace(t.UserName)
		}
		if params != "" && userID != "" && userName != "" {
			break
		}
	}

	scenes := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if scene := strings.TrimSpace(t.Scene); scene != "" {
			scenes = append(scenes, scene)
		}
	}

	log.Info().
		Str("scenario", "group").
		Str("group_id", groupID).
		Str("app", app).
		Str("params", params).
		Str("user_id", userID).
		Strs("scenes", scenes).
		Msg("sending group webhook")

	opts := Options{
		App:         app,
		Params:      params,
		UserID:      userID,
		UserName:    userName,
		GroupID:     groupID,
		RecordLimit: -1,
	}
	err := w.sendSummary(ctx, opts)
	if err != nil && !stdErrors.Is(err, ErrNoCaptureRecords) {
		return err
	}
	return nil
}

func eligibleForWebhook(row taskagent.FeishuTaskRow) bool {
	status := strings.ToLower(strings.TrimSpace(row.Status))
	webhook := strings.ToLower(strings.TrimSpace(row.Webhook))
	if status != taskagent.StatusSuccess {
		return false
	}
	return webhook == taskagent.WebhookPending || webhook == taskagent.WebhookFailed
}

func limitEligibleRows(rows []taskagent.FeishuTaskRow, limit int) ([]taskagent.FeishuTaskRow, int) {
	if limit <= 0 || limit > len(rows) {
		limit = len(rows)
	}
	eligible := make([]taskagent.FeishuTaskRow, 0, limit)
	skipped := 0
	for _, row := range rows {
		if eligibleForWebhook(row) {
			eligible = append(eligible, row)
			if len(eligible) >= limit {
				break
			}
			continue
		}
		skipped++
	}
	return eligible, skipped
}

func (w *WebhookWorker) fetchWebhookCandidates(ctx context.Context) (*taskagent.FeishuTaskTable, error) {
	totalLimit := w.batchLimit
	if totalLimit <= 0 {
		totalLimit = 20
	}

	var combined *taskagent.FeishuTaskTable
	totalSkipped := 0

	for _, spec := range webhookCandidatePriority {
		remaining := totalLimit
		if combined != nil {
			remaining -= len(combined.Rows)
			if remaining <= 0 {
				break
			}
		}
		table, skipped, err := w.fetchWebhookBatch(ctx, spec.scene, spec.webhookState, remaining)
		if err != nil {
			return nil, err
		}
		totalSkipped += skipped
		if table == nil || len(table.Rows) == 0 {
			continue
		}
		if combined == nil {
			combined = table
			continue
		}
		combined.Rows = append(combined.Rows, table.Rows...)
		combined.Invalid = append(combined.Invalid, table.Invalid...)
	}

	if combined == nil {
		return nil, nil
	}

	if len(combined.Invalid) > 0 {
		log.Warn().
			Int("invalid_rows", len(combined.Invalid)).
			Str("sample_error", combined.Invalid[0].Err.Error()).
			Msg("webhook worker skipped invalid task rows")
	}
	if totalSkipped > 0 {
		log.Debug().
			Int("skipped", totalSkipped).
			Msg("webhook worker dropped ineligible rows from batches")
	}
	return combined, nil
}

func (w *WebhookWorker) fetchWebhookBatch(ctx context.Context, scene, webhookState string, need int) (*taskagent.FeishuTaskTable, int, error) {
	if need <= 0 {
		return nil, 0, nil
	}
	requestLimit := need * 5
	if requestLimit < need {
		requestLimit = need
	}
	if requestLimit > 200 {
		requestLimit = 200
	}
	opts := &taskagent.FeishuTaskQueryOptions{
		Filter: w.buildFilterInfo(scene, webhookState),
		Limit:  requestLimit,
	}
	if opts.Filter != nil {
		log.Debug().
			Str("filter", filterToJSON(opts.Filter)).
			Str("scene", scene).
			Str("webhook_state", webhookState).
			Int("limit", requestLimit).
			Msg("webhook worker query filter")
	}
	table, err := w.client.FetchTaskTableWithOptions(ctx, w.bitableURL, nil, opts)
	if err != nil {
		return nil, 0, errors.Wrap(err, "fetch webhook candidates failed")
	}
	if table == nil {
		return nil, 0, nil
	}
	skipped := 0
	if len(table.Rows) > 0 {
		today := time.Now()
		table.Rows = filterRowsByDay(table.Rows, today)
	}
	if scene == taskagent.SceneProfileSearch {
		filtered := make([]taskagent.FeishuTaskRow, 0, len(table.Rows))
		for _, row := range table.Rows {
			grp := strings.TrimSpace(row.GroupID)
			if grp != "" && w.shouldSkipGroup(grp) {
				skipped++
				continue
			}
			filtered = append(filtered, row)
		}
		table.Rows = filtered
	}
	eligible, dropped := limitEligibleRows(table.Rows, need)
	skipped += dropped
	table.Rows = eligible
	return table, skipped, nil
}

func (w *WebhookWorker) buildFilterInfo(scene, webhookState string) *taskagent.FeishuFilterInfo {
	fields := taskagent.DefaultTaskFields()
	filter := taskagent.NewFeishuFilterInfo("and")
	if appRef := strings.TrimSpace(fields.App); appRef != "" && strings.TrimSpace(w.app) != "" {
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(appRef, "is", strings.TrimSpace(w.app)))
	}
	if statusRef := strings.TrimSpace(fields.Status); statusRef != "" {
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(statusRef, "is", taskagent.StatusSuccess))
	}
	if webhookRef := strings.TrimSpace(fields.Webhook); webhookRef != "" {
		if strings.TrimSpace(webhookState) != "" {
			filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(webhookRef, "is", strings.TrimSpace(webhookState)))
		}
	}
	if datetimeRef := strings.TrimSpace(fields.Datetime); datetimeRef != "" {
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(datetimeRef, "is", "Today"))
	}
	if sceneRef := strings.TrimSpace(fields.Scene); sceneRef != "" && strings.TrimSpace(scene) != "" {
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(sceneRef, "is", strings.TrimSpace(scene)))
	}
	if emptyFilter(filter) {
		return nil
	}
	return filter
}

func (w *WebhookWorker) sendSummary(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.WebhookURL) == "" {
		opts.WebhookURL = w.webhookURL
	}
	trimmedScene := strings.TrimSpace(opts.Scene)
	trimmedItemID := strings.TrimSpace(opts.ItemID)
	if trimmedScene == taskagent.SceneVideoScreenCapture {
		if strings.TrimSpace(opts.ItemID) == "" {
			return fmt.Errorf("video capture webhook missing item id")
		}
		if !opts.SkipDramaLookup {
			opts.SkipDramaLookup = true
		}
		if !opts.PreferLatest {
			opts.PreferLatest = true
		}
		if opts.RecordLimit == 0 {
			opts.RecordLimit = 1
		}
	}
	if trimmedItemID != "" {
		opts.ItemID = trimmedItemID
	}
	limit := opts.RecordLimit
	switch {
	case limit < 0:
		limit = 0
	case limit == 0:
		limit = 200
	}
	opts.RecordLimit = limit

	sqliteErr := w.sendSummaryWithSource(ctx, opts, SourceSQLite)
	if sqliteErr == nil {
		return nil
	}
	if isContextError(sqliteErr) {
		return sqliteErr
	}

	log.Warn().
		Err(sqliteErr).
		Str("params", strings.TrimSpace(opts.Params)).
		Str("app", strings.TrimSpace(opts.App)).
		Str("group_id", strings.TrimSpace(opts.GroupID)).
		Msg("sqlite summary lookup failed, falling back to Feishu")

	feishuErr := w.sendSummaryWithSource(ctx, opts, SourceFeishu)
	if feishuErr != nil {
		return errors.Wrapf(feishuErr, "send summary webhook via Feishu failed after sqlite error: %v", sqliteErr)
	}
	return nil
}

func (w *WebhookWorker) sendSummaryWithSource(ctx context.Context, opts Options, source Source) error {
	opts.Source = source
	_, err := SendSummaryWebhook(ctx, opts)
	return err
}

func isContextError(err error) bool {
	if err == nil {
		return false
	}
	return stdErrors.Is(err, context.Canceled) || stdErrors.Is(err, context.DeadlineExceeded)
}

func (w *WebhookWorker) updateWebhookState(ctx context.Context, table *taskagent.FeishuTaskTable, taskIDs []int64, state string) error {
	if table == nil || len(taskIDs) == 0 {
		return nil
	}
	field := strings.TrimSpace(table.Fields.Webhook)
	if field == "" {
		return errors.New("webhook column is not configured in task table")
	}
	var errs []string
	payload := map[string]any{field: state}
	for _, taskID := range taskIDs {
		if taskID == 0 {
			continue
		}
		if err := w.client.UpdateTaskFields(ctx, table, taskID, payload); err != nil {
			errs = append(errs, fmt.Sprintf("task %d: %v", taskID, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// emptyFilter returns true if the filter has no conditions or child groups.
func emptyFilter(filter *taskagent.FeishuFilterInfo) bool {
	if filter == nil {
		return true
	}
	return len(filter.Conditions) == 0 && len(filter.Children) == 0
}

// filterToJSON marshals a FilterInfo for logging/debugging.
func filterToJSON(filter *taskagent.FeishuFilterInfo) string {
	if filter == nil {
		return ""
	}
	raw, err := json.Marshal(filter)
	if err != nil {
		return ""
	}
	return string(raw)
}
