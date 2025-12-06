package piracy

import (
	"context"
	stdErrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
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
	client        *feishu.Client
	bitableURL    string
	webhookURL    string
	app           string
	pollInterval  time.Duration
	batchLimit    int
	groupCooldown map[string]time.Time
	cooldownDur   time.Duration
}

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
	{scene: pool.SceneVideoScreenCapture, webhookState: feishu.WebhookPending},
	{scene: pool.SceneVideoScreenCapture, webhookState: feishu.WebhookFailed},
	{scene: pool.SceneProfileSearch, webhookState: feishu.WebhookPending},
	{scene: pool.SceneProfileSearch, webhookState: feishu.WebhookFailed},
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
	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "init feishu client for webhook worker failed")
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

func (w *WebhookWorker) processRow(ctx context.Context, table *feishu.TaskTable, row feishu.TaskRow, processedGroups map[string]bool) (webhookOutcome, error) {
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
	case scene == pool.SceneProfileSearch && groupID != "":
		return w.handleGroupRow(ctx, table, row, app, processedGroups)
	case scene == pool.SceneVideoScreenCapture:
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

func (w *WebhookWorker) handleGroupRow(ctx context.Context, table *feishu.TaskTable, row feishu.TaskRow, app string, processedGroups map[string]bool) (webhookOutcome, error) {
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
		outcome.failedIDs = append(outcome.failedIDs, taskIDs...)
		if updErr := w.updateWebhookState(ctx, table, taskIDs, feishu.WebhookFailed); updErr != nil {
			return outcome, updErr
		}
		return outcome, nil
	}

	log.Info().
		Str("scene", row.Scene).
		Str("group_id", groupID).
		Str("app", app).
		Int("task_count", len(dayTasks)).
		Msg("group webhook delivered")
	outcome.successIDs = append(outcome.successIDs, taskIDs...)
	w.clearGroupCooldown(groupID)
	return outcome, w.updateWebhookState(ctx, table, taskIDs, feishu.WebhookSuccess)
}

func normalizeTaskDay(ts *time.Time) (time.Time, bool) {
	if ts == nil {
		return time.Time{}, false
	}
	local := ts.In(time.Local)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, local.Location())
	return day, true
}

func filterTasksByDate(tasks []feishu.TaskRow, referenceDay time.Time) []feishu.TaskRow {
	if len(tasks) == 0 {
		return nil
	}
	ref := referenceDay.In(time.Local)
	filtered := make([]feishu.TaskRow, 0, len(tasks))
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

func (w *WebhookWorker) handleVideoCaptureRow(ctx context.Context, table *feishu.TaskTable, row feishu.TaskRow, app string) (webhookOutcome, error) {
	var outcome webhookOutcome
	itemID := strings.TrimSpace(row.ItemID)
	if itemID == "" {
		log.Warn().
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Msg("video capture task missing item id, marking as error")
		outcome.errorIDs = append(outcome.errorIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookError)
	}

	opts := WebhookOptions{
		App:             app,
		Params:          strings.TrimSpace(row.Params),
		UserID:          strings.TrimSpace(row.UserID),
		UserName:        strings.TrimSpace(row.UserName),
		Scene:           row.Scene,
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
			Str("app", app).
			Str("item_id", itemID).
			Msg("webhook delivered")
		outcome.successIDs = append(outcome.successIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookSuccess)
	case stdErrors.Is(sendErr, ErrNoCaptureRecords):
		log.Warn().
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("app", app).
			Str("item_id", itemID).
			Msg("webhook skipped: no capture records")
		outcome.errorIDs = append(outcome.errorIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookError)
	default:
		log.Error().Err(sendErr).
			Str("scene", row.Scene).
			Str("webhook_url", w.webhookURL).
			Int64("task_id", row.TaskID).
			Str("item_id", itemID).
			Msg("video capture webhook delivery failed")
		outcome.failedIDs = append(outcome.failedIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookFailed)
	}
}

func (w *WebhookWorker) handleSingleRow(ctx context.Context, table *feishu.TaskTable, row feishu.TaskRow, app, params string) (webhookOutcome, error) {
	var outcome webhookOutcome
	opts := WebhookOptions{
		App:      app,
		Params:   params,
		UserID:   strings.TrimSpace(row.UserID),
		UserName: strings.TrimSpace(row.UserName),
		Scene:    row.Scene,
		ItemID:   strings.TrimSpace(row.ItemID),
	}

	sendErr := w.sendSummary(ctx, opts)
	switch {
	case sendErr == nil:
		log.Info().
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("params", params).
			Str("app", app).
			Str("user_id", strings.TrimSpace(row.UserID)).
			Str("user_name", strings.TrimSpace(row.UserName)).
			Msg("webhook delivered")
		outcome.successIDs = append(outcome.successIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookSuccess)
	case stdErrors.Is(sendErr, ErrNoCaptureRecords):
		log.Warn().
			Str("scene", row.Scene).
			Int64("task_id", row.TaskID).
			Str("params", params).
			Str("app", app).
			Str("user_id", strings.TrimSpace(row.UserID)).
			Msg("webhook skipped: no capture records")
		outcome.errorIDs = append(outcome.errorIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookError)
	default:
		log.Error().Err(sendErr).
			Str("scene", row.Scene).
			Str("webhook_url", w.webhookURL).
			Int64("task_id", row.TaskID).
			Str("params", params).
			Msg("webhook delivery failed")
		outcome.failedIDs = append(outcome.failedIDs, row.TaskID)
		return outcome, w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookFailed)
	}
}

// fetchTasksByGroupID retrieves all tasks that share the same GroupID.
func (w *WebhookWorker) fetchTasksByGroupID(ctx context.Context, groupID string) ([]feishu.TaskRow, error) {
	if groupID == "" {
		return nil, errors.New("groupID is empty")
	}

	fields := feishu.DefaultTaskFields
	filter := feishu.NewFilterInfo("and")

	// Filter by GroupID
	if groupIDRef := strings.TrimSpace(fields.GroupID); groupIDRef != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(groupIDRef, "is", groupID))
	}

	// Optionally filter by App if configured
	if appRef := strings.TrimSpace(fields.App); appRef != "" && strings.TrimSpace(w.app) != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(appRef, "is", strings.TrimSpace(w.app)))
	}

	opts := &feishu.TaskQueryOptions{
		Filter: filter,
		Limit:  100, // Reasonable limit for tasks in a single group
	}

	table, err := w.client.FetchTaskTableWithOptions(ctx, w.bitableURL, nil, opts)
	if err != nil {
		return nil, errors.Wrap(err, "fetch tasks by group id failed")
	}
	if table == nil {
		return nil, nil
	}

	return table.Rows, nil
}

func filterTasksWithStatus(tasks []feishu.TaskRow) []feishu.TaskRow {
	if len(tasks) == 0 {
		return nil
	}
	filtered := make([]feishu.TaskRow, 0, len(tasks))
	for _, t := range tasks {
		status := strings.ToLower(strings.TrimSpace(t.Status))
		webhook := strings.TrimSpace(t.Webhook)
		if status == "" || status == feishu.StatusError {
			continue
		}
		if webhook == "" {
			continue
		}
		filtered = append(filtered, t)
	}
	return filtered
}

// allGroupTasksSuccess checks if all tasks in the group have Status = "success".
func allGroupTasksSuccess(tasks []feishu.TaskRow) bool {
	if len(tasks) == 0 {
		return false
	}
	found := false
	for _, t := range tasks {
		status := strings.ToLower(strings.TrimSpace(t.Status))
		if status == "" {
			continue
		}
		found = true
		if status != feishu.StatusSuccess {
			return false
		}
	}
	return found
}

// countSuccessTasks counts how many tasks have Status = "success".
func countSuccessTasks(tasks []feishu.TaskRow) int {
	count := 0
	for _, t := range tasks {
		status := strings.ToLower(strings.TrimSpace(t.Status))
		if status == "" {
			continue
		}
		if status == feishu.StatusSuccess {
			count++
		}
	}
	return count
}

// extractTaskIDs extracts TaskID from a list of TaskRows.
func extractTaskIDs(tasks []feishu.TaskRow) []int64 {
	ids := make([]int64, 0, len(tasks))
	for _, t := range tasks {
		if t.TaskID != 0 {
			ids = append(ids, t.TaskID)
		}
	}
	return ids
}

// sendGroupWebhook sends a webhook for a group of tasks, aggregating results from all scenes.
func (w *WebhookWorker) sendGroupWebhook(ctx context.Context, app, groupID string, tasks []feishu.TaskRow) error {
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

	opts := WebhookOptions{
		App:         app,
		Params:      params,
		UserID:      userID,
		UserName:    userName,
		RecordLimit: -1,
	}
	err := w.sendSummary(ctx, opts)
	if err != nil && !stdErrors.Is(err, ErrNoCaptureRecords) {
		return err
	}
	return nil
}

func eligibleForWebhook(row feishu.TaskRow) bool {
	status := strings.ToLower(strings.TrimSpace(row.Status))
	webhook := strings.ToLower(strings.TrimSpace(row.Webhook))
	if status != feishu.StatusSuccess {
		return false
	}
	return webhook == feishu.WebhookPending || webhook == feishu.WebhookFailed
}

func limitEligibleRows(rows []feishu.TaskRow, limit int) ([]feishu.TaskRow, int) {
	if limit <= 0 || limit > len(rows) {
		limit = len(rows)
	}
	eligible := make([]feishu.TaskRow, 0, limit)
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

func (w *WebhookWorker) fetchWebhookCandidates(ctx context.Context) (*feishu.TaskTable, error) {
	totalLimit := w.batchLimit
	if totalLimit <= 0 {
		totalLimit = 20
	}

	var combined *feishu.TaskTable
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

func (w *WebhookWorker) fetchWebhookBatch(ctx context.Context, scene, webhookState string, need int) (*feishu.TaskTable, int, error) {
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
	opts := &feishu.TaskQueryOptions{
		Filter: w.buildFilterInfo(scene, webhookState),
		Limit:  requestLimit,
	}
	if opts.Filter != nil {
		log.Debug().
			Str("filter", FilterToJSON(opts.Filter)).
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
	if scene == pool.SceneProfileSearch {
		filtered := make([]feishu.TaskRow, 0, len(table.Rows))
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

func (w *WebhookWorker) buildFilterInfo(scene, webhookState string) *feishu.FilterInfo {
	fields := feishu.DefaultTaskFields
	filter := feishu.NewFilterInfo("and")
	if appRef := strings.TrimSpace(fields.App); appRef != "" && strings.TrimSpace(w.app) != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(appRef, "is", strings.TrimSpace(w.app)))
	}
	if statusRef := strings.TrimSpace(fields.Status); statusRef != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(statusRef, "is", feishu.StatusSuccess))
	}
	if webhookRef := strings.TrimSpace(fields.Webhook); webhookRef != "" {
		if strings.TrimSpace(webhookState) != "" {
			filter.Conditions = append(filter.Conditions, feishu.NewCondition(webhookRef, "is", strings.TrimSpace(webhookState)))
		}
	}
	if sceneRef := strings.TrimSpace(fields.Scene); sceneRef != "" && strings.TrimSpace(scene) != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(sceneRef, "is", strings.TrimSpace(scene)))
	}
	if EmptyFilter(filter) {
		return nil
	}
	return filter
}

func (w *WebhookWorker) sendSummary(ctx context.Context, opts WebhookOptions) error {
	if strings.TrimSpace(opts.WebhookURL) == "" {
		opts.WebhookURL = w.webhookURL
	}
	trimmedScene := strings.TrimSpace(opts.Scene)
	trimmedItemID := strings.TrimSpace(opts.ItemID)
	if trimmedScene == pool.SceneVideoScreenCapture {
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
		limit = defaultRecordLimit
	}
	opts.RecordLimit = limit

	sqliteErr := w.sendSummaryWithSource(ctx, opts, WebhookSourceSQLite)
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
		Msg("sqlite summary lookup failed, falling back to feishu")

	feishuErr := w.sendSummaryWithSource(ctx, opts, WebhookSourceFeishu)
	if feishuErr != nil {
		return errors.Wrapf(feishuErr, "send summary webhook via feishu failed after sqlite error: %v", sqliteErr)
	}
	return nil
}

func (w *WebhookWorker) sendSummaryWithSource(ctx context.Context, opts WebhookOptions, source WebhookSource) error {
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

func (w *WebhookWorker) updateWebhookState(ctx context.Context, table *feishu.TaskTable, taskIDs []int64, state string) error {
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
