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
}

// WebhookWorker periodically scans Feishu tasks for pending/failed webhook rows
// and retries the synchronization independently from the search workflow.
type WebhookWorker struct {
	client       *feishu.Client
	bitableURL   string
	webhookURL   string
	app          string
	pollInterval time.Duration
	batchLimit   int
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
	return &WebhookWorker{
		client:       client,
		bitableURL:   trimmedURL,
		webhookURL:   webhookURL,
		app:          strings.TrimSpace(cfg.App),
		pollInterval: interval,
		batchLimit:   batch,
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

	if err := w.processOnce(ctx); err != nil {
		log.Error().Err(err).Msg("webhook worker initial scan failed")
	}

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
		params := strings.TrimSpace(row.Params)
		if params == "" || row.TaskID == 0 {
			log.Warn().
				Int64("task_id", row.TaskID).
				Str("params", params).
				Msg("skip task missing params or task_id")
			continue
		}

		groupID := strings.TrimSpace(row.GroupID)

		// Handle grouped tasks
		if groupID != "" {
			if processedGroups[groupID] {
				continue // Already processed this group
			}

			// Fetch all tasks in the same group
			groupTasks, err := w.fetchTasksByGroupID(ctx, groupID)
			if err != nil {
				errs = append(errs, fmt.Sprintf("fetch group %s tasks failed: %v", groupID, err))
				continue
			}

			// Check if all tasks in the group are successful
			if !allGroupTasksSuccess(groupTasks) {
				log.Debug().
					Str("group_id", groupID).
					Int("total", len(groupTasks)).
					Int("success", countSuccessTasks(groupTasks)).
					Msg("group not ready, waiting for all tasks to succeed")
				continue
			}

			// All tasks succeeded, send grouped webhook
			app := w.resolveApp(row.App)
			if app == "" {
				errs = append(errs, fmt.Sprintf("group %s missing app", groupID))
				processedGroups[groupID] = true
				continue
			}

			if err := w.sendGroupWebhook(ctx, app, groupID, groupTasks); err != nil {
				log.Error().Err(err).
					Str("group_id", groupID).
					Str("app", app).
					Int("task_count", len(groupTasks)).
					Msg("group webhook delivery failed")
				// Update all tasks in the group as failed
				taskIDs := extractTaskIDs(groupTasks)
				failedIDs = append(failedIDs, taskIDs...)
				if err := w.updateWebhookState(ctx, table, taskIDs, feishu.WebhookFailed); err != nil {
					errs = append(errs, err.Error())
				}
			} else {
				log.Info().
					Str("group_id", groupID).
					Str("app", app).
					Int("task_count", len(groupTasks)).
					Msg("group webhook delivered")
				taskIDs := extractTaskIDs(groupTasks)
				successIDs = append(successIDs, taskIDs...)
				if err := w.updateWebhookState(ctx, table, taskIDs, feishu.WebhookSuccess); err != nil {
					errs = append(errs, err.Error())
				}
			}

			processedGroups[groupID] = true
			continue
		}

		// Handle single task (no GroupID) - original logic
		app := w.resolveApp(row.App)
		if app == "" {
			errs = append(errs, fmt.Sprintf("task %d missing app", row.TaskID))
			continue
		}
		if err := w.sendSummary(ctx, app, params, strings.TrimSpace(row.UserID), strings.TrimSpace(row.UserName), strings.TrimSpace(row.Scene)); err != nil {
			if stdErrors.Is(err, ErrNoCaptureRecords) {
				log.Warn().
					Int64("task_id", row.TaskID).
					Str("params", params).
					Str("app", app).
					Str("scene", strings.TrimSpace(row.Scene)).
					Str("user_id", strings.TrimSpace(row.UserID)).
					Msg("webhook skipped: no capture records")
				errorIDs = append(errorIDs, row.TaskID)
				if err := w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookError); err != nil {
					errs = append(errs, err.Error())
				}
			} else {
				log.Error().Err(err).
					Str("webhook_url", w.webhookURL).
					Int64("task_id", row.TaskID).
					Str("params", params).
					Msg("webhook delivery failed")
				failedIDs = append(failedIDs, row.TaskID)
				if err := w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookFailed); err != nil {
					errs = append(errs, err.Error())
				}
			}
		} else {
			log.Info().
				Int64("task_id", row.TaskID).
				Str("params", params).
				Str("app", app).
				Str("scene", strings.TrimSpace(row.Scene)).
				Str("user_id", strings.TrimSpace(row.UserID)).
				Str("user_name", strings.TrimSpace(row.UserName)).
				Msg("webhook delivered")
			successIDs = append(successIDs, row.TaskID)
			if err := w.updateWebhookState(ctx, table, []int64{row.TaskID}, feishu.WebhookSuccess); err != nil {
				errs = append(errs, err.Error())
			}
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

// allGroupTasksSuccess checks if all tasks in the group have Status = "success".
func allGroupTasksSuccess(tasks []feishu.TaskRow) bool {
	if len(tasks) == 0 {
		return false
	}
	for _, t := range tasks {
		status := strings.ToLower(strings.TrimSpace(t.Status))
		if status != feishu.StatusSuccess {
			return false
		}
	}
	return true
}

// countSuccessTasks counts how many tasks have Status = "success".
func countSuccessTasks(tasks []feishu.TaskRow) int {
	count := 0
	for _, t := range tasks {
		if strings.ToLower(strings.TrimSpace(t.Status)) == feishu.StatusSuccess {
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

	// Use the first task's params, userID, userName as they should be the same within a group
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
		if params != "" && userID != "" {
			break
		}
	}

	// Collect all scenes in the group
	scenes := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if scene := strings.TrimSpace(t.Scene); scene != "" {
			scenes = append(scenes, scene)
		}
	}

	log.Info().
		Str("group_id", groupID).
		Str("app", app).
		Str("params", params).
		Str("user_id", userID).
		Strs("scenes", scenes).
		Msg("sending group webhook")

	// TODO(droid): 组装所有场景的详细信息并通过单次 webhook 统一下发
	var scene string
	if len(scenes) == 1 {
		scene = scenes[0]
	}
	err := w.sendSummary(ctx, app, params, userID, userName, scene)
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

	webhookStates := []string{feishu.WebhookPending, feishu.WebhookFailed}
	var combined *feishu.TaskTable
	totalSkipped := 0

	for _, state := range webhookStates {
		remaining := totalLimit
		if combined != nil {
			remaining -= len(combined.Rows)
			if remaining <= 0 {
				break
			}
		}
		table, skipped, err := w.fetchWebhookBatch(ctx, state, remaining)
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

func (w *WebhookWorker) fetchWebhookBatch(ctx context.Context, webhookState string, need int) (*feishu.TaskTable, int, error) {
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
		Filter: w.buildFilterInfo(webhookState),
		Limit:  requestLimit,
	}
	if opts.Filter != nil {
		log.Debug().
			Str("filter", FilterToJSON(opts.Filter)).
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
	eligible, skipped := limitEligibleRows(table.Rows, need)
	table.Rows = eligible
	return table, skipped, nil
}

func (w *WebhookWorker) buildFilterInfo(webhookState string) *feishu.FilterInfo {
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
	if EmptyFilter(filter) {
		return nil
	}
	return filter
}

func (w *WebhookWorker) resolveApp(taskApp string) string {
	if trimmed := strings.TrimSpace(taskApp); trimmed != "" {
		return trimmed
	}
	return w.app
}

func (w *WebhookWorker) sendSummary(ctx context.Context, app, params, userID, userName, scene string) error {
	baseOpts := WebhookOptions{
		App:        app,
		Params:     params,
		UserID:     userID,
		UserName:   userName,
		WebhookURL: w.webhookURL,
	}
	if itemID, skip := deriveCaptureLookupHints(scene, params); skip {
		baseOpts.SkipDramaLookup = true
		baseOpts.ItemIDHint = itemID
	}

	sqliteErr := w.sendSummaryWithSource(ctx, baseOpts, WebhookSourceSQLite)
	if sqliteErr == nil {
		return nil
	}
	if isContextError(sqliteErr) {
		return sqliteErr
	}

	log.Warn().
		Err(sqliteErr).
		Str("params", params).
		Str("app", app).
		Msg("sqlite summary lookup failed, falling back to feishu")

	feishuErr := w.sendSummaryWithSource(ctx, baseOpts, WebhookSourceFeishu)
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

func deriveCaptureLookupHints(scene, params string) (itemID string, skip bool) {
	if strings.TrimSpace(scene) != pool.SceneVideoScreenCapture {
		return "", false
	}
	return extractItemIDFromParams(params), true
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
