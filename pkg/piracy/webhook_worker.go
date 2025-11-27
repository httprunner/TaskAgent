package piracy

import (
	"context"
	stdErrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

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
		app := w.resolveApp(row.App)
		if app == "" {
			errs = append(errs, fmt.Sprintf("task %d missing app", row.TaskID))
			continue
		}
		if err := w.sendSummary(ctx, app, params, row.Scene, strings.TrimSpace(row.UserID), strings.TrimSpace(row.UserName)); err != nil {
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
			Msg("webhook worker iteration completed")
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
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

func (w *WebhookWorker) sendSummary(ctx context.Context, app, params, scene, userID, userName string) error {
	opts := WebhookOptions{
		App:        app,
		Params:     params,
		Scene:      scene,
		UserID:     userID,
		UserName:   userName,
		WebhookURL: w.webhookURL,
		Source:     WebhookSourceFeishu,
	}
	_, err := SendSummaryWebhook(ctx, opts)
	if err == nil || stdErrors.Is(err, ErrNoCaptureRecords) {
		return err
	}
	return errors.Wrapf(err, "send summary webhook failed")
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
