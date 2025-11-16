package piracy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

// WebhookWorkerConfig captures the knobs for the standalone webhook worker.
type WebhookWorkerConfig struct {
	TargetBitableURL  string
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
	trimmedURL := strings.TrimSpace(cfg.TargetBitableURL)
	if trimmedURL == "" {
		return nil, errors.New("target bitable url is required for webhook worker")
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
	var successIDs []int64
	var failedIDs []int64
	var errs []string

	for _, row := range table.Rows {
		params := strings.TrimSpace(row.Params)
		if params == "" || row.TaskID == 0 {
			continue
		}
		app := w.resolveApp(row.App)
		if app == "" {
			errs = append(errs, fmt.Sprintf("task %d missing app", row.TaskID))
			continue
		}
		if err := w.sendSummary(ctx, app, params, strings.TrimSpace(row.UserID), strings.TrimSpace(row.UserName)); err != nil {
			log.Error().Err(err).
				Int64("task_id", row.TaskID).
				Str("params", params).
				Msg("webhook delivery failed")
			failedIDs = append(failedIDs, row.TaskID)
		} else {
			log.Info().
				Int64("task_id", row.TaskID).
				Str("params", params).
				Str("app", app).
				Msg("webhook delivered")
			successIDs = append(successIDs, row.TaskID)
		}
	}

	if len(successIDs) > 0 {
		if err := w.updateWebhookState(ctx, table, successIDs, feishu.WebhookSuccess); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(failedIDs) > 0 {
		if err := w.updateWebhookState(ctx, table, failedIDs, feishu.WebhookFailed); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(successIDs)+len(failedIDs) > 0 {
		log.Info().
			Int("success", len(successIDs)).
			Int("failed", len(failedIDs)).
			Msg("webhook worker iteration completed")
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (w *WebhookWorker) fetchWebhookCandidates(ctx context.Context) (*feishu.TargetTable, error) {
	opts := &feishu.TargetQueryOptions{
		Filter: w.buildFilterExpression(),
		Limit:  w.batchLimit,
	}
	table, err := w.client.FetchTargetTableWithOptions(ctx, w.bitableURL, nil, opts)
	if err != nil {
		return nil, errors.Wrap(err, "fetch webhook candidates failed")
	}
	return table, nil
}

func (w *WebhookWorker) buildFilterExpression() string {
	fields := feishu.DefaultTargetFields
	var clauses []string
	if appRef := strings.TrimSpace(fields.App); appRef != "" && strings.TrimSpace(w.app) != "" {
		clauses = append(clauses, fmt.Sprintf("%s = \"%s\"", bitableFieldRef(appRef), escapeBitableFilterValue(w.app)))
	}
	if statusRef := strings.TrimSpace(fields.Status); statusRef != "" {
		clauses = append(clauses, fmt.Sprintf("%s = \"%s\"", bitableFieldRef(statusRef), feishu.StatusSuccess))
	}
	if webhookRef := strings.TrimSpace(fields.Webhook); webhookRef != "" {
		ref := bitableFieldRef(webhookRef)
		clauses = append(clauses, fmt.Sprintf("OR(%s = \"%s\", %s = \"%s\")", ref, feishu.WebhookPending, ref, feishu.WebhookFailed))
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

func (w *WebhookWorker) resolveApp(taskApp string) string {
	if trimmed := strings.TrimSpace(taskApp); trimmed != "" {
		return trimmed
	}
	return w.app
}

func (w *WebhookWorker) sendSummary(ctx context.Context, app, params, userID, userName string) error {
	opts := WebhookOptions{
		App:        app,
		Params:     params,
		UserID:     userID,
		UserName:   userName,
		WebhookURL: w.webhookURL,
		Source:     WebhookSourceFeishu,
	}
	if _, err := SendSummaryWebhook(ctx, opts); err != nil {
		return errors.Wrapf(err, "send summary webhook failed")
	}
	return nil
}

func (w *WebhookWorker) updateWebhookState(ctx context.Context, table *feishu.TargetTable, taskIDs []int64, state string) error {
	if table == nil || len(taskIDs) == 0 {
		return nil
	}
	field := strings.TrimSpace(table.Fields.Webhook)
	if field == "" {
		return errors.New("webhook column is not configured in target table")
	}
	var errs []string
	payload := map[string]any{field: state}
	for _, taskID := range taskIDs {
		if taskID == 0 {
			continue
		}
		if err := w.client.UpdateTargetFields(ctx, table, taskID, payload); err != nil {
			errs = append(errs, fmt.Sprintf("task %d: %v", taskID, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func bitableFieldRef(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	return fmt.Sprintf("CurrentValue.[%s]", trimmed)
}

func escapeBitableFilterValue(value string) string {
	return strings.ReplaceAll(value, "\"", "\\\"")
}
