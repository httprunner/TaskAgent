package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type WebhookResultWorkerConfig struct {
	TaskBitableURL    string
	WebhookBitableURL string
	SummaryWebhookURL string
	PollInterval      time.Duration
	BatchLimit        int
	GroupCooldown     time.Duration
}

type WebhookResultWorker struct {
	store        *webhookResultStore
	taskClient   *taskagent.FeishuClient
	taskTableURL string
	webhookURL   string
	pollInterval time.Duration
	batchLimit   int
	cooldownDur  time.Duration
	groupCD      map[string]time.Time
}

const maxWebhookResultRetries = 3

func NewWebhookResultWorker(cfg WebhookResultWorkerConfig) (*WebhookResultWorker, error) {
	taskURL := strings.TrimSpace(firstNonEmpty(cfg.TaskBitableURL,
		taskagent.EnvString(taskagent.EnvTaskBitableURL, "")))
	if taskURL == "" {
		return nil, errors.New("task bitable url is required")
	}
	webhookURL := strings.TrimSpace(cfg.SummaryWebhookURL)
	if webhookURL == "" {
		return nil, errors.New("summary webhook url is required")
	}
	store, err := newWebhookResultStore(firstNonEmpty(cfg.WebhookBitableURL,
		taskagent.EnvString(taskagent.EnvWebhookBitableURL, "")))
	if err != nil {
		return nil, err
	}
	if store == nil || strings.TrimSpace(store.table()) == "" {
		return nil, errors.New("webhook result bitable url is required (WEBHOOK_BITABLE_URL)")
	}
	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		return nil, err
	}
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	batch := cfg.BatchLimit
	if batch <= 0 {
		batch = 20
	}
	cd := cfg.GroupCooldown
	if cd <= 0 {
		cd = 2 * time.Minute
	}
	return &WebhookResultWorker{
		store:        store,
		taskClient:   client,
		taskTableURL: taskURL,
		webhookURL:   webhookURL,
		pollInterval: interval,
		batchLimit:   batch,
		cooldownDur:  cd,
		groupCD:      make(map[string]time.Time),
	}, nil
}

func (w *WebhookResultWorker) Run(ctx context.Context) error {
	if w == nil {
		return errors.New("webhook result worker is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	log.Info().
		Str("task_bitable", w.taskTableURL).
		Str("webhook_bitable", w.store.table()).
		Dur("poll_interval", w.pollInterval).
		Int("batch_limit", w.batchLimit).
		Msg("webhook result worker started")

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("webhook result worker stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := w.processOnce(ctx); err != nil {
				log.Error().Err(err).Msg("webhook result worker scan failed")
			}
		}
	}
}

func (w *WebhookResultWorker) processOnce(ctx context.Context) error {
	rows, err := w.store.listCandidates(ctx, w.batchLimit)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var errs []string
	for _, row := range rows {
		if strings.TrimSpace(row.RecordID) == "" {
			continue
		}
		key := webhookResultCooldownKey(row)
		if w.shouldSkip(key) {
			continue
		}
		if err := w.handleRow(ctx, row); err != nil {
			errs = append(errs, err.Error())
			w.markCooldown(key, "error")
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func webhookResultCooldownKey(row webhookResultRow) string {
	biz := strings.TrimSpace(row.BizType)
	if biz == "" {
		biz = WebhookBizTypePiracyGeneralSearch
	}
	if biz == WebhookBizTypePiracyGeneralSearch {
		return fmt.Sprintf("%d|%s|%s", row.ParentTaskID, strings.TrimSpace(row.GroupID), biz)
	}
	return fmt.Sprintf("%s|%s", strings.TrimSpace(row.RecordID), biz)
}

func (w *WebhookResultWorker) shouldSkip(key string) bool {
	if w == nil || strings.TrimSpace(key) == "" {
		return false
	}
	exp, ok := w.groupCD[key]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(w.groupCD, key)
		return false
	}
	return true
}

func (w *WebhookResultWorker) markCooldown(key, reason string) {
	if w == nil || strings.TrimSpace(key) == "" || w.cooldownDur <= 0 {
		return
	}
	w.groupCD[key] = time.Now().Add(w.cooldownDur)
	log.Debug().Str("key", key).Str("reason", reason).Dur("cooldown", w.cooldownDur).Msg("webhook result row added to cooldown")
}

func (w *WebhookResultWorker) handleRow(ctx context.Context, row webhookResultRow) error {
	status := strings.ToLower(strings.TrimSpace(row.Status))
	switch status {
	case WebhookResultPending, WebhookResultFailed:
	default:
		return nil
	}
	if status == WebhookResultFailed && row.RetryCount >= maxWebhookResultRetries {
		now := time.Now().UTC().UnixMilli()
		msg := fmt.Sprintf("retry exceeded: %d", row.RetryCount)
		state := WebhookResultError
		return w.store.update(ctx, row.RecordID, webhookResultUpdate{
			Status:    &state,
			EndAtMs:   &now,
			LastError: &msg,
		})
	}

	taskIDs := row.TaskIDs
	if len(taskIDs) == 0 {
		w.markCooldown(row.RecordID, "empty_task_ids")
		return nil
	}
	tasks, err := w.fetchTasksByIDs(ctx, taskIDs)
	if err != nil {
		return err
	}
	if !allTasksReady(tasks, taskIDs) {
		w.markCooldown(row.RecordID, "tasks_not_ready")
		return nil
	}

	nowMs := time.Now().UTC().UnixMilli()
	if row.StartAtMs == 0 {
		if err := w.store.update(ctx, row.RecordID, webhookResultUpdate{StartAtMs: &nowMs}); err != nil {
			return err
		}
	}

	bizType := strings.TrimSpace(row.BizType)
	if bizType == "" {
		bizType = WebhookBizTypePiracyGeneralSearch
	}

	meta := pickTaskMeta(tasks)
	dramaRaw, err := decodeDramaInfo(row.DramaInfo)
	if err != nil {
		return w.markFailed(ctx, row, fmt.Errorf("decode drama info failed: %v", err))
	}

	var records []CaptureRecordPayload
	switch bizType {
	case WebhookBizTypePiracyGeneralSearch:
		records, err = fetchCaptureRecordsByTaskIDs(ctx, taskIDs)
		if err != nil {
			return w.markFailed(ctx, row, err)
		}
		if len(records) == 0 {
			// Keep behavior aligned with legacy group worker: no records -> treat as done to avoid infinite retries.
			state := WebhookResultSuccess
			empty := ""
			return w.store.update(ctx, row.RecordID, webhookResultUpdate{
				Status:    &state,
				EndAtMs:   &nowMs,
				LastError: &empty,
			})
		}
	case WebhookBizTypeVideoScreenCapture:
		task := pickVideoScreenCaptureTask(tasks)
		if strings.TrimSpace(task.ItemID) == "" {
			return w.markFailed(ctx, row, fmt.Errorf("video screen capture task missing item id, task_id=%d", task.TaskID))
		}
		query := recordQuery{
			App:          strings.TrimSpace(task.App),
			Scene:        strings.TrimSpace(task.Scene),
			ItemID:       strings.TrimSpace(task.ItemID),
			Limit:        1,
			PreferLatest: true,
		}
		records, err = fetchCaptureRecordsByQuery(ctx, query)
		if err != nil {
			return w.markFailed(ctx, row, err)
		}
		if len(records) == 0 {
			return w.markFailed(ctx, row, ErrNoCaptureRecords)
		}
	default:
		return w.markFailed(ctx, row, fmt.Errorf("unknown biz type: %s", bizType))
	}

	payload := buildWebhookResultPayload(dramaRaw, records)
	if val, ok := payload["DramaName"]; !ok || strings.TrimSpace(fmt.Sprint(val)) == "" {
		payload["DramaName"] = strings.TrimSpace(meta.Params)
	}
	taskItemIDs := buildTaskItemIDs(records)
	recordsPayloadJSON, _ := json.Marshal(map[string]any{
		"total":  len(taskItemIDs),
		"format": "TaskID_ItemID",
		"items":  taskItemIDs,
	})
	userInfoJSON, _ := json.Marshal(map[string]any{
		"UserID":   meta.UserID,
		"UserName": meta.UserName,
	})
	recordsStr := strings.TrimSpace(string(recordsPayloadJSON))
	if recordsStr == "" {
		recordsStr = `{"total":0,"format":"TaskID_ItemID","items":[]}`
	}
	userInfoStr := strings.TrimSpace(string(userInfoJSON))
	if userInfoStr == "" {
		userInfoStr = "{}"
	}
	// Persist payload artifacts before webhook delivery so operators can inspect failures.
	if err := w.store.update(ctx, row.RecordID, webhookResultUpdate{
		Records:  ptrString(recordsStr),
		UserInfo: ptrString(userInfoStr),
	}); err != nil {
		return err
	}

	if err := PostWebhook(ctx, w.webhookURL, payload, nil); err != nil {
		return w.markFailed(ctx, row, err)
	}

	end := time.Now().UTC().UnixMilli()
	state := WebhookResultSuccess
	zero := 0
	empty := ""
	return w.store.update(ctx, row.RecordID, webhookResultUpdate{
		Status:     &state,
		EndAtMs:    &end,
		RetryCount: &zero,
		LastError:  &empty,
		UserInfo:   ptrString(userInfoStr),
		Records:    ptrString(recordsStr),
	})
}

func buildWebhookResultPayload(dramaRaw map[string]any, records []CaptureRecordPayload) map[string]any {
	payload := flattenDramaFields(dramaRaw, taskagent.DefaultDramaFields())
	flat, _ := FlattenRecordsAndCollectItemIDs(records, taskagent.DefaultResultFields())
	payload["records"] = flat
	return payload
}

func buildTaskItemIDs(records []CaptureRecordPayload) []string {
	if len(records) == 0 {
		return nil
	}
	fields := taskagent.DefaultResultFields()
	taskKey := strings.TrimSpace(fields.TaskID)
	itemKey := strings.TrimSpace(fields.ItemID)
	seen := make(map[string]struct{}, len(records))
	out := make([]string, 0, len(records))
	for _, rec := range records {
		if rec.Fields == nil {
			continue
		}
		taskID := strings.TrimSpace(getString(rec.Fields, "TaskID"))
		if taskID == "" && taskKey != "" {
			taskID = strings.TrimSpace(getString(rec.Fields, taskKey))
		}
		itemID := strings.TrimSpace(getString(rec.Fields, "ItemID"))
		if itemID == "" && itemKey != "" {
			itemID = strings.TrimSpace(getString(rec.Fields, itemKey))
		}
		if taskID == "" || itemID == "" {
			continue
		}
		key := fmt.Sprintf("%s_%s", taskID, itemID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func (w *WebhookResultWorker) markFailed(ctx context.Context, row webhookResultRow, err error) error {
	if err == nil {
		return nil
	}
	now := time.Now().UTC().UnixMilli()
	next := row.RetryCount + 1
	msg := strings.TrimSpace(err.Error())
	state := WebhookResultFailed
	if next >= maxWebhookResultRetries {
		state = WebhookResultError
	}
	return w.store.update(ctx, row.RecordID, webhookResultUpdate{
		Status:     &state,
		EndAtMs:    &now,
		RetryCount: &next,
		LastError:  &msg,
	})
}

type taskMeta struct {
	Params   string
	UserID   string
	UserName string
	App      string
}

func pickTaskMeta(tasks []taskagent.FeishuTaskRow) taskMeta {
	var meta taskMeta
	for _, t := range tasks {
		if meta.App == "" {
			meta.App = strings.TrimSpace(t.App)
		}
		if meta.Params == "" {
			meta.Params = strings.TrimSpace(t.Params)
		}
		if meta.UserID == "" {
			meta.UserID = strings.TrimSpace(t.UserID)
		}
		if meta.UserName == "" {
			meta.UserName = strings.TrimSpace(t.UserName)
		}
		if meta.App != "" && meta.Params != "" && meta.UserID != "" && meta.UserName != "" {
			break
		}
	}
	return meta
}

func allTasksReady(tasks []taskagent.FeishuTaskRow, taskIDs []int64) bool {
	if len(taskIDs) == 0 {
		return false
	}
	byID := make(map[int64]string, len(tasks))
	for _, t := range tasks {
		if t.TaskID == 0 {
			continue
		}
		byID[t.TaskID] = strings.ToLower(strings.TrimSpace(t.Status))
	}
	for _, id := range taskIDs {
		st, ok := byID[id]
		if !ok {
			return false
		}
		switch st {
		case taskagent.StatusSuccess, taskagent.StatusError:
			continue
		default:
			return false
		}
	}
	return true
}

func (w *WebhookResultWorker) fetchTasksByIDs(ctx context.Context, taskIDs []int64) ([]taskagent.FeishuTaskRow, error) {
	if w == nil || w.taskClient == nil {
		return nil, errors.New("task client is nil")
	}
	fields := taskagent.DefaultTaskFields()
	taskIDField := strings.TrimSpace(fields.TaskID)
	if taskIDField == "" {
		return nil, errors.New("task table TaskID field mapping is empty")
	}
	filter := taskagent.NewFeishuFilterInfo("or")
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(taskIDField, "is", fmt.Sprintf("%d", id)))
	}
	if len(filter.Conditions) == 0 {
		return nil, nil
	}
	limit := len(filter.Conditions) * 200
	if limit < 200 {
		limit = 200
	}
	if limit > 2000 {
		limit = 2000
	}
	table, err := w.taskClient.FetchTaskTableWithOptions(ctx, w.taskTableURL, nil, &taskagent.FeishuTaskQueryOptions{
		Filter:     filter,
		Limit:      limit,
		IgnoreView: true,
	})
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, nil
	}
	return table.Rows, nil
}

func ptrString(v string) *string { return &v }

func pickVideoScreenCaptureTask(tasks []taskagent.FeishuTaskRow) taskagent.FeishuTaskRow {
	if len(tasks) == 0 {
		return taskagent.FeishuTaskRow{}
	}
	for _, t := range tasks {
		if strings.TrimSpace(t.Scene) == taskagent.SceneVideoScreenCapture {
			return t
		}
	}
	return tasks[0]
}
