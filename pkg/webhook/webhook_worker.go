package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/pkg/singleurl"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

var webhookPlanScenesByBizType = map[string][]string{
	WebhookBizTypePiracyGeneralSearch: {
		taskagent.SceneGeneralSearch,
		taskagent.SceneProfileSearch,
		taskagent.SceneCollection,
		taskagent.SceneAnchorCapture,
	},
	WebhookBizTypeVideoScreenCapture: {taskagent.SceneVideoScreenCapture},
	WebhookBizTypeSingleURLCapture:   {taskagent.SceneSingleURLCapture},
}

type WebhookResultWorkerConfig struct {
	TaskBitableURL    string
	WebhookBitableURL string
	SummaryWebhookURL string
	PollInterval      time.Duration
	BatchLimit        int
	GroupCooldown     time.Duration
	// DatePresets optionally limits scanning to specific date presets
	// (e.g. Today/Yesterday). When empty, no Date filter is applied.
	DatePresets []string
	// TargetGroupID optionally narrows processing to a single webhook
	// result group (matching the GroupID field on the result table).
	TargetGroupID string
	// TargetDate optionally narrows processing to a specific logical
	// task day in "2006-01-02" format, matched against the Date field.
	TargetDate string

	// AllowEmptyStatusAsReady controls whether an empty task status should be treated as a
	// terminal state when deciding if a group is ready.
	AllowEmptyStatusAsReady bool
	// AllowErrorStatusAsReady controls whether status "error" should be treated as ready (terminal).
	AllowErrorStatusAsReady bool
	// AllowFailedStatusBeforeToday controls whether status "failed" can be treated as ready
	// when the plan Date is strictly before today (local time).
	AllowFailedStatusBeforeToday bool

	// NodeIndex and NodeTotal control optional sharding for piracy webhook processing.
	// When NodeTotal <= 1, sharding is disabled and all rows are processed.
	// When NodeTotal > 1, rows will be routed to a single shard based on (BookID, App) for
	// piracy_general_search groups to keep capture_results lookups co-located with the
	// node that executed general + child tasks.
	NodeIndex int
	NodeTotal int
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
	allowEmpty   bool
	allowError   bool
	allowStale   bool

	nodeIndex int
	nodeTotal int

	targetGroupID string
	targetDate    string
	datePresets   []string

	singleRun bool
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

	nodeTotal := cfg.NodeTotal
	if nodeTotal <= 0 {
		nodeTotal = 1
	}
	nodeIndex := cfg.NodeIndex
	if nodeIndex < 0 || nodeIndex >= nodeTotal {
		nodeIndex = 0
	}

	targetGroupID := strings.TrimSpace(cfg.TargetGroupID)
	targetDate := strings.TrimSpace(cfg.TargetDate)
	datePresets := make([]string, 0, len(cfg.DatePresets))
	for _, preset := range cfg.DatePresets {
		if trimmed := strings.TrimSpace(preset); trimmed != "" {
			datePresets = append(datePresets, trimmed)
		}
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
		allowEmpty:   cfg.AllowEmptyStatusAsReady,
		allowError:   cfg.AllowErrorStatusAsReady,
		allowStale:   cfg.AllowFailedStatusBeforeToday,

		nodeIndex: nodeIndex,
		nodeTotal: nodeTotal,

		targetGroupID: targetGroupID,
		targetDate:    targetDate,
		datePresets:   datePresets,
		singleRun:     targetGroupID != "" || targetDate != "",
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
		Str("filter_group_id", w.targetGroupID).
		Str("filter_date", w.targetDate).
		Int("node_index", w.nodeIndex).
		Int("node_total", w.nodeTotal).
		Msg("webhook result worker started")

	// When running in targeted debug mode (group/date filter configured),
	// execute a single scan and return instead of polling.
	if w.singleRun {
		return w.processOnce(ctx)
	}

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
	// Determine how many rows to fetch from the result table this round.
	// We intentionally fetch more than we plan to process so that "stuck"
	// rows at the top (tasks_not_ready) do not starve later ready rows.
	baseLimit := w.batchLimit
	if baseLimit <= 0 {
		baseLimit = 20
	}
	fetchLimit := baseLimit * 5
	if fetchLimit < 100 {
		fetchLimit = 100
	}
	if fetchLimit > 200 {
		fetchLimit = 200
	}

	rows, err := w.store.listCandidates(ctx, listCandidatesOptions{
		BatchLimit:  fetchLimit,
		DatePresets: w.datePresets,
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var errs []string
	processed := 0
	for _, row := range rows {
		if processed >= baseLimit {
			break
		}
		if strings.TrimSpace(row.RecordID) == "" {
			continue
		}
		// Optional filters for targeted debugging: when configured, only
		// process rows that match the desired GroupID and/or logical Date.
		if w.targetGroupID != "" && strings.TrimSpace(row.GroupID) != w.targetGroupID {
			continue
		}
		if w.targetDate != "" {
			day := ""
			if row.DateMs > 0 {
				t := time.UnixMilli(row.DateMs).In(time.Local)
				day = t.Format("2006-01-02")
			}
			if day != w.targetDate {
				continue
			}
		}
		key := webhookResultCooldownKey(row)
		if w.shouldSkip(key) {
			continue
		}
		if err := w.handleRow(ctx, row); err != nil {
			errs = append(errs, err.Error())
			w.markCooldown(key, "error")
		}
		processed++
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
		day := ""
		if row.DateMs > 0 {
			t := time.UnixMilli(row.DateMs).In(time.Local)
			day = t.Format("2006-01-02")
		}
		return fmt.Sprintf("%s|%s|%s", day, strings.TrimSpace(row.GroupID), biz)
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

func (w *WebhookResultWorker) markPlanError(ctx context.Context, row webhookResultRow, msg string) error {
	if w == nil || w.store == nil {
		return errors.New("webhook result store is nil")
	}
	if strings.TrimSpace(row.RecordID) == "" {
		return errors.New("webhook result record id is empty")
	}
	now := time.Now().UTC().UnixMilli()
	state := WebhookResultError
	trimmed := strings.TrimSpace(msg)
	if trimmed == "" {
		trimmed = "plan error"
	}
	return w.store.update(ctx, row.RecordID, webhookResultUpdate{
		Status:    &state,
		EndAtMs:   &now,
		LastError: &trimmed,
	})
}

func (w *WebhookResultWorker) fetchPlanTasks(ctx context.Context, bizType, groupID, day string) ([]taskagent.FeishuTaskRow, error) {
	if w == nil || w.taskClient == nil {
		return nil, errors.New("task client is nil")
	}
	bizType = strings.TrimSpace(bizType)
	groupID = strings.TrimSpace(groupID)
	day = strings.TrimSpace(day)
	if bizType == "" {
		return nil, errors.New("biz type is empty")
	}
	if groupID == "" {
		return nil, errors.New("group id is empty")
	}
	if day == "" {
		return nil, errors.New("day is empty")
	}
	scenes := webhookPlanScenesByBizType[bizType]
	if len(scenes) == 0 {
		return nil, errors.Errorf("unsupported biz type: %s", bizType)
	}
	fields := taskagent.DefaultTaskFields()
	groupField := strings.TrimSpace(fields.GroupID)
	sceneField := strings.TrimSpace(fields.Scene)
	dateField := strings.TrimSpace(fields.Date)
	if groupField == "" || sceneField == "" || dateField == "" {
		return nil, errors.New("task table field mapping is missing (GroupID/Scene/Date)")
	}
	dateCond := exactDateCondition(dateField, day)
	if dateCond == nil {
		return nil, errors.Errorf("invalid day: %s", day)
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions,
		taskagent.NewFeishuCondition(groupField, "is", groupID),
		dateCond,
	)
	sceneConds := make([]*taskagent.FeishuCondition, 0, len(scenes))
	for _, scene := range scenes {
		if trimmed := strings.TrimSpace(scene); trimmed != "" {
			sceneConds = append(sceneConds, taskagent.NewFeishuCondition(sceneField, "is", trimmed))
		}
	}
	if len(sceneConds) == 1 {
		filter.Conditions = append(filter.Conditions, sceneConds[0])
	} else if len(sceneConds) > 1 {
		filter.Children = append(filter.Children, taskagent.NewFeishuChildrenFilter("or", sceneConds...))
	}

	table, err := w.taskClient.FetchTaskTableWithOptions(ctx, w.taskTableURL, nil, &taskagent.FeishuTaskQueryOptions{
		Filter:     filter,
		Limit:      0,
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

	bizType := strings.TrimSpace(row.BizType)
	if bizType == "" {
		bizType = WebhookBizTypePiracyGeneralSearch
	}

	groupID := strings.TrimSpace(row.GroupID)
	if groupID == "" {
		return w.markPlanError(ctx, row, "missing GroupID")
	}
	day, err := webhookRowDayString(row)
	if err != nil {
		return w.markPlanError(ctx, row, fmt.Sprintf("invalid Date: %v", err))
	}
	tasks, err := w.fetchPlanTasks(ctx, bizType, groupID, day)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return w.markPlanError(ctx, row, fmt.Sprintf("no tasks found for biz=%s group=%s date=%s", bizType, groupID, day))
	}
	taskIDs := extractTaskIDsFromRows(tasks)
	if len(taskIDs) == 0 {
		return w.markPlanError(ctx, row, fmt.Sprintf("no valid TaskID found for biz=%s group=%s date=%s", bizType, groupID, day))
	}
	nextTaskIDsByStatus := buildTaskIDsByStatusFromTasks(tasks)
	if !equalTaskIDsByStatus(row.TaskIDsByStatus, nextTaskIDsByStatus) {
		if err := w.store.update(ctx, row.RecordID, webhookResultUpdate{
			TaskIDsByStatus: &nextTaskIDsByStatus,
		}); err != nil {
			return err
		}
	}
	log.Debug().
		Str("record_id", strings.TrimSpace(row.RecordID)).
		Str("biz_type", bizType).
		Str("group_id", groupID).
		Str("date", day).
		Int("task_id_count", len(taskIDs)).
		Int("fetched_tasks", len(tasks)).
		Interface("task_ids", taskIDs).
		Msg("webhook: fetched tasks for result row")
	policy := taskTerminalPolicy{
		AllowEmpty:             w.allowEmpty,
		AllowError:             w.allowError,
		AllowFailedBeforeToday: w.allowStale,
	}
	if !allTasksTerminal(tasks, day, time.Now(), policy) {
		logTaskTerminalDebug(tasks, day, time.Now(), policy)
		w.markCooldown(row.RecordID, "tasks_not_ready")
		return nil
	}

	nowMs := time.Now().UTC().UnixMilli()
	if row.StartAtMs == 0 {
		if err := w.store.update(ctx, row.RecordID, webhookResultUpdate{StartAtMs: &nowMs}); err != nil {
			return err
		}
	}

	meta := pickTaskMeta(tasks)
	dramaRaw, err := decodeDramaInfo(row.DramaInfo)
	if err != nil {
		return w.markFailed(ctx, row, fmt.Errorf("decode drama info failed: %v", err))
	}

	if w.nodeTotal > 1 {
		bookID, app := shardKeyForWebhookPlan(bizType, row, tasks, meta, dramaRaw)
		if bookID != "" && app != "" && !w.shouldHandleShard(bookID, app) {
			log.Debug().
				Str("record_id", strings.TrimSpace(row.RecordID)).
				Str("biz_type", bizType).
				Str("group_id", strings.TrimSpace(row.GroupID)).
				Str("book_id", bookID).
				Str("app", app).
				Int("node_index", w.nodeIndex).
				Int("node_total", w.nodeTotal).
				Interface("task_ids", taskIDs).
				Msg("webhook: skip result row due to shard mismatch")
			return nil
		}
	}

	var records []CaptureRecordPayload
	switch bizType {
	case WebhookBizTypePiracyGeneralSearch:
		records, err = w.fetchPiracyGeneralSearchRecords(ctx, meta, tasks, taskIDs)
		if err != nil {
			return w.markFailed(ctx, row, err)
		}
	case WebhookBizTypeVideoScreenCapture:
		task := pickVideoScreenCaptureTask(tasks)
		groupUserID := strings.TrimSpace(task.UserID)

		records, err = fetchCaptureRecordsByTaskIDs(ctx, taskIDs, groupUserID)
		if err != nil {
			return w.markFailed(ctx, row, err)
		}
		if len(records) == 0 {
			return w.markFailed(ctx, row, ErrNoCaptureRecords)
		}
	case WebhookBizTypeSingleURLCapture:
		records = buildSingleURLCaptureRecordsFromTasks(tasks, taskIDs)
		if len(records) == 0 {
			return w.markFailed(ctx, row, ErrNoCaptureRecords)
		}
	default:
		return w.markFailed(ctx, row, fmt.Errorf("unknown biz type: %s", bizType))
	}

	// For single_url_capture webhook groups, also emit a grouped summary
	// to the downloader service so external consumers can track per-group
	// completion alongside the summary webhook payload.
	if bizType == WebhookBizTypeSingleURLCapture {
		if summary := buildSingleURLGroupSummary(row, tasks, taskIDs); summary != nil {
			if err := sendSingleURLGroupSummary(ctx, summary); err != nil {
				return w.markFailed(ctx, row, err)
			}
		}
	}

	if bizType == WebhookBizTypePiracyGeneralSearch {
		next := computeGroupTotalRatioString(records, dramaRaw)
		if shouldUpdateDramaInfoRatio(dramaRaw, DramaInfoKeyGroupTotalRatio, next) {
			if dramaRaw == nil {
				dramaRaw = make(map[string]any)
			}
			dramaRaw[DramaInfoKeyGroupTotalRatio] = next
			if merged, err := json.Marshal(dramaRaw); err != nil {
				log.Warn().Err(err).Str("record_id", strings.TrimSpace(row.RecordID)).Msg("webhook: marshal DramaInfo with ratio failed")
			} else {
				mergedStr := strings.TrimSpace(string(merged))
				if mergedStr == "" {
					mergedStr = "{}"
				}
				if err := w.store.update(ctx, row.RecordID, webhookResultUpdate{DramaInfo: ptrString(mergedStr)}); err != nil {
					return err
				}
			}
		}
	}

	payload := buildWebhookResultPayload(dramaRaw, records)
	if val, ok := payload["DramaName"]; !ok || strings.TrimSpace(fmt.Sprint(val)) == "" {
		payload["DramaName"] = strings.TrimSpace(meta.Params)
	}

	fields := taskagent.DefaultResultFields()
	userAlias := pickFirstNonEmptyCaptureFieldByTaskIDs(
		records, taskIDs, strings.TrimSpace(fields.TaskID),
		"UserAlias", strings.TrimSpace(fields.UserAlias))
	userAuthEntity := pickFirstNonEmptyCaptureFieldByTaskIDs(
		records, taskIDs, strings.TrimSpace(fields.TaskID),
		"UserAuthEntity", strings.TrimSpace(fields.UserAuthEntity))
	userInfo := map[string]any{
		"UserID":         meta.UserID,
		"UserName":       meta.UserName,
		"UserAlias":      userAlias,
		"UserAuthEntity": userAuthEntity,
	}
	payload["UserInfo"] = userInfo

	recordsPayloadJSON, _ := json.Marshal(buildTaskItemsByTaskID(records, taskIDs))
	userInfoJSON, _ := json.Marshal(userInfo)
	recordsStr := strings.TrimSpace(string(recordsPayloadJSON))
	if recordsStr == "" {
		recordsStr = "{}"
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
	for key, val := range dramaRaw {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		if _, exists := payload[trimmed]; exists {
			continue
		}
		payload[trimmed] = val
	}
	flat, _ := FlattenRecordsAndCollectItemIDs(records, taskagent.DefaultResultFields())
	payload["records"] = flat
	return payload
}

type taskItemsPayload struct {
	Total int      `json:"total"`
	Items []string `json:"items"`
}

func buildTaskItemsByTaskID(records []CaptureRecordPayload, taskIDs []int64) map[string]taskItemsPayload {
	result := make(map[string]taskItemsPayload)
	seen := make(map[string]map[string]struct{})

	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		key := fmt.Sprintf("%d", id)
		result[key] = taskItemsPayload{Total: 0, Items: []string{}}
		seen[key] = make(map[string]struct{})
	}

	fields := taskagent.DefaultResultFields()
	taskKey := strings.TrimSpace(fields.TaskID)
	itemKey := strings.TrimSpace(fields.ItemID)
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

		if _, ok := result[taskID]; !ok {
			result[taskID] = taskItemsPayload{Total: 0, Items: []string{}}
		}
		if _, ok := seen[taskID]; !ok {
			seen[taskID] = make(map[string]struct{})
		}
		if _, ok := seen[taskID][itemID]; ok {
			continue
		}
		seen[taskID][itemID] = struct{}{}

		group := result[taskID]
		group.Items = append(group.Items, itemID)
		result[taskID] = group
	}

	for taskID, group := range result {
		sort.Strings(group.Items)
		group.Total = len(group.Items)
		result[taskID] = group
	}
	return result
}

// singleURLGroupSummary captures the aggregated state for a single
// single_url_capture webhook group, which is forwarded to the downloader
// service's /download/tasks/finish endpoint.
type singleURLGroupSummary struct {
	TaskID             string
	Total              int
	Done               int
	UniqueCombinations []singleurl.TaskSummaryCombination
}

func buildSingleURLGroupSummary(row webhookResultRow, tasks []taskagent.FeishuTaskRow, taskIDs []int64) *singleURLGroupSummary {
	groupID := strings.TrimSpace(row.GroupID)
	if groupID == "" || len(taskIDs) == 0 || len(tasks) == 0 {
		return nil
	}

	byID := make(map[int64]taskagent.FeishuTaskRow, len(tasks))
	for _, t := range tasks {
		if t.TaskID <= 0 {
			continue
		}
		byID[t.TaskID] = t
	}

	total := 0
	done := 0
	seenCombo := make(map[string]struct{})
	combinations := make([]singleurl.TaskSummaryCombination, 0, len(taskIDs))

	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		task, ok := byID[id]
		if !ok {
			continue
		}
		total++

		status := strings.ToLower(strings.TrimSpace(task.Status))
		if status == taskagent.StatusSuccess {
			done++
		}

		bid := strings.TrimSpace(task.BookID)
		accountID := strings.TrimSpace(task.UserID)
		if bid == "" || accountID == "" {
			continue
		}
		key := bid + "|" + accountID
		if _, exists := seenCombo[key]; exists {
			continue
		}
		seenCombo[key] = struct{}{}
		combinations = append(combinations, singleurl.TaskSummaryCombination{
			Bid:       bid,
			AccountID: accountID,
		})
	}

	if total == 0 {
		return nil
	}

	return &singleURLGroupSummary{
		TaskID:             groupID,
		Total:              total,
		Done:               done,
		UniqueCombinations: combinations,
	}
}

func sendSingleURLGroupSummary(ctx context.Context, summary *singleURLGroupSummary) error {
	if summary == nil {
		return nil
	}

	baseURL := strings.TrimSpace(taskagent.EnvString("CRAWLER_SERVICE_BASE_URL", ""))
	if baseURL == "" {
		// Downloader integration is optional; skip when base URL is not configured.
		return nil
	}

	groupID := strings.TrimSpace(summary.TaskID)
	taskName := strings.TrimSpace(groupID)
	if taskName != "" {
		taskName = fmt.Sprintf("%s_%s", taskName, time.Now().Format("20060102_150405"))
	}

	payload := singleurl.TaskSummaryPayload{
		Status:             "finished",
		Total:              summary.Total,
		Done:               summary.Done,
		UniqueCombinations: summary.UniqueCombinations,
		TaskName:           taskName,
		Email:              env.String("SUMMARY_WEBHOOK_EMAIL", ""),
	}
	if err := singleurl.SendTaskSummaryToCrawler(ctx, baseURL, payload); err != nil {
		return err
	}

	log.Info().
		Str("task_name", taskName).
		Int("total", summary.Total).
		Int("done", summary.Done).
		Int("unique_count", len(summary.UniqueCombinations)).
		Msg("single_url_capture group summary sent to crawler")
	return nil
}

// buildSingleURLCaptureRecordsFromTasks builds synthetic capture records for single_url_capture
// webhook payloads directly from task rows, without relying on SQLite or the capture result table.
// Each ready task in taskIDs contributes at most one record.
func buildSingleURLCaptureRecordsFromTasks(tasks []taskagent.FeishuTaskRow, taskIDs []int64) []CaptureRecordPayload {
	if len(tasks) == 0 || len(taskIDs) == 0 {
		return nil
	}
	allowed := make(map[int64]struct{}, len(taskIDs))
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		allowed[id] = struct{}{}
	}
	fields := taskagent.DefaultResultFields()

	out := make([]CaptureRecordPayload, 0, len(taskIDs))
	for _, t := range tasks {
		if t.TaskID <= 0 {
			continue
		}
		if _, ok := allowed[t.TaskID]; !ok {
			continue
		}
		status := strings.ToLower(strings.TrimSpace(t.Status))
		if status != taskagent.StatusSuccess && status != taskagent.StatusError {
			continue
		}

		recordFields := make(map[string]any)
		if key := strings.TrimSpace(fields.TaskID); key != "" {
			recordFields[key] = fmt.Sprintf("%d", t.TaskID)
		}
		if key := strings.TrimSpace(fields.App); key != "" && strings.TrimSpace(t.App) != "" {
			recordFields[key] = strings.TrimSpace(t.App)
		}
		if key := strings.TrimSpace(fields.Scene); key != "" && strings.TrimSpace(t.Scene) != "" {
			recordFields[key] = strings.TrimSpace(t.Scene)
		}
		if key := strings.TrimSpace(fields.Params); key != "" && strings.TrimSpace(t.Params) != "" {
			recordFields[key] = strings.TrimSpace(t.Params)
		}
		if key := strings.TrimSpace(fields.ItemURL); key != "" && strings.TrimSpace(t.URL) != "" {
			recordFields[key] = strings.TrimSpace(t.URL)
		}
		if key := strings.TrimSpace(fields.UserID); key != "" && strings.TrimSpace(t.UserID) != "" {
			recordFields[key] = strings.TrimSpace(t.UserID)
		}
		if key := strings.TrimSpace(fields.UserName); key != "" && strings.TrimSpace(t.UserName) != "" {
			recordFields[key] = strings.TrimSpace(t.UserName)
		}
		if key := strings.TrimSpace(fields.Extra); key != "" && strings.TrimSpace(t.Logs) != "" {
			recordFields[key] = strings.TrimSpace(t.Logs)
		}
		// Use BookID or TaskID as a stable synthetic ItemID so downstream
		// payloads can still leverage ItemID-based grouping when needed.
		if key := strings.TrimSpace(fields.ItemID); key != "" {
			val := strings.TrimSpace(t.BookID)
			if val == "" {
				val = fmt.Sprintf("%d", t.TaskID)
			}
			recordFields[key] = val
		}

		out = append(out, CaptureRecordPayload{
			RecordID: fmt.Sprintf("%d", t.TaskID),
			Fields:   recordFields,
		})
	}
	return out
}

func pickFirstNonEmptyCaptureFieldByTaskIDs(
	records []CaptureRecordPayload, taskIDs []int64, taskIDFieldRaw, fieldEng, fieldRaw string) string {
	if len(records) == 0 || len(taskIDs) == 0 {
		return ""
	}
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		taskIDStr := fmt.Sprintf("%d", id)
		for _, rec := range records {
			if rec.Fields == nil {
				continue
			}
			recTaskID := strings.TrimSpace(getString(rec.Fields, "TaskID"))
			if recTaskID == "" && strings.TrimSpace(taskIDFieldRaw) != "" {
				recTaskID = strings.TrimSpace(getString(rec.Fields, taskIDFieldRaw))
			}
			if recTaskID != taskIDStr {
				continue
			}
			val := strings.TrimSpace(getString(rec.Fields, fieldEng))
			if val == "" && strings.TrimSpace(fieldRaw) != "" {
				val = strings.TrimSpace(getString(rec.Fields, fieldRaw))
			}
			if strings.EqualFold(val, "null") {
				val = ""
			}
			if val != "" {
				return val
			}
		}
	}
	return ""
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

func shardKeyForWebhookPlan(
	bizType string,
	row webhookResultRow,
	tasks []taskagent.FeishuTaskRow,
	meta taskMeta,
	dramaRaw map[string]any,
) (bookID, app string) {
	for _, t := range tasks {
		if app == "" {
			app = strings.TrimSpace(t.App)
		}
		if bookID == "" {
			bookID = strings.TrimSpace(t.BookID)
		}
		if app != "" && bookID != "" {
			return bookID, app
		}
	}
	if app == "" {
		app = strings.TrimSpace(meta.App)
	}

	if bookID == "" && dramaRaw != nil {
		if v, ok := dramaRaw["DramaID"]; ok {
			bookID = strings.TrimSpace(fmt.Sprint(v))
		}
	}

	if app != "" && bookID != "" {
		return bookID, app
	}
	// Fallback to GroupID parsing: "{App}_{BookID}_{UserID}".
	trimmed := strings.TrimSpace(row.GroupID)
	if trimmed == "" {
		return bookID, app
	}
	parts := strings.Split(trimmed, "_")
	if len(parts) >= 2 {
		if app == "" {
			app = strings.TrimSpace(parts[0])
		}
		if bookID == "" {
			bookID = strings.TrimSpace(parts[1])
		}
	}
	_ = bizType
	return bookID, app
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

func webhookRowDayString(row webhookResultRow) (string, error) {
	if row.DateMs <= 0 {
		return "", errors.New("missing Date")
	}
	t := time.UnixMilli(row.DateMs).In(time.Local)
	return t.Format("2006-01-02"), nil
}

func extractTaskIDsFromRows(tasks []taskagent.FeishuTaskRow) []int64 {
	if len(tasks) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(tasks))
	for _, t := range tasks {
		if t.TaskID > 0 {
			ids = append(ids, t.TaskID)
		}
	}
	ids = uniqueInt64(ids)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func buildTaskIDsByStatusFromTasks(tasks []taskagent.FeishuTaskRow) map[string][]int64 {
	if len(tasks) == 0 {
		return nil
	}
	out := make(map[string][]int64)
	for _, t := range tasks {
		if t.TaskID <= 0 {
			continue
		}
		status := normalizeTaskIDsStatusKey(t.Status)
		out[status] = append(out[status], t.TaskID)
	}
	return normalizeTaskIDsByStatus(out)
}

type taskTerminalPolicy struct {
	AllowEmpty             bool
	AllowError             bool
	AllowFailedBeforeToday bool
}

func allTasksTerminal(tasks []taskagent.FeishuTaskRow, day string, now time.Time, policy taskTerminalPolicy) bool {
	if len(tasks) == 0 {
		return false
	}
	for _, t := range tasks {
		if !taskIsTerminal(t, day, now, policy) {
			return false
		}
	}
	return true
}

func taskIsTerminal(task taskagent.FeishuTaskRow, day string, now time.Time, policy taskTerminalPolicy) bool {
	st := strings.ToLower(strings.TrimSpace(task.Status))
	switch st {
	case taskagent.StatusSuccess:
		return true
	case taskagent.StatusError:
		return policy.AllowError
	case taskagent.StatusFailed:
		return policy.AllowFailedBeforeToday && dayBeforeToday(day, now)
	case "":
		return policy.AllowEmpty
	default:
		return false
	}
}

func dayBeforeToday(day string, now time.Time) bool {
	trimmed := strings.TrimSpace(day)
	if trimmed == "" {
		return false
	}
	loc := time.Local
	if !now.IsZero() {
		now = now.In(loc)
	} else {
		now = time.Now().In(loc)
	}
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	dayTime, err := time.ParseInLocation("2006-01-02", trimmed, loc)
	if err != nil {
		return false
	}
	return dayTime.Before(todayStart)
}

func logTaskTerminalDebug(tasks []taskagent.FeishuTaskRow, day string, now time.Time, policy taskTerminalPolicy) {
	if len(tasks) == 0 {
		return
	}
	type taskState struct {
		TaskID   int64  `json:"task_id"`
		Status   string `json:"status"`
		Datetime string `json:"datetime,omitempty"`
		Terminal bool   `json:"terminal"`
	}
	states := make([]taskState, 0, len(tasks))
	for _, task := range tasks {
		if task.TaskID <= 0 {
			continue
		}
		st := strings.ToLower(strings.TrimSpace(task.Status))
		ts := taskState{
			TaskID:   task.TaskID,
			Status:   st,
			Terminal: taskIsTerminal(task, day, now, policy),
		}
		if task.Datetime != nil && !task.Datetime.IsZero() {
			ts.Datetime = task.Datetime.In(time.Local).Format(time.RFC3339)
		} else if strings.TrimSpace(task.DatetimeRaw) != "" {
			ts.Datetime = strings.TrimSpace(task.DatetimeRaw)
		}
		states = append(states, ts)
	}
	log.Debug().Interface("task_states", states).Msg("webhook: tasks not terminal for result row")
}

func (w *WebhookResultWorker) shouldHandleShard(bookID, app string) bool {
	if w == nil || w.nodeTotal <= 1 {
		return true
	}
	row := taskagent.FeishuTaskRow{BookID: bookID, App: app}
	shardKey := taskagent.ResolveShardIDForTaskRow(row)
	shard := int(shardKey % int64(w.nodeTotal))
	return shard == w.nodeIndex
}

func (w *WebhookResultWorker) fetchPiracyGeneralSearchRecords(
	ctx context.Context, meta taskMeta,
	_ []taskagent.FeishuTaskRow, taskIDs []int64,
) ([]CaptureRecordPayload, error) {
	userID := strings.TrimSpace(meta.UserID)
	return fetchCaptureRecordsByTaskIDs(ctx, taskIDs, userID)
}

// filterRecordsByTaskAndUser narrows capture records using TaskID and UserID
// from the capture rows themselves, independent of the task table. This keeps
// webhook workers resilient when task rows are pruned while SQLite still
// holds valid capture history.
func filterRecordsByTaskAndUser(
	records []CaptureRecordPayload,
	taskIDs []int64,
	userID string,
) []CaptureRecordPayload {
	if len(records) == 0 {
		return records
	}

	allowedTaskIDs := make(map[string]struct{}, len(taskIDs))
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		allowedTaskIDs[fmt.Sprintf("%d", id)] = struct{}{}
	}

	trimmedUserID := strings.TrimSpace(userID)
	resultFields := taskagent.DefaultResultFields()
	rawTaskIDField := strings.TrimSpace(resultFields.TaskID)
	rawUserIDField := strings.TrimSpace(resultFields.UserID)

	seenRecord := make(map[string]struct{}, len(records))
	filtered := make([]CaptureRecordPayload, 0, len(records))

	for _, rec := range records {
		// Deduplicate by record ID when present.
		recordID := strings.TrimSpace(rec.RecordID)
		if recordID == "" {
			recordID = strings.TrimSpace(fmt.Sprint(rec.Fields["id"]))
		}
		if recordID != "" {
			if _, ok := seenRecord[recordID]; ok {
				continue
			}
			seenRecord[recordID] = struct{}{}
		}

		// Resolve TaskID from capture fields.
		taskIDStr := strings.TrimSpace(getString(rec.Fields, "TaskID"))
		if taskIDStr == "" && rawTaskIDField != "" {
			taskIDStr = strings.TrimSpace(getString(rec.Fields, rawTaskIDField))
		}
		if taskIDStr == "" {
			continue
		}
		if len(allowedTaskIDs) > 0 {
			if _, ok := allowedTaskIDs[taskIDStr]; !ok {
				continue
			}
		}

		// Optionally enforce UserID equality from capture fields.
		if trimmedUserID != "" {
			recUserID := strings.TrimSpace(getString(rec.Fields, "UserID"))
			if recUserID == "" && rawUserIDField != "" {
				recUserID = strings.TrimSpace(getString(rec.Fields, rawUserIDField))
			}
			if recUserID == "" || !strings.EqualFold(recUserID, trimmedUserID) {
				continue
			}
		}

		filtered = append(filtered, rec)
	}

	return filtered
}

func shouldUpdateDramaInfoRatio(dramaRaw map[string]any, key string, next string) bool {
	if strings.TrimSpace(key) == "" {
		return false
	}
	if strings.TrimSpace(next) == "" {
		return false
	}
	if dramaRaw == nil {
		return true
	}
	cur := strings.TrimSpace(fmt.Sprint(dramaRaw[key]))
	if cur == "" {
		return true
	}
	return cur != strings.TrimSpace(next)
}

func computeGroupTotalRatioString(records []CaptureRecordPayload, dramaRaw map[string]any) string {
	totalDur, ok := parseDramaTotalDurationSeconds(dramaRaw)
	if !ok || totalDur <= 0 {
		return DramaInfoRatioNA
	}
	sum, ok := sumUniqueItemDurations(records)
	if !ok || sum <= 0 {
		return DramaInfoRatioNA
	}
	return fmt.Sprintf("%.2f%%", (sum/totalDur)*100)
}

func parseDramaTotalDurationSeconds(dramaRaw map[string]any) (float64, bool) {
	if dramaRaw == nil {
		return 0, false
	}
	raw := strings.TrimSpace(fmt.Sprint(dramaRaw["TotalDuration"]))
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return v, v > 0
}

func sumUniqueItemDurations(records []CaptureRecordPayload) (float64, bool) {
	if len(records) == 0 {
		return 0, false
	}
	fields := taskagent.DefaultResultFields()
	fieldMap := taskagent.StructFieldMap(fields)
	rawItemID := strings.TrimSpace(fieldMap["ItemID"])
	rawDuration := strings.TrimSpace(fieldMap["ItemDuration"])
	seen := make(map[string]struct{}, len(records))
	sum := 0.0
	count := 0
	for idx, rec := range records {
		if rec.Fields == nil {
			continue
		}
		itemID := strings.TrimSpace(getString(rec.Fields, "ItemID"))
		if itemID == "" && rawItemID != "" {
			itemID = strings.TrimSpace(getString(rec.Fields, rawItemID))
		}
		key := itemID
		if key == "" {
			key = strings.TrimSpace(rec.RecordID)
		}
		if key == "" {
			key = fmt.Sprintf("idx-%d", idx)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		durStr := strings.TrimSpace(getString(rec.Fields, "ItemDuration"))
		if durStr == "" && rawDuration != "" {
			durStr = strings.TrimSpace(getString(rec.Fields, rawDuration))
		}
		if durStr == "" {
			continue
		}
		dur, err := strconv.ParseFloat(durStr, 64)
		if err != nil || dur <= 0 {
			continue
		}
		sum += dur
		count++
	}
	return sum, count > 0
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
