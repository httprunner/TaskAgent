package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
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
	// AllowEmptyStatusAsReady controls whether an empty task status should be treated as a
	// terminal state when deciding if a group is ready.
	AllowEmptyStatusAsReady bool
	// AllowErrorStatusAsReady controls whether status "error" should be treated as ready (terminal).
	// When nil, it defaults to true for backward compatibility.
	AllowErrorStatusAsReady bool
	// AllowFailedStatusBeforeToday controls whether status "failed" can be treated as ready
	// when the task's Datetime is strictly before today (local time).
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
		Int("node_index", w.nodeIndex).
		Int("node_total", w.nodeTotal).
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

	rows, err := w.store.listCandidates(ctx, fetchLimit)
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
	policy := taskReadyPolicy{
		AllowEmpty:             w.allowEmpty,
		AllowError:             w.allowError,
		AllowFailedBeforeToday: w.allowStale,
	}
	now := time.Now()
	log.Debug().
		Str("record_id", strings.TrimSpace(row.RecordID)).
		Str("biz_type", strings.TrimSpace(row.BizType)).
		Str("group_id", strings.TrimSpace(row.GroupID)).
		Int("task_id_count", len(taskIDs)).
		Int("fetched_tasks", len(tasks)).
		Interface("task_ids", taskIDs).
		Msg("webhook: fetched tasks for result row")
	if !allTasksReady(tasks, taskIDs, now, policy) {
		logTaskReadinessDebug(tasks, taskIDs, now, policy)
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

	if bizType == WebhookBizTypePiracyGeneralSearch && w.nodeTotal > 1 {
		bookID := ""
		if dramaRaw != nil {
			if v, ok := dramaRaw["DramaID"]; ok {
				bookID = strings.TrimSpace(fmt.Sprint(v))
			}
		}
		app := strings.TrimSpace(meta.App)
		if bookID != "" && app != "" && !w.shouldHandleShard(bookID, app) {
			log.Debug().
				Str("record_id", strings.TrimSpace(row.RecordID)).
				Str("group_id", strings.TrimSpace(row.GroupID)).
				Str("book_id", bookID).
				Str("app", app).
				Int("node_index", w.nodeIndex).
				Int("node_total", w.nodeTotal).
				Msg("webhook: skip piracy group due to shard mismatch")
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
		if !w.shouldHandleVideoScreenCapture(taskIDs) {
			log.Debug().
				Str("record_id", strings.TrimSpace(row.RecordID)).
				Str("group_id", strings.TrimSpace(row.GroupID)).
				Int("node_index", w.nodeIndex).
				Int("node_total", w.nodeTotal).
				Interface("task_ids", taskIDs).
				Msg("webhook: skip video screen capture row due to shard mismatch")
			return nil
		}
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
		groupUserID := strings.TrimSpace(meta.UserID)

		records, err = fetchCaptureRecordsByTaskIDs(ctx, taskIDs, groupUserID)
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

type taskReadyPolicy struct {
	AllowEmpty             bool
	AllowError             bool
	AllowFailedBeforeToday bool
}

func allTasksReady(tasks []taskagent.FeishuTaskRow, taskIDs []int64, now time.Time, policy taskReadyPolicy) bool {
	if len(taskIDs) == 0 {
		return false
	}
	byID := make(map[int64]taskagent.FeishuTaskRow, len(tasks))
	for _, t := range tasks {
		if t.TaskID == 0 {
			continue
		}
		byID[t.TaskID] = t
	}
	for _, id := range taskIDs {
		task, ok := byID[id]
		if !ok {
			return false
		}
		st := strings.ToLower(strings.TrimSpace(task.Status))
		switch st {
		case taskagent.StatusSuccess:
			continue
		case taskagent.StatusError:
			if policy.AllowError {
				continue
			}
			return false
		case taskagent.StatusFailed:
			if policy.AllowFailedBeforeToday && taskDatetimeBeforeToday(task, now) {
				continue
			}
			return false
		case "":
			if policy.AllowEmpty {
				continue
			}
			return false
		default:
			return false
		}
	}
	return true
}

func logTaskReadinessDebug(tasks []taskagent.FeishuTaskRow, taskIDs []int64, now time.Time, policy taskReadyPolicy) {
	if len(taskIDs) == 0 {
		return
	}
	byID := make(map[int64]taskagent.FeishuTaskRow, len(tasks))
	for _, t := range tasks {
		if t.TaskID == 0 {
			continue
		}
		byID[t.TaskID] = t
	}

	type taskState struct {
		TaskID   int64  `json:"task_id"`
		Status   string `json:"status"`
		Datetime string `json:"datetime,omitempty"`
		Missing  bool   `json:"missing"`
		Ready    bool   `json:"ready"`
	}

	states := make([]taskState, 0, len(taskIDs))
	for _, id := range taskIDs {
		ts := taskState{TaskID: id}
		task, ok := byID[id]
		if !ok {
			ts.Missing = true
			states = append(states, ts)
			continue
		}
		st := strings.ToLower(strings.TrimSpace(task.Status))
		ts.Status = st

		if task.Datetime != nil && !task.Datetime.IsZero() {
			ts.Datetime = task.Datetime.In(time.Local).Format(time.RFC3339)
		} else if strings.TrimSpace(task.DatetimeRaw) != "" {
			ts.Datetime = strings.TrimSpace(task.DatetimeRaw)
		}

		switch st {
		case taskagent.StatusSuccess:
			ts.Ready = true
		case taskagent.StatusError:
			ts.Ready = policy.AllowError
		case taskagent.StatusFailed:
			ts.Ready = policy.AllowFailedBeforeToday && taskDatetimeBeforeToday(task, now)
		case "":
			ts.Ready = policy.AllowEmpty
		default:
			ts.Ready = false
		}

		states = append(states, ts)
	}

	log.Debug().
		Interface("task_readiness", states).
		Msg("webhook: tasks not ready for result row")
}

func taskDatetimeBeforeToday(task taskagent.FeishuTaskRow, now time.Time) bool {
	loc := time.Local
	if !now.IsZero() {
		now = now.In(loc)
	} else {
		now = time.Now().In(loc)
	}
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)

	taskTime := task.Datetime
	if taskTime == nil || taskTime.IsZero() {
		parsed := parseFeishuDatetime(task.DatetimeRaw)
		if parsed == nil || parsed.IsZero() {
			return false
		}
		taskTime = parsed
	}
	return taskTime.In(loc).Before(todayStart)
}

func parseFeishuDatetime(raw string) *time.Time {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	if strings.EqualFold(trimmed, "today") {
		t := time.Now()
		return &t
	}
	if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		var t time.Time
		if len(trimmed) == 10 {
			t = time.Unix(n, 0)
		} else {
			t = time.UnixMilli(n)
		}
		t = t.In(time.Local)
		return &t
	}
	if parsed, err := time.ParseInLocation("2006-01-02", trimmed, time.Local); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		parsed = parsed.In(time.Local)
		return &parsed
	}
	return nil
}

func (w *WebhookResultWorker) shouldHandleShard(bookID, app string) bool {
	if w == nil || w.nodeTotal <= 1 {
		return true
	}
	bookID = strings.TrimSpace(bookID)
	app = strings.TrimSpace(app)
	if bookID == "" || app == "" {
		return true
	}
	key := bookID + "|" + app
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	shard := int(h.Sum32() % uint32(w.nodeTotal))
	return shard == w.nodeIndex
}

func (w *WebhookResultWorker) shouldHandleVideoScreenCapture(taskIDs []int64) bool {
	if w == nil || w.nodeTotal <= 1 {
		return true
	}
	var keyID int64
	for _, id := range taskIDs {
		if id > 0 {
			keyID = id
			break
		}
	}
	if keyID <= 0 {
		// Fallback to no sharding when TaskIDs are empty or invalid.
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(fmt.Sprintf("%d", keyID)))
	shard := int(h.Sum32() % uint32(w.nodeTotal))
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
