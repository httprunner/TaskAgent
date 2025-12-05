package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	crawlerServiceBaseURLEnv     = "CRAWLER_SERVICE_BASE_URL"
	defaultCrawlerServiceBaseURL = "http://localhost:8000"
	singleURLStatusQueued        = "queued"
	singleURLGroupFetchLimit     = 200
)

type singleURLMetadata struct {
	JobID string `json:"job_id,omitempty"`
	VID   string `json:"vid,omitempty"`
	Error string `json:"error,omitempty"`
}

func decodeSingleURLMetadata(raw string) singleURLMetadata {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return singleURLMetadata{}
	}
	var meta singleURLMetadata
	if err := json.Unmarshal([]byte(trimmed), &meta); err != nil {
		return singleURLMetadata{}
	}
	return meta
}

func encodeSingleURLMetadata(meta singleURLMetadata) string {
	buf, err := json.Marshal(meta)
	if err != nil {
		return ""
	}
	return string(buf)
}

// SingleURLWorkerConfig captures the dependencies required to process
// "单个链接采集" (single URL) tasks independently from device runners.
type SingleURLWorkerConfig struct {
	Client         targetTableClient
	BitableURL     string
	Limit          int
	PollInterval   time.Duration
	Clock          func() time.Time
	CrawlerClient  crawlerTaskClient
	CookieProvider CookieProvider
}

// SingleURLWorker pulls single-URL capture tasks and dispatches them via
// API stubs without using physical devices.
type SingleURLWorker struct {
	client         targetTableClient
	bitableURL     string
	limit          int
	pollInterval   time.Duration
	clock          func() time.Time
	crawler        crawlerTaskClient
	cookieProvider CookieProvider
}

// NewSingleURLWorker builds a worker using the provided configuration.
func NewSingleURLWorker(cfg SingleURLWorkerConfig) (*SingleURLWorker, error) {
	client := cfg.Client
	if client == nil {
		return nil, errors.New("single url worker: client is nil")
	}
	crawler := cfg.CrawlerClient
	if crawler == nil {
		return nil, errors.New("single url worker: crawler client is nil")
	}
	bitableURL := strings.TrimSpace(cfg.BitableURL)
	if bitableURL == "" {
		return nil, errors.New("single url worker: bitable url is empty")
	}
	limit := cfg.Limit
	if limit <= 0 {
		limit = maxFeishuTasksPerApp
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = 30 * time.Second
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &SingleURLWorker{
		client:         client,
		bitableURL:     bitableURL,
		limit:          limit,
		pollInterval:   poll,
		clock:          clock,
		crawler:        crawler,
		cookieProvider: cfg.CookieProvider,
	}, nil
}

// NewSingleURLWorkerFromEnv builds a worker using Feishu credentials from env.
func NewSingleURLWorkerFromEnv(bitableURL string, limit int, pollInterval time.Duration) (*SingleURLWorker, error) {
	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	baseURL := strings.TrimSpace(os.Getenv(crawlerServiceBaseURLEnv))
	if baseURL == "" {
		baseURL = defaultCrawlerServiceBaseURL
	}
	crawler, err := newRESTCrawlerTaskClient(baseURL, nil)
	if err != nil {
		return nil, err
	}
	var cookieProvider CookieProvider
	cookieTableURL := strings.TrimSpace(os.Getenv(feishu.EnvCookieBitableURL))
	if cookieTableURL != "" {
		provider, err := NewCookieProvider(client, cookieTableURL, defaultCookiePlatform, 0)
		if err != nil {
			return nil, err
		}
		cookieProvider = provider
	}
	return NewSingleURLWorker(SingleURLWorkerConfig{
		Client:         client,
		BitableURL:     bitableURL,
		Limit:          limit,
		PollInterval:   pollInterval,
		CrawlerClient:  crawler,
		CookieProvider: cookieProvider,
	})
}

// Run starts the polling loop until ctx is cancelled.
func (w *SingleURLWorker) Run(ctx context.Context) error {
	if w == nil {
		return errors.New("single url worker: nil instance")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.ProcessOnce(ctx); err != nil {
				log.Error().Err(err).Msg("single url worker pass failed")
			}
		}
	}
}

// ProcessOnce executes a single fetch-and-dispatch cycle.
func (w *SingleURLWorker) ProcessOnce(ctx context.Context) error {
	if w == nil {
		return errors.New("single url worker: nil instance")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	newTasks, err := w.fetchSingleURLTasks(ctx, []string{feishu.StatusPending, feishu.StatusFailed}, w.limit)
	if err != nil {
		return err
	}
	for _, task := range newTasks {
		if err := w.handleSingleURLTask(ctx, task); err != nil {
			log.Error().Err(err).
				Int64("task_id", task.TaskID).
				Msg("single url worker dispatch failed")
		}
	}
	activeTasks, err := w.fetchSingleURLTasks(ctx, []string{singleURLStatusQueued, feishu.StatusRunning}, w.limit)
	if err != nil {
		return err
	}
	for _, task := range activeTasks {
		if err := w.reconcileSingleURLTask(ctx, task); err != nil {
			log.Error().Err(err).
				Int64("task_id", task.TaskID).
				Msg("single url worker polling failed")
		}
	}
	return nil
}

func (w *SingleURLWorker) fetchSingleURLTasks(ctx context.Context, statuses []string, limit int) ([]*FeishuTask, error) {
	if limit <= 0 {
		limit = w.limit
	}
	if limit <= 0 {
		limit = maxFeishuTasksPerApp
	}
	result := make([]*FeishuTask, 0, limit)
	seen := make(map[int64]struct{}, limit)
	fields := feishu.DefaultTaskFields
	for idx, status := range statuses {
		if limit > 0 && len(result) >= limit {
			break
		}
		remaining := limit
		if remaining > 0 {
			remaining -= len(result)
			if remaining <= 0 {
				break
			}
		}
		subset, err := fetchFeishuTasksWithStrategy(ctx, w.client, w.bitableURL, fields, "", []string{status}, remaining, SceneSingleURLCapture, idx)
		if err != nil {
			log.Warn().Err(err).
				Str("status", status).
				Msg("single url worker fetch failed")
			continue
		}
		result = appendUniqueFeishuTasks(result, subset, limit, seen)
	}
	return result, nil
}

func (w *SingleURLWorker) handleSingleURLTask(ctx context.Context, task *FeishuTask) error {
	if task == nil {
		return errors.New("single url worker: nil task")
	}
	bookID := strings.TrimSpace(task.BookID)
	userID := strings.TrimSpace(task.UserID)
	url := strings.TrimSpace(task.URL)
	missingFields := make([]string, 0, 3)
	if bookID == "" {
		missingFields = append(missingFields, "BookID")
	}
	if userID == "" {
		missingFields = append(missingFields, "UserID")
	}
	if url == "" {
		missingFields = append(missingFields, "URL")
	}
	if len(missingFields) > 0 {
		reason := fmt.Sprintf("missing fields: %s", strings.Join(missingFields, ","))
		return w.failSingleURLTask(ctx, task, reason, nil)
	}
	meta := decodeSingleURLMetadata(task.Extra)
	if meta.JobID != "" {
		log.Info().
			Int64("task_id", task.TaskID).
			Str("job_id", meta.JobID).
			Msg("single url task already has job id; skip creation")
		return nil
	}
	cookies := w.collectCookies(ctx)
	metaPayload := make(map[string]string, 3)
	if platform := strings.TrimSpace(task.App); platform != "" {
		metaPayload["platform"] = platform
	}
	if bookID != "" {
		metaPayload["bid"] = bookID
	}
	if userID != "" {
		metaPayload["uid"] = userID
	}
	jobID, err := w.crawler.CreateTask(ctx, url, cookies, metaPayload)
	if err != nil {
		return w.failSingleURLTask(ctx, task, fmt.Sprintf("create job failed: %v", err), nil)
	}
	meta.JobID = jobID
	meta.VID = ""
	meta.Error = ""
	groupID := fmt.Sprintf("%s_%s", bookID, userID)
	if err := w.markSingleURLTaskQueued(ctx, task, groupID, meta); err != nil {
		return err
	}
	log.Info().
		Int64("task_id", task.TaskID).
		Str("job_id", jobID).
		Str("book_id", bookID).
		Str("user_id", userID).
		Str("url", url).
		Msg("single url capture job queued")
	return nil
}

func (w *SingleURLWorker) reconcileSingleURLTask(ctx context.Context, task *FeishuTask) error {
	if task == nil {
		return errors.New("single url worker: nil task")
	}
	meta := decodeSingleURLMetadata(task.Extra)
	if meta.JobID == "" {
		return w.failSingleURLTask(ctx, task, "missing job_id for queued task", nil)
	}
	status, err := w.crawler.GetTask(ctx, meta.JobID)
	if err != nil {
		if errors.Is(err, errCrawlerJobNotFound) {
			return w.failSingleURLTask(ctx, task, "crawler job not found", &meta)
		}
		return err
	}
	switch strings.ToLower(strings.TrimSpace(status.Status)) {
	case "", singleURLStatusQueued:
		if task.Status != singleURLStatusQueued {
			return w.markSingleURLTaskQueued(ctx, task, task.GroupID, meta)
		}
		return nil
	case "running":
		return updateFeishuTaskStatuses(ctx, []*FeishuTask{task}, feishu.StatusRunning, "", nil)
	case "done":
		meta.VID = status.VID
		meta.Error = ""
		if err := w.updateTaskExtra(ctx, task, meta); err != nil {
			return err
		}
		completed := w.clock()
		if err := updateFeishuTaskStatuses(ctx, []*FeishuTask{task}, feishu.StatusSuccess, "", &taskStatusMeta{completedAt: &completed}); err != nil {
			return err
		}
		task.Status = feishu.StatusSuccess
		if err := w.maybeSendGroupSummary(ctx, task); err != nil {
			log.Error().
				Err(err).
				Str("group_id", strings.TrimSpace(task.GroupID)).
				Int64("task_id", task.TaskID).
				Msg("single url worker: send group summary failed")
		}
		return nil
	case "failed":
		reason := strings.TrimSpace(status.Error)
		if reason == "" {
			reason = "crawler job failed"
		}
		meta.Error = reason
		return w.failSingleURLTask(ctx, task, reason, &meta)
	default:
		log.Warn().
			Int64("task_id", task.TaskID).
			Str("job_id", meta.JobID).
			Str("status", status.Status).
			Msg("single url worker: unknown crawler status")
		return nil
	}
}

func (w *SingleURLWorker) updateTaskExtra(ctx context.Context, task *FeishuTask, meta singleURLMetadata) error {
	src := task.source
	if src == nil || src.client == nil || src.table == nil {
		return errors.New("single url worker: task missing source")
	}
	extraField := strings.TrimSpace(src.table.Fields.Extra)
	if extraField == "" {
		return nil
	}
	encoded := encodeSingleURLMetadata(meta)
	payload := map[string]any{extraField: encoded}
	if err := src.client.UpdateTaskFields(ctx, src.table, task.TaskID, payload); err != nil {
		return err
	}
	task.Extra = encoded
	return nil
}

func (w *SingleURLWorker) failSingleURLTask(ctx context.Context, task *FeishuTask, reason string, meta *singleURLMetadata) error {
	src := task.source
	if src == nil || src.client == nil || src.table == nil {
		return errors.New("single url worker: task missing source")
	}
	fields := map[string]any{}
	statusField := strings.TrimSpace(src.table.Fields.Status)
	if statusField != "" {
		fields[statusField] = feishu.StatusFailed
	}
	if extraField := strings.TrimSpace(src.table.Fields.Extra); extraField != "" {
		if meta != nil {
			if strings.TrimSpace(reason) != "" {
				meta.Error = reason
			}
			fields[extraField] = encodeSingleURLMetadata(*meta)
		} else {
			fields[extraField] = reason
		}
	}
	if len(fields) == 0 {
		return errors.New("single url worker: no fields to update for failure")
	}
	if err := src.client.UpdateTaskFields(ctx, src.table, task.TaskID, fields); err != nil {
		return err
	}
	task.Status = feishu.StatusFailed
	if extra, ok := fields[strings.TrimSpace(src.table.Fields.Extra)].(string); ok {
		task.Extra = extra
	} else {
		task.Extra = reason
	}
	return nil
}

func (w *SingleURLWorker) markSingleURLTaskQueued(ctx context.Context, task *FeishuTask, groupID string, meta singleURLMetadata) error {
	src := task.source
	if src == nil || src.client == nil || src.table == nil {
		return errors.New("single url worker: task missing source")
	}
	now := w.clock()
	nowMillis := now.UTC().UnixMilli()
	fields := map[string]any{}
	if statusField := strings.TrimSpace(src.table.Fields.Status); statusField != "" {
		fields[statusField] = singleURLStatusQueued
	}
	if groupField := strings.TrimSpace(src.table.Fields.GroupID); groupField != "" && strings.TrimSpace(groupID) != "" {
		fields[groupField] = groupID
	}
	if extraField := strings.TrimSpace(src.table.Fields.Extra); extraField != "" {
		fields[extraField] = encodeSingleURLMetadata(meta)
	}
	if dispatchedField := strings.TrimSpace(src.table.Fields.DispatchedAt); dispatchedField != "" {
		fields[dispatchedField] = nowMillis
	}
	if startField := strings.TrimSpace(src.table.Fields.StartAt); startField != "" {
		fields[startField] = nowMillis
	}
	if len(fields) == 0 {
		return errors.New("single url worker: no fields to update for queued state")
	}
	if err := src.client.UpdateTaskFields(ctx, src.table, task.TaskID, fields); err != nil {
		return err
	}
	task.Status = singleURLStatusQueued
	if strings.TrimSpace(groupID) != "" {
		task.GroupID = groupID
	}
	if extra, ok := fields[strings.TrimSpace(src.table.Fields.Extra)].(string); ok {
		task.Extra = extra
	}
	updateTimestampFields(task, now, nowMillis)
	return nil
}

func (w *SingleURLWorker) collectCookies(ctx context.Context) []string {
	if w == nil || w.cookieProvider == nil {
		return nil
	}
	rec, err := w.cookieProvider.PickCookie(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("single url worker: pick cookie failed")
		return nil
	}
	if rec == nil || strings.TrimSpace(rec.Value) == "" {
		return nil
	}
	return []string{rec.Value}
}

func updateTimestampFields(task *FeishuTask, ts time.Time, millis int64) {
	if task == nil {
		return
	}
	if millis > 0 {
		raw := strconv.FormatInt(millis, 10)
		task.DispatchedAtRaw = raw
		task.StartAtRaw = raw
	}
	if !ts.IsZero() {
		copy := ts
		task.DispatchedAt = &copy
		task.StartAt = &copy
	}
}

func (w *SingleURLWorker) maybeSendGroupSummary(ctx context.Context, task *FeishuTask) error {
	if w == nil || task == nil || w.crawler == nil {
		return nil
	}
	groupID := strings.TrimSpace(task.GroupID)
	if groupID == "" {
		return nil
	}
	tasks, err := w.fetchSingleURLGroupTasks(ctx, groupID)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	total := len(tasks)
	allSuccess := true
	allWebhookSuccess := true
	fallbackName := firstNonEmpty(strings.TrimSpace(task.Params), strings.TrimSpace(task.BookID))
	var nameCandidate string
	for _, candidate := range tasks {
		if candidate == nil {
			continue
		}
		if strings.TrimSpace(candidate.Status) != feishu.StatusSuccess {
			allSuccess = false
			break
		}
		if strings.TrimSpace(candidate.Webhook) != feishu.WebhookSuccess {
			allWebhookSuccess = false
		}
		if nameCandidate == "" {
			nameCandidate = firstNonEmpty(strings.TrimSpace(candidate.Params), strings.TrimSpace(candidate.BookID))
		}
	}
	if !allSuccess || total == 0 || allWebhookSuccess {
		return nil
	}
	combos := collectSummaryCombinations(tasks)
	clock := w.clock()
	payload := TaskSummaryPayload{
		TaskID:             groupID,
		Total:              total,
		Done:               total,
		UniqueCombinations: combos,
		UniqueCount:        len(combos),
		CreatedAt:          clock.UTC().Unix(),
		TaskName:           firstNonEmpty(nameCandidate, fallbackName, groupID),
		Email:              "",
	}
	if err := w.crawler.SendTaskSummary(ctx, payload); err != nil {
		return err
	}
	if err := updateFeishuTaskWebhooks(ctx, tasks, feishu.WebhookSuccess); err != nil {
		return err
	}
	log.Info().
		Str("group_id", groupID).
		Int("task_count", total).
		Msg("single url worker: sent task summary for group")
	return nil
}

func (w *SingleURLWorker) fetchSingleURLGroupTasks(ctx context.Context, groupID string) ([]*FeishuTask, error) {
	if w == nil {
		return nil, errors.New("single url worker: nil instance")
	}
	groupField := strings.TrimSpace(feishu.DefaultTaskFields.GroupID)
	if groupField == "" {
		return nil, errors.New("single url worker: group id field is not configured")
	}
	filter := feishu.NewFilterInfo("and")
	if sceneField := strings.TrimSpace(feishu.DefaultTaskFields.Scene); sceneField != "" {
		if cond := feishu.NewCondition(sceneField, "is", SceneSingleURLCapture); cond != nil {
			filter.Conditions = append(filter.Conditions, cond)
		}
	}
	if cond := feishu.NewCondition(groupField, "is", strings.TrimSpace(groupID)); cond != nil {
		filter.Conditions = append(filter.Conditions, cond)
	}
	if len(filter.Conditions) == 0 {
		return nil, nil
	}
	return fetchFeishuTasksWithFilter(ctx, w.client, w.bitableURL, filter, singleURLGroupFetchLimit)
}

func collectSummaryCombinations(tasks []*FeishuTask) []TaskSummaryCombination {
	if len(tasks) == 0 {
		return nil
	}
	combos := make(map[string]TaskSummaryCombination, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		bid := strings.TrimSpace(task.BookID)
		uid := strings.TrimSpace(task.UserID)
		if bid == "" || uid == "" {
			continue
		}
		key := bid + "|" + uid
		combos[key] = TaskSummaryCombination{Bid: bid, AccountID: uid}
	}
	if len(combos) == 0 {
		return nil
	}
	keys := make([]string, 0, len(combos))
	for key := range combos {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]TaskSummaryCombination, 0, len(keys))
	for _, key := range keys {
		result = append(result, combos[key])
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
