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
	feishufields "github.com/httprunner/TaskAgent/pkg/feishu/fields"
	feishusource "github.com/httprunner/TaskAgent/pkg/tasksource/feishu"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	crawlerServiceBaseURLEnv     = "CRAWLER_SERVICE_BASE_URL"
	defaultCrawlerServiceBaseURL = "http://localhost:8000"
	singleURLStatusQueued        = "queued"
	singleURLGroupFetchLimit     = 200
	// DefaultSingleURLWorkerLimit caps each fetch cycle when no explicit limit is provided.
	DefaultSingleURLWorkerLimit = 20
	singleURLMaxAttempts        = 3
)

type singleURLAttempt struct {
	JobID       string `json:"job_id,omitempty"`
	VID         string `json:"vid,omitempty"`
	Error       string `json:"error,omitempty"`
	Status      string `json:"status,omitempty"`
	CreatedAt   int64  `json:"created_at,omitempty"`
	CompletedAt int64  `json:"completed_at,omitempty"`
}

type singleURLMetadata struct {
	Attempts []singleURLAttempt `json:"attempts,omitempty"`
}

func decodeSingleURLMetadata(raw string) singleURLMetadata {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return singleURLMetadata{}
	}
	if strings.HasPrefix(trimmed, "[") {
		var attempts []singleURLAttempt
		if err := json.Unmarshal([]byte(trimmed), &attempts); err == nil {
			return singleURLMetadata{Attempts: attempts}
		}
		return singleURLMetadata{}
	}
	var legacy singleURLAttempt
	if err := json.Unmarshal([]byte(trimmed), &legacy); err != nil {
		return singleURLMetadata{}
	}
	if strings.TrimSpace(legacy.JobID) == "" && strings.TrimSpace(legacy.VID) == "" && strings.TrimSpace(legacy.Error) == "" {
		return singleURLMetadata{}
	}
	return singleURLMetadata{Attempts: []singleURLAttempt{legacy}}
}

func encodeSingleURLMetadata(meta singleURLMetadata) string {
	if len(meta.Attempts) == 0 {
		return "[]"
	}
	buf, err := json.Marshal(meta.Attempts)
	if err != nil {
		return ""
	}
	return string(buf)
}

func (m *singleURLMetadata) latestAttempt() *singleURLAttempt {
	if m == nil || len(m.Attempts) == 0 {
		return nil
	}
	return &m.Attempts[len(m.Attempts)-1]
}

func (m *singleURLMetadata) attemptsWithJobID() int {
	if m == nil || len(m.Attempts) == 0 {
		return 0
	}
	count := 0
	for _, attempt := range m.Attempts {
		if strings.TrimSpace(attempt.JobID) != "" {
			count++
		}
	}
	return count
}

func (m *singleURLMetadata) reachedRetryCap(limit int) bool {
	if limit <= 0 {
		return false
	}
	return m.attemptsWithJobID() >= limit
}

func (m *singleURLMetadata) appendAttempt(jobID string, status string, ts time.Time) *singleURLAttempt {
	if m == nil {
		return nil
	}
	trimmed := strings.TrimSpace(jobID)
	if trimmed == "" {
		return nil
	}
	attempt := singleURLAttempt{JobID: trimmed, Status: strings.TrimSpace(status)}
	if !ts.IsZero() {
		attempt.CreatedAt = ts.UTC().Unix()
	}
	m.Attempts = append(m.Attempts, attempt)
	return m.latestAttempt()
}

func (m *singleURLMetadata) latestJobID() string {
	if latest := m.latestAttempt(); latest != nil {
		return strings.TrimSpace(latest.JobID)
	}
	return ""
}

func (m *singleURLMetadata) markQueued(ts time.Time) {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = singleURLStatusQueued
		if latest.CreatedAt == 0 && !ts.IsZero() {
			latest.CreatedAt = ts.UTC().Unix()
		}
	}
}

func (m *singleURLMetadata) markRunning() {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = feishu.StatusRunning
	}
}

func (m *singleURLMetadata) markSuccess(vid string, ts time.Time) {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = feishu.StatusSuccess
		latest.VID = strings.TrimSpace(vid)
		latest.Error = ""
		if !ts.IsZero() {
			latest.CompletedAt = ts.UTC().Unix()
		}
	}
}

func (m *singleURLMetadata) markFailure(reason string, ts time.Time) {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = feishu.StatusFailed
		latest.Error = strings.TrimSpace(reason)
		if !ts.IsZero() {
			latest.CompletedAt = ts.UTC().Unix()
		}
	}
}

// SingleURLWorkerConfig captures the dependencies required to process
// "单个链接采集" (single URL) tasks independently from device runners.
type SingleURLWorkerConfig struct {
	Client         feishusource.TargetTableClient
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
	client         feishusource.TargetTableClient
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
		limit = DefaultSingleURLWorkerLimit
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
		limit = DefaultSingleURLWorkerLimit
	}
	result := make([]*FeishuTask, 0, limit)
	seen := make(map[int64]struct{}, limit)
	fields := feishu.DefaultTaskFields
	for _, status := range statuses {
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
		subset, err := feishusource.FetchFeishuTasksWithStrategy(ctx, w.client, w.bitableURL, fields, "", []string{status}, remaining, feishusource.SceneSingleURLCapture)
		if err != nil {
			log.Warn().Err(err).
				Str("status", status).
				Msg("single url worker fetch failed")
			continue
		}
		result = feishusource.AppendUniqueFeishuTasks(result, subset, limit, seen)
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
	if meta.reachedRetryCap(singleURLMaxAttempts) {
		reason := fmt.Sprintf("retry limit reached after %d attempts", meta.attemptsWithJobID())
		if latest := meta.latestAttempt(); latest != nil {
			latest.Error = reason
			if latest.CompletedAt == 0 {
				if ts := w.clock(); !ts.IsZero() {
					latest.CompletedAt = ts.UTC().Unix()
				}
			}
		}
		log.Warn().
			Int64("task_id", task.TaskID).
			Str("book_id", bookID).
			Str("user_id", userID).
			Int("attempts", meta.attemptsWithJobID()).
			Msg("single url worker retry cap reached; marking task error")
		return w.markSingleURLTaskError(ctx, task, meta)
	}
	retryRequired := strings.TrimSpace(task.Status) == feishu.StatusFailed
	if !retryRequired {
		if jobID := meta.latestJobID(); jobID != "" {
			log.Info().
				Int64("task_id", task.TaskID).
				Str("job_id", jobID).
				Msg("single url task already has job id; skip creation")
			return nil
		}
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
	createdAt := w.clock()
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	meta.appendAttempt(jobID, singleURLStatusQueued, createdAt)
	groupID := buildSingleURLGroupID(task.App, bookID, userID)
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
	jobID := meta.latestJobID()
	if jobID == "" {
		return w.failSingleURLTask(ctx, task, "missing job_id for queued task", nil)
	}
	status, err := w.crawler.GetTask(ctx, jobID)
	if err != nil {
		if errors.Is(err, errCrawlerJobNotFound) {
			meta.markFailure("crawler job not found", w.clock())
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
		meta.markRunning()
		if err := w.updateTaskExtra(ctx, task, meta); err != nil {
			return err
		}
		return feishusource.UpdateFeishuTaskStatuses(ctx, []*FeishuTask{task}, feishu.StatusRunning, "", nil)
	case "done":
		completed := w.clock()
		meta.markSuccess(status.VID, completed)
		if err := w.updateTaskExtra(ctx, task, meta); err != nil {
			return err
		}
		if err := feishusource.UpdateFeishuTaskStatuses(ctx, []*FeishuTask{task}, feishu.StatusSuccess, "", &feishusource.TaskStatusMeta{CompletedAt: &completed}); err != nil {
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
		meta.markFailure(reason, w.clock())
		return w.failSingleURLTask(ctx, task, reason, &meta)
	default:
		log.Warn().
			Int64("task_id", task.TaskID).
			Str("job_id", jobID).
			Str("status", status.Status).
			Msg("single url worker: unknown crawler status")
		return nil
	}
}

func (w *SingleURLWorker) updateTaskExtra(ctx context.Context, task *FeishuTask, meta singleURLMetadata) error {
	_, table, err := feishusource.TaskSourceContext(task)
	if err != nil {
		return err
	}
	extraField := strings.TrimSpace(table.Fields.Extra)
	if extraField == "" {
		return nil
	}
	encoded := encodeSingleURLMetadata(meta)
	payload := map[string]any{extraField: encoded}
	if err := feishusource.UpdateTaskFields(ctx, task, payload); err != nil {
		return err
	}
	task.Extra = encoded
	return nil
}

func (w *SingleURLWorker) failSingleURLTask(ctx context.Context, task *FeishuTask, reason string, meta *singleURLMetadata) error {
	_, table, err := feishusource.TaskSourceContext(task)
	if err != nil {
		return err
	}
	fields := map[string]any{}
	statusField := strings.TrimSpace(table.Fields.Status)
	if statusField != "" {
		fields[statusField] = feishu.StatusFailed
	}
	if extraField := strings.TrimSpace(table.Fields.Extra); extraField != "" {
		if meta != nil {
			fields[extraField] = encodeSingleURLMetadata(*meta)
		} else {
			fields[extraField] = reason
		}
	}
	if len(fields) == 0 {
		return errors.New("single url worker: no fields to update for failure")
	}
	if err := feishusource.UpdateTaskFields(ctx, task, fields); err != nil {
		return err
	}
	task.Status = feishu.StatusFailed
	if extra, ok := fields[strings.TrimSpace(table.Fields.Extra)].(string); ok {
		task.Extra = extra
	} else {
		task.Extra = reason
	}
	return nil
}

func (w *SingleURLWorker) markSingleURLTaskError(ctx context.Context, task *FeishuTask, meta singleURLMetadata) error {
	_, table, err := feishusource.TaskSourceContext(task)
	if err != nil {
		return err
	}
	fields := map[string]any{}
	statusField := strings.TrimSpace(table.Fields.Status)
	if statusField != "" {
		fields[statusField] = feishu.StatusError
	}
	if extraField := strings.TrimSpace(table.Fields.Extra); extraField != "" {
		fields[extraField] = encodeSingleURLMetadata(meta)
	}
	if len(fields) == 0 {
		return errors.New("single url worker: no fields to update for error state")
	}
	if err := feishusource.UpdateTaskFields(ctx, task, fields); err != nil {
		return err
	}
	task.Status = feishu.StatusError
	if extraField := strings.TrimSpace(table.Fields.Extra); extraField != "" {
		if val, ok := fields[extraField].(string); ok {
			task.Extra = val
		}
	}
	return nil
}

func (w *SingleURLWorker) markSingleURLTaskQueued(ctx context.Context, task *FeishuTask, groupID string, meta singleURLMetadata) error {
	_, table, err := feishusource.TaskSourceContext(task)
	if err != nil {
		return err
	}
	now := w.clock()
	if now.IsZero() {
		now = time.Now()
	}
	nowMillis := now.UTC().UnixMilli()
	meta.markQueued(now)
	fields := map[string]any{}
	if statusField := strings.TrimSpace(table.Fields.Status); statusField != "" {
		fields[statusField] = singleURLStatusQueued
	}
	if groupField := strings.TrimSpace(table.Fields.GroupID); groupField != "" && strings.TrimSpace(groupID) != "" {
		fields[groupField] = groupID
	}
	if extraField := strings.TrimSpace(table.Fields.Extra); extraField != "" {
		fields[extraField] = encodeSingleURLMetadata(meta)
	}
	if dispatchedField := strings.TrimSpace(table.Fields.DispatchedAt); dispatchedField != "" {
		fields[dispatchedField] = nowMillis
	}
	if startField := strings.TrimSpace(table.Fields.StartAt); startField != "" {
		fields[startField] = nowMillis
	}
	if len(fields) == 0 {
		return errors.New("single url worker: no fields to update for queued state")
	}
	if err := feishusource.UpdateTaskFields(ctx, task, fields); err != nil {
		return err
	}
	task.Status = singleURLStatusQueued
	if strings.TrimSpace(groupID) != "" {
		task.GroupID = groupID
	}
	if extra, ok := fields[strings.TrimSpace(table.Fields.Extra)].(string); ok {
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
	if err := feishusource.UpdateFeishuTaskWebhooks(ctx, tasks, feishu.WebhookSuccess); err != nil {
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
		if cond := feishu.NewCondition(sceneField, "is", feishusource.SceneSingleURLCapture); cond != nil {
			filter.Conditions = append(filter.Conditions, cond)
		}
	}
	if cond := feishu.NewCondition(groupField, "is", strings.TrimSpace(groupID)); cond != nil {
		filter.Conditions = append(filter.Conditions, cond)
	}
	if len(filter.Conditions) == 0 {
		return nil, nil
	}
	return feishusource.FetchFeishuTasksWithFilter(ctx, w.client, w.bitableURL, filter, singleURLGroupFetchLimit)
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

func buildSingleURLGroupID(app, bookID, userID string) string {
	mappedApp := feishufields.MapAppValue(strings.TrimSpace(app))
	if mappedApp == "" {
		mappedApp = strings.TrimSpace(app)
	}
	trimmedBook := strings.TrimSpace(bookID)
	if trimmedBook == "" {
		trimmedBook = "unknown_book"
	}
	trimmedUser := strings.TrimSpace(userID)
	if trimmedUser == "" {
		trimmedUser = "unknown_user"
	}
	return fmt.Sprintf("%s_%s_%s", mappedApp, trimmedBook, trimmedUser)
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
