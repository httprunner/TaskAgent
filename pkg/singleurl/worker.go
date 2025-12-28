package singleurl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	crawlerServiceBaseURLEnv     = "CRAWLER_SERVICE_BASE_URL"
	singleURLConcurrencyEnv      = "SINGLE_URL_CONCURRENCY"
	defaultCrawlerServiceBaseURL = "http://localhost:8000"
	// DefaultSingleURLWorkerLimit is the per-device multiplier used when the worker
	// auto-resolves fetch limits (Limit<=0).
	DefaultSingleURLWorkerLimit = 20
	// MaxSingleURLWorkerFetchLimit caps a single fetch cycle to protect the Feishu API.
	MaxSingleURLWorkerFetchLimit = 200
	singleURLMaxAttempts         = 3
)

// SceneSingleURLCapture mirrors the Feishu scene name used for single URL capture tasks.
const SceneSingleURLCapture = taskagent.SceneSingleURLCapture

// FeishuTask is a local alias for the tasksource Feishu task type.
type FeishuTask = taskagent.FeishuTask

type singleURLAttempt struct {
	TaskID      string `json:"task_id,omitempty"`
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
	return singleURLMetadata{}
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

func (m *singleURLMetadata) attemptsWithTaskID() int {
	if m == nil || len(m.Attempts) == 0 {
		return 0
	}
	count := 0
	for _, attempt := range m.Attempts {
		if strings.TrimSpace(attempt.TaskID) != "" {
			count++
		}
	}
	return count
}

func (m *singleURLMetadata) reachedRetryCap(limit int) bool {
	if limit <= 0 {
		return false
	}
	return m.attemptsWithTaskID() >= limit
}

func (m *singleURLMetadata) appendAttempt(jobID string, status string, ts time.Time) *singleURLAttempt {
	if m == nil {
		return nil
	}
	trimmed := strings.TrimSpace(jobID)
	if trimmed == "" {
		return nil
	}
	attempt := singleURLAttempt{TaskID: trimmed, Status: strings.TrimSpace(status)}
	if !ts.IsZero() {
		attempt.CreatedAt = ts.UTC().Unix()
	}
	m.Attempts = append(m.Attempts, attempt)
	return m.latestAttempt()
}

func (m *singleURLMetadata) latestTaskID() string {
	if latest := m.latestAttempt(); latest != nil {
		return strings.TrimSpace(latest.TaskID)
	}
	return ""
}

func (m *singleURLMetadata) markQueued(ts time.Time) {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = feishusdk.StatusDownloaderQueued
		if latest.CreatedAt == 0 && !ts.IsZero() {
			latest.CreatedAt = ts.UTC().Unix()
		}
	}
}

func (m *singleURLMetadata) markRunning() {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = feishusdk.StatusDownloaderProcessing
	}
}

func (m *singleURLMetadata) markSuccess(vid string, ts time.Time) {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = feishusdk.StatusSuccess
		latest.VID = strings.TrimSpace(vid)
		latest.Error = ""
		if !ts.IsZero() {
			latest.CompletedAt = ts.UTC().Unix()
		}
	}
}

func (m *singleURLMetadata) markFailure(reason string, ts time.Time) {
	if latest := m.latestAttempt(); latest != nil {
		latest.Status = feishusdk.StatusDownloaderFailed
		latest.Error = strings.TrimSpace(reason)
		if !ts.IsZero() {
			latest.CompletedAt = ts.UTC().Unix()
		}
	}
}

// SingleURLWorkerConfig captures the dependencies required to process
// "单个链接采集" (single URL) tasks independently from device runners.
type SingleURLWorkerConfig struct {
	Client        taskagent.TargetTableClient
	BitableURL    string
	Limit         int
	PollInterval  time.Duration
	Clock         func() time.Time
	CrawlerClient crawlerTaskClient
	// Concurrency controls how many crawler CreateTask requests can run in parallel
	// within a single ProcessOnce cycle. Feishu updates are still applied serially to avoid
	// triggering rate limits.
	Concurrency int
	// AppFilter optionally scopes tasks by the App column in the task table.
	AppFilter string
	// NodeIndex and NodeTotal enable optional sharding (BookID+App) across multiple nodes.
	NodeIndex int
	NodeTotal int
}

// SingleURLWorker pulls single-URL capture tasks and dispatches them via
// API stubs without using physical devices.
type SingleURLWorker struct {
	client       taskagent.TargetTableClient
	bitableURL   string
	limit        int
	pollInterval time.Duration
	clock        func() time.Time
	crawler      crawlerTaskClient
	concurrency  int
	appFilter    string
	nodeIndex    int
	nodeTotal    int

	newTaskStatuses    []string
	activeTaskStatuses []string
	datePresets        []string

	activePageTokens map[string]string
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
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = 30 * time.Second
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = env.Int(singleURLConcurrencyEnv, 1)
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	nodeIndex, nodeTotal := taskagent.NormalizeShardConfig(cfg.NodeIndex, cfg.NodeTotal)
	return &SingleURLWorker{
		client:       client,
		bitableURL:   bitableURL,
		limit:        limit,
		pollInterval: poll,
		clock:        clock,
		crawler:      crawler,
		concurrency:  concurrency,
		appFilter:    strings.TrimSpace(cfg.AppFilter),
		nodeIndex:    nodeIndex,
		nodeTotal:    nodeTotal,
		newTaskStatuses: []string{
			feishusdk.StatusPending,
			feishusdk.StatusFailed,
			feishusdk.StatusDownloaderFailed,
		},
		activeTaskStatuses: []string{
			feishusdk.StatusDownloaderProcessing,
			feishusdk.StatusDownloaderQueued,
		},
		datePresets:      []string{taskagent.TaskDateToday},
		activePageTokens: make(map[string]string, 4),
	}, nil
}

// NewSingleURLWorkerFromEnv builds a worker using Feishu credentials from env.
func NewSingleURLWorkerFromEnv(bitableURL string, limit int, pollInterval time.Duration) (*SingleURLWorker, error) {
	return NewSingleURLWorkerFromEnvWithOptions(SingleURLWorkerConfig{
		BitableURL:   bitableURL,
		Limit:        limit,
		PollInterval: pollInterval,
	})
}

// NewSingleURLWorkerFromEnvWithOptions builds a worker using Feishu credentials from env.
// Additional behavior such as app scoping and sharding can be configured via cfg.
func NewSingleURLWorkerFromEnvWithOptions(cfg SingleURLWorkerConfig) (*SingleURLWorker, error) {
	client, err := feishusdk.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	baseURL := env.String(crawlerServiceBaseURLEnv, defaultCrawlerServiceBaseURL)
	crawler, err := newRESTCrawlerTaskClient(baseURL, nil)
	if err != nil {
		return nil, err
	}
	return NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		BitableURL:    cfg.BitableURL,
		Limit:         cfg.Limit,
		PollInterval:  cfg.PollInterval,
		CrawlerClient: crawler,
		Concurrency:   cfg.Concurrency,
		AppFilter:     cfg.AppFilter,
		NodeIndex:     cfg.NodeIndex,
		NodeTotal:     cfg.NodeTotal,
	})
}

// NewSingleURLReadyWorkerFromEnv builds a worker that consumes tasks already
// prepared by the device stage (Status=ready) and dispatches them to the
// downloader service.
func NewSingleURLReadyWorkerFromEnv(bitableURL string, limit int, pollInterval time.Duration) (*SingleURLWorker, error) {
	return NewSingleURLReadyWorkerFromEnvWithOptions(SingleURLWorkerConfig{
		BitableURL:   bitableURL,
		Limit:        limit,
		PollInterval: pollInterval,
	})
}

// NewSingleURLReadyWorkerFromEnvWithOptions builds a worker that consumes tasks already
// prepared by the device stage (Status=ready) and dispatches them to the
// downloader service. It also supports optional app scoping and sharding.
func NewSingleURLReadyWorkerFromEnvWithOptions(cfg SingleURLWorkerConfig) (*SingleURLWorker, error) {
	worker, err := NewSingleURLWorkerFromEnvWithOptions(cfg)
	if err != nil {
		return nil, err
	}
	if worker == nil {
		return nil, errors.New("single url worker: nil instance")
	}
	worker.newTaskStatuses = []string{
		feishusdk.StatusReady,
		feishusdk.StatusDownloaderFailed,
	}
	worker.datePresets = []string{
		taskagent.TaskDateToday,
		taskagent.TaskDateYesterday,
	}
	return worker, nil
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
func (w *SingleURLWorker) ProcessOnce(ctx context.Context) (retErr error) {
	if w == nil {
		return errors.New("single url worker: nil instance")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "single url worker panic (ProcessOnce): %v\n", r)
			retErr = errors.Errorf("single url worker panic: %v", r)
		}
	}()
	// Always poll active tasks first so dl-processing tasks don't get starved when
	// the table is dominated by ready/queued tasks.
	activeTasks, err := w.fetchSingleURLActiveTasksRotating(ctx, w.limit)
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

	newTasks, err := w.fetchSingleURLTasks(ctx, w.newTaskStatuses, w.limit)
	if err != nil {
		return err
	}
	if w.concurrency <= 1 || len(newTasks) <= 1 {
		for _, task := range newTasks {
			if err := w.handleSingleURLTask(ctx, task); err != nil {
				log.Error().Err(err).
					Int64("task_id", task.TaskID).
					Msg("single url worker dispatch failed")
			}
		}
	} else {
		w.dispatchSingleURLTasks(ctx, newTasks)
	}
	return nil
}

type singleURLUpdateWork struct {
	taskID int64
	apply  func(context.Context) error
}

func (w *SingleURLWorker) dispatchSingleURLTasks(ctx context.Context, tasks []*FeishuTask) {
	if w == nil || len(tasks) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	works := make([]singleURLUpdateWork, len(tasks))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(w.concurrency)
	for idx, task := range tasks {
		idx := idx
		task := task
		group.Go(func() error {
			defer func() {
				if r := recover(); r != nil {
					taskID := int64(0)
					if task != nil {
						taskID = task.TaskID
					}
					fmt.Fprintf(os.Stderr, "single url worker panic (dispatch task_id=%d): %v\n", taskID, r)
					if task != nil {
						reason := fmt.Sprintf("panic: %v", r)
						works[idx] = singleURLUpdateWork{
							taskID: taskID,
							apply: func(ctx context.Context) error {
								return w.failSingleURLTask(ctx, task, reason, nil)
							},
						}
					}
				}
			}()
			if task == nil {
				return nil
			}
			works[idx] = w.buildSingleURLDispatchWork(groupCtx, task)
			return nil
		})
	}
	_ = group.Wait()
	for _, work := range works {
		if work.apply == nil {
			continue
		}
		if err := work.apply(ctx); err != nil {
			log.Error().Err(err).
				Int64("task_id", work.taskID).
				Msg("single url worker update failed")
		}
	}
}

func (w *SingleURLWorker) buildSingleURLDispatchWork(ctx context.Context, task *FeishuTask) singleURLUpdateWork {
	work := singleURLUpdateWork{taskID: 0}
	if task == nil {
		return work
	}
	work.taskID = task.TaskID
	if w == nil {
		work.apply = func(context.Context) error { return errors.New("single url worker: nil instance") }
		return work
	}
	meta := decodeSingleURLMetadata(task.Logs)
	if meta.reachedRetryCap(singleURLMaxAttempts) {
		reason := fmt.Sprintf("retry limit reached after %d attempts", meta.attemptsWithTaskID())
		bookID := strings.TrimSpace(task.BookID)
		userID := strings.TrimSpace(task.UserID)
		attempts := meta.attemptsWithTaskID()
		if latest := meta.latestAttempt(); latest != nil {
			latest.Error = reason
			if latest.CompletedAt == 0 {
				if ts := w.clock(); !ts.IsZero() {
					latest.CompletedAt = ts.UTC().Unix()
				}
			}
		}
		work.apply = func(ctx context.Context) error {
			log.Warn().
				Int64("task_id", task.TaskID).
				Str("book_id", bookID).
				Str("user_id", userID).
				Int("attempts", attempts).
				Msg("single url worker retry cap reached; marking task error")
			return w.markSingleURLTaskError(ctx, task, meta)
		}
		return work
	}

	retryRequired := false
	switch strings.TrimSpace(task.Status) {
	case feishusdk.StatusFailed, feishusdk.StatusDownloaderFailed:
		retryRequired = true
	}

	bookID := strings.TrimSpace(task.BookID)
	userID := strings.TrimSpace(task.UserID)
	url := strings.TrimSpace(task.URL)
	if bookID == "" || userID == "" || url == "" {
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
		reason := fmt.Sprintf("missing fields: %s", strings.Join(missingFields, ","))
		work.apply = func(ctx context.Context) error {
			return w.failSingleURLTask(ctx, task, reason, nil)
		}
		return work
	}

	if !retryRequired {
		if taskID := meta.latestTaskID(); taskID != "" {
			work.apply = func(ctx context.Context) error {
				log.Info().
					Int64("task_id", task.TaskID).
					Str("crawler_task_id", taskID).
					Msg("single url task already has crawler task id; reconciling")
				return w.reconcileSingleURLTask(ctx, task)
			}
			return work
		}
	}

	metaPayload := make(map[string]string, 3)
	metaPayload["platform"] = defaultCookiePlatform
	metaPayload["bid"] = bookID
	metaPayload["uid"] = userID
	cdnURL := extractSingleURLCDNURL(task.Extra)
	if (strings.TrimSpace(task.Status) == feishusdk.StatusReady || strings.TrimSpace(task.Status) == feishusdk.StatusDownloaderFailed) && cdnURL == "" {
		work.apply = func(ctx context.Context) error {
			return w.markSingleURLTaskFailedForDeviceStage(ctx, task)
		}
		return work
	}
	if cdnURL != "" {
		metaPayload["cdn_url"] = cdnURL
	}
	taskID, err := w.crawler.CreateTask(ctx, url, metaPayload)
	if err != nil {
		reason := fmt.Sprintf("create crawler task failed: %v", err)
		work.apply = func(ctx context.Context) error {
			return w.failSingleURLTask(ctx, task, reason, nil)
		}
		return work
	}

	createdAt := w.clock()
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	meta.appendAttempt(taskID, feishusdk.StatusDownloaderQueued, createdAt)
	groupID := buildSingleURLGroupID(task.App, bookID, userID)
	work.apply = func(ctx context.Context) error {
		if err := w.markSingleURLTaskQueued(ctx, task, groupID, meta); err != nil {
			return err
		}
		log.Info().
			Int64("task_id", task.TaskID).
			Str("crawler_task_id", taskID).
			Str("book_id", bookID).
			Str("user_id", userID).
			Str("url", url).
			Msg("single url capture task queued")
		return nil
	}
	return work
}

func (w *SingleURLWorker) handleSingleURLTask(ctx context.Context, task *FeishuTask) error {
	if w == nil {
		return errors.New("single url worker: nil instance")
	}
	if task == nil {
		return errors.New("single url worker: nil task")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	work := w.buildSingleURLDispatchWork(ctx, task)
	if work.apply == nil {
		return nil
	}
	return work.apply(ctx)
}

func (w *SingleURLWorker) fetchSingleURLTasks(ctx context.Context,
	statuses []string, limit int) ([]*FeishuTask, error) {
	return w.fetchSingleURLTasksWithDatePresets(ctx, statuses, limit, nil)
}

func (w *SingleURLWorker) fetchSingleURLTasksWithDatePresets(ctx context.Context,
	statuses []string, limit int, datePresets []string) ([]*FeishuTask, error) {
	if limit <= 0 {
		limit = w.limit
	}
	if limit <= 0 && w != nil {
		online := taskagent.OnlineDeviceCount()
		if online <= 0 {
			online = 1
		}
		limit = DefaultSingleURLWorkerLimit * online
	}
	if limit > MaxSingleURLWorkerFetchLimit {
		limit = MaxSingleURLWorkerFetchLimit
	}
	if limit <= 0 {
		limit = DefaultSingleURLWorkerLimit
	}
	if len(datePresets) == 0 {
		datePresets = w.datePresets
		if len(datePresets) == 0 {
			datePresets = []string{taskagent.TaskDateToday}
		}
	}
	result := make([]*FeishuTask, 0, limit)
	seen := make(map[int64]struct{}, limit)
	fields := feishusdk.DefaultTaskFields
	for _, status := range statuses {
		for _, preset := range datePresets {
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
			subset, _, err := taskagent.FetchFeishuTasksWithStrategyPage(
				ctx, w.client, w.bitableURL, fields, w.appFilter, []string{status},
				remaining, taskagent.SceneSingleURLCapture, preset,
				taskagent.FeishuTaskQueryOptions{IgnoreView: true})
			if err != nil {
				log.Warn().Err(err).
					Str("status", status).
					Str("date_preset", preset).
					Msg("single url worker fetch failed")
				continue
			}
			if w.nodeTotal > 1 && len(subset) > 0 {
				subset = filterFeishuTasksByShard(subset, w.nodeIndex, w.nodeTotal)
			}
			result = taskagent.AppendUniqueFeishuTasks(result, subset, limit, seen)
		}
	}
	return result, nil
}

func (w *SingleURLWorker) fetchSingleURLActiveTasksRotating(ctx context.Context, limit int) ([]*FeishuTask, error) {
	if w == nil {
		return nil, errors.New("single url worker: nil instance")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if limit <= 0 {
		limit = w.limit
	}
	if limit <= 0 {
		limit = DefaultSingleURLWorkerLimit
	}
	statuses := w.activeTaskStatuses
	if len(statuses) == 0 {
		return nil, nil
	}
	perStatus := limit / len(statuses)
	if perStatus <= 0 {
		perStatus = 1
	}
	seen := make(map[int64]struct{}, limit)
	result := make([]*FeishuTask, 0, limit)
	fields := feishusdk.DefaultTaskFields
	for _, status := range statuses {
		if len(result) >= limit {
			break
		}
		fetchCap := perStatus
		remaining := limit - len(result)
		if remaining > 0 && fetchCap > remaining {
			fetchCap = remaining
		}
		if fetchCap <= 0 {
			fetchCap = perStatus
		}
		pageToken := ""
		if w.activePageTokens != nil {
			pageToken = strings.TrimSpace(w.activePageTokens[status])
		}
		subset, pageInfo, err := taskagent.FetchFeishuTasksWithStrategyPage(
			ctx, w.client, w.bitableURL, fields, w.appFilter, []string{status},
			fetchCap, taskagent.SceneSingleURLCapture, taskagent.TaskDateAny,
			taskagent.FeishuTaskQueryOptions{IgnoreView: true, PageToken: pageToken, MaxPages: 1},
		)
		if err != nil {
			log.Warn().Err(err).
				Str("status", status).
				Str("page_token", pageToken).
				Msg("single url worker active fetch failed")
			continue
		}
		if w.activePageTokens != nil {
			next := strings.TrimSpace(pageInfo.NextPageToken)
			if pageInfo.HasMore && next != "" {
				w.activePageTokens[status] = next
			} else {
				w.activePageTokens[status] = ""
			}
		}
		if w.nodeTotal > 1 && len(subset) > 0 {
			subset = filterFeishuTasksByShard(subset, w.nodeIndex, w.nodeTotal)
		}
		result = taskagent.AppendUniqueFeishuTasks(result, subset, limit, seen)
	}
	return result, nil
}

func filterFeishuTasksByShard(tasks []*FeishuTask, nodeIndex, nodeTotal int) []*FeishuTask {
	if nodeTotal <= 1 || len(tasks) == 0 {
		return tasks
	}
	out := make([]*FeishuTask, 0, len(tasks))
	for _, t := range tasks {
		if t == nil {
			continue
		}
		key := taskagent.ResolveShardIDForTaskRow(taskagent.FeishuTaskRow{BookID: t.BookID, App: t.App})
		shard := int(key % int64(nodeTotal))
		if shard == nodeIndex {
			out = append(out, t)
		}
	}
	return out
}

func (w *SingleURLWorker) markSingleURLTaskFailedForDeviceStage(ctx context.Context, task *FeishuTask) error {
	_, table, err := taskagent.TaskSourceContext(task)
	if err != nil {
		return err
	}
	statusField := strings.TrimSpace(table.Fields.Status)
	if statusField == "" {
		return errors.New("single url worker: status field is empty")
	}
	fields := map[string]any{statusField: feishusdk.StatusFailed}
	if err := taskagent.UpdateTaskFields(ctx, task, fields); err != nil {
		return err
	}
	task.Status = feishusdk.StatusFailed
	return nil
}

func (w *SingleURLWorker) reconcileSingleURLTask(ctx context.Context, task *FeishuTask) error {
	if task == nil {
		return errors.New("single url worker: nil task")
	}
	meta := decodeSingleURLMetadata(task.Logs)
	taskID := meta.latestTaskID()
	if taskID == "" {
		return w.failSingleURLTask(ctx, task, "missing task_id for queued task", nil)
	}
	status, err := w.crawler.GetTask(ctx, taskID)
	if err != nil {
		if errors.Is(err, errCrawlerTaskNotFound) {
			meta.markFailure("crawler task not found", w.clock())
			return w.failSingleURLTask(ctx, task, "crawler task not found", &meta)
		}
		return err
	}
	switch strings.ToUpper(strings.TrimSpace(status.Status)) {
	case "WAITING":
		if task.Status != feishusdk.StatusDownloaderQueued {
			groupID := strings.TrimSpace(task.GroupID)
			if groupID == "" {
				groupID = buildSingleURLGroupID(task.App, strings.TrimSpace(task.BookID), strings.TrimSpace(task.UserID))
			}
			return w.markSingleURLTaskQueued(ctx, task, groupID, meta)
		}
		return nil
	case "PROCESSING":
		meta.markRunning()
		if err := w.updateTaskExtra(ctx, task, meta); err != nil {
			return err
		}
		if err := taskagent.UpdateFeishuTaskStatuses(ctx, []*FeishuTask{task}, feishusdk.StatusDownloaderProcessing, "", nil); err != nil {
			return err
		}
		task.Status = feishusdk.StatusDownloaderProcessing
		return nil
	case "COMPLETED":
		vid := strings.TrimSpace(status.VID)
		if vid == "" {
			// Downloader may briefly return status=done before persisting vid.
			// Only mark success when vid is present; otherwise keep polling.
			meta.markRunning()
			if latest := meta.latestAttempt(); latest != nil {
				if latest.CompletedAt != 0 {
					latest.CompletedAt = 0
				}
				if strings.TrimSpace(latest.Error) == "" {
					latest.Error = "waiting for vid"
				}
			}
			log.Warn().
				Int64("task_id", task.TaskID).
				Str("crawler_task_id", taskID).
				Msg("single url worker: crawler returned completed but vid is empty; keep polling")
			if err := w.updateTaskExtra(ctx, task, meta); err != nil {
				return err
			}
			if task.Status != feishusdk.StatusDownloaderProcessing {
				if err := taskagent.UpdateFeishuTaskStatuses(ctx, []*FeishuTask{task}, feishusdk.StatusDownloaderProcessing, "", nil); err != nil {
					return err
				}
				task.Status = feishusdk.StatusDownloaderProcessing
			}
			return nil
		}
		completed := w.clock()
		meta.markSuccess(vid, completed)
		if err := w.updateTaskExtra(ctx, task, meta); err != nil {
			return err
		}
		if err := taskagent.UpdateFeishuTaskStatuses(ctx, []*FeishuTask{task}, feishusdk.StatusSuccess, "", &taskagent.TaskStatusMeta{CompletedAt: &completed}); err != nil {
			return err
		}
		task.Status = feishusdk.StatusSuccess
		return nil
	case "FAILED":
		reason := strings.TrimSpace(status.Error)
		if reason == "" {
			reason = "crawler task failed"
		}
		meta.markFailure(reason, w.clock())
		return w.failSingleURLTask(ctx, task, reason, &meta)
	default:
		log.Warn().
			Int64("task_id", task.TaskID).
			Str("crawler_task_id", taskID).
			Str("status", status.Status).
			Msg("single url worker: unknown crawler status")
		return nil
	}
}

func extractSingleURLCDNURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	var decoded struct {
		CDNURL  string `json:"cdn_url"`
		FeedURL string `json:"feed_url"`
	}
	if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil {
		if strings.TrimSpace(decoded.CDNURL) != "" {
			return strings.TrimSpace(decoded.CDNURL)
		}
		return strings.TrimSpace(decoded.FeedURL)
	}
	var generic map[string]any
	if err := json.Unmarshal([]byte(trimmed), &generic); err != nil {
		return ""
	}
	if val, ok := generic["cdn_url"]; ok {
		if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	if val, ok := generic["feed_url"]; ok {
		if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func (w *SingleURLWorker) updateTaskExtra(ctx context.Context, task *FeishuTask, meta singleURLMetadata) error {
	_, table, err := taskagent.TaskSourceContext(task)
	if err != nil {
		return err
	}
	logsField := strings.TrimSpace(table.Fields.Logs)
	if logsField == "" {
		return nil
	}
	encoded := encodeSingleURLMetadata(meta)
	payload := map[string]any{logsField: encoded}
	if err := taskagent.UpdateTaskFields(ctx, task, payload); err != nil {
		return err
	}
	task.Logs = encoded
	return nil
}

func (w *SingleURLWorker) failSingleURLTask(ctx context.Context, task *FeishuTask, reason string, meta *singleURLMetadata) error {
	_, table, err := taskagent.TaskSourceContext(task)
	if err != nil {
		return err
	}
	fields := map[string]any{}
	statusField := strings.TrimSpace(table.Fields.Status)
	if statusField != "" {
		fields[statusField] = feishusdk.StatusDownloaderFailed
	}
	if logsField := strings.TrimSpace(table.Fields.Logs); logsField != "" {
		encoded := ""
		if meta != nil {
			encoded = encodeSingleURLMetadata(*meta)
		} else {
			encoded = strings.TrimSpace(reason)
		}
		if encoded != "" {
			fields[logsField] = encoded
		}
	}
	if len(fields) == 0 {
		return errors.New("single url worker: no fields to update for failure")
	}
	if err := taskagent.UpdateTaskFields(ctx, task, fields); err != nil {
		return err
	}
	task.Status = feishusdk.StatusDownloaderFailed
	if logsField := strings.TrimSpace(table.Fields.Logs); logsField != "" {
		if logs, ok := fields[logsField].(string); ok {
			task.Logs = logs
		}
	}
	return nil
}

func (w *SingleURLWorker) markSingleURLTaskError(ctx context.Context, task *FeishuTask, meta singleURLMetadata) error {
	_, table, err := taskagent.TaskSourceContext(task)
	if err != nil {
		return err
	}
	fields := map[string]any{}
	statusField := strings.TrimSpace(table.Fields.Status)
	if statusField != "" {
		fields[statusField] = feishusdk.StatusError
	}
	if logsField := strings.TrimSpace(table.Fields.Logs); logsField != "" {
		fields[logsField] = encodeSingleURLMetadata(meta)
	}
	if len(fields) == 0 {
		return errors.New("single url worker: no fields to update for error state")
	}
	if err := taskagent.UpdateTaskFields(ctx, task, fields); err != nil {
		return err
	}
	task.Status = feishusdk.StatusError
	if logsField := strings.TrimSpace(table.Fields.Logs); logsField != "" {
		if val, ok := fields[logsField].(string); ok {
			task.Logs = val
		}
	}
	return nil
}

func (w *SingleURLWorker) markSingleURLTaskQueued(ctx context.Context, task *FeishuTask, groupID string, meta singleURLMetadata) error {
	_, table, err := taskagent.TaskSourceContext(task)
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
		fields[statusField] = feishusdk.StatusDownloaderQueued
	}
	if groupField := strings.TrimSpace(table.Fields.GroupID); groupField != "" && strings.TrimSpace(groupID) != "" {
		fields[groupField] = groupID
	}
	if logsField := strings.TrimSpace(table.Fields.Logs); logsField != "" {
		fields[logsField] = encodeSingleURLMetadata(meta)
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
	if err := taskagent.UpdateTaskFields(ctx, task, fields); err != nil {
		return err
	}
	task.Status = feishusdk.StatusDownloaderQueued
	if strings.TrimSpace(groupID) != "" {
		task.GroupID = groupID
	}
	if logsField := strings.TrimSpace(table.Fields.Logs); logsField != "" {
		if logs, ok := fields[logsField].(string); ok {
			task.Logs = logs
		}
	}
	updateTimestampFields(task, now, nowMillis)
	return nil
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

func buildSingleURLGroupID(app, bookID, userID string) string {
	mappedApp := feishusdk.MapAppValue(strings.TrimSpace(app))
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
