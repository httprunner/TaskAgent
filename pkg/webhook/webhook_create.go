package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type WebhookResultCreateOptions struct {
	TaskBitableURL    string
	WebhookBitableURL string
	DramaBitableURL   string

	ParentTaskID      int64
	ParentDatetime    *time.Time
	ParentDatetimeRaw string

	BookID   string
	GroupIDs []string
}

const (
	WebhookBizTypePiracyGeneralSearch = "piracy_general_search"
	WebhookBizTypeVideoScreenCapture  = "video_screen_capture"
)

// CreateWebhookResultsForGroups creates one webhook result row per GroupID under a parent task.
// It is designed for the "综合页搜索 -> 盗版筛查" flow where child tasks are generated and webhook
// should only fire after all TaskIDs complete (success/error).
func CreateWebhookResultsForGroups(ctx context.Context, opts WebhookResultCreateOptions) error {
	log.Info().Msg("creating webhook tasks for groups")
	store, err := newWebhookResultStore(firstNonEmpty(opts.WebhookBitableURL, taskagent.EnvString(taskagent.EnvWebhookBitableURL, "")))
	if err != nil {
		return err
	}
	if store == nil || strings.TrimSpace(store.table()) == "" {
		log.Debug().Msg("webhook result table not configured; skip creating webhook result rows")
		return nil
	}
	if opts.ParentTaskID <= 0 {
		return errors.New("parent task id is required")
	}
	bookID := strings.TrimSpace(opts.BookID)
	if bookID == "" {
		return errors.New("book id is required")
	}
	groupIDs := uniqueStrings(opts.GroupIDs)
	if len(groupIDs) == 0 {
		return nil
	}

	taskTableURL := strings.TrimSpace(firstNonEmpty(opts.TaskBitableURL, taskagent.EnvString(taskagent.EnvTaskBitableURL, "")))
	if taskTableURL == "" {
		return errors.New("task bitable url is required")
	}

	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		return err
	}

	dramaInfoJSON, _ := fetchDramaInfoJSONByBookID(ctx, client, firstNonEmpty(opts.DramaBitableURL, taskagent.EnvString("DRAMA_BITABLE_URL", "")), bookID)
	day := dayString(opts.ParentDatetime, opts.ParentDatetimeRaw)

	for _, groupID := range groupIDs {
		existing, err := store.getExistingByParentAndGroup(ctx, WebhookBizTypePiracyGeneralSearch, opts.ParentTaskID, groupID)
		if err != nil {
			return err
		}
		if existing != nil && strings.TrimSpace(existing.RecordID) != "" {
			continue
		}

		taskIDs, err := fetchGroupTaskIDsWithRetry(ctx, client, taskTableURL, groupID, day, 3)
		if err != nil {
			return err
		}
		if len(taskIDs) == 0 {
			log.Warn().
				Str("group_id", groupID).
				Int64("parent_task_id", opts.ParentTaskID).
				Msg("webhook result: group has no child tasks; skip creating row")
			continue
		}
		if _, err := store.createPending(ctx, webhookResultCreateInput{
			BizType:      WebhookBizTypePiracyGeneralSearch,
			ParentTaskID: opts.ParentTaskID,
			GroupID:      groupID,
			TaskIDs:      taskIDs,
			DramaInfo:    dramaInfoJSON,
		}); err != nil {
			return err
		}
	}
	return nil
}

func fetchDramaInfoJSONByBookID(ctx context.Context, client *taskagent.FeishuClient, dramaTableURL, bookID string) (string, error) {
	if client == nil {
		return "", errors.New("feishu client is nil")
	}
	rawURL := strings.TrimSpace(dramaTableURL)
	if rawURL == "" {
		return "", nil
	}
	field := strings.TrimSpace(taskagent.DefaultDramaFields().DramaID)
	if field == "" {
		return "", errors.New("drama id field mapping is empty")
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(field, "is", strings.TrimSpace(bookID)))
	rows, err := client.FetchBitableRows(ctx, rawURL, &taskagent.FeishuTaskQueryOptions{Filter: filter, Limit: 1})
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", sql.ErrNoRows
	}
	raw, err := json.Marshal(rows[0].Fields)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func fetchGroupTaskIDsWithRetry(ctx context.Context, client *taskagent.FeishuClient, taskTableURL, groupID, day string, attempts int) ([]int64, error) {
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		ids, err := fetchGroupTaskIDs(ctx, client, taskTableURL, groupID, day)
		if err == nil && len(ids) > 0 {
			return ids, nil
		}
		if err != nil {
			lastErr = err
		}
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(i+1) * time.Second):
			}
		}
	}
	return nil, lastErr
}

func fetchGroupTaskIDs(ctx context.Context, client *taskagent.FeishuClient, taskTableURL, groupID, day string) ([]int64, error) {
	if client == nil {
		return nil, errors.New("feishu client is nil")
	}
	fields := taskagent.DefaultTaskFields()
	groupField := strings.TrimSpace(fields.GroupID)
	if groupField == "" {
		return nil, errors.New("task table group id field mapping is empty")
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(groupField, "is", strings.TrimSpace(groupID)))
	if datetimeField := strings.TrimSpace(fields.Datetime); datetimeField != "" && strings.TrimSpace(day) != "" {
		if dayTime, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(day), time.Local); err == nil {
			tsMs := strconv.FormatInt(dayTime.UnixMilli(), 10)
			filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(datetimeField, "is", "ExactDate", tsMs))
		}
	}
	opts := &taskagent.FeishuTaskQueryOptions{
		Filter:     filter,
		Limit:      200,
		IgnoreView: true,
	}
	table, err := client.FetchTaskTableWithOptions(ctx, strings.TrimSpace(taskTableURL), nil, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "fetch group tasks failed, group=%s", groupID)
	}
	if table == nil || len(table.Rows) == 0 {
		return nil, nil
	}
	ids := make([]int64, 0, len(table.Rows))
	for _, row := range table.Rows {
		if row.TaskID != 0 {
			ids = append(ids, row.TaskID)
		}
	}
	return uniqueInt64(ids), nil
}

func dayString(parent *time.Time, raw string) string {
	if parent != nil {
		return parent.In(time.Local).Format("2006-01-02")
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		if len(trimmed) == 10 {
			return time.Unix(n, 0).In(time.Local).Format("2006-01-02")
		}
		return time.UnixMilli(n).In(time.Local).Format("2006-01-02")
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.In(time.Local).Format("2006-01-02")
	}
	return ""
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// WebhookResultCreatorConfig controls how the creator scans the task table
// and creates webhook result rows for external-task flows (e.g. 视频录屏采集).
type WebhookResultCreatorConfig struct {
	TaskBitableURL    string
	WebhookBitableURL string

	// AppFilter optionally filters tasks by App column.
	AppFilter string

	// PollInterval controls how often the creator scans the task table.
	PollInterval time.Duration

	// BatchLimit caps how many tasks are processed per scan.
	BatchLimit int

	// ScanTodayOnly applies Datetime=Today filter when querying Feishu.
	ScanTodayOnly bool
}

// WebhookResultCreator creates pending webhook result rows for tasks that are
// created outside of the agent workflow (so the agent cannot create result rows
// at task creation time).
//
// Current planned usage:
// - BizType=video_screen_capture: one task -> one webhook result row.
type WebhookResultCreator struct {
	store        *webhookResultStore
	taskClient   *taskagent.FeishuClient
	taskTableURL string

	appFilter  string
	interval   time.Duration
	batchLimit int
	todayOnly  bool

	// seen avoids creating duplicate webhook result rows for the same TaskID
	// within a single long-running creator process.
	seen map[int64]time.Time
}

func NewWebhookResultCreator(cfg WebhookResultCreatorConfig) (*WebhookResultCreator, error) {
	taskURL := strings.TrimSpace(firstNonEmpty(cfg.TaskBitableURL, taskagent.EnvString(taskagent.EnvTaskBitableURL, "")))
	if taskURL == "" {
		return nil, errors.New("task bitable url is required")
	}
	store, err := newWebhookResultStore(firstNonEmpty(cfg.WebhookBitableURL, taskagent.EnvString(taskagent.EnvWebhookBitableURL, "")))
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
		batch = 50
	}
	return &WebhookResultCreator{
		store:        store,
		taskClient:   client,
		taskTableURL: taskURL,
		appFilter:    strings.TrimSpace(cfg.AppFilter),
		interval:     interval,
		batchLimit:   batch,
		todayOnly:    cfg.ScanTodayOnly,
		seen:         make(map[int64]time.Time),
	}, nil
}

func (c *WebhookResultCreator) Run(ctx context.Context) error {
	if c == nil {
		return errors.New("webhook result creator is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	log.Info().
		Str("task_bitable", c.taskTableURL).
		Str("webhook_bitable", c.store.table()).
		Str("app_filter", c.appFilter).
		Dur("poll_interval", c.interval).
		Int("batch_limit", c.batchLimit).
		Bool("today_only", c.todayOnly).
		Msg("webhook result creator started")

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("webhook result creator stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := c.processOnce(ctx); err != nil {
				log.Error().Err(err).Msg("webhook result creator scan failed")
			}
		}
	}
}

func (c *WebhookResultCreator) processOnce(ctx context.Context) error {
	tasks, err := c.fetchVideoScreenCaptureTasks(ctx, c.batchLimit)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}

	now := time.Now()
	c.gcSeen(now)

	var errs []string
	created := 0
	skipped := 0
	for _, t := range tasks {
		if t.TaskID <= 0 {
			continue
		}
		if _, ok := c.seen[t.TaskID]; ok {
			skipped++
			continue
		}
		if strings.TrimSpace(t.ItemID) == "" {
			skipped++
			continue
		}
		groupID := strings.TrimSpace(t.GroupID)
		if _, err := c.store.createPending(ctx, webhookResultCreateInput{
			BizType:   WebhookBizTypeVideoScreenCapture,
			GroupID:   groupID,
			TaskIDs:   []int64{t.TaskID},
			DramaInfo: "{}",
		}); err != nil {
			errs = append(errs, fmt.Sprintf("task %d: %v", t.TaskID, err))
			continue
		}
		c.seen[t.TaskID] = now
		created++
	}

	if created > 0 || skipped > 0 {
		log.Info().
			Int("created", created).
			Int("skipped", skipped).
			Int("fetched", len(tasks)).
			Msg("webhook result creator iteration completed")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (c *WebhookResultCreator) fetchVideoScreenCaptureTasks(ctx context.Context, limit int) ([]taskagent.FeishuTaskRow, error) {
	if c == nil || c.taskClient == nil {
		return nil, errors.New("task client is nil")
	}
	if limit <= 0 {
		limit = 50
	}
	fields := taskagent.DefaultTaskFields()
	sceneField := strings.TrimSpace(fields.Scene)
	statusField := strings.TrimSpace(fields.Status)
	if sceneField == "" || statusField == "" {
		return nil, errors.New("task table field mapping is missing (Scene/Status)")
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(sceneField, "is", taskagent.SceneVideoScreenCapture))
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(statusField, "is", taskagent.StatusSuccess))
	if c.todayOnly {
		if dtField := strings.TrimSpace(fields.Datetime); dtField != "" {
			filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(dtField, "is", "Today"))
		}
	}
	if app := strings.TrimSpace(c.appFilter); app != "" {
		if appField := strings.TrimSpace(fields.App); appField != "" {
			filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(appField, "is", app))
		}
	}

	table, err := c.taskClient.FetchTaskTableWithOptions(ctx, c.taskTableURL, nil, &taskagent.FeishuTaskQueryOptions{
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

func (c *WebhookResultCreator) gcSeen(now time.Time) {
	if c == nil || len(c.seen) == 0 {
		return
	}
	// Keep only recent keys to prevent unbounded growth.
	ttl := 24 * time.Hour
	for id, ts := range c.seen {
		if now.Sub(ts) > ttl {
			delete(c.seen, id)
		}
	}
}
