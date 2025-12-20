package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type WebhookResultCreateOptions struct {
	TaskBitableURL    string
	WebhookBitableURL string
	DramaBitableURL   string

	ParentDatetime    *time.Time
	ParentDatetimeRaw string

	BookID       string
	GroupIDs     []string
	ExtraTaskIDs []int64
}

const (
	WebhookBizTypePiracyGeneralSearch = "piracy_general_search"
	WebhookBizTypeVideoScreenCapture  = "video_screen_capture"
	WebhookBizTypeSingleURLCapture    = "single_url_capture"
)

// CreateWebhookResultsForGroups creates one webhook result row per GroupID under a parent task.
// It is designed for the "综合页搜索 -> 盗版筛查" flow where child tasks are generated and webhook
// should only fire after all TaskIDs complete (success/error).
func CreateWebhookResultsForGroups(ctx context.Context, opts WebhookResultCreateOptions) error {
	log.Info().Str("book_id", opts.BookID).Strs("group_ids", opts.GroupIDs).Msg("creating webhook tasks for groups")
	store, err := newWebhookResultStore(firstNonEmpty(opts.WebhookBitableURL, taskagent.EnvString(taskagent.EnvWebhookBitableURL, "")))
	if err != nil {
		return err
	}
	if store == nil || strings.TrimSpace(store.table()) == "" {
		log.Warn().Msg("webhook result table not configured; skip creating webhook result rows")
		return nil
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

	dramaInfoJSON, _ := fetchDramaInfoJSONByBookID(ctx, client,
		firstNonEmpty(opts.DramaBitableURL, taskagent.EnvString("DRAMA_BITABLE_URL", "")), bookID)
	day := dayString(opts.ParentDatetime, opts.ParentDatetimeRaw)
	var dateMs int64
	if strings.TrimSpace(day) != "" {
		if dayTime, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(day), time.Local); err == nil {
			dateMs = dayTime.UTC().UnixMilli()
		}
	}

	for _, groupID := range groupIDs {
		existing, err := store.getExistingByBizGroupAndDay(ctx, WebhookBizTypePiracyGeneralSearch, groupID, day)
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
				Str("group_id", groupID).Str("date", day).
				Msg("webhook result: group has no child tasks; skip creating row")
			continue
		}

		allTaskIDs := append(taskIDs, opts.ExtraTaskIDs...)
		allTaskIDs = uniqueInt64(allTaskIDs)

		if _, err := store.createPending(ctx, webhookResultCreateInput{
			BizType:   WebhookBizTypePiracyGeneralSearch,
			GroupID:   groupID,
			TaskIDs:   allTaskIDs,
			DramaInfo: dramaInfoJSON,
			DateMs:    dateMs,
		}); err != nil {
			return err
		}
		log.Info().Str("group_id", groupID).Ints64("task_ids", allTaskIDs).Str("date", day).
			Msg("webhook piracy creator: create webhook result for group successful")
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
	normalized := normalizeDramaInfoForStorage(rows[0].Fields)
	raw, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func normalizeDramaInfoForStorage(raw map[string]any) map[string]any {
	flat := flattenDramaFields(raw, taskagent.DefaultDramaFields())
	out := make(map[string]any, len(flat))
	for key, val := range flat {
		str, _ := val.(string)
		str = strings.TrimSpace(str)
		if str == "" {
			continue
		}
		out[key] = str
	}
	return out
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
			log.Error().Err(err).Str("group_id", groupID).Str("day", day).
				Int("attempt", i+1).Msg("fetch group task ids failed; retrying")
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

	// ScanDate applies Datetime=ExactDate(YYYY-MM-DD, local time) filter when querying Feishu.
	// When empty, no Datetime filter is applied.
	ScanDate string
	// ScanDateToday overwrites ScanDate with today's local date on each iteration.
	// When true, the creator only scans tasks for the current day.
	ScanDateToday bool

	// EnableSingleURLCapture enables creating rows for Scene=单个链接采集
	// (BizType=single_url_capture). Rows are keyed by (GroupID, DatetimeDay).
	EnableSingleURLCapture bool

	// BizType controls which biz type the creator should process. When empty, the
	// creator uses the legacy behavior: process video_screen_capture tasks and,
	// when EnableSingleURLCapture is true, also process single_url_capture tasks.
	// Supported values:
	//   - piracy_general_search
	//   - video_screen_capture
	//   - single_url_capture
	BizType string

	// SkipExisting controls whether the creator should skip creating or updating
	// webhook rows when an existing row for <BizType, GroupID, Date> is found.
	// This is primarily used for backfill flows where only missing rows should
	// be created while leaving historical rows untouched.
	SkipExisting bool
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

	appFilter    string
	interval     time.Duration
	batchLimit   int
	scanDate     string
	enableSU     bool
	bizType      string
	skipExisting bool
	scanDateToday bool

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
	batch := cfg.BatchLimit
	if batch <= 0 {
		batch = 50
	}
	scanDate := strings.TrimSpace(cfg.ScanDate)
	bizType := strings.TrimSpace(cfg.BizType)

	enableSU := cfg.EnableSingleURLCapture
	if bizType == WebhookBizTypeSingleURLCapture {
		enableSU = true
	}

	return &WebhookResultCreator{
		store:        store,
		taskClient:   client,
		taskTableURL: taskURL,
		appFilter:    strings.TrimSpace(cfg.AppFilter),
		interval:     interval,
		batchLimit:   batch,
		scanDate:     scanDate,
		enableSU:     enableSU,
		bizType:      bizType,
		skipExisting: cfg.SkipExisting,
		scanDateToday: cfg.ScanDateToday,
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
	interval := c.interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	log.Info().
		Str("task_bitable", c.taskTableURL).
		Str("webhook_bitable", c.store.table()).
		Str("app_filter", c.appFilter).
		Str("biz_type", c.bizType).
		Dur("poll_interval", interval).
		Int("batch_limit", c.batchLimit).
		Str("scan_date", c.scanDate).
		Bool("enable_single_url_capture", c.enableSU).
		Msg("webhook result creator started")

	c.refreshScanDate()
	if err := c.processOnce(ctx); err != nil {
		log.Error().Err(err).Msg("webhook result creator scan failed")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("webhook result creator stopped")
			return ctx.Err()
		case <-ticker.C:
			c.refreshScanDate()
			if err := c.processOnce(ctx); err != nil {
				log.Error().Err(err).Msg("webhook result creator scan failed")
			}
		}
	}
}

func (c *WebhookResultCreator) RunOnce(ctx context.Context) error {
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
		Str("biz_type", c.bizType).
		Int("batch_limit", c.batchLimit).
		Str("scan_date", c.scanDate).
		Bool("enable_single_url_capture", c.enableSU).
		Bool("run_once", true).
		Msg("webhook result creator started")
	c.refreshScanDate()
	return c.processOnce(ctx)
}

func (c *WebhookResultCreator) refreshScanDate() {
	if c == nil || !c.scanDateToday {
		return
	}
	c.scanDate = time.Now().In(time.Local).Format("2006-01-02")
}

func (c *WebhookResultCreator) processOnce(ctx context.Context) error {
	biz := strings.TrimSpace(c.bizType)
	switch biz {
	case WebhookBizTypePiracyGeneralSearch:
		return c.processOncePiracyGroups(ctx)
	case WebhookBizTypeVideoScreenCapture:
		return c.processOnceVideoScreenCapture(ctx)
	case WebhookBizTypeSingleURLCapture:
		created, updated, skipped, err := c.createSingleURLCaptureWebhookResults(ctx)
		if created > 0 || updated > 0 || skipped > 0 {
			log.Info().
				Int("created", created).
				Int("updated", updated).
				Int("skipped", skipped).
				Str("scan_date", c.scanDate).
				Str("app_filter", c.appFilter).
				Msg("single_url_capture creator iteration completed")
		}
		return err
	default:
		// process video_screen_capture tasks and,
		// when enabled, also process single_url_capture tasks.
		if err := c.processOnceVideoScreenCapture(ctx); err != nil {
			return err
		}
		if c.enableSU {
			created, updated, skipped, err := c.createSingleURLCaptureWebhookResults(ctx)
			if created > 0 || updated > 0 || skipped > 0 {
				log.Info().
					Int("created", created).
					Int("updated", updated).
					Int("skipped", skipped).
					Str("scan_date", c.scanDate).
					Str("app_filter", c.appFilter).
					Msg("single_url_capture creator iteration completed")
			}
			return err
		}
		return nil
	}
}

func (c *WebhookResultCreator) processOnceVideoScreenCapture(ctx context.Context) error {
	tasks, err := c.fetchVideoScreenCaptureTasks(ctx, c.batchLimit)
	if err != nil {
		return err
	}

	now := time.Now()
	c.gcSeen(now)

	var errs []string
	created := 0
	updated := 0
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
		day := dayString(t.Datetime, t.DatetimeRaw)
		var dateMs int64
		if strings.TrimSpace(day) != "" {
			if dayTime, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(day), time.Local); err == nil {
				dateMs = dayTime.UTC().UnixMilli()
			}
		}
		groupID := strings.TrimSpace(t.GroupID)
		if _, err := c.store.createPending(ctx, webhookResultCreateInput{
			BizType:   WebhookBizTypeVideoScreenCapture,
			GroupID:   groupID,
			TaskIDs:   []int64{t.TaskID},
			DramaInfo: "{}",
			DateMs:    dateMs,
		}); err != nil {
			errs = append(errs, fmt.Sprintf("task %d: %v", t.TaskID, err))
			continue
		}
		c.seen[t.TaskID] = now
		created++
	}

	if c.enableSU {
		cr, up, sk, err := c.createSingleURLCaptureWebhookResults(ctx)
		created += cr
		updated += up
		skipped += sk
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if created > 0 || skipped > 0 {
		log.Info().
			Int("created", created).
			Int("updated", updated).
			Int("skipped", skipped).
			Int("fetched", len(tasks)).
			Msg("webhook result creator iteration completed")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// processOncePiracyGroups scans the task table for the given ScanDate and
// creates piracy_general_search webhook result rows per GroupID. TaskIDs for
// each row contain:
//   - all child task IDs under the same GroupID (profile / collection / anchor)
//   - all general_search TaskIDs for the same BookID (and App) on that day
func (c *WebhookResultCreator) processOncePiracyGroups(ctx context.Context) error {
	if c == nil || c.taskClient == nil {
		return errors.New("webhook result creator is nil or task client is nil")
	}

	rows, err := c.fetchPiracyTasksForDay(ctx)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		log.Warn().Str("scan_date", c.scanDate).Str("app_filter", c.appFilter).
			Msg("webhook piracy creator: no tasks found for date")
		return nil
	}
	log.Info().Str("scan_date", c.scanDate).Str("app_filter", c.appFilter).
		Int("fetched", len(rows)).
		Msg("webhook piracy creator: tasks fetched for date")

	type bookKey struct {
		BookID string
		App    string
	}

	groupsByBook := make(map[bookKey]map[string]struct{})
	generalByBook := make(map[bookKey][]int64)

	for _, row := range rows {
		bookID := strings.TrimSpace(row.BookID)
		app := strings.TrimSpace(row.App)
		if bookID == "" || app == "" {
			continue
		}
		key := bookKey{BookID: bookID, App: app}
		scene := strings.TrimSpace(row.Scene)
		switch scene {
		case taskagent.SceneGeneralSearch:
			if row.TaskID > 0 {
				generalByBook[key] = append(generalByBook[key], row.TaskID)
			}
		case taskagent.SceneProfileSearch, taskagent.SceneCollection, taskagent.SceneAnchorCapture:
			gid := strings.TrimSpace(row.GroupID)
			if gid == "" {
				continue
			}
			m := groupsByBook[key]
			if m == nil {
				m = make(map[string]struct{})
				groupsByBook[key] = m
			}
			m[gid] = struct{}{}
		default:
			continue
		}
	}

	if len(groupsByBook) == 0 {
		log.Warn().Str("scan_date", c.scanDate).Str("app_filter", c.appFilter).
			Msg("webhook piracy creator: no groups found for date")
		return nil
	}
	log.Info().Str("scan_date", c.scanDate).Str("app_filter", c.appFilter).
		Int("group_count", len(groupsByBook)).
		Msg("webhook piracy creator: groups found for date")

	totalGroups := 0
	for _, groupSet := range groupsByBook {
		totalGroups += len(groupSet)
	}

	var parentDatetime *time.Time
	if trimmed := strings.TrimSpace(c.scanDate); trimmed != "" {
		if dayTime, err := time.ParseInLocation("2006-01-02", trimmed, time.Local); err == nil {
			parentDatetime = &dayTime
		}
	}

	created := 0
	skipped := 0
	groupIndex := 0
	var errs []string

	for key, groupSet := range groupsByBook {
		extraTaskIDs := uniqueInt64(generalByBook[key])

		for groupID := range groupSet {
			groupIndex++
			log.Info().
				Int("group_index", groupIndex).
				Int("group_total", totalGroups).
				Str("group_id", groupID).
				Str("date", c.scanDate).
				Msg("webhook piracy creator: processing group")
			trimmedGroup := strings.TrimSpace(groupID)
			if trimmedGroup == "" {
				continue
			}

			if c.skipExisting {
				existing, err := c.store.getExistingByBizGroupAndDay(ctx, WebhookBizTypePiracyGeneralSearch, trimmedGroup, c.scanDate)
				if err != nil {
					errs = append(errs, err.Error())
					continue
				}
				if existing != nil && strings.TrimSpace(existing.RecordID) != "" {
					skipped++
					log.Warn().Str("group_id", trimmedGroup).Str("date", c.scanDate).Int("skipped", skipped).
						Msg("webhook piracy creator: existing webhook result found; skipping")
					continue
				}
			}

			opts := WebhookResultCreateOptions{
				TaskBitableURL:    c.taskTableURL,
				WebhookBitableURL: c.store.table(),
				ParentDatetime:    parentDatetime,
				ParentDatetimeRaw: "",
				BookID:            key.BookID,
				GroupIDs:          []string{trimmedGroup},
				ExtraTaskIDs:      extraTaskIDs,
			}
			if err := CreateWebhookResultsForGroups(ctx, opts); err != nil {
				log.Error().Err(err).Str("group_id", trimmedGroup).Str("date", c.scanDate).
					Msg("webhook piracy creator: failed to create webhook result for group")
				errs = append(errs, fmt.Sprintf("book=%s, group=%s: %v", key.BookID, trimmedGroup, err))
				continue
			}
			created++
		}
	}

	log.Info().
		Int("created", created).
		Int("skipped", skipped).
		Int("book_count", len(groupsByBook)).
		Int("fetched", len(rows)).
		Str("scan_date", c.scanDate).
		Str("app_filter", c.appFilter).
		Msg("webhook piracy creator iteration completed")

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

type singleURLCaptureGroupCandidate struct {
	GroupID string
	Day     string
	DayKey  int64
	BookID  string
	TaskIDs []int64
}

func (c *WebhookResultCreator) createSingleURLCaptureWebhookResults(ctx context.Context) (created, updated, skipped int, retErr error) {
	rows, err := c.fetchSingleURLCaptureTasks(ctx, 0)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(rows) == 0 {
		log.Warn().
			Str("scan_date", c.scanDate).
			Str("app_filter", c.appFilter).
			Msg("single_url_capture creator: no tasks found for date")
		return 0, 0, 0, nil
	}

	candidates := make(map[string]singleURLCaptureGroupCandidate, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.Scene) != taskagent.SceneSingleURLCapture {
			skipped++
			continue
		}
		if row.TaskID <= 0 {
			skipped++
			continue
		}
		groupID := strings.TrimSpace(row.GroupID)
		if groupID == "" {
			mappedApp := feishusdk.MapAppValue(strings.TrimSpace(row.App))
			if mappedApp == "" {
				mappedApp = strings.TrimSpace(row.App)
			}
			trimmedBook := strings.TrimSpace(row.BookID)
			if trimmedBook == "" {
				trimmedBook = "unknown_book"
			}
			trimmedUser := strings.TrimSpace(row.UserID)
			if trimmedUser == "" {
				trimmedUser = "unknown_user"
			}
			groupID = fmt.Sprintf("%s_%s_%s", mappedApp, trimmedBook, trimmedUser)
		}
		day := dayString(row.Datetime, row.DatetimeRaw)
		if day == "" {
			day = strings.TrimSpace(c.scanDate)
		}
		if strings.TrimSpace(day) == "" {
			skipped++
			continue
		}
		dayKey := dayKeyYYYYMMDD(day)
		if dayKey <= 0 {
			skipped++
			continue
		}
		key := fmt.Sprintf("%s|%d", groupID, dayKey)
		cand := candidates[key]
		if cand.GroupID == "" {
			cand = singleURLCaptureGroupCandidate{
				GroupID: groupID,
				Day:     day,
				DayKey:  dayKey,
				BookID:  strings.TrimSpace(row.BookID),
			}
		} else if cand.BookID == "" {
			cand.BookID = strings.TrimSpace(row.BookID)
		}
		cand.TaskIDs = append(cand.TaskIDs, row.TaskID)
		candidates[key] = cand
	}

	if len(candidates) == 0 {
		log.Warn().
			Int("skipped", skipped).
			Str("scan_date", c.scanDate).
			Str("app_filter", c.appFilter).
			Msg("single_url_capture creator: no candidates with valid group/day found")
		return 0, 0, skipped, nil
	}

	for _, cand := range candidates {
		taskIDs := uniqueInt64(cand.TaskIDs)
		sort.Slice(taskIDs, func(i, j int) bool { return taskIDs[i] < taskIDs[j] })
		if len(taskIDs) == 0 {
			skipped++
			continue
		}

		dramaInfo := "{}"
		if strings.TrimSpace(cand.BookID) != "" {
			if raw, err := fetchDramaInfoJSONByBookID(ctx, c.taskClient, taskagent.EnvString("DRAMA_BITABLE_URL", ""), cand.BookID); err == nil {
				if strings.TrimSpace(raw) != "" {
					dramaInfo = raw
				}
			}
		}

		existing, err := c.store.getExistingByBizGroupAndDay(ctx, WebhookBizTypeSingleURLCapture, cand.GroupID, cand.Day)
		if err != nil {
			return created, updated, skipped, err
		}
		if existing == nil || strings.TrimSpace(existing.RecordID) == "" {
			var dateMs int64
			if strings.TrimSpace(cand.Day) != "" {
				if dayTime, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(cand.Day), time.Local); err == nil {
					dateMs = dayTime.UTC().UnixMilli()
				}
			}
			if _, err := c.store.createPending(ctx, webhookResultCreateInput{
				BizType:   WebhookBizTypeSingleURLCapture,
				GroupID:   cand.GroupID,
				TaskIDs:   taskIDs,
				DramaInfo: dramaInfo,
				DateMs:    dateMs,
			}); err != nil {
				return created, updated, skipped, err
			}
			created++
			continue
		}

		state := strings.ToLower(strings.TrimSpace(existing.Status))
		if state == WebhookResultSuccess || state == WebhookResultError {
			skipped++
			continue
		}

		existingIDs := uniqueInt64(existing.TaskIDs)
		sort.Slice(existingIDs, func(i, j int) bool { return existingIDs[i] < existingIDs[j] })

		needsTaskUpdate := !sameInt64Slice(existingIDs, taskIDs)
		needsDramaUpdate := strings.TrimSpace(existing.DramaInfo) == "" || strings.TrimSpace(existing.DramaInfo) == "{}"
		if !needsTaskUpdate && !needsDramaUpdate {
			skipped++
			continue
		}

		upd := webhookResultUpdate{}
		if needsTaskUpdate {
			upd.TaskIDs = &taskIDs
		}
		if needsDramaUpdate && strings.TrimSpace(dramaInfo) != "" {
			upd.DramaInfo = ptrString(strings.TrimSpace(dramaInfo))
		}
		if err := c.store.update(ctx, existing.RecordID, upd); err != nil {
			return created, updated, skipped, err
		}
		updated++
	}
	log.Info().
		Int("created", created).
		Int("updated", updated).
		Int("skipped", skipped).
		Str("scan_date", c.scanDate).
		Str("app_filter", c.appFilter).
		Msg("single_url_capture creator: processed single_url_capture tasks")
	return created, updated, skipped, nil
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
	if dtField := strings.TrimSpace(fields.Datetime); dtField != "" {
		if cond := exactDateCondition(dtField, c.scanDate); cond != nil {
			filter.Conditions = append(filter.Conditions, cond)
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

func (c *WebhookResultCreator) fetchPiracyTasksForDay(ctx context.Context) ([]taskagent.FeishuTaskRow, error) {
	if c == nil || c.taskClient == nil {
		return nil, errors.New("task client is nil")
	}
	fields := taskagent.DefaultTaskFields()
	sceneField := strings.TrimSpace(fields.Scene)
	datetimeField := strings.TrimSpace(fields.Datetime)
	if sceneField == "" || datetimeField == "" {
		return nil, errors.New("task table field mapping is missing (Scene/Datetime)")
	}

	filter := taskagent.NewFeishuFilterInfo("and")
	if cond := exactDateCondition(datetimeField, c.scanDate); cond != nil {
		filter.Conditions = append(filter.Conditions, cond)
	}
	if app := strings.TrimSpace(c.appFilter); app != "" {
		if appField := strings.TrimSpace(fields.App); appField != "" {
			filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(appField, "is", app))
		}
	}

	// Scenes involved in piracy group flows: general_search + profile_search + collection + anchor_capture.
	filter.Children = append(filter.Children, taskagent.NewFeishuChildrenFilter("or",
		taskagent.NewFeishuCondition(sceneField, "is", taskagent.SceneGeneralSearch),
		taskagent.NewFeishuCondition(sceneField, "is", taskagent.SceneProfileSearch),
		taskagent.NewFeishuCondition(sceneField, "is", taskagent.SceneCollection),
		taskagent.NewFeishuCondition(sceneField, "is", taskagent.SceneAnchorCapture),
	))

	table, err := c.taskClient.FetchTaskTableWithOptions(ctx, c.taskTableURL, nil, &taskagent.FeishuTaskQueryOptions{
		Filter:     filter,
		Limit:      0, // no explicit limit; fetch all matching rows for the day
		IgnoreView: true,
	})
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, nil
	}

	rows := table.Rows
	if len(rows) == 0 {
		return nil, nil
	}

	// Extra safety: re-filter by scanDate on the client side to ensure the
	// behavior matches the expected "Datetime = ExactDate(ScanDate)" semantics,
	// even if the upstream filter behaves differently.
	targetDay := strings.TrimSpace(c.scanDate)
	if targetDay == "" {
		return rows, nil
	}
	filtered := make([]taskagent.FeishuTaskRow, 0, len(rows))
	for _, row := range rows {
		if day := dayString(row.Datetime, row.DatetimeRaw); day == targetDay {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

func (c *WebhookResultCreator) fetchSingleURLCaptureTasks(ctx context.Context, limit int) ([]taskagent.FeishuTaskRow, error) {
	if c == nil || c.taskClient == nil {
		return nil, errors.New("task client is nil")
	}
	// limit <= 0 means no explicit limit (fetch all matching rows).
	if limit < 0 {
		limit = 0
	}
	fields := taskagent.DefaultTaskFields()
	sceneField := strings.TrimSpace(fields.Scene)
	statusField := strings.TrimSpace(fields.Status)
	if sceneField == "" || statusField == "" {
		return nil, errors.New("task table field mapping is missing (Scene/Status)")
	}

	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(sceneField, "is", taskagent.SceneSingleURLCapture))
	filter.Children = append(filter.Children, taskagent.NewFeishuChildrenFilter("or",
		taskagent.NewFeishuCondition(statusField, "is", taskagent.StatusSuccess),
		taskagent.NewFeishuCondition(statusField, "is", taskagent.StatusFailed),
		taskagent.NewFeishuCondition(statusField, "is", taskagent.StatusError),
	))

	if dtField := strings.TrimSpace(fields.Datetime); dtField != "" {
		if cond := exactDateCondition(dtField, c.scanDate); cond != nil {
			filter.Conditions = append(filter.Conditions, cond)
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

func dayKeyYYYYMMDD(day string) int64 {
	trimmed := strings.TrimSpace(day)
	if trimmed == "" {
		return 0
	}
	parsed, err := time.ParseInLocation("2006-01-02", trimmed, time.Local)
	if err != nil {
		return 0
	}
	y, m, d := parsed.Date()
	return int64(y*10000 + int(m)*100 + d)
}

func sameInt64Slice(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func exactDateCondition(fieldName, date string) *taskagent.FeishuCondition {
	trimmed := strings.TrimSpace(date)
	if trimmed == "" || strings.TrimSpace(fieldName) == "" {
		return nil
	}
	dayTime, err := time.ParseInLocation("2006-01-02", trimmed, time.Local)
	if err != nil {
		return nil
	}
	tsMs := strconv.FormatInt(dayTime.UnixMilli(), 10)
	return taskagent.NewFeishuCondition(fieldName, "is", "ExactDate", tsMs)
}
