package piracy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/storage"
	"github.com/rs/zerolog/log"
)

// BackfillConfig 描述回放综合页任务的配置。
type BackfillConfig struct {
	Date         string // YYYY-MM-DD，可选
	Sync         bool   // true 时写回子任务
	SkipExisting bool   // true 时若已有 group 任务则跳过
	DBPath       string // 覆盖 capture_tasks sqlite 路径
	TaskTableURL string // 覆盖任务状态表 URL
	AppOverride  string // 当任务缺少 App 字段时使用
	Scene        string // 默认综合页搜索
	Status       string // 默认 success
}

// BackfillStats 记录执行统计。
type BackfillStats struct {
	Scanned         int
	Matched         int
	Created         int
	SkippedExisting int
	Failures        map[int64]string
}

// BackfillTasks 根据配置重新检测综合页任务，并在需要时补写子任务。
func BackfillTasks(ctx context.Context, cfg BackfillConfig) (*BackfillStats, error) {
	stats := &BackfillStats{Failures: make(map[int64]string)}
	if ctx == nil {
		ctx = context.Background()
	}
	applyBackfillDefaults(&cfg)

	tasks, err := loadBackfillTasks(ctx, cfg)
	if err != nil {
		return stats, err
	}
	if len(tasks) == 0 {
		log.Info().Msg("piracy backfill: 没有满足条件的综合页任务")
		return stats, nil
	}

	reporter := NewReporter()
	if reporter == nil || !reporter.IsConfigured() {
		return stats, fmt.Errorf("piracy reporter 配置不完整，请确认 RESULT_BITABLE_URL/DRAMA_BITABLE_URL/TASK_BITABLE_URL 已设置")
	}

	var feishuClient *feishu.Client
	if cfg.Sync || cfg.SkipExisting {
		if strings.TrimSpace(cfg.TaskTableURL) == "" {
			return stats, fmt.Errorf("未配置任务表 URL，请设置 --task-url 或环境变量 %s", feishu.EnvTaskBitableURL)
		}
		client, err := feishu.NewClientFromEnv()
		if err != nil {
			return stats, fmt.Errorf("初始化 Feishu Client 失败: %w", err)
		}
		feishuClient = client
	}
	if cfg.SkipExisting && !cfg.Sync {
		log.Warn().Msg("piracy backfill: --skip-existing 在未开启 --sync 时无效，将被忽略")
	}

	results := make([]*backfillResult, 0, len(tasks))
	var errs []string
	for _, task := range tasks {
		res := &backfillResult{TaskID: task.TaskID}
		res.Params = strings.TrimSpace(task.Params)
		res.App = firstNonEmpty(task.App, cfg.AppOverride)
		res.Scene = cfg.Scene
		results = append(results, res)
		stats.Scanned++
		details, err := reporter.DetectMatchesWithDetails(ctx, []string{task.Params})
		if err != nil {
			log.Error().Err(err).Int64("task_id", task.TaskID).Msg("piracy backfill: 盗版检测失败")
			errs = append(errs, fmt.Sprintf("task %d detect: %v", task.TaskID, err))
			stats.Failures[task.TaskID] = err.Error()
			res.Action = "detect-error"
			res.Error = err.Error()
			continue
		}
		if len(details) == 0 {
			log.Info().Int64("task_id", task.TaskID).Msg("piracy backfill: 未检测到疑似账号")
			res.Action = "no-match"
			continue
		}
		stats.Matched++
		res.Matches = len(details)
		if top := firstMatchDetail(details); top != nil {
			res.UserID = strings.TrimSpace(top.Match.UserID)
			res.UserName = strings.TrimSpace(top.Match.UserName)
			res.ItemID = firstVideoItemID(top.Videos)
		}

		if !cfg.Sync {
			log.Info().Int64("task_id", task.TaskID).
				Int("match_groups", len(details)).
				Msg("piracy backfill: 仅检测不写表")
			res.Action = "dry-run"
			continue
		}

		appName := task.App
		if strings.TrimSpace(appName) == "" {
			appName = cfg.AppOverride
		}
		if strings.TrimSpace(appName) == "" {
			msg := "任务缺少 App 字段且未提供 --app/bundleID"
			log.Error().Int64("task_id", task.TaskID).Msg("piracy backfill: " + msg)
			stats.Failures[task.TaskID] = msg
			errs = append(errs, fmt.Sprintf("task %d: %s", task.TaskID, msg))
			res.Action = "invalid"
			res.Error = msg
			continue
		}

		if cfg.SkipExisting && feishuClient != nil {
			day := taskDayString(task.Datetime, task.DatetimeRaw)
			groupIDs := collectTargetGroupIDs(appName, task.BookID, details)
			existing, err := reporter.fetchExistingGroupIDs(ctx, feishuClient, strings.TrimSpace(appName), groupIDs, day)
			if err != nil {
				log.Error().Err(err).Int64("task_id", task.TaskID).Msg("piracy backfill: 检查已有子任务失败")
				errs = append(errs, fmt.Sprintf("task %d check: %v", task.TaskID, err))
				stats.Failures[task.TaskID] = err.Error()
				res.Action = "check-error"
				res.Error = err.Error()
				continue
			}
			if len(existing) > 0 {
				stats.SkippedExisting++
				log.Info().Int64("task_id", task.TaskID).Msg("piracy backfill: 已存在当日子任务，跳过写入")
				res.Action = "skip-existing"
				continue
			}
		}

		if err := reporter.CreateGroupTasksForPiracyMatches(
			ctx, appName, task.TaskID, task.Datetime, task.DatetimeRaw, task.BookID, details); err != nil {
			log.Error().Err(err).Int64("task_id", task.TaskID).Msg("piracy backfill: 写入子任务失败")
			stats.Failures[task.TaskID] = err.Error()
			errs = append(errs, fmt.Sprintf("task %d report: %v", task.TaskID, err))
			res.Action = "write-error"
			res.Error = err.Error()
			continue
		}

		stats.Created++
		log.Info().Int64("task_id", task.TaskID).
			Int("match_groups", len(details)).
			Msg("piracy backfill: 子任务写入完成")
		res.Action = "created"
	}

	printBackfillResults(results)

	log.Info().
		Int("scanned", stats.Scanned).
		Int("matched", stats.Matched).
		Int("created", stats.Created).
		Int("skipped_existing", stats.SkippedExisting).
		Int("failed", len(stats.Failures)).
		Msg("piracy backfill: 汇总")

	if len(errs) > 0 {
		return stats, errors.New(strings.Join(errs, "; "))
	}
	return stats, nil
}

type backfillTask struct {
	TaskID      int64
	Params      string
	App         string
	BookID      string
	DatetimeRaw string
	Datetime    *time.Time
}

type backfillResult struct {
	TaskID   int64
	Matches  int
	Action   string
	Error    string
	App      string
	Scene    string
	Params   string
	UserID   string
	UserName string
	ItemID   string
}

func applyBackfillDefaults(cfg *BackfillConfig) {
	if cfg.Scene == "" {
		cfg.Scene = pool.SceneGeneralSearch
	}
	if cfg.Status == "" {
		cfg.Status = feishu.StatusSuccess
	}
	if cfg.TaskTableURL == "" {
		cfg.TaskTableURL = strings.TrimSpace(os.Getenv(feishu.EnvTaskBitableURL))
	}
	if cfg.AppOverride == "" {
		cfg.AppOverride = strings.TrimSpace(os.Getenv("BUNDLE_ID"))
	}
}

func loadBackfillTasks(ctx context.Context, cfg BackfillConfig) ([]*backfillTask, error) {
	path := strings.TrimSpace(cfg.DBPath)
	if path == "" {
		resolved, err := storage.ResolveDatabasePath()
		if err != nil {
			return nil, fmt.Errorf("解析 sqlite 路径失败: %w", err)
		}
		path = resolved
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("打开 sqlite 失败: %w", err)
	}
	defer db.Close()

	query := `SELECT TaskID, Params, App, BookID, Datetime FROM capture_tasks WHERE Scene = ? AND Status = ? ORDER BY TaskID`
	rows, err := db.QueryContext(ctx, query, cfg.Scene, cfg.Status)
	if err != nil {
		return nil, fmt.Errorf("查询 capture_tasks 失败: %w", err)
	}
	defer rows.Close()

	var (
		tasks      []*backfillTask
		targetDate string
	)
	if trimmed := strings.TrimSpace(cfg.Date); trimmed != "" {
		parsed, err := time.ParseInLocation("2006-01-02", trimmed, time.Local)
		if err != nil {
			return nil, fmt.Errorf("--date 解析失败: %w", err)
		}
		targetDate = parsed.Format("2006-01-02")
	}

	for rows.Next() {
		var (
			taskID   int64
			params   sql.NullString
			app      sql.NullString
			bookID   sql.NullString
			datetime sql.NullString
		)
		if err := rows.Scan(&taskID, &params, &app, &bookID, &datetime); err != nil {
			return nil, fmt.Errorf("读取 capture_tasks 行失败: %w", err)
		}
		if taskID == 0 || strings.TrimSpace(params.String) == "" {
			continue
		}

		parsedTime, parsedRaw := parseBackfillDatetime(datetime.String)
		if targetDate != "" {
			if parsedTime == nil {
				continue
			}
			if parsedTime.In(time.Local).Format("2006-01-02") != targetDate {
				continue
			}
		}

		tasks = append(tasks, &backfillTask{
			TaskID:      taskID,
			Params:      strings.TrimSpace(params.String),
			App:         strings.TrimSpace(app.String),
			BookID:      strings.TrimSpace(bookID.String),
			DatetimeRaw: parsedRaw,
			Datetime:    parsedTime,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历 capture_tasks 行失败: %w", err)
	}
	return tasks, nil
}

func parseBackfillDatetime(raw string) (*time.Time, string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, ""
	}
	layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, trimmed, time.Local); err == nil {
			return &t, trimmed
		}
	}
	if ts, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		var parsed time.Time
		switch {
		case len(trimmed) >= 13 || ts > 1e12:
			parsed = time.UnixMilli(ts)
		default:
			parsed = time.Unix(ts, 0)
		}
		parsed = parsed.In(time.Local)
		return &parsed, trimmed
	}
	return nil, trimmed
}

func groupTasksExist(ctx context.Context, client *feishu.Client, tableURL string, parentTaskID int64) (bool, error) {
	if client == nil || strings.TrimSpace(tableURL) == "" || parentTaskID == 0 {
		return false, fmt.Errorf("检测子任务参数缺失")
	}
	groupField := strings.TrimSpace(feishu.DefaultTaskFields.GroupID)
	if groupField == "" {
		return false, fmt.Errorf("任务表未配置 GroupID 字段")
	}
	filter := feishu.NewFilterInfo("and")
	groupID := fmt.Sprintf("%d_1", parentTaskID)
	filter.Conditions = append(filter.Conditions, feishu.NewCondition(groupField, "is", groupID))

	opts := &feishu.TaskQueryOptions{Filter: filter, Limit: 1}
	table, err := client.FetchTaskTableWithOptions(ctx, tableURL, nil, opts)
	if err != nil {
		return false, err
	}
	return len(table.Rows) > 0, nil
}

func printBackfillResults(results []*backfillResult) {
	if len(results) == 0 {
		return
	}
	table := buildBackfillTable(results)
	if table != "" {
		fmt.Printf("\npiracy backfill results:\n%s\n", table)
	}
}

func sanitizeTableField(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
}

type tableColumn struct {
	Header string
	Value  func(*backfillResult) string
}

func buildBackfillTable(results []*backfillResult) string {
	columns := []tableColumn{
		{Header: "TaskID", Value: func(r *backfillResult) string { return fmt.Sprintf("%d", r.TaskID) }},
		{Header: "App", Value: func(r *backfillResult) string { return r.App }},
		{Header: "Scene", Value: func(r *backfillResult) string { return r.Scene }},
		{Header: "Params", Value: func(r *backfillResult) string { return r.Params }},
		{Header: "UserID", Value: func(r *backfillResult) string { return r.UserID }},
		{Header: "UserName", Value: func(r *backfillResult) string { return r.UserName }},
		{Header: "ItemID", Value: func(r *backfillResult) string { return r.ItemID }},
		{Header: "Matches", Value: func(r *backfillResult) string { return fmt.Sprintf("%d", r.Matches) }},
		{Header: "Action", Value: func(r *backfillResult) string { return r.Action }},
		{Header: "Error", Value: func(r *backfillResult) string { return r.Error }},
	}

	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = displayWidth(col.Header)
	}
	rows := make([][]string, 0, len(results))
	for _, res := range results {
		if res == nil {
			continue
		}
		row := make([]string, len(columns))
		for i, col := range columns {
			val := sanitizeTableField(col.Value(res))
			row[i] = val
			if w := displayWidth(val); w > widths[i] {
				widths[i] = w
			}
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return ""
	}

	var b strings.Builder
	separator := buildTableSeparator(widths)
	b.WriteString(separator)
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = col.Header
	}
	b.WriteString(buildTableRow(headers, widths))
	b.WriteString(separator)
	for _, row := range rows {
		b.WriteString(buildTableRow(row, widths))
	}
	b.WriteString(separator)
	return b.String()
}

func buildTableSeparator(widths []int) string {
	var b strings.Builder
	b.WriteByte('+')
	for _, w := range widths {
		b.WriteString(strings.Repeat("-", w+2))
		b.WriteByte('+')
	}
	b.WriteByte('\n')
	return b.String()
}

func buildTableRow(values []string, widths []int) string {
	var b strings.Builder
	b.WriteByte('|')
	for i, val := range values {
		cell := padDisplayWidth(val, widths[i])
		b.WriteByte(' ')
		b.WriteString(cell)
		b.WriteByte(' ')
		b.WriteByte('|')
	}
	b.WriteByte('\n')
	return b.String()
}

func padDisplayWidth(s string, width int) string {
	diff := width - displayWidth(s)
	if diff <= 0 {
		return s
	}
	return s + strings.Repeat(" ", diff)
}

func displayWidth(s string) int {
	width := 0
	for _, r := range s {
		if r == '\t' {
			width += 4
			continue
		}
		if r == '\n' || r == '\r' {
			continue
		}
		if r <= 0x1F {
			continue
		}
		if r <= 0x7F {
			width++
			continue
		}
		width += 2
	}
	return width
}

func firstMatchDetail(details []MatchDetail) *MatchDetail {
	for i := range details {
		if details[i].Match.Params != "" {
			return &details[i]
		}
	}
	if len(details) > 0 {
		return &details[0]
	}
	return nil
}

func firstVideoItemID(videos []VideoDetail) string {
	for _, v := range videos {
		if trimmed := strings.TrimSpace(v.ItemID); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
