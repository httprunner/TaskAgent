package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newDramaTasksCmd() *cobra.Command {
	var (
		flagDate      string
		flagDramaURL  string
		flagAliasSeps []string
		flagBatchSize int
		flagSkip      bool
	)

	cmd := &cobra.Command{
		Use:   "drama-tasks",
		Short: "Create 综合页搜索 tasks from drama catalog aliases",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := TaskConfig{
				Date:              flagDate,
				App:               firstNonEmpty(rootApp, defaultTaskApp),
				Scene:             taskagent.SceneGeneralSearch,
				TaskTableURL:      firstNonEmpty(rootTaskURL, ""),
				SourceTableURL:    taskagent.EnvString(taskagent.EnvDramaBitableURL, flagDramaURL),
				KeywordSeparators: flagAliasSeps,
				BatchSize:         flagBatchSize,
				SkipExisting:      flagSkip,
			}
			res, err := CreateSearchTasks(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			log.Info().
				Str("date", res.Date).
				Int("dramas", res.SourceCount).
				Int("params", res.TotalParams).
				Int("created", res.CreatedCount).
				Msg("drama tasks created")
			for _, detail := range res.Details {
				log.Debug().
					Str("drama", detail.DramaName).
					Str("book_id", detail.BookID).
					Int("params", detail.ExpandedParams).
					Msg("drama params expanded")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagDate, "date", "", "登记日期 (YYYY-MM-DD)")
	cmd.Flags().StringVar(&flagDramaURL, "drama-url", "", "覆盖 $DRAMA_BITABLE_URL 的剧单表 URL")
	cmd.Flags().StringSliceVar(&flagAliasSeps, "alias-sep", []string{"|", "｜"}, "搜索别名分隔符列表（默认支持 | 和 ｜）")
	cmd.Flags().IntVar(&flagBatchSize, "batch-size", 500, "写入任务时的批大小 (<=500)")
	cmd.Flags().BoolVar(&flagSkip, "skip-existing", false, "若源表中 TaskID 非空则跳过创建")
	_ = cmd.MarkFlagRequired("date")

	return cmd
}

func newAccountTasksCmd() *cobra.Command {
	var (
		flagDate        string
		flagAccountURL  string
		flagKeywordSeps []string
		flagBatchSize   int
		flagSkip        bool
	)

	cmd := &cobra.Command{
		Use:   "account-tasks",
		Short: "Create 个人页搜索 tasks from account registry",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := TaskConfig{
				Date:              flagDate,
				App:               firstNonEmpty(rootApp, defaultTaskApp),
				Scene:             taskagent.SceneProfileSearch,
				TaskTableURL:      firstNonEmpty(rootTaskURL, ""),
				SourceTableURL:    taskagent.EnvString(taskagent.EnvAccountBitableURL, flagAccountURL),
				KeywordSeparators: flagKeywordSeps,
				BatchSize:         flagBatchSize,
				SkipExisting:      flagSkip,
			}
			res, err := CreateSearchTasks(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			log.Info().
				Str("date", res.Date).
				Int("accounts", res.SourceCount).
				Int("params", res.TotalParams).
				Int("created", res.CreatedCount).
				Msg("profile search tasks created")
			for _, detail := range res.Details {
				log.Debug().
					Str("biz_task_id", detail.BizTaskID).
					Str("account_id", detail.AccountID).
					Str("book_id", detail.BookID).
					Int("params", detail.ExpandedParams).
					Msg("profile search params expanded")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagDate, "date", "", "采集日期 (YYYY-MM-DD)")
	cmd.Flags().StringVar(&flagAccountURL, "account-url", "", "覆盖 $ACCOUNT_BITABLE_URL 的账号登记表 URL")
	cmd.Flags().StringSliceVar(&flagKeywordSeps, "keyword-sep", []string{"|", "｜"}, "搜索词分隔符列表（默认支持 | 和 ｜）")
	cmd.Flags().IntVar(&flagBatchSize, "batch-size", 500, "写入任务时的批大小 (<=500)")
	cmd.Flags().BoolVar(&flagSkip, "skip-existing", false, "若源表中 TaskID 非空则跳过创建")
	_ = cmd.MarkFlagRequired("date")

	return cmd
}

const (
	defaultTaskApp        = "com.smile.gifmaker"
	defaultAliasSeparator = "|"
	defaultTaskBatchSize  = 500
	dateLayoutYYYYMMDD    = "2006-01-02"
)

var defaultAliasSeparators = []string{defaultAliasSeparator, "｜"}

type dramaTaskFeishuClient interface {
	FetchBitableRows(ctx context.Context, rawURL string, opts *taskagent.FeishuTaskQueryOptions) ([]taskagent.BitableRow, error)
	CreateTaskRecords(ctx context.Context, rawURL string, records []taskagent.TaskRecordInput, override *taskagent.FeishuTaskFields) ([]string, error)
}

// TaskConfig controls how drama catalog rows are converted into tasks.
type TaskConfig struct {
	SourceTableURL    string // 剧单登记表、账号登记表
	Date              string
	App               string
	Scene             string
	TaskTableURL      string
	Status            string
	KeywordSeparators []string
	BatchSize         int
	SkipExisting      bool

	client dramaTaskFeishuClient
}

// TaskResult reports how many dramas and Params were converted into tasks.
type TaskResult struct {
	Date         string
	SourceCount  int
	TotalParams  int
	CreatedCount int
	Details      []TaskDetail
}

// TaskDetail summarizes per-drama fan-out.
type TaskDetail struct {
	BizTaskID      string
	DramaName      string
	BookID         string
	AccountID      string
	ExpandedParams int
}

// CreateSearchTasks loads source rows filtered by date and writes pending search tasks into the task table.
// When AccountID is present, it creates 个人页搜索 tasks; otherwise it creates 综合页搜索 tasks.
func CreateSearchTasks(ctx context.Context, cfg TaskConfig) (*TaskResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg.applyDefaults()
	result := &TaskResult{Date: cfg.Date}

	if strings.TrimSpace(cfg.Date) == "" {
		return result, errors.New("date is required (YYYY-MM-DD)")
	}
	targetDate, err := time.ParseInLocation(dateLayoutYYYYMMDD, cfg.Date, time.Local)
	if err != nil {
		return result, fmt.Errorf("invalid date %q: %w", cfg.Date, err)
	}
	if strings.TrimSpace(cfg.SourceTableURL) == "" {
		return result, errors.New("source table url is required")
	}
	if strings.TrimSpace(cfg.TaskTableURL) == "" {
		return result, errors.New("task table url is required (set TASK_BITABLE_URL or --task-url)")
	}

	client := cfg.client
	if client == nil {
		feishuClient, err := taskagent.NewFeishuClientFromEnv()
		if err != nil {
			return result, err
		}
		client = feishuClient
	}

	sourceFields := taskagent.DefaultSourceFields()

	opts := &taskagent.FeishuTaskQueryOptions{}
	rows, err := client.FetchBitableRows(ctx, cfg.SourceTableURL, opts)
	if err != nil {
		return result, fmt.Errorf("fetch source table rows failed: %w", err)
	}

	keywordsSeps := normalizeKeywordsSeparators(cfg.KeywordSeparators)
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultTaskBatchSize
	}

	type recordKey struct {
		bizTaskID string
		bookID    string
		accountID string
		param     string
		app       string
		date      string
	}
	seen := make(map[recordKey]struct{})
	records := make([]taskagent.TaskRecordInput, 0)
	details := make([]TaskDetail, 0)

	for _, row := range rows {
		log.Debug().Any("row", row).Msg("processing source row")
		if row.Fields == nil {
			continue
		}

		accountID := strings.TrimSpace(getString(row.Fields, sourceFields.AccountID))
		rowTaskID := strings.TrimSpace(getString(row.Fields, sourceFields.TaskID))
		if cfg.SkipExisting && rowTaskID != "" {
			log.Warn().Any("row", row).Msg("skip existing task")
			continue
		}
		captureRaw := getString(row.Fields, sourceFields.CaptureDate)
		captureTime, ok := matchCaptureDate(captureRaw, targetDate)
		if !ok {
			continue
		}
		captureRaw = strings.TrimSpace(captureRaw)
		if captureRaw == "" && captureTime != nil {
			captureRaw = captureTime.Format(dateLayoutYYYYMMDD)
		}

		bookID := strings.TrimSpace(getString(row.Fields, sourceFields.DramaID))
		if bookID == "" {
			continue
		}

		app := cfg.App
		if platform := strings.TrimSpace(getString(row.Fields, sourceFields.Platform)); platform != "" {
			app = normalizeAppPackage(platform, cfg.App)
		}
		if strings.TrimSpace(app) == "" {
			continue
		}

		groupID := ""
		bizTaskID := ""
		params := []string(nil)
		name := ""
		searchRaw := ""
		if accountID != "" {
			name = strings.TrimSpace(getString(row.Fields, sourceFields.DramaName))
			bizTaskID = strings.TrimSpace(getString(row.Fields, sourceFields.BizTaskID))
			searchRaw = strings.TrimSpace(getString(row.Fields, sourceFields.SearchKeywords))
			if searchRaw == "" {
				continue
			}
			groupApp := taskagent.MapAppValue(app)
			if strings.TrimSpace(groupApp) == "" {
				groupApp = app
			}
			groupID = fmt.Sprintf("%s_%s_%s", groupApp, bookID, accountID)
		} else {
			name = strings.TrimSpace(getString(row.Fields, sourceFields.DramaName))
			if name == "" {
				continue
			}
			searchRaw = strings.TrimSpace(getString(row.Fields, sourceFields.SearchKeywords))
		}
		params = buildKeywordsParams(name, searchRaw, keywordsSeps)
		if len(params) == 0 {
			continue
		}

		added := 0
		for _, param := range params {
			key := recordKey{
				bizTaskID: bizTaskID,
				bookID:    bookID,
				accountID: accountID,
				param:     param,
				app:       app,
				date:      captureRaw,
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			record := taskagent.TaskRecordInput{
				App:         app,
				Scene:       cfg.Scene,
				Params:      param,
				BookID:      bookID,
				Status:      cfg.Status,
				Datetime:    captureTime,
				DatetimeRaw: captureRaw,
			}
			if accountID != "" {
				record.BizTaskID = bizTaskID
				record.UserID = accountID
				record.GroupID = groupID
			}
			records = append(records, record)
			added++
		}
		if added > 0 {
			details = append(details, TaskDetail{
				BizTaskID:      bizTaskID,
				DramaName:      name,
				BookID:         bookID,
				AccountID:      accountID,
				ExpandedParams: added,
			})
		}
	}

	result.Details = details
	result.SourceCount = len(details)
	result.TotalParams = len(records)

	if len(records) == 0 {
		log.Info().
			Str("date", cfg.Date).
			Msg("no search rows matched date, nothing to create")
		return result, nil
	}
	created := 0
	for start := 0; start < len(records); start += batchSize {
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[start:end]
		ids, err := client.CreateTaskRecords(ctx, cfg.TaskTableURL, chunk, nil)
		if err != nil {
			return result, fmt.Errorf("create task records failed: %w", err)
		}
		created += len(ids)
		log.Info().
			Int("chunk", len(ids)).
			Int("offset", start).
			Msg("search tasks chunk written")
	}
	result.CreatedCount = created
	return result, nil
}

func (cfg *TaskConfig) applyDefaults() {
	if cfg.App == "" {
		cfg.App = defaultTaskApp
	}
	if cfg.Status == "" {
		cfg.Status = taskagent.StatusPending
	}
	if cfg.TaskTableURL == "" {
		cfg.TaskTableURL = taskagent.EnvString(taskagent.EnvTaskBitableURL, "")
	}
	cfg.KeywordSeparators = normalizeKeywordsSeparators(cfg.KeywordSeparators)
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultTaskBatchSize
	}
}

func matchCaptureDate(raw string, target time.Time) (*time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, false
	}
	if trimmed == target.Format(dateLayoutYYYYMMDD) {
		ts := target
		return &ts, true
	}
	if t, ok := parseFlexibleDate(trimmed); ok {
		local := t.In(time.Local)
		if local.Format(dateLayoutYYYYMMDD) == target.Format(dateLayoutYYYYMMDD) {
			return &local, true
		}
	}
	return nil, false
}

func parseFlexibleDate(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	if ts, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return timestampToTime(ts), true
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return timestampToTime(int64(f)), true
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02",
		"2006/01/02",
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		"20060102",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, trimmed, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func timestampToTime(ts int64) time.Time {
	if ts <= 0 {
		return time.Unix(0, 0).In(time.Local)
	}
	switch {
	case ts > 1e12:
		return time.UnixMilli(ts).In(time.Local)
	case ts > 1e9:
		return time.Unix(ts, 0).In(time.Local)
	default:
		return time.Unix(ts, 0).In(time.Local)
	}
}

func normalizeKeywordsSeparators(seps []string) []string {
	seen := make(map[string]struct{})
	cleaned := make([]string, 0, len(seps))
	for _, sep := range seps {
		trimmed := strings.TrimSpace(sep)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		cleaned = append(cleaned, trimmed)
	}
	if len(cleaned) == 0 {
		return append([]string(nil), defaultAliasSeparators...)
	}
	return cleaned
}

func buildKeywordsParams(primary, aliasRaw string, seps []string) []string {
	add := func(order *[]string, seen map[string]struct{}, value string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return
		}
		if _, exists := seen[trimmed]; exists {
			return
		}
		seen[trimmed] = struct{}{}
		*order = append(*order, trimmed)
	}

	params := make([]string, 0)
	seen := make(map[string]struct{})
	add(&params, seen, primary)

	if strings.TrimSpace(aliasRaw) == "" {
		return params
	}
	normalizedSeps := normalizeKeywordsSeparators(seps)
	primarySep := normalizedSeps[0]
	replacePairs := make([]string, 0, 2*len(normalizedSeps))
	for _, sep := range normalizedSeps[1:] {
		replacePairs = append(replacePairs, sep, primarySep)
	}
	processed := aliasRaw
	if len(replacePairs) > 0 {
		replacer := strings.NewReplacer(replacePairs...)
		processed = replacer.Replace(aliasRaw)
	}
	parts := strings.Split(processed, primarySep)
	for _, part := range parts {
		add(&params, seen, part)
	}
	return params
}

func normalizeAppPackage(platform, fallback string) string {
	trimmed := strings.TrimSpace(platform)
	if trimmed == "" {
		return strings.TrimSpace(fallback)
	}
	switch trimmed {
	case "快手":
		return defaultTaskApp
	default:
		return trimmed
	}
}

// getString reads a field value from a Feishu bitable row fields map as string.
func getString(fields map[string]any, name string) string {
	if fields == nil || strings.TrimSpace(name) == "" {
		return ""
	}
	if val, ok := fields[name]; ok {
		switch v := val.(type) {
		case string:
			return strings.TrimSpace(v)
		case []byte:
			return strings.TrimSpace(string(v))
		case json.Number:
			return strings.TrimSpace(v.String())
		case map[string]any:
			if text := extractTextFromAny(v); text != "" {
				return text
			}
		case []interface{}:
			if text := extractTextFromAny(v); text != "" {
				return text
			}
		default:
			if b, err := json.Marshal(v); err == nil {
				return strings.TrimSpace(string(b))
			}
			return ""
		}
	}
	return ""
}

// extractTextFromAny tries to normalize Feishu rich-text values into a plain string.
func extractTextFromAny(val any) string {
	switch v := val.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	case []interface{}:
		return extractTextArray(v)
	case map[string]any:
		if text, ok := v["text"].(string); ok {
			return strings.TrimSpace(text)
		}
		if nested, ok := v["value"]; ok {
			return extractTextFromAny(nested)
		}
		if nested, ok := v["elements"]; ok {
			return extractTextFromAny(nested)
		}
		if nested, ok := v["content"]; ok {
			return extractTextFromAny(nested)
		}
	}
	return ""
}

// extractTextArray flattens Feishu rich-text array values into a plain string.
func extractTextArray(arr []interface{}) string {
	if len(arr) == 0 {
		return ""
	}
	parts := make([]string, 0, len(arr))
	for _, item := range arr {
		switch v := item.(type) {
		case map[string]any:
			if txt, ok := v["text"].(string); ok {
				trimmed := strings.TrimSpace(txt)
				if trimmed != "" {
					parts = append(parts, trimmed)
				}
			} else if nested, ok := v["value"]; ok {
				if nestedText := extractTextFromAny(nested); nestedText != "" {
					parts = append(parts, nestedText)
				}
			}
		case string:
			trimmed := strings.TrimSpace(v)
			if trimmed != "" {
				parts = append(parts, trimmed)
			}
		case []interface{}:
			if nested := extractTextArray(v); nested != "" {
				parts = append(parts, nested)
			}
		}
	}
	return strings.Join(parts, " ")
}
