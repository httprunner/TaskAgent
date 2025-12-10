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
	)

	cmd := &cobra.Command{
		Use:   "drama-tasks",
		Short: "Create 综合页搜索 tasks from drama catalog aliases",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := DramaTaskConfig{
				Date:            flagDate,
				App:             firstNonEmpty(rootApp, ""),
				Scene:           "",
				TaskTableURL:    firstNonEmpty(rootTaskURL, ""),
				DramaTableURL:   flagDramaURL,
				AliasSeparators: flagAliasSeps,
				BatchSize:       flagBatchSize,
			}
			if cfg.App == "" {
				cfg.App = defaultDramaTaskApp
			}

			res, err := CreateDramaSearchTasks(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			log.Info().
				Str("date", res.Date).
				Int("dramas", res.DramaCount).
				Int("params", res.ParamCount).
				Int("created", res.CreatedCount).
				Msg("drama tasks created")
			for _, detail := range res.Details {
				log.Debug().
					Str("drama", detail.DramaName).
					Str("book_id", detail.BookID).
					Int("params", detail.ParamCount).
					Msg("drama params expanded")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagDate, "date", "", "采集日期 (YYYY-MM-DD)")
	cmd.Flags().StringVar(&flagDramaURL, "drama-url", "", "覆盖 $DRAMA_BITABLE_URL 的剧单表 URL")
	cmd.Flags().StringSliceVar(&flagAliasSeps, "alias-sep", []string{"|", "｜"}, "搜索别名分隔符列表（默认支持 | 和 ｜）")
	cmd.Flags().IntVar(&flagBatchSize, "batch-size", 500, "写入任务时的批大小 (<=500)")
	_ = cmd.MarkFlagRequired("date")

	return cmd
}

const (
	defaultDramaTaskApp       = "com.smile.gifmaker"
	defaultAliasSeparator     = "|"
	defaultDramaTaskBatchSize = 500
	dateLayoutYYYYMMDD        = "2006-01-02"
)

var defaultAliasSeparators = []string{defaultAliasSeparator, "｜"}

type dramaTaskFeishuClient interface {
	FetchBitableRows(ctx context.Context, rawURL string, opts *taskagent.FeishuTaskQueryOptions) ([]taskagent.BitableRow, error)
	CreateTaskRecords(ctx context.Context, rawURL string, records []taskagent.TaskRecordInput, override *taskagent.FeishuTaskFields) ([]string, error)
}

// DramaTaskConfig controls how drama catalog rows are converted into tasks.
type DramaTaskConfig struct {
	Date            string
	App             string
	Scene           string
	TaskTableURL    string
	DramaTableURL   string
	Status          string
	AliasSeparators []string
	BatchSize       int

	client dramaTaskFeishuClient
}

// DramaTaskResult reports how many dramas and Params were converted into tasks.
type DramaTaskResult struct {
	Date         string
	DramaCount   int
	ParamCount   int
	CreatedCount int
	Details      []DramaTaskDetail
}

// DramaTaskDetail summarizes per-drama fan-out.
type DramaTaskDetail struct {
	DramaName  string
	BookID     string
	ParamCount int
}

// CreateDramaSearchTasks loads drama metadata filtered by capture date and writes
// pending 综合页搜索 tasks (one per drama name + alias) into the task table.
func CreateDramaSearchTasks(ctx context.Context, cfg DramaTaskConfig) (*DramaTaskResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg.applyDefaults()
	result := &DramaTaskResult{Date: cfg.Date}

	if strings.TrimSpace(cfg.Date) == "" {
		return result, errors.New("date is required (YYYY-MM-DD)")
	}
	targetDate, err := time.ParseInLocation(dateLayoutYYYYMMDD, cfg.Date, time.Local)
	if err != nil {
		return result, fmt.Errorf("invalid date %q: %w", cfg.Date, err)
	}
	if strings.TrimSpace(cfg.DramaTableURL) == "" {
		return result, errors.New("drama table url is required (set DRAMA_BITABLE_URL or --drama-url)")
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

	fields := taskagent.DefaultDramaFields()
	if strings.TrimSpace(fields.CaptureDate) == "" {
		return result, errors.New("drama capture date field is not configured (set DRAMA_FIELD_CAPTURE_DATE)")
	}

	opts := &taskagent.FeishuTaskQueryOptions{}
	rows, err := client.FetchBitableRows(ctx, cfg.DramaTableURL, opts)
	if err != nil {
		return result, fmt.Errorf("fetch drama table rows failed: %w", err)
	}

	aliasSeps := normalizeAliasSeparators(cfg.AliasSeparators)
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultDramaTaskBatchSize
	}

	type recordKey struct {
		bookID string
		param  string
	}
	seen := make(map[recordKey]struct{})
	records := make([]taskagent.TaskRecordInput, 0)
	details := make([]DramaTaskDetail, 0)

	for _, row := range rows {
		if row.Fields == nil {
			continue
		}
		captureRaw := getString(row.Fields, fields.CaptureDate)
		captureTime, ok := matchCaptureDate(captureRaw, targetDate)
		if !ok {
			continue
		}
		captureRaw = strings.TrimSpace(captureRaw)

		name := strings.TrimSpace(getString(row.Fields, fields.DramaName))
		bookID := strings.TrimSpace(getString(row.Fields, fields.DramaID))
		if name == "" || bookID == "" {
			continue
		}

		aliasField := ""
		if strings.TrimSpace(fields.SearchAlias) != "" {
			aliasField = getString(row.Fields, fields.SearchAlias)
		}
		params := buildAliasParams(name, aliasField, aliasSeps)
		if len(params) == 0 {
			continue
		}

		added := 0
		for _, param := range params {
			key := recordKey{bookID: bookID, param: param}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			records = append(records, taskagent.TaskRecordInput{
				App:         cfg.App,
				Scene:       cfg.Scene,
				Params:      param,
				BookID:      bookID,
				Status:      cfg.Status,
				Datetime:    captureTime,
				DatetimeRaw: captureRaw,
			})
			added++
		}
		if added > 0 {
			details = append(details, DramaTaskDetail{DramaName: name, BookID: bookID, ParamCount: added})
		}
	}

	result.Details = details
	result.DramaCount = len(details)
	result.ParamCount = len(records)

	if len(records) == 0 {
		log.Info().
			Str("date", cfg.Date).
			Msg("no drama rows matched capture date, nothing to create")
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
			Msg("drama tasks chunk written")
	}

	result.CreatedCount = created
	return result, nil
}

func (cfg *DramaTaskConfig) applyDefaults() {
	if cfg.App == "" {
		cfg.App = defaultDramaTaskApp
	}
	if cfg.Scene == "" {
		cfg.Scene = taskagent.SceneGeneralSearch
	}
	if cfg.Status == "" {
		cfg.Status = taskagent.StatusPending
	}
	if cfg.TaskTableURL == "" {
		cfg.TaskTableURL = taskagent.EnvString(taskagent.EnvTaskBitableURL, "")
	}
	if cfg.DramaTableURL == "" {
		cfg.DramaTableURL = taskagent.EnvString("DRAMA_BITABLE_URL", "")
	}
	cfg.AliasSeparators = normalizeAliasSeparators(cfg.AliasSeparators)
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultDramaTaskBatchSize
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

func normalizeAliasSeparators(seps []string) []string {
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

func buildAliasParams(primary, aliasRaw string, seps []string) []string {
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
	normalizedSeps := normalizeAliasSeparators(seps)
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
		case []interface{}:
			if text := extractTextArray(v); text != "" {
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

// extractTextArray flattens Feishu rich-text array values into a plain string.
func extractTextArray(arr []interface{}) string {
	if len(arr) == 0 {
		return ""
	}
	parts := make([]string, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			if txt, ok := m["text"].(string); ok {
				trimmed := strings.TrimSpace(txt)
				if trimmed != "" {
					parts = append(parts, trimmed)
				}
			}
		}
	}
	return strings.Join(parts, " ")
}
