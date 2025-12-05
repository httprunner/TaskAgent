package piracy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/rs/zerolog/log"
)

const (
	defaultDramaTaskApp       = "com.smile.gifmaker"
	defaultAliasSeparator     = "|"
	defaultDramaTaskBatchSize = 500
	dateLayoutYYYYMMDD        = "2006-01-02"
)

type dramaTaskFeishuClient interface {
	FetchBitableRows(ctx context.Context, rawURL string, opts *feishu.TaskQueryOptions) ([]feishu.BitableRow, error)
	CreateTaskRecords(ctx context.Context, rawURL string, records []feishu.TaskRecordInput, override *feishu.TaskFields) ([]string, error)
}

// DramaTaskConfig controls how drama catalog rows are converted into tasks.
type DramaTaskConfig struct {
	Date           string
	App            string
	Scene          string
	TaskTableURL   string
	DramaTableURL  string
	Status         string
	AliasSeparator string
	BatchSize      int

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
		feishuClient, err := feishu.NewClientFromEnv()
		if err != nil {
			return result, err
		}
		client = feishuClient
	}

	fields := feishu.DefaultDramaFields
	if strings.TrimSpace(fields.CaptureDate) == "" {
		return result, errors.New("drama capture date field is not configured (set DRAMA_FIELD_CAPTURE_DATE)")
	}

	opts := &feishu.TaskQueryOptions{}
	rows, err := client.FetchBitableRows(ctx, cfg.DramaTableURL, opts)
	if err != nil {
		return result, fmt.Errorf("fetch drama table rows failed: %w", err)
	}

	aliasSep := cfg.AliasSeparator
	if aliasSep == "" {
		aliasSep = defaultAliasSeparator
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultDramaTaskBatchSize
	}

	type recordKey struct {
		bookID string
		param  string
	}
	seen := make(map[recordKey]struct{})
	records := make([]feishu.TaskRecordInput, 0)
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
		params := buildAliasParams(name, aliasField, aliasSep)
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
			records = append(records, feishu.TaskRecordInput{
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
		cfg.Scene = pool.SceneGeneralSearch
	}
	if cfg.Status == "" {
		cfg.Status = feishu.StatusPending
	}
	if cfg.TaskTableURL == "" {
		cfg.TaskTableURL = strings.TrimSpace(os.Getenv(feishu.EnvTaskBitableURL))
	}
	if cfg.DramaTableURL == "" {
		cfg.DramaTableURL = strings.TrimSpace(os.Getenv("DRAMA_BITABLE_URL"))
	}
	if cfg.AliasSeparator == "" {
		cfg.AliasSeparator = defaultAliasSeparator
	}
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

func buildAliasParams(primary, aliasRaw, sep string) []string {
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
	replacer := strings.NewReplacer("\n", sep, "\r", sep, "，", sep, ",", sep, "｜", sep)
	normalized := replacer.Replace(aliasRaw)
	parts := strings.Split(normalized, sep)
	for _, part := range parts {
		add(&params, seen, part)
	}
	return params
}
