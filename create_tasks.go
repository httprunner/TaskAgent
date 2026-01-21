package taskagent

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/rs/zerolog/log"
)

const (
	DefaultSearchTaskApp       = "com.smile.gifmaker"
	DefaultSearchTaskBatchSize = 500
	dateLayoutYYYYMMDD         = "2006-01-02"
)

var defaultSearchKeywordSeparators = []string{"|", "｜"}

// DefaultSearchKeywordSeparators returns a copy of default separators.
func DefaultSearchKeywordSeparators() []string {
	return append([]string(nil), defaultSearchKeywordSeparators...)
}

type SearchTaskClient interface {
	FetchBitableRows(ctx context.Context, rawURL string, opts *FeishuTaskQueryOptions) ([]BitableRow, error)
	CreateTaskRecords(ctx context.Context, rawURL string, records []TaskRecordInput, override *FeishuTaskFields) ([]string, error)
}

// SearchTaskConfig controls how drama/account rows are converted into tasks.
type SearchTaskConfig struct {
	SourceTableURL    string // 剧单登记表、账号登记表
	Date              string
	App               string
	Scene             string
	TaskTableURL      string
	Status            string
	KeywordSeparators []string
	BatchSize         int
	SkipExisting      bool
	Client            SearchTaskClient
}

// SearchTaskResult reports how many source rows and Params were converted into tasks.
type SearchTaskResult struct {
	Date         string
	SourceCount  int
	TotalParams  int
	CreatedCount int
	Details      []SearchTaskDetail
}

// SearchTaskDetail summarizes per-source fan-out.
type SearchTaskDetail struct {
	BizTaskID      string
	DramaName      string
	BookID         string
	AccountID      string
	ExpandedParams int
}

// CreateSearchTasks loads source rows filtered by date and writes pending search tasks into the task table.
// When AccountID is present, it creates 个人页搜索 tasks; otherwise it creates 综合页搜索 tasks.
func CreateSearchTasks(ctx context.Context, cfg SearchTaskConfig) (*SearchTaskResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg.applyDefaults()
	result := &SearchTaskResult{Date: cfg.Date}

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

	client := cfg.Client
	if client == nil {
		feishuClient, err := NewFeishuClientFromEnv()
		if err != nil {
			return result, err
		}
		client = feishuClient
	}

	sourceFields := DefaultSourceFields()

	opts := &FeishuTaskQueryOptions{}
	rows, err := client.FetchBitableRows(ctx, cfg.SourceTableURL, opts)
	if err != nil {
		return result, fmt.Errorf("fetch source table rows failed: %w", err)
	}

	keywordsSeps := normalizeKeywordsSeparators(cfg.KeywordSeparators)
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultSearchTaskBatchSize
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
	records := make([]TaskRecordInput, 0)
	details := make([]SearchTaskDetail, 0)

	for _, row := range rows {
		log.Debug().Any("row", row).Msg("processing source row")
		if row.Fields == nil {
			continue
		}

		accountID := feishusdk.BitableFieldString(row.Fields, sourceFields.AccountID)
		accountName := feishusdk.BitableFieldString(row.Fields, sourceFields.AccountName)
		rowTaskID := feishusdk.BitableFieldString(row.Fields, sourceFields.TaskID)
		if cfg.SkipExisting && rowTaskID != "" {
			log.Warn().Any("row", row).Msg("skip existing task")
			continue
		}
		captureRaw := feishusdk.BitableFieldString(row.Fields, sourceFields.CaptureDate)
		captureTime, ok := matchCaptureDate(captureRaw, targetDate)
		if !ok {
			continue
		}
		captureRaw = strings.TrimSpace(captureRaw)
		if captureRaw == "" && captureTime != nil {
			captureRaw = captureTime.Format(dateLayoutYYYYMMDD)
		}

		bookID := feishusdk.BitableFieldString(row.Fields, sourceFields.DramaID)
		if bookID == "" {
			continue
		}

		app := cfg.App
		if platform := feishusdk.BitableFieldString(row.Fields, sourceFields.Platform); platform != "" {
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
			name = feishusdk.BitableFieldString(row.Fields, sourceFields.DramaName)
			bizTaskID = feishusdk.BitableFieldString(row.Fields, sourceFields.BizTaskID)
			searchRaw = feishusdk.BitableFieldString(row.Fields, sourceFields.SearchKeywords)
			if searchRaw == "" {
				continue
			}
			groupApp := MapAppValue(app)
			if strings.TrimSpace(groupApp) == "" {
				groupApp = app
			}
			groupID = fmt.Sprintf("%s_%s_%s", groupApp, bookID, accountID)
		} else {
			name = feishusdk.BitableFieldString(row.Fields, sourceFields.DramaName)
			if name == "" {
				continue
			}
			searchRaw = feishusdk.BitableFieldString(row.Fields, sourceFields.SearchKeywords)
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
			record := TaskRecordInput{
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
				record.UserName = accountName
				record.GroupID = groupID
			}
			records = append(records, record)
			added++
		}
		if added > 0 {
			details = append(details, SearchTaskDetail{
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

func (cfg *SearchTaskConfig) applyDefaults() {
	if cfg.App == "" {
		cfg.App = DefaultSearchTaskApp
	}
	if cfg.Status == "" {
		cfg.Status = StatusPending
	}
	if cfg.TaskTableURL == "" {
		cfg.TaskTableURL = EnvString(EnvTaskBitableURL, "")
	}
	cfg.KeywordSeparators = normalizeKeywordsSeparators(cfg.KeywordSeparators)
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultSearchTaskBatchSize
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
		return append([]string(nil), defaultSearchKeywordSeparators...)
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
		return DefaultSearchTaskApp
	default:
		return trimmed
	}
}
