package piracy

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/storage"
	"github.com/rs/zerolog/log"
)

const keySeparator = "\u241F" // Unit Separator symbol to avoid conflicts

// ContentRecord represents a record for piracy detection (通用输入结构)
type ContentRecord struct {
	Params      string  // 搜索词/Params（用于辅助展示）
	BookID      string  // 短剧 ID
	App         string  // App/平台
	UserID      string  // 用户ID
	UserName    string  // 用户名称
	ItemID      string  // 视频 ItemID，用于去重
	DurationSec float64 // 时长（秒）
}

// DramaRecord represents a drama record (通用剧单结构)
type DramaRecord struct {
	BookID   string  // 短剧 ID
	Name     string  // 短剧名称
	Duration float64 // 总时长（秒）
}

type paramTaskKey struct {
	param string
	app   string
}

type paramTaskInfo struct {
	BookID string
	App    string
	Param  string
}

// Detect performs piracy detection and returns a report.
func Detect(ctx context.Context, opts Options) (*Report, error) {
	// Apply defaults
	opts.Config.ApplyDefaults()

	if strings.TrimSpace(opts.TaskTable.URL) == "" {
		return nil, fmt.Errorf("task table url is required for piracy detection")
	}

	// Create Feishu client
	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, err
	}

	// Fetch result rows (source A)
	resultRows, err := fetchRows(ctx, client, opts.ResultTable)
	if err != nil {
		return nil, err
	}
	if len(resultRows) == 0 {
		return &Report{Threshold: opts.Config.Threshold}, nil
	}

	params := collectParamsFromRows(resultRows, opts.Config.ParamsField)
	if len(params) == 0 {
		return &Report{Threshold: opts.Config.Threshold}, nil
	}

	paramTasks, err := fetchParamTaskMap(ctx, client, opts.TaskTable, params, opts.Config)
	if err != nil {
		return nil, err
	}

	bookIDs := collectBookIDs(paramTasks)
	if len(bookIDs) == 0 {
		log.Warn().Msg("no book ids resolved from task table; results will be skipped")
	}

	// Fetch original drama rows (source B) - for duration information
	dramaCfg := opts.DramaTable
	dramaCfg.Filter = CombineFiltersAND(dramaCfg.Filter, BuildParamsFilter(bookIDs, opts.Config.DramaIDField))
	dramaRows, err := fetchRows(ctx, client, dramaCfg)
	if err != nil {
		return nil, err
	}

	return analyzeRows(resultRows, dramaRows, opts.Config, paramTasks), nil
}

// fetchRows retrieves rows from a Feishu table.
func fetchRows(ctx context.Context, client *feishu.Client, cfg TableConfig) ([]Row, error) {
	queryOpts := &feishu.TaskQueryOptions{}
	if cfg.ViewID != "" {
		queryOpts.ViewID = cfg.ViewID
	}
	if cfg.Filter != nil {
		queryOpts.Filter = cfg.Filter
	}
	if cfg.Limit > 0 {
		queryOpts.Limit = cfg.Limit
	}
	rows, err := fetchRowsWithRetry(ctx, client, cfg.URL, queryOpts, 3)
	if err != nil {
		return nil, err
	}
	storage.MirrorDramaRowsIfNeeded(cfg.URL, rows)
	return rows, nil
}

func fetchRowsWithRetry(ctx context.Context, client *feishu.Client, url string, opts *feishu.TaskQueryOptions, attempts int) ([]Row, error) {
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		rows, err := client.FetchBitableRows(ctx, url, opts)
		if err == nil {
			return rows, nil
		}
		lastErr = err
		if !isTransientFeishuError(err) || i == attempts-1 {
			break
		}
		backoff := time.Duration(1<<i) * 500 * time.Millisecond
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

func isTransientFeishuError(err error) bool {
	return isFeishuDataNotReady(err)
}

func isFeishuDataNotReady(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "data not ready") {
		return true
	}
	return strings.Contains(err.Error(), "\"code\":1254607")
}

func collectParamsFromRows(rows []Row, field string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		param := strings.TrimSpace(getString(row.Fields, field))
		if param == "" {
			continue
		}
		if _, ok := seen[param]; ok {
			continue
		}
		seen[param] = struct{}{}
		result = append(result, param)
	}
	return result
}

func fetchParamTaskMap(ctx context.Context, client *feishu.Client, cfg TableConfig, params []string, c Config) (map[paramTaskKey]paramTaskInfo, error) {
	if len(params) == 0 {
		return nil, nil
	}
	filters := []*feishu.FilterInfo{
		cfg.Filter,
		BuildParamsFilter(params, c.TaskParamsField),
		EqFilter(c.TaskStatusField, feishu.StatusSuccess),
		EqFilter(c.TaskSceneField, pool.SceneGeneralSearch),
	}
	taskCfg := cfg
	taskCfg.Filter = CombineFiltersAND(filters...)
	taskRows, err := fetchRows(ctx, client, taskCfg)
	if err != nil {
		return nil, err
	}
	return buildParamTaskMap(taskRows, c), nil
}

func buildParamTaskMap(taskRows []Row, cfg Config) map[paramTaskKey]paramTaskInfo {
	result := make(map[paramTaskKey]paramTaskInfo)
	for _, row := range taskRows {
		param := strings.TrimSpace(getString(row.Fields, cfg.TaskParamsField))
		bookID := strings.TrimSpace(getString(row.Fields, cfg.TaskBookIDField))
		if param == "" || bookID == "" {
			continue
		}
		app := strings.TrimSpace(getString(row.Fields, cfg.TaskAppField))
		info := paramTaskInfo{
			BookID: bookID,
			App:    app,
			Param:  param,
		}
		key := makeParamTaskKey(param, app)
		if _, exists := result[key]; !exists {
			result[key] = info
		}
		emptyKey := makeParamTaskKey(param, "")
		if _, exists := result[emptyKey]; !exists {
			result[emptyKey] = info
		}
	}
	return result
}

func collectBookIDs(mapping map[paramTaskKey]paramTaskInfo) []string {
	if len(mapping) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	result := make([]string, 0, len(mapping))
	for _, info := range mapping {
		id := strings.TrimSpace(info.BookID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func normalizeKeyPart(val string) string {
	return strings.ToLower(strings.TrimSpace(val))
}

func makeParamTaskKey(param, app string) paramTaskKey {
	return paramTaskKey{param: normalizeKeyPart(param), app: normalizeKeyPart(app)}
}

func lookupParamTaskInfo(mapping map[paramTaskKey]paramTaskInfo, param, app string) (paramTaskInfo, bool) {
	if len(mapping) == 0 {
		return paramTaskInfo{}, false
	}
	if info, ok := mapping[makeParamTaskKey(param, app)]; ok {
		return info, true
	}
	info, ok := mapping[makeParamTaskKey(param, "")]
	return info, ok
}

// analyzeRows performs the piracy detection analysis on the fetched rows.
func analyzeRows(resultRows, dramaRows []Row, cfg Config, paramTasks map[paramTaskKey]paramTaskInfo) *Report {
	log.Info().Msg("analyzing result rows with drama rows")

	// Convert Feishu rows to common formats
	var contentRecords []ContentRecord
	for _, row := range resultRows {
		params := strings.TrimSpace(getString(row.Fields, cfg.ParamsField))
		userID := strings.TrimSpace(getString(row.Fields, cfg.UserIDField))
		if params == "" || userID == "" {
			continue
		}

		duration, ok := getFloat(row.Fields, cfg.DurationField)
		if !ok || duration <= 0 {
			continue
		}

		userName := strings.TrimSpace(getString(row.Fields, feishu.DefaultResultFields.UserName))
		itemID := strings.TrimSpace(getString(row.Fields, cfg.ItemIDField))
		app := strings.TrimSpace(getString(row.Fields, cfg.ResultAppField))
		bookID := strings.TrimSpace(getString(row.Fields, cfg.TaskBookIDField))
		if info, ok := lookupParamTaskInfo(paramTasks, params, app); ok {
			if bookID == "" {
				bookID = strings.TrimSpace(info.BookID)
			}
			if app == "" {
				app = strings.TrimSpace(info.App)
			}
		}
		if bookID == "" || app == "" {
			continue
		}

		contentRecords = append(contentRecords, ContentRecord{
			Params:      params,
			BookID:      bookID,
			App:         app,
			UserID:      userID,
			UserName:    userName,
			ItemID:      itemID,
			DurationSec: duration,
		})
	}

	dramaIndex := make(map[string]DramaRecord)
	for _, row := range dramaRows {
		bookID := strings.TrimSpace(getString(row.Fields, cfg.DramaIDField))
		if bookID == "" {
			continue
		}
		duration, ok := getFloat(row.Fields, cfg.DramaDurationField)
		if !ok || duration <= 0 {
			continue
		}
		name := strings.TrimSpace(getString(row.Fields, cfg.DramaNameField))
		if existing, ok := dramaIndex[bookID]; !ok || duration > existing.Duration {
			dramaIndex[bookID] = DramaRecord{BookID: bookID, Name: name, Duration: duration}
		}
	}
	dramaRecords := make([]DramaRecord, 0, len(dramaIndex))
	for _, dr := range dramaIndex {
		dramaRecords = append(dramaRecords, dr)
	}

	return DetectCommon(contentRecords, dramaRecords, cfg.Threshold)
}

// DetectCommon performs the core piracy detection logic using common data structures.
// This function is used by both Feishu and local file modes.
func DetectCommon(contentRecords []ContentRecord, dramaRecords []DramaRecord, threshold float64) *Report {
	// Build drama index: BookID -> record
	dramaIndex := make(map[string]DramaRecord)
	for _, drama := range dramaRecords {
		bookID := strings.TrimSpace(drama.BookID)
		if bookID == "" || drama.Duration <= 0 {
			continue
		}
		if existing, ok := dramaIndex[bookID]; !ok || drama.Duration > existing.Duration {
			dramaIndex[bookID] = drama
		}
	}

	// Aggregate content records by BookID + UserID + App
	type aggEntry struct {
		sum         float64
		count       int
		userName    string
		seenItems   map[string]struct{}
		sampleParam string
	}
	resultAgg := make(map[string]aggEntry)

	for _, record := range contentRecords {
		bookID := strings.TrimSpace(record.BookID)
		userID := strings.TrimSpace(record.UserID)
		app := strings.TrimSpace(record.App)
		if bookID == "" || userID == "" || app == "" {
			continue
		}

		key := strings.Join([]string{bookID, userID, app}, keySeparator)
		entry := resultAgg[key]
		if record.ItemID != "" {
			if entry.seenItems == nil {
				entry.seenItems = make(map[string]struct{})
			}
			if _, exists := entry.seenItems[record.ItemID]; exists {
				resultAgg[key] = entry
				continue
			}
			entry.seenItems[record.ItemID] = struct{}{}
		}
		entry.sum += record.DurationSec
		entry.count++
		if entry.userName == "" && record.UserName != "" {
			entry.userName = record.UserName
		}
		if entry.sampleParam == "" && record.Params != "" {
			entry.sampleParam = record.Params
		}
		resultAgg[key] = entry
	}

	var matches []Match
	missingBookIDs := make(map[string]string)

	for key, entry := range resultAgg {
		parts := strings.SplitN(key, keySeparator, 3)
		bookID := parts[0]
		userID := ""
		if len(parts) > 1 {
			userID = parts[1]
		}
		app := ""
		if len(parts) > 2 {
			app = parts[2]
		}

		drama, exists := dramaIndex[bookID]
		if !exists || drama.Duration <= 0 {
			if _, recorded := missingBookIDs[bookID]; !recorded {
				missingBookIDs[bookID] = entry.sampleParam
			}
			continue
		}

		ratio := entry.sum / drama.Duration
		if ratio > threshold {
			matches = append(matches, Match{
				Params:        entry.sampleParam,
				BookID:        bookID,
				DramaName:     strings.TrimSpace(drama.Name),
				App:           app,
				UserID:        userID,
				UserName:      entry.userName,
				SumDuration:   entry.sum,
				TotalDuration: drama.Duration,
				Ratio:         ratio,
				RecordCount:   entry.count,
			})
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Ratio > matches[j].Ratio
	})

	missingParams := make([]string, 0, len(missingBookIDs))
	for bookID, sample := range missingBookIDs {
		if strings.TrimSpace(sample) != "" {
			missingParams = append(missingParams, fmt.Sprintf("%s (%s)", bookID, sample))
		} else {
			missingParams = append(missingParams, bookID)
		}
	}
	sort.Strings(missingParams)

	return &Report{
		Matches:       matches,
		ResultRows:    len(contentRecords),
		TaskRows:      len(dramaRecords),
		MissingParams: missingParams,
		Threshold:     threshold,
	}
}
