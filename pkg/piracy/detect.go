package piracy

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/rs/zerolog/log"
)

const keySeparator = "\u241F" // Unit Separator symbol to avoid conflicts

// ContentRecord represents a record for piracy detection (通用输入结构)
type ContentRecord struct {
	Params      string  // 短剧名称/Params
	UserID      string  // 用户ID
	UserName    string  // 用户名称
	DurationSec float64 // 时长（秒）
}

// DramaRecord represents a drama record (通用剧单结构)
type DramaRecord struct {
	Params   string  // 短剧名称/Params
	Duration float64 // 总时长（秒）
}

// ApplyDefaults populates missing config fields from environment variables or sensible defaults.
func (c *Config) ApplyDefaults() {
	// Read from environment or use Feishu defaults
	if strings.TrimSpace(c.ParamsField) == "" {
		if paramField := os.Getenv("RESULT_FIELD_PARAMS"); paramField != "" {
			c.ParamsField = paramField
		} else {
			c.ParamsField = feishu.DefaultResultFields.Params
		}
	}
	if strings.TrimSpace(c.UserIDField) == "" {
		if userField := os.Getenv("RESULT_FIELD_USERID"); userField != "" {
			c.UserIDField = userField
		} else {
			c.UserIDField = feishu.DefaultResultFields.UserID
		}
	}
	if strings.TrimSpace(c.DurationField) == "" {
		if durationField := os.Getenv("RESULT_FIELD_DURATION"); durationField != "" {
			c.DurationField = durationField
		} else {
			c.DurationField = feishu.DefaultResultFields.ItemDuration
		}
	}
	if strings.TrimSpace(c.TargetParamsField) == "" {
		if targetParamsField := os.Getenv("TARGET_FIELD_PARAMS"); targetParamsField != "" {
			c.TargetParamsField = targetParamsField
		} else {
			c.TargetParamsField = "Params"
		}
	}
	if strings.TrimSpace(c.DramaIDField) == "" {
		if dramaIDField := os.Getenv("DRAMA_FIELD_ID"); dramaIDField != "" {
			c.DramaIDField = dramaIDField
		} else {
			c.DramaIDField = "DramaID"
		}
	}
	if strings.TrimSpace(c.DramaNameField) == "" {
		if dramaNameField := os.Getenv("DRAMA_FIELD_NAME"); dramaNameField != "" {
			c.DramaNameField = dramaNameField
		} else {
			c.DramaNameField = feishu.DefaultDramaFields.DramaName
		}
	}
	if strings.TrimSpace(c.DramaDurationField) == "" {
		if dramaDurationField := os.Getenv("DRAMA_FIELD_DURATION"); dramaDurationField != "" {
			c.DramaDurationField = dramaDurationField
		} else {
			c.DramaDurationField = "TotalDuration"
		}
	}
	if c.Threshold <= 0 {
		if threshold := os.Getenv("THRESHOLD"); threshold != "" {
			if thresholdFloat, err := strconv.ParseFloat(threshold, 64); err == nil && thresholdFloat > 0 {
				c.Threshold = thresholdFloat
			} else {
				c.Threshold = 0.5
			}
		} else {
			c.Threshold = 0.5
		}
	}
}

// Detect performs piracy detection and returns a report.
func Detect(ctx context.Context, opts Options) (*Report, error) {
	// Apply defaults
	opts.Config.ApplyDefaults()

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

	// Fetch original drama rows (source B) - for duration information
	dramaRows, err := fetchRows(ctx, client, opts.DramaTable)
	if err != nil {
		return nil, err
	}

	return analyzeRows(resultRows, dramaRows, opts.Config), nil
}

// fetchRows retrieves rows from a Feishu table.
func fetchRows(ctx context.Context, client *feishu.Client, cfg TableConfig) ([]Row, error) {
	queryOpts := &feishu.TargetQueryOptions{}
	if cfg.ViewID != "" {
		queryOpts.ViewID = cfg.ViewID
	}
	if cfg.Filter != "" {
		queryOpts.Filter = cfg.Filter
	}
	if cfg.Limit > 0 {
		queryOpts.Limit = cfg.Limit
	}
	return fetchRowsWithRetry(ctx, client, cfg.URL, queryOpts, 3)
}

func fetchRowsWithRetry(ctx context.Context, client *feishu.Client, url string, opts *feishu.TargetQueryOptions, attempts int) ([]Row, error) {
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

// analyzeRows performs the piracy detection analysis on the fetched rows.
func analyzeRows(resultRows, dramaRows []Row, cfg Config) *Report {
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

		userName := strings.TrimSpace(getString(row.Fields, "UserName"))

		contentRecords = append(contentRecords, ContentRecord{
			Params:      params,
			UserID:      userID,
			UserName:    userName,
			DurationSec: duration,
		})
	}

	var dramaRecords []DramaRecord
	for _, row := range dramaRows {
		params := strings.TrimSpace(getString(row.Fields, cfg.DramaNameField))
		if params == "" {
			continue
		}
		duration, ok := getFloat(row.Fields, cfg.DramaDurationField)
		if !ok || duration <= 0 {
			continue
		}
		// Store the maximum duration if there are duplicates
		found := false
		for i, dr := range dramaRecords {
			if dr.Params == params && duration > dr.Duration {
				dramaRecords[i].Duration = duration
				found = true
				break
			}
		}
		if !found {
			dramaRecords = append(dramaRecords, DramaRecord{
				Params:   params,
				Duration: duration,
			})
		}
	}

	return DetectCommon(contentRecords, dramaRecords, cfg.Threshold)
}

// DetectCommon performs the core piracy detection logic using common data structures.
// This function is used by both Feishu and local file modes.
func DetectCommon(contentRecords []ContentRecord, dramaRecords []DramaRecord, threshold float64) *Report {
	// Build drama index: params -> duration
	dramaIndex := make(map[string]float64)
	missingDramas := make(map[string]struct{})

	for _, drama := range dramaRecords {
		params := strings.TrimSpace(drama.Params)
		if params == "" {
			continue
		}
		if drama.Duration <= 0 {
			missingDramas[params] = struct{}{}
			continue
		}
		dramaIndex[params] = drama.Duration
	}

	// Aggregate content records by Params + UserID
	type aggEntry struct {
		sum      float64
		count    int
		userName string
	}
	resultAgg := make(map[string]aggEntry) // key: params + separator + userID

	for _, record := range contentRecords {
		params := strings.TrimSpace(record.Params)
		userID := strings.TrimSpace(record.UserID)
		if params == "" || userID == "" {
			continue
		}

		key := params + keySeparator + userID
		entry := resultAgg[key]
		entry.sum += record.DurationSec
		entry.count++
		// Get user name (first non-empty one if duplicates)
		if entry.userName == "" && record.UserName != "" {
			entry.userName = record.UserName
		}
		resultAgg[key] = entry
	}

	// Build matches where ratio exceeds threshold
	var matches []Match
	missingParamsSet := make(map[string]struct{})

	for key, entry := range resultAgg {
		// Extract params from key
		parts := strings.SplitN(key, keySeparator, 2)
		params := parts[0]

		dramaDuration, exists := dramaIndex[params]
		if !exists || dramaDuration <= 0 {
			missingParamsSet[params] = struct{}{}
			continue
		}

		ratio := entry.sum / dramaDuration
		if ratio > threshold {
			userID := ""
			if len(parts) > 1 {
				userID = parts[1]
			}
			matches = append(matches, Match{
				Params:        params,
				UserID:        userID,
				UserName:      entry.userName,
				SumDuration:   entry.sum,
				TotalDuration: dramaDuration,
				Ratio:         ratio,
				RecordCount:   entry.count,
			})
		}
	}

	// Sort matches by ratio descending
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Ratio > matches[j].Ratio
	})

	// Collect missing params
	missingParams := make([]string, 0, len(missingParamsSet))
	for p := range missingParamsSet {
		missingParams = append(missingParams, p)
	}
	sort.Strings(missingParams)

	// Also add missing dramas
	for p := range missingDramas {
		found := false
		for _, mp := range missingParams {
			if mp == p {
				found = true
				break
			}
		}
		if !found {
			missingParams = append(missingParams, p)
		}
	}

	return &Report{
		Matches:       matches,
		ResultRows:    len(contentRecords),
		TargetRows:    len(dramaRecords),
		MissingParams: missingParams,
		Threshold:     threshold,
	}
}

// extractKeys converts a map's keys to a slice.
func extractKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
