package piracydetect

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/httprunner/TaskAgent/feishu"
)

const keySeparator = "\u241F" // Unit Separator symbol to avoid conflicts

// ApplyDefaults populates missing config fields from environment variables or sensible defaults.
func (c *Config) ApplyDefaults() {
	// Read from environment or use Feishu defaults
	if strings.TrimSpace(c.ParamsField) == "" {
		if paramField := os.Getenv("PARAMS_FIELD"); paramField != "" {
			c.ParamsField = paramField
		} else {
			c.ParamsField = feishu.DefaultResultFields.Params
		}
	}
	if strings.TrimSpace(c.UserIDField) == "" {
		if userField := os.Getenv("USERID_FIELD"); userField != "" {
			c.UserIDField = userField
		} else {
			c.UserIDField = feishu.DefaultResultFields.UserID
		}
	}
	if strings.TrimSpace(c.DurationField) == "" {
		if durationField := os.Getenv("DURATION_FIELD"); durationField != "" {
			c.DurationField = durationField
		} else {
			c.DurationField = feishu.DefaultResultFields.ItemDuration
		}
	}
	if strings.TrimSpace(c.TargetDurationField) == "" {
		if targetDurationField := os.Getenv("TARGET_DURATION_FIELD"); targetDurationField != "" {
			c.TargetDurationField = targetDurationField
		} else {
			c.TargetDurationField = "TotalDuration"
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

	// Fetch target rows (source B)
	targetRows, err := fetchRows(ctx, client, opts.TargetTable)
	if err != nil {
		return nil, err
	}

	return analyzeRows(resultRows, targetRows, opts.Config), nil
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
	return client.FetchBitableRows(ctx, cfg.URL, queryOpts)
}

// analyzeRows performs the piracy detection analysis on the fetched rows.
func analyzeRows(resultRows, targetRows []Row, cfg Config) *Report {
	// Index target durations by Params
	targetIndex := make(map[string]float64)
	missingTargets := make(map[string]struct{})

	for _, row := range targetRows {
		params := strings.TrimSpace(getString(row.Fields, cfg.ParamsField))
		if params == "" {
			continue
		}
		duration, ok := getFloat(row.Fields, cfg.TargetDurationField)
		if !ok || duration <= 0 {
			missingTargets[params] = struct{}{}
			continue
		}
		// Store the maximum duration if there are duplicates
		if current, exists := targetIndex[params]; !exists || duration > current {
			targetIndex[params] = duration
		}
	}

	// Aggregate result rows by Params+UserID key
	type aggEntry struct {
		sum      float64
		count    int
		userName string
	}
	resultAgg := make(map[string]aggEntry)

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

		// Get UserName from the row
		userName := strings.TrimSpace(getString(row.Fields, "UserName"))

		key := params + keySeparator + userID
		entry := resultAgg[key]
		entry.sum += duration
		entry.count++
		// Store the UserName (use the first non-empty one if there are duplicates)
		if entry.userName == "" && userName != "" {
			entry.userName = userName
		}
		resultAgg[key] = entry
	}

	// Build matches where ratio exceeds threshold
	matches := make([]Match, 0)
	missingParamsSet := make(map[string]struct{})

	for key, entry := range resultAgg {
		// Extract params from key (remove userID part)
		parts := strings.SplitN(key, keySeparator, 2)
		params := parts[0]

		targetDuration, exists := targetIndex[params]
		if !exists || targetDuration <= 0 {
			missingParamsSet[params] = struct{}{}
			continue
		}

		ratio := entry.sum / targetDuration
		if ratio > cfg.Threshold {
			userID := ""
			if len(parts) > 1 {
				userID = parts[1]
			}
			matches = append(matches, Match{
				Params:        params,
				UserID:        userID,
				UserName:      entry.userName,
				SumDuration:   entry.sum,
				TotalDuration: targetDuration,
				Ratio:         ratio,
				RecordCount:   entry.count,
			})
		}
	}

	// Sort matches by ratio descending
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Ratio > matches[j].Ratio
	})

	// Collect missing params list
	missingParams := make([]string, 0, len(missingParamsSet))
	for p := range missingParamsSet {
		missingParams = append(missingParams, p)
	}
	sort.Strings(missingParams)

	return &Report{
		Matches:       matches,
		ResultRows:    len(resultRows),
		TargetRows:    len(targetRows),
		MissingParams: append(missingParams, extractKeys(missingTargets)...),
		Threshold:     cfg.Threshold,
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
