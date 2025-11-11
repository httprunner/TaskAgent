package piracydetect

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/httprunner/TaskAgent/feishu"
	"github.com/rs/zerolog/log"
)

const keySeparator = "\u241F" // Unit Separator symbol to avoid conflicts

// ApplyDefaults populates missing config fields from environment variables or sensible defaults.
func (c *Config) ApplyDefaults() {
	// Read from environment or use Feishu defaults
	if strings.TrimSpace(c.ParamsField) == "" {
		if paramField := os.Getenv("RESULT_PARAMS_FIELD"); paramField != "" {
			c.ParamsField = paramField
		} else if paramField := os.Getenv("PARAMS_FIELD"); paramField != "" {
			c.ParamsField = paramField
		} else {
			c.ParamsField = feishu.DefaultResultFields.Params
		}
	}
	if strings.TrimSpace(c.UserIDField) == "" {
		if userField := os.Getenv("RESULT_USERID_FIELD"); userField != "" {
			c.UserIDField = userField
		} else if userField := os.Getenv("USERID_FIELD"); userField != "" {
			c.UserIDField = userField
		} else {
			c.UserIDField = feishu.DefaultResultFields.UserID
		}
	}
	if strings.TrimSpace(c.DurationField) == "" {
		if durationField := os.Getenv("RESULT_DURATION_FIELD"); durationField != "" {
			c.DurationField = durationField
		} else if durationField := os.Getenv("DURATION_FIELD"); durationField != "" {
			c.DurationField = durationField
		} else {
			c.DurationField = feishu.DefaultResultFields.ItemDuration
		}
	}
	if strings.TrimSpace(c.TargetParamsField) == "" {
		if targetParamsField := os.Getenv("TARGET_PARAMS_FIELD"); targetParamsField != "" {
			c.TargetParamsField = targetParamsField
		} else if targetParamsField := os.Getenv("PARAMS_FIELD"); targetParamsField != "" {
			c.TargetParamsField = targetParamsField
		} else {
			c.TargetParamsField = "Params"
		}
	}
	if strings.TrimSpace(c.TargetDurationField) == "" {
		if targetDurationField := os.Getenv("TARGET_DURATION_FIELD"); targetDurationField != "" {
			c.TargetDurationField = targetDurationField
		} else {
			c.TargetDurationField = "TotalDuration"
		}
	}
	if strings.TrimSpace(c.DramaIDField) == "" {
		if dramaIDField := os.Getenv("DRAMA_ID_FIELD"); dramaIDField != "" {
			c.DramaIDField = dramaIDField
		} else {
			c.DramaIDField = "DramaID"
		}
	}
	if strings.TrimSpace(c.DramaParamsField) == "" {
		if dramaParamsField := os.Getenv("DRAMA_PARAMS_FIELD"); dramaParamsField != "" {
			c.DramaParamsField = dramaParamsField
		} else {
			c.DramaParamsField = "Params"
		}
	}
	if strings.TrimSpace(c.DramaDurationField) == "" {
		if dramaDurationField := os.Getenv("DRAMA_DURATION_FIELD"); dramaDurationField != "" {
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
	return client.FetchBitableRows(ctx, cfg.URL, queryOpts)
}

// analyzeRows performs the piracy detection analysis on the fetched rows.
func analyzeRows(resultRows, dramaRows []Row, cfg Config) *Report {
	log.Info().Msg("analyzing result rows with drama rows")
	// Index drama durations by Params
	dramaIndex := make(map[string]float64)
	missingDramas := make(map[string]struct{})

	for _, row := range dramaRows {
		params := strings.TrimSpace(getString(row.Fields, cfg.DramaParamsField))
		if params == "" {
			continue
		}
		duration, ok := getFloat(row.Fields, cfg.DramaDurationField)
		if !ok || duration <= 0 {
			missingDramas[params] = struct{}{}
			continue
		}
		// Store the maximum duration if there are duplicates
		if current, exists := dramaIndex[params]; !exists || duration > current {
			dramaIndex[params] = duration
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

		dramaDuration, exists := dramaIndex[params]
		if !exists || dramaDuration <= 0 {
			missingParamsSet[params] = struct{}{}
			continue
		}

		ratio := entry.sum / dramaDuration
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

	// Collect missing params list
	missingParams := make([]string, 0, len(missingParamsSet))
	for p := range missingParamsSet {
		missingParams = append(missingParams, p)
	}
	sort.Strings(missingParams)

	return &Report{
		Matches:       matches,
		ResultRows:    len(resultRows),
		TargetRows:    len(dramaRows), // Now using drama rows count instead of target rows
		MissingParams: append(missingParams, extractKeys(missingDramas)...),
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
