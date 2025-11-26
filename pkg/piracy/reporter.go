package piracy

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/rs/zerolog/log"
)

// Reporter handles piracy detection and reporting to task table
type Reporter struct {
	resultTableURL string // Result table containing video data
	dramaTableURL  string // Original drama table containing drama durations
	taskTableURL   string // Task table where piracy reports are written
	threshold      float64
	config         Config
	sqliteSource   *sqliteResultSource
}

// NewReporter creates a new piracy reporter
func NewReporter() *Reporter {
	// Get table URLs from environment variables
	resultTableURL := os.Getenv("RESULT_BITABLE_URL") // Video data
	dramaTableURL := os.Getenv("DRAMA_BITABLE_URL")   // Drama durations
	taskTableURL := os.Getenv("TASK_BITABLE_URL")     // Where to write reports

	cfg := Config{}
	cfg.ApplyDefaults()
	threshold := cfg.Threshold
	if threshold <= 0 {
		threshold = 0.5
	}

	if resultTableURL == "" {
		log.Warn().Msg("Result table URL not configured, piracy detection will be skipped")
	}
	if dramaTableURL == "" {
		log.Warn().Msg("Drama table URL not configured, piracy detection will be skipped")
	}
	if taskTableURL == "" {
		log.Warn().Msg("Task table URL not configured, piracy detection will be skipped")
	}

	reporter := &Reporter{
		resultTableURL: resultTableURL,
		dramaTableURL:  dramaTableURL,
		taskTableURL:   taskTableURL,
		threshold:      threshold,
		config:         cfg,
	}
	if sqliteSrc, err := newSQLiteResultSource(); err != nil {
		log.Warn().Err(err).Msg("piracy reporter: sqlite result source disabled")
	} else {
		reporter.sqliteSource = sqliteSrc
	}
	return reporter
}

// IsConfigured returns true if all required table URLs are configured
func (pr *Reporter) IsConfigured() bool {
	return pr.resultTableURL != "" && pr.dramaTableURL != "" && pr.taskTableURL != ""
}

// Config returns a copy of the reporter configuration with defaults applied.
func (pr *Reporter) Config() Config {
	cfg := pr.config
	if cfg.Threshold <= 0 {
		cfg.Threshold = pr.threshold
	}
	return cfg
}

// ResultTableURL exposes the configured result table URL.
func (pr *Reporter) ResultTableURL() string { return pr.resultTableURL }

// DramaTableURL exposes the configured drama table URL.
func (pr *Reporter) DramaTableURL() string { return pr.dramaTableURL }

// TaskTableURL exposes the configured task status table URL.
func (pr *Reporter) TaskTableURL() string { return pr.taskTableURL }

// Threshold returns the configured detection threshold.
func (pr *Reporter) Threshold() float64 { return pr.threshold }

// ReportPiracyForParams detects piracy for specific params and reports to drama table
func (pr *Reporter) ReportPiracyForParams(ctx context.Context, app string, paramsList []string) error {
	if !pr.IsConfigured() {
		log.Warn().Msg("Reporter not configured, skipping piracy detection")
		return nil
	}

	if len(paramsList) == 0 {
		log.Warn().Msg("No params provided, skipping piracy detection")
		return nil
	}

	report, err := pr.DetectWithFilters(ctx, paramsList, nil, nil)
	if err != nil {
		return fmt.Errorf("piracy detection failed: %w", err)
	}

	log.Info().
		Int("task_rows", report.TaskRows).
		Int("result_rows", report.ResultRows).
		Int("suspicious_combos", len(report.Matches)).
		Int("missing_params", len(report.MissingParams)).
		Float64("threshold", report.Threshold).
		Msg("Piracy detection completed")

	return pr.ReportMatches(ctx, app, report.Matches)
}

// DetectMatchesForParams returns detection report for the provided params without writing to tables.
func (pr *Reporter) DetectMatchesForParams(ctx context.Context, paramsList []string) (*Report, error) {
	return pr.DetectWithFilters(ctx, paramsList, nil, nil)
}

// DetectWithFilters returns detection report for the provided params with additional table filters.
func (pr *Reporter) DetectWithFilters(ctx context.Context, paramsList []string, resultExtraFilter, dramaExtraFilter *feishu.FilterInfo) (*Report, error) {
	return pr.detectWithFiltersInternal(ctx, paramsList, resultExtraFilter, dramaExtraFilter, nil)
}

// DetectWithFiltersThreshold runs piracy detection with an explicit threshold override.
func (pr *Reporter) DetectWithFiltersThreshold(ctx context.Context, paramsList []string, resultExtraFilter, dramaExtraFilter *feishu.FilterInfo, threshold float64) (*Report, error) {
	override := &Config{}
	override.Threshold = threshold
	return pr.detectWithFiltersInternal(ctx, paramsList, resultExtraFilter, dramaExtraFilter, override)
}

func (pr *Reporter) detectWithFiltersInternal(ctx context.Context, paramsList []string, resultExtraFilter, dramaExtraFilter *feishu.FilterInfo, cfgOverride *Config) (*Report, error) {
	if len(paramsList) == 0 {
		return &Report{Threshold: pr.threshold}, nil
	}

	paramsFilter := BuildParamsFilter(paramsList, pr.config.ParamsField)
	dramaFilter := BuildParamsFilter(paramsList, pr.config.DramaNameField)

	finalResultFilter := CombineFiltersAND(resultExtraFilter, paramsFilter)
	finalDramaFilter := CombineFiltersAND(dramaExtraFilter, dramaFilter)

	log.Info().
		Int("params_count", len(paramsList)).
		Str("result_filter", FilterToJSON(finalResultFilter)).
		Str("drama_filter", FilterToJSON(finalDramaFilter)).
		Msg("Running piracy detection for params")

	ops := Options{
		ResultTable: TableConfig{
			URL:    pr.resultTableURL,
			Filter: finalResultFilter,
		},
		DramaTable: TableConfig{
			URL:    pr.dramaTableURL,
			Filter: finalDramaFilter,
		},
		Config: pr.Config(),
	}
	if cfgOverride != nil {
		ops.Config.Threshold = cfgOverride.Threshold
		if cfgOverride.ParamsField != "" {
			ops.Config.ParamsField = cfgOverride.ParamsField
		}
		if cfgOverride.DramaNameField != "" {
			ops.Config.DramaNameField = cfgOverride.DramaNameField
		}
	}

	ops.Config.ApplyDefaults()

	if pr.sqliteSource != nil {
		sqliteRows, err := pr.sqliteSource.FetchRows(ctx, ops.Config, paramsList)
		if err != nil {
			log.Warn().Err(err).Msg("piracy reporter: sqlite fetch failed, fallback to Feishu")
		} else if len(sqliteRows) > 0 {
			client, err := feishu.NewClientFromEnv()
			if err != nil {
				return nil, err
			}
			dramaRows, err := fetchRows(ctx, client, ops.DramaTable)
			if err != nil {
				return nil, err
			}
			return analyzeRows(sqliteRows, dramaRows, ops.Config), nil
		}
	}

	report, err := Detect(ctx, ops)
	if err != nil {
		return nil, err
	}
	return report, nil
}

// ReportMatches writes suspicious combos to the task table if any exceed the threshold.
func (pr *Reporter) ReportMatches(ctx context.Context, app string, matches []Match) error {
	if len(matches) == 0 {
		log.Info().Msg("No suspicious combos found, nothing to report")
		return nil
	}

	log.Info().
		Int("match_count", len(matches)).
		Msg("Writing piracy matches to task table")

	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return fmt.Errorf("failed to create feishu client: %w", err)
	}

	records := make([]feishu.TaskRecordInput, 0, len(matches))
	for _, match := range matches {
		if match.Ratio < pr.threshold {
			continue
		}
		records = append(records, feishu.TaskRecordInput{
			App:      strings.TrimSpace(app),
			Scene:    "个人页搜索",
			Params:   strings.TrimSpace(match.Params),
			UserID:   strings.TrimSpace(match.UserID),
			UserName: strings.TrimSpace(match.UserName),
			Extra:    fmt.Sprintf("ratio=%.2f%%", match.Ratio*100), // 存储实际检测比例值（百分比形式）
			Status:   feishu.StatusPending,
			Webhook:  feishu.WebhookPending,
		})
	}

	if len(records) == 0 {
		log.Info().Msg("No records meet the threshold, nothing to write")
		return nil
	}

	log.Info().
		Int("record_count", len(records)).
		Str("table_url", pr.taskTableURL).
		Msg("Writing records to task table")

	recordIDs, err := client.CreateTaskRecords(ctx, pr.taskTableURL, records, nil)
	if err != nil {
		return fmt.Errorf("failed to write piracy report records: %w", err)
	}

	log.Info().
		Int("record_count", len(recordIDs)).
		Msg("Successfully wrote piracy report records to task table")
	return nil
}
