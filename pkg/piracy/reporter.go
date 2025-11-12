package piracy

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/feishu"
	"github.com/rs/zerolog/log"
)

// Reporter handles piracy detection and reporting to target table
type Reporter struct {
	resultTableURL string // Result table containing video data
	dramaTableURL  string // Original drama table containing drama durations
	targetTableURL string // Target table where piracy reports are written
	threshold      float64
	config         Config
}

// NewReporter creates a new piracy reporter
func NewReporter() *Reporter {
	// Get table URLs from environment variables
	resultTableURL := os.Getenv("RESULT_BITABLE_URL") // Video data
	dramaTableURL := os.Getenv("DRAMA_BITABLE_URL")   // Drama durations
	targetTableURL := os.Getenv("TARGET_BITABLE_URL") // Where to write reports

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
	if targetTableURL == "" {
		log.Warn().Msg("Target table URL not configured, piracy detection will be skipped")
	}

	return &Reporter{
		resultTableURL: resultTableURL,
		dramaTableURL:  dramaTableURL,
		targetTableURL: targetTableURL,
		threshold:      threshold,
		config:         cfg,
	}
}

// IsConfigured returns true if all required table URLs are configured
func (pr *Reporter) IsConfigured() bool {
	return pr.resultTableURL != "" && pr.dramaTableURL != "" && pr.targetTableURL != ""
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

	report, err := pr.DetectWithFilters(ctx, paramsList, "", "")
	if err != nil {
		return fmt.Errorf("piracy detection failed: %w", err)
	}

	log.Info().
		Int("target_rows", report.TargetRows).
		Int("result_rows", report.ResultRows).
		Int("suspicious_combos", len(report.Matches)).
		Int("missing_params", len(report.MissingParams)).
		Float64("threshold", report.Threshold).
		Msg("Piracy detection completed")

	return pr.ReportMatches(ctx, app, report.Matches)
}

// DetectMatchesForParams returns detection report for the provided params without writing to tables.
func (pr *Reporter) DetectMatchesForParams(ctx context.Context, paramsList []string) (*Report, error) {
	return pr.DetectWithFilters(ctx, paramsList, "", "")
}

// DetectWithFilters returns detection report for the provided params with additional table filters.
func (pr *Reporter) DetectWithFilters(ctx context.Context, paramsList []string, resultExtraFilter, dramaExtraFilter string) (*Report, error) {
	if len(paramsList) == 0 {
		return &Report{Threshold: pr.threshold}, nil
	}

	paramsFilter := buildParamsFilter(paramsList, pr.config.ParamsField)
	dramaFilter := buildParamsFilter(paramsList, pr.config.DramaParamsField)

	finalResultFilter := combineFilters(resultExtraFilter, paramsFilter)
	finalDramaFilter := combineFilters(dramaExtraFilter, dramaFilter)

	log.Info().
		Int("params_count", len(paramsList)).
		Str("result_filter", finalResultFilter).
		Str("drama_filter", finalDramaFilter).
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

	ops.Config.ApplyDefaults()

	report, err := Detect(ctx, ops)
	if err != nil {
		return nil, err
	}
	return report, nil
}

// ReportMatches writes suspicious combos to the target table if any exceed the threshold.
func (pr *Reporter) ReportMatches(ctx context.Context, app string, matches []Match) error {
	if len(matches) == 0 {
		log.Info().Msg("No suspicious combos found, nothing to report")
		return nil
	}

	log.Info().
		Int("match_count", len(matches)).
		Msg("Writing piracy matches to target table")

	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return fmt.Errorf("failed to create feishu client: %w", err)
	}

	records := make([]feishu.TargetRecordInput, 0, len(matches))
	for _, match := range matches {
		if match.Ratio < pr.threshold {
			continue
		}
		records = append(records, feishu.TargetRecordInput{
			App:    strings.TrimSpace(app),
			Scene:  "个人页搜索",
			Params: strings.TrimSpace(match.Params),
			User:   strings.TrimSpace(match.UserID),
			Status: "PiracyDetected",
		})
	}

	if len(records) == 0 {
		log.Info().Msg("No records meet the threshold, nothing to write")
		return nil
	}

	log.Info().
		Int("record_count", len(records)).
		Str("table_url", pr.targetTableURL).
		Msg("Writing records to target table")

	recordIDs, err := client.CreateTargetRecords(ctx, pr.targetTableURL, records, nil)
	if err != nil {
		return fmt.Errorf("failed to write piracy report records: %w", err)
	}

	log.Info().
		Int("record_count", len(recordIDs)).
		Msg("Successfully wrote piracy report records to target table")
	return nil
}

func buildParamsFilter(paramsList []string, fieldName string) string {
	formattedValues := make([]string, 0, len(paramsList))
	for _, raw := range paramsList {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		escaped := strings.ReplaceAll(s, "\"", "\\\"")
		formattedValues = append(formattedValues, fmt.Sprintf("\"%s\"", escaped))
	}
	if len(formattedValues) == 0 {
		return ""
	}

	fieldExpr := formatFieldExpression(fieldName)
	if len(formattedValues) == 1 {
		return fmt.Sprintf("%s=%s", fieldExpr, formattedValues[0])
	}

	conditions := make([]string, 0, len(formattedValues))
	for _, val := range formattedValues {
		conditions = append(conditions, fmt.Sprintf("%s=%s", fieldExpr, val))
	}
	return fmt.Sprintf("OR(%s)", strings.Join(conditions, ", "))
}

func formatFieldExpression(fieldName string) string {
	f := strings.TrimSpace(fieldName)
	if f == "" {
		f = "Params"
	}
	if strings.HasPrefix(f, "CurrentValue.") {
		return f
	}
	if strings.HasPrefix(f, "[") && strings.HasSuffix(f, "]") {
		return fmt.Sprintf("CurrentValue.%s", f)
	}
	return fmt.Sprintf("CurrentValue.[%s]", f)
}

func combineFilters(base, addition string) string {
	base = strings.TrimSpace(base)
	addition = strings.TrimSpace(addition)
	switch {
	case base == "" && addition == "":
		return ""
	case base == "":
		return addition
	case addition == "":
		return base
	default:
		return fmt.Sprintf("AND(%s, %s)", base, addition)
	}
}
