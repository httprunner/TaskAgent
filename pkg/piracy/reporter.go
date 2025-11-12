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
}

// NewReporter creates a new piracy reporter
func NewReporter() *Reporter {
	// Get table URLs from environment variables
	resultTableURL := os.Getenv("RESULT_BITABLE_URL") // Video data
	dramaTableURL := os.Getenv("DRAMA_BITABLE_URL")   // Drama durations
	targetTableURL := os.Getenv("TARGET_BITABLE_URL") // Where to write reports

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
		threshold:      0.5, // Default 50% threshold
	}
}

// IsConfigured returns true if all required table URLs are configured
func (pr *Reporter) IsConfigured() bool {
	return pr.resultTableURL != "" && pr.dramaTableURL != "" && pr.targetTableURL != ""
}

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

	// Build filter for specific params
	paramsFilter := buildParamsFilter(paramsList)

	log.Info().
		Str("app", app).
		Int("params_count", len(paramsList)).
		Str("filter", paramsFilter).
		Msg("Starting piracy detection for params")

	// Configure piracy detection options
	// - ResultTable: contains video data (source)
	// - DramaTable: contains original drama durations (source)
	opts := Options{
		ResultTable: TableConfig{
			URL:    pr.resultTableURL,
			Filter: paramsFilter,
		},
		DramaTable: TableConfig{
			URL: pr.dramaTableURL,
		},
		Config: Config{
			Threshold: pr.threshold,
		},
	}

	// Apply defaults
	opts.Config.ApplyDefaults()

	// Run piracy detection
	report, err := Detect(ctx, opts)
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

	// If no suspicious combos found, return early
	if len(report.Matches) == 0 {
		log.Info().Msg("No suspicious combos found, nothing to report")
		return nil
	}

	// Write suspicious combos to target table
	return pr.writeMatchesToTargetTable(ctx, app, report.Matches)
}

// buildParamsFilter builds a Feishu filter for specific params
func buildParamsFilter(paramsList []string) string {
	if len(paramsList) == 0 {
		return ""
	}

	// Escape params for Feishu filter
	escapedParams := make([]string, 0, len(paramsList))
	for _, p := range paramsList {
		if p == "" {
			continue
		}
		// Double quotes for Feishu filter
		escaped := strings.ReplaceAll(p, "\"", "\\\"")
		escapedParams = append(escapedParams, fmt.Sprintf("\"%s\"", escaped))
	}

	if len(escapedParams) == 1 {
		return fmt.Sprintf("CurrentValue.[Params]=%s", escapedParams[0])
	}

	// Use OR for multiple params
	return fmt.Sprintf("OR(%s)", strings.Join(
		buildParamConditions(escapedParams), ", "))
}

// buildParamConditions builds individual param conditions
func buildParamConditions(escapedParams []string) []string {
	conditions := make([]string, 0, len(escapedParams))
	for _, param := range escapedParams {
		conditions = append(conditions, fmt.Sprintf("CurrentValue.[Params]=%s", param))
	}
	return conditions
}

// writeMatchesToTargetTable writes piracy matches to target table
func (pr *Reporter) writeMatchesToTargetTable(ctx context.Context, app string, matches []Match) error {
	log.Info().
		Int("match_count", len(matches)).
		Msg("Writing piracy matches to target table")

	// Create Feishu client
	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return fmt.Errorf("failed to create feishu client: %w", err)
	}

	// Prepare records to write using TargetRecordInput (matches target table schema)
	records := make([]feishu.TargetRecordInput, 0, len(matches))
	for _, match := range matches {
		// Only report matches that meet the threshold
		if match.Ratio < pr.threshold {
			continue
		}

		record := feishu.TargetRecordInput{
			App:    strings.TrimSpace(app),
			Scene:  "个人页搜索",
			Params: strings.TrimSpace(match.Params),
			User:   strings.TrimSpace(match.UserID),
			Status: "PiracyDetected", // Mark as piracy detected
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		log.Info().Msg("No records meet the threshold, nothing to write")
		return nil
	}

	// Write records to target table
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