package piracy

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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

// OverrideTaskTableURL replaces the task table URL (e.g., CLI flag override).
func (pr *Reporter) OverrideTaskTableURL(url string) {
	if pr == nil {
		return
	}
	if trimmed := strings.TrimSpace(url); trimmed != "" {
		pr.taskTableURL = trimmed
	}
}

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

// DetectMatchesWithDetails detects piracy matches and fetches video details for each match.
// This method returns MatchDetail which includes the Match and associated VideoDetails (ItemID, Tags, AnchorPoint).
func (pr *Reporter) DetectMatchesWithDetails(ctx context.Context, paramsList []string) ([]MatchDetail, error) {
	if !pr.IsConfigured() {
		log.Warn().Msg("Reporter not configured, skipping piracy detection")
		return nil, nil
	}

	if len(paramsList) == 0 {
		log.Warn().Msg("No params provided, skipping piracy detection")
		return nil, nil
	}

	// Step 1: Detect piracy matches
	report, err := pr.DetectWithFilters(ctx, paramsList, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("piracy detection failed: %w", err)
	}

	if len(report.Matches) == 0 {
		return nil, nil
	}

	// Filter matches by threshold
	filteredMatches := make([]Match, 0, len(report.Matches))
	for _, match := range report.Matches {
		if match.Ratio >= pr.threshold {
			filteredMatches = append(filteredMatches, match)
		}
	}

	if len(filteredMatches) == 0 {
		return nil, nil
	}

	log.Info().
		Int("matches", len(filteredMatches)).
		Msg("Fetching video details for piracy matches")

	// Step 2: Fetch video details for each match
	// Feishu client is created lazily only when sqlite fallback is needed
	var feishuClient *feishu.Client

	details := make([]MatchDetail, 0, len(filteredMatches))
	for _, match := range filteredMatches {
		videos, err := pr.fetchVideosForMatch(ctx, &feishuClient, match.Params, match.UserID)
		if err != nil {
			log.Warn().Err(err).
				Str("params", match.Params).
				Str("user_id", match.UserID).
				Msg("Failed to fetch videos for match, fallback to empty video details")
		}
		details = append(details, MatchDetail{
			Match:  match,
			Videos: videos,
		})
	}

	log.Info().
		Int("match_details", len(details)).
		Msg("Video details fetched for piracy matches")

	return details, nil
}

// fetchVideosForMatch retrieves video details (ItemID, Tags, AnchorPoint) for a specific match.
// It tries sqlite first (local data, always up-to-date), then falls back to Feishu if sqlite fails or returns empty.
// The feishu client is created lazily only when fallback is needed.
func (pr *Reporter) fetchVideosForMatch(ctx context.Context, clientPtr **feishu.Client, params, userID string) ([]VideoDetail, error) {
	// Try sqlite first (preferred: local data is immediately available after capture)
	if pr.sqliteSource != nil {
		videos, err := pr.sqliteSource.FetchVideoDetails(ctx, params, userID)
		if err != nil {
			log.Warn().Err(err).
				Str("params", params).
				Str("user_id", userID).
				Msg("sqlite video details fetch failed, fallback to Feishu")
		} else if len(videos) > 0 {
			log.Debug().
				Str("params", params).
				Str("user_id", userID).
				Int("video_count", len(videos)).
				Msg("video details fetched from sqlite")
			return videos, nil
		}
	}

	// Fallback to Feishu - create client lazily
	if *clientPtr == nil {
		client, err := feishu.NewClientFromEnv()
		if err != nil {
			return nil, fmt.Errorf("failed to create feishu client: %w", err)
		}
		*clientPtr = client
	}
	return pr.fetchVideosFromFeishu(ctx, *clientPtr, params, userID)
}

// fetchVideosFromFeishu retrieves video details from Feishu bitable.
func (pr *Reporter) fetchVideosFromFeishu(ctx context.Context, client *feishu.Client, params, userID string) ([]VideoDetail, error) {
	fields := feishu.DefaultResultFields

	// Build filter for params + userID
	filter := feishu.NewFilterInfo("and")
	if paramsField := strings.TrimSpace(fields.Params); paramsField != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(paramsField, "is", params))
	}
	if userIDField := strings.TrimSpace(fields.UserID); userIDField != "" {
		filter.Conditions = append(filter.Conditions, feishu.NewCondition(userIDField, "is", userID))
	}

	rows, err := fetchRows(ctx, client, TableConfig{
		URL:    pr.resultTableURL,
		Filter: filter,
		Limit:  500, // Reasonable limit for videos per match
	})
	if err != nil {
		return nil, err
	}

	videos := make([]VideoDetail, 0, len(rows))
	seenItemIDs := make(map[string]struct{})
	for _, row := range rows {
		itemID := getString(row.Fields, fields.ItemID)
		if itemID == "" {
			continue
		}
		// Deduplicate by ItemID
		if _, seen := seenItemIDs[itemID]; seen {
			continue
		}
		seenItemIDs[itemID] = struct{}{}

		videos = append(videos, VideoDetail{
			ItemID:      itemID,
			Tags:        getString(row.Fields, fields.Tags),
			AnchorPoint: getString(row.Fields, fields.AnchorPoint),
		})
	}

	return videos, nil
}

// CreateGroupTasksForPiracyMatches creates group tasks for each piracy match based on video details.
// For each MatchDetail, it creates:
//   - 1 "个人页搜索" task
//   - 1 "合集视频采集" task if any video has "合集" or "短剧" tag (uses first matching video's ItemID)
//   - N "视频锚点采集" tasks for each video with appLink in AnchorPoint
//
// All tasks in the same group share the GroupID for webhook aggregation.
func (pr *Reporter) CreateGroupTasksForPiracyMatches(ctx context.Context, app string, parentTaskID int64, parentDatetime *time.Time, parentDatetimeRaw string, details []MatchDetail) error {
	if len(details) == 0 {
		log.Info().Msg("No match details provided, nothing to report")
		return nil
	}

	log.Info().
		Int("detail_count", len(details)).
		Int64("parent_task_id", parentTaskID).
		Msg("Creating group tasks for piracy matches")

	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return fmt.Errorf("failed to create feishu client: %w", err)
	}

	var records []feishu.TaskRecordInput
	inheritRaw := inheritDatetimeRaw(parentDatetimeRaw, parentDatetime)

	for idx, detail := range details {
		// GroupID format: {parentTaskID}_{index}, index starts from 1
		groupID := fmt.Sprintf("%d_%d", parentTaskID, idx+1)

		// 1. Create "个人页搜索" task
		records = append(records, feishu.TaskRecordInput{
			App:         strings.TrimSpace(app),
			Scene:       "个人页搜索",
			Params:      strings.TrimSpace(detail.Match.Params),
			UserID:      strings.TrimSpace(detail.Match.UserID),
			UserName:    strings.TrimSpace(detail.Match.UserName),
			Extra:       fmt.Sprintf("ratio=%.2f%%", detail.Match.Ratio*100),
			GroupID:     groupID,
			Datetime:    parentDatetime,
			DatetimeRaw: inheritRaw,
			Status:      feishu.StatusPending,
			Webhook:     feishu.WebhookPending,
		})

		// 2. Create "合集视频采集" task if any video has collection tag
		if collectionItemID := FindFirstCollectionVideo(detail.Videos); collectionItemID != "" {
			records = append(records, feishu.TaskRecordInput{
				App:         strings.TrimSpace(app),
				Scene:       "合集视频采集",
				Params:      collectionItemID,
				UserID:      strings.TrimSpace(detail.Match.UserID),
				UserName:    strings.TrimSpace(detail.Match.UserName),
				GroupID:     groupID,
				Datetime:    parentDatetime,
				DatetimeRaw: inheritRaw,
				Status:      feishu.StatusPending,
				Webhook:     feishu.WebhookPending,
			})
		}

		// 3. Create "视频锚点采集" tasks for each video with appLink
		seenAppLinks := make(map[string]struct{})
		for _, video := range detail.Videos {
			appLink := ExtractAppLink(video.AnchorPoint)
			if appLink == "" {
				continue
			}
			// Deduplicate by appLink
			if _, seen := seenAppLinks[appLink]; seen {
				continue
			}
			seenAppLinks[appLink] = struct{}{}

			records = append(records, feishu.TaskRecordInput{
				App:         strings.TrimSpace(app),
				Scene:       "视频锚点采集",
				Params:      appLink,
				UserID:      strings.TrimSpace(detail.Match.UserID),
				UserName:    strings.TrimSpace(detail.Match.UserName),
				GroupID:     groupID,
				Datetime:    parentDatetime,
				DatetimeRaw: inheritRaw,
				Status:      feishu.StatusPending,
				Webhook:     feishu.WebhookPending,
			})
		}
	}

	if len(records) == 0 {
		log.Info().Msg("No child tasks to create")
		return nil
	}

	log.Info().
		Int("record_count", len(records)).
		Int("group_count", len(details)).
		Str("table_url", pr.taskTableURL).
		Msg("Writing child tasks to task table")

	recordIDs, err := client.CreateTaskRecords(ctx, pr.taskTableURL, records, nil)
	if err != nil {
		return fmt.Errorf("failed to write child task records: %w", err)
	}

	log.Info().
		Int("record_count", len(recordIDs)).
		Int("group_count", len(details)).
		Msg("Successfully wrote child task records to task table")
	return nil
}

func inheritDatetimeRaw(parentRaw string, parent *time.Time) string {
	if trimmed := strings.TrimSpace(parentRaw); trimmed != "" {
		return trimmed
	}
	if parent == nil {
		return ""
	}
	return strconv.FormatInt(parent.UTC().UnixMilli(), 10)
}
