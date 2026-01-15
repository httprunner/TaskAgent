package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

const (
	sheetProgressColumn = "贴工具多维表进度"
	sheetTaskIDColumn   = "任务ID"
	sheetBookIDColumn   = "短剧BID"
	sheetUserIDColumn   = "账号ID"
	sheetURLColumn      = "侵权链接"
	sheetPlatformColumn = "平台"

	sheetProgressPending = "待传"
	sheetProgressDone    = "已传"

	sheetSceneSingleURL = "单个链接采集"
	sheetAppKuaishou    = "com.smile.gifmaker"
	sheetStatusPending  = "pending"

	kuaishouLongLinkToken  = "www.kuaishou.com/short-video"
	kuaishouShortLinkToken = "v.kuaishou.com"
)

type sheetTaskRow struct {
	Row      int
	TaskID   string
	BookID   string
	UserID   string
	URL      string
	Platform string
	GroupID  string
	DateRaw  string
	Source   string
}

func newSheetTasksCmd() *cobra.Command {
	var (
		flagSourceURL string
		flagTaskURL   string
		flagBatchSize int
		flagLimit     int
		flagPoll      time.Duration
	)

	cmd := &cobra.Command{
		Use:   "create-tasks",
		Short: "Create task records from a Feishu spreadsheet",
		Long:  "读取飞书表格中的原始任务信息，筛选未传行并创建多维表格任务，同时回写已传状态。",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			sourceURLs := parseSourceURLs(firstNonEmpty(flagSourceURL, env.String(feishusdk.EnvSourceSheetURL, "")))
			if len(sourceURLs) == 0 {
				return fmt.Errorf("--source-url or $%s is required", feishusdk.EnvSourceSheetURL)
			}
			taskURL := firstNonEmpty(flagTaskURL, rootTaskURL, env.String(feishusdk.EnvTaskBitableURL, ""))
			if strings.TrimSpace(taskURL) == "" {
				return fmt.Errorf("--task-url or $%s is required", feishusdk.EnvTaskBitableURL)
			}
			batchSize := flagBatchSize
			if batchSize <= 0 {
				batchSize = 500
			}
			limit := flagLimit
			if limit < 0 {
				limit = 0
			}

			client, err := feishusdk.NewClientFromEnv()
			if err != nil {
				return err
			}

			runOnce := func() error {
				pending := make([]sheetTaskRow, 0)
				dateRaw := fmt.Sprintf("%d", time.Now().UnixMilli())
				progressCols := make(map[string]int)
				for _, sourceURL := range sourceURLs {
					if limit > 0 && len(pending) >= limit {
						break
					}
					sheet, err := client.FetchSheet(ctx, sourceURL)
					if err != nil {
						return err
					}

					headerIndex := make(map[string]int, len(sheet.Header))
					for i, name := range sheet.Header {
						headerIndex[strings.TrimSpace(name)] = i
					}

					progressIdx, ok := headerIndex[sheetProgressColumn]
					if !ok {
						return fmt.Errorf("source sheet missing column %q", sheetProgressColumn)
					}
					taskIDIdx, ok := headerIndex[sheetTaskIDColumn]
					if !ok {
						return fmt.Errorf("source sheet missing column %q", sheetTaskIDColumn)
					}
					bookIDIdx, ok := headerIndex[sheetBookIDColumn]
					if !ok {
						return fmt.Errorf("source sheet missing column %q", sheetBookIDColumn)
					}
					userIDIdx, ok := headerIndex[sheetUserIDColumn]
					if !ok {
						return fmt.Errorf("source sheet missing column %q", sheetUserIDColumn)
					}
					urlIdx, ok := headerIndex[sheetURLColumn]
					if !ok {
						return fmt.Errorf("source sheet missing column %q", sheetURLColumn)
					}
					platformIdx, ok := headerIndex[sheetPlatformColumn]
					if !ok {
						return fmt.Errorf("source sheet missing column %q", sheetPlatformColumn)
					}

					progressCols[sourceURL] = progressIdx + 1
					for i, row := range sheet.Rows {
						if limit > 0 && len(pending) >= limit {
							break
						}
						rowNum := i + 2
						progress := cellValue(row, progressIdx)
						if progress != sheetProgressPending {
							continue
						}

						taskID := cellValue(row, taskIDIdx)
						if taskID == "" {
							log.Error().Int("row", rowNum).Str("column", sheetTaskIDColumn).Msg("source sheet missing required value")
							continue
						}
						bookID := cellValue(row, bookIDIdx)
						if bookID == "" {
							log.Error().Int("row", rowNum).Str("column", sheetBookIDColumn).Msg("source sheet missing required value")
							continue
						}
						userID := cellValue(row, userIDIdx)
						if userID == "" {
							log.Error().Int("row", rowNum).Str("column", sheetUserIDColumn).Msg("source sheet missing required value")
							continue
						}
						url := cellValue(row, urlIdx)
						if url == "" {
							log.Error().Int("row", rowNum).Str("column", sheetURLColumn).Msg("source sheet missing required value")
							continue
						}
						platform := cellValue(row, platformIdx)
						if platform == "" {
							log.Error().Int("row", rowNum).Str("column", sheetPlatformColumn).Msg("source sheet missing required value")
							continue
						}

						pending = append(pending, sheetTaskRow{
							Row:      rowNum,
							TaskID:   taskID,
							BookID:   bookID,
							UserID:   userID,
							URL:      url,
							Platform: platform,
							GroupID:  fmt.Sprintf("%s_%s_%s", platform, bookID, userID),
							DateRaw:  dateRaw,
							Source:   sourceURL,
						})
					}
				}

				if len(pending) == 0 {
					log.Info().Msg("no pending rows in source sheet")
					return nil
				}

				for start := 0; start < len(pending); start += batchSize {
					end := start + batchSize
					if end > len(pending) {
						end = len(pending)
					}
					batch := pending[start:end]

					records := make([]feishusdk.TaskRecordInput, 0, len(batch))
					updatesBySource := make(map[string][]feishusdk.SheetCellUpdate)
					for _, item := range batch {
						record := feishusdk.TaskRecordInput{
							BizTaskID:   item.TaskID,
							BookID:      item.BookID,
							UserID:      item.UserID,
							App:         sheetAppKuaishou,
							Scene:       sheetSceneSingleURL,
							Status:      sheetStatusPending,
							GroupID:     item.GroupID,
							DatetimeRaw: item.DateRaw,
						}
						switch {
						case strings.Contains(item.URL, kuaishouLongLinkToken):
							record.URL = item.URL
						case strings.Contains(item.URL, kuaishouShortLinkToken):
							record.Params = item.URL
						default:
							log.Error().
								Int("row", item.Row).
								Str("url", item.URL).
								Msg("unsupported url pattern in source sheet")
							continue
						}
						records = append(records, record)
						updatesBySource[item.Source] = append(updatesBySource[item.Source], feishusdk.SheetCellUpdate{
							Row:   item.Row,
							Col:   progressCols[item.Source],
							Value: sheetProgressDone,
						})
					}

					if len(records) == 0 {
						log.Warn().Msg("no valid records to create in this batch")
						continue
					}

					ids, err := client.CreateTaskRecords(ctx, taskURL, records, nil)
					if err != nil {
						return err
					}
					log.Info().Int("created", len(ids)).Msg("task records created")

					for sourceURL, updates := range updatesBySource {
						if err := client.UpdateSheetCells(ctx, sourceURL, updates); err != nil {
							return err
						}
						log.Info().Int("updated", len(updates)).Str("source", sourceURL).Msg("source sheet status updated")
					}
				}

				return nil
			}

			if flagPoll <= 0 {
				return runOnce()
			}

			log.Info().Dur("poll_interval", flagPoll).Int("limit", limit).Msg("sheet task poller started")
			for {
				if err := runOnce(); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(flagPoll):
				}
			}
		},
	}

	cmd.Flags().StringVar(&flagSourceURL, "source-url", "", "Source Feishu sheet URL overriding $SOURCE_SHEET_URL")
	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Task status table URL overriding $TASK_BITABLE_URL")
	cmd.Flags().IntVar(&flagBatchSize, "batch-size", 500, "Batch size for task creation and sheet updates")
	cmd.Flags().IntVar(&flagLimit, "limit", 2000, "Maximum number of tasks to create in this run (0 means no limit)")
	cmd.Flags().DurationVar(&flagPoll, "poll-interval", 0, "Polling interval for continuous task creation (0 means run once)")

	return cmd
}

func parseSourceURLs(raw string) []string {
	out := make([]string, 0)
	for _, val := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func cellValue(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}
