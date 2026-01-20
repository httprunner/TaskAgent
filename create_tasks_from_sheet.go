package taskagent

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
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

	sheetAppKuaishou   = "com.smile.gifmaker"
	sheetStatusPending = "pending"

	kuaishouLongLinkToken  = "kuaishou.com/short-video"
	kuaishouShortLinkToken = "v.kuaishou.com"

	sheetRowPageSize = 500
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

type sheetColumnIndex struct {
	progress int
	taskID   int
	bookID   int
	userID   int
	url      int
	platform int
}

// SheetTaskOptions controls how sheet-driven tasks are created.
type SheetTaskOptions struct {
	SourceURLs   []string
	TaskURL      string
	BatchSize    int
	Limit        int
	PollInterval time.Duration
}

// ParseSheetSourceURLs splits comma-separated Feishu sheet URLs.
func ParseSheetSourceURLs(raw string) []string {
	out := make([]string, 0)
	for _, val := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// RunSheetTasks reads a Feishu sheet and creates task records in bitable.
func RunSheetTasks(ctx context.Context, opts SheetTaskOptions) error {
	if len(opts.SourceURLs) == 0 {
		return fmt.Errorf("source URLs are required")
	}
	if strings.TrimSpace(opts.TaskURL) == "" {
		return fmt.Errorf("task URL is required")
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	limit := opts.Limit
	if limit < 0 {
		limit = 0
	}

	client, err := NewFeishuClientFromEnv()
	if err != nil {
		return err
	}

	runOnce := func() error {
		pending := make([]sheetTaskRow, 0)
		dateRaw := fmt.Sprintf("%d", time.Now().UnixMilli())
		progressCols := make(map[string]int)
		currentGroupID := ""
		currentGroup := make([]sheetTaskRow, 0)
		stopAll := false
		totalSelected := 0
		flushGroup := func() bool {
			if len(currentGroup) == 0 {
				return false
			}
			pending = append(pending, currentGroup...)
			totalSelected += len(currentGroup)
			currentGroup = currentGroup[:0]
			currentGroupID = ""
			if limit > 0 && totalSelected >= limit {
				return true
			}
			return false
		}
		for _, sourceURL := range opts.SourceURLs {
			if stopAll {
				break
			}
			meta, err := client.FetchSheetMeta(ctx, sourceURL)
			if err != nil {
				return err
			}

			cols, err := sheetColumnsFromHeader(meta.Header)
			if err != nil {
				return err
			}

			progressCols[sourceURL] = cols.progress + 1
			startRow := 2
			maxRows := meta.RowCount
			for {
				if stopAll {
					break
				}
				if maxRows > 0 && startRow > maxRows {
					break
				}
				endRow := startRow + sheetRowPageSize - 1
				if maxRows > 0 && endRow > maxRows {
					endRow = maxRows
				}
				rows, err := client.FetchSheetRowsByRange(ctx, meta, startRow, endRow)
				if err != nil {
					return err
				}
				log.Debug().Int("fetched", len(rows)).
					Int("row_start", startRow).
					Int("row_end", endRow).Str("source", sourceURL).
					Msg("fetched sheet rows")
				if len(rows) == 0 {
					break
				}
				for i, row := range rows {
					if stopAll {
						break
					}
					rowNum := startRow + i
					progress := cellValue(row, cols.progress)
					if !isPendingProgress(progress) {
						continue
					}

					taskID, ok := requiredCell(row, cols.taskID, sheetTaskIDColumn, rowNum)
					if !ok {
						continue
					}
					bookID, ok := requiredCell(row, cols.bookID, sheetBookIDColumn, rowNum)
					if !ok {
						continue
					}
					userID, ok := requiredCell(row, cols.userID, sheetUserIDColumn, rowNum)
					if !ok {
						continue
					}
					url, ok := requiredCell(row, cols.url, sheetURLColumn, rowNum)
					if !ok {
						continue
					}
					platform := cellValue(row, cols.platform)
					if platform == "" {
						log.Warn().Int("row", rowNum).Str("column", sheetPlatformColumn).Msg("source sheet missing required value")
						platform = "快手"
					}

					groupID := fmt.Sprintf("%s_%s_%s", platform, bookID, userID)
					if currentGroupID != "" && groupID != currentGroupID {
						if flushGroup() {
							stopAll = true
							break
						}
					}
					if currentGroupID == "" {
						currentGroupID = groupID
					}
					currentGroup = append(currentGroup, sheetTaskRow{
						Row:      rowNum,
						TaskID:   taskID,
						BookID:   bookID,
						UserID:   userID,
						URL:      url,
						Platform: platform,
						GroupID:  groupID,
						DateRaw:  dateRaw,
						Source:   sourceURL,
					})
				}
				startRow = endRow + 1
			}
		}
		if !stopAll {
			if flushGroup() {
				stopAll = true
			}
		}

		if len(pending) == 0 {
			log.Info().Msg("no pending rows in source sheet")
			return nil
		}

		createdCounts := make(map[string]int)
		for start := 0; start < len(pending); start += batchSize {
			end := start + batchSize
			if end > len(pending) {
				end = len(pending)
			}
			batch := pending[start:end]

			records := make([]TaskRecordInput, 0, len(batch))
			updatesBySource := make(map[string][]SheetCellUpdate)
			groupCounts := make(map[string]int)
			for _, item := range batch {
				record := TaskRecordInput{
					BizTaskID:   item.TaskID,
					BookID:      item.BookID,
					UserID:      item.UserID,
					App:         sheetAppKuaishou,
					Scene:       SceneSingleURLCapture,
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
				groupCounts[item.GroupID]++
				updatesBySource[item.Source] = append(updatesBySource[item.Source], SheetCellUpdate{
					Row:   item.Row,
					Col:   progressCols[item.Source],
					Value: sheetProgressDone,
				})
			}

			if len(records) == 0 {
				log.Warn().Msg("no valid records to create in this batch")
				continue
			}

			ids, err := client.CreateTaskRecords(ctx, opts.TaskURL, records, nil)
			if err != nil {
				return err
			}
			log.Info().Int("created", len(ids)).Msg("task records created in total")
			for groupID, count := range groupCounts {
				createdCounts[groupID] += count
			}

			for sourceURL, updates := range updatesBySource {
				if err := client.UpdateSheetCells(ctx, sourceURL, updates); err != nil {
					return err
				}
				log.Info().Int("updated", len(updates)).Str("source", sourceURL).Msg("source sheet status updated")
			}
		}

		if len(createdCounts) > 0 {
			groupIDs := make([]string, 0, len(createdCounts))
			for groupID := range createdCounts {
				groupIDs = append(groupIDs, groupID)
			}
			sort.Strings(groupIDs)
			for _, groupID := range groupIDs {
				log.Info().Str("group_id", groupID).Int("count", createdCounts[groupID]).Msg("created tasks for group")
			}
		}

		return nil
	}

	if opts.PollInterval <= 0 {
		return runOnce()
	}

	log.Info().Dur("poll_interval", opts.PollInterval).Int("limit", limit).Msg("sheet task poller started")
	for {
		if err := runOnce(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(opts.PollInterval):
		}
	}
}

func sheetColumnsFromHeader(header []string) (sheetColumnIndex, error) {
	idx := make(map[string]int, len(header))
	for i, name := range header {
		idx[strings.TrimSpace(name)] = i
	}
	return sheetColumnIndex{
		progress: mustHeaderIndex(idx, sheetProgressColumn),
		taskID:   mustHeaderIndex(idx, sheetTaskIDColumn),
		bookID:   mustHeaderIndex(idx, sheetBookIDColumn),
		userID:   mustHeaderIndex(idx, sheetUserIDColumn),
		url:      mustHeaderIndex(idx, sheetURLColumn),
		platform: mustHeaderIndex(idx, sheetPlatformColumn),
	}, headerIndexError(idx)
}

func mustHeaderIndex(index map[string]int, name string) int {
	if idx, ok := index[name]; ok {
		return idx
	}
	return -1
}

func headerIndexError(index map[string]int) error {
	required := []string{
		sheetProgressColumn,
		sheetTaskIDColumn,
		sheetBookIDColumn,
		sheetUserIDColumn,
		sheetURLColumn,
		sheetPlatformColumn,
	}
	for _, name := range required {
		if _, ok := index[name]; !ok {
			return fmt.Errorf("source sheet missing column %q", name)
		}
	}
	return nil
}

func requiredCell(row []string, idx int, column string, rowNum int) (string, bool) {
	val := cellValue(row, idx)
	if val == "" {
		log.Error().Int("row", rowNum).Str("column", column).Msg("source sheet missing required value")
		return "", false
	}
	return val, true
}

func isPendingProgress(value string) bool {
	return value == sheetProgressPending
}

func cellValue(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}
