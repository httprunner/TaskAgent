package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type WebhookResultCreateOptions struct {
	TaskBitableURL    string
	WebhookBitableURL string
	DramaBitableURL   string

	ParentTaskID      int64
	ParentDatetime    *time.Time
	ParentDatetimeRaw string

	BookID   string
	GroupIDs []string
}

// CreateWebhookResultsForGroups creates one webhook result row per GroupID under a parent task.
// It is designed for the "综合页搜索 -> 盗版筛查" flow where child tasks are generated and webhook
// should only fire after all TaskIDs complete (success/error).
func CreateWebhookResultsForGroups(ctx context.Context, opts WebhookResultCreateOptions) error {
	store, err := newWebhookResultStore(firstNonEmpty(opts.WebhookBitableURL, taskagent.EnvString(taskagent.EnvWebhookBitableURL, ""), taskagent.EnvString(legacyPushResultBitableURL, "")))
	if err != nil {
		return err
	}
	if store == nil || strings.TrimSpace(store.table()) == "" {
		log.Debug().Msg("webhook result table not configured; skip creating webhook result rows")
		return nil
	}
	if opts.ParentTaskID <= 0 {
		return errors.New("parent task id is required")
	}
	bookID := strings.TrimSpace(opts.BookID)
	if bookID == "" {
		return errors.New("book id is required")
	}
	groupIDs := uniqueStrings(opts.GroupIDs)
	if len(groupIDs) == 0 {
		return nil
	}

	taskTableURL := strings.TrimSpace(firstNonEmpty(opts.TaskBitableURL, taskagent.EnvString(taskagent.EnvTaskBitableURL, "")))
	if taskTableURL == "" {
		return errors.New("task bitable url is required")
	}

	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		return err
	}

	dramaInfoJSON, _ := fetchDramaInfoJSONByBookID(ctx, client, firstNonEmpty(opts.DramaBitableURL, taskagent.EnvString("DRAMA_BITABLE_URL", "")), bookID)
	day := dayString(opts.ParentDatetime, opts.ParentDatetimeRaw)

	for _, groupID := range groupIDs {
		existing, err := store.getExistingByParentAndGroup(ctx, opts.ParentTaskID, groupID)
		if err != nil {
			return err
		}
		if existing != nil && strings.TrimSpace(existing.RecordID) != "" {
			continue
		}

		taskIDs, err := fetchGroupTaskIDsWithRetry(ctx, client, taskTableURL, groupID, day, 3)
		if err != nil {
			return err
		}
		if len(taskIDs) == 0 {
			log.Warn().
				Str("group_id", groupID).
				Int64("parent_task_id", opts.ParentTaskID).
				Msg("webhook result: group has no child tasks; skip creating row")
			continue
		}
		if _, err := store.createPending(ctx, opts.ParentTaskID, groupID, taskIDs, dramaInfoJSON); err != nil {
			return err
		}
	}
	return nil
}

func fetchDramaInfoJSONByBookID(ctx context.Context, client *taskagent.FeishuClient, dramaTableURL, bookID string) (string, error) {
	if client == nil {
		return "", errors.New("feishu client is nil")
	}
	rawURL := strings.TrimSpace(dramaTableURL)
	if rawURL == "" {
		return "", nil
	}
	field := strings.TrimSpace(taskagent.DefaultDramaFields().DramaID)
	if field == "" {
		return "", errors.New("drama id field mapping is empty")
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(field, "is", strings.TrimSpace(bookID)))
	rows, err := client.FetchBitableRows(ctx, rawURL, &taskagent.FeishuTaskQueryOptions{Filter: filter, Limit: 1})
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", sql.ErrNoRows
	}
	raw, err := json.Marshal(rows[0].Fields)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func fetchGroupTaskIDsWithRetry(ctx context.Context, client *taskagent.FeishuClient, taskTableURL, groupID, day string, attempts int) ([]int64, error) {
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		ids, err := fetchGroupTaskIDs(ctx, client, taskTableURL, groupID, day)
		if err == nil && len(ids) > 0 {
			return ids, nil
		}
		if err != nil {
			lastErr = err
		}
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(i+1) * time.Second):
			}
		}
	}
	return nil, lastErr
}

func fetchGroupTaskIDs(ctx context.Context, client *taskagent.FeishuClient, taskTableURL, groupID, day string) ([]int64, error) {
	if client == nil {
		return nil, errors.New("feishu client is nil")
	}
	fields := taskagent.DefaultTaskFields()
	groupField := strings.TrimSpace(fields.GroupID)
	if groupField == "" {
		return nil, errors.New("task table group id field mapping is empty")
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(groupField, "is", strings.TrimSpace(groupID)))
	if datetimeField := strings.TrimSpace(fields.Datetime); datetimeField != "" && strings.TrimSpace(day) != "" {
		if dayTime, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(day), time.Local); err == nil {
			tsMs := strconv.FormatInt(dayTime.UnixMilli(), 10)
			filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(datetimeField, "is", "ExactDate", tsMs))
		}
	}
	opts := &taskagent.FeishuTaskQueryOptions{
		Filter:     filter,
		Limit:      200,
		IgnoreView: true,
	}
	table, err := client.FetchTaskTableWithOptions(ctx, strings.TrimSpace(taskTableURL), nil, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "fetch group tasks failed, group=%s", groupID)
	}
	if table == nil || len(table.Rows) == 0 {
		return nil, nil
	}
	ids := make([]int64, 0, len(table.Rows))
	for _, row := range table.Rows {
		if row.TaskID != 0 {
			ids = append(ids, row.TaskID)
		}
	}
	return uniqueInt64(ids), nil
}

func dayString(parent *time.Time, raw string) string {
	if parent != nil {
		return parent.In(time.Local).Format("2006-01-02")
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		if len(trimmed) == 10 {
			return time.Unix(n, 0).In(time.Local).Format("2006-01-02")
		}
		return time.UnixMilli(n).In(time.Local).Format("2006-01-02")
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.In(time.Local).Format("2006-01-02")
	}
	return ""
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
