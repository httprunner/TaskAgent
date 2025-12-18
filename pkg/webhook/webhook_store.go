package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
)

type webhookResultRow struct {
	RecordID   string
	BizType    string
	GroupID    string
	Status     string
	TaskIDs    []int64
	DramaInfo  string
	UserInfo   string
	Records    string
	CreateAtMs int64
	StartAtMs  int64
	EndAtMs    int64
	RetryCount int
	LastError  string
}

type webhookResultStore struct {
	client   *taskagent.FeishuClient
	tableURL string
	fields   webhookResultFields
}

type webhookResultCreateInput struct {
	BizType    string
	GroupID    string
	TaskIDs    []int64
	DramaInfo  string
	CreateAtMs int64
}

func newWebhookResultStore(tableURL string) (*webhookResultStore, error) {
	trimmed := strings.TrimSpace(tableURL)
	if trimmed == "" {
		return nil, nil
	}
	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &webhookResultStore{
		client:   client,
		tableURL: trimmed,
		fields:   defaultWebhookResultFields(),
	}, nil
}

func (s *webhookResultStore) table() string {
	if s == nil {
		return ""
	}
	return s.tableURL
}

// getExistingByBizGroupAndDay returns the first webhook result row that matches
// the given biz type, group id and day (derived from CreateAt field).
// This is the preferred uniqueness check for piracy webhook groups where
// <BizType, GroupID, Date> should correspond to a single webhook plan.
func (s *webhookResultStore) getExistingByBizGroupAndDay(ctx context.Context, bizType, groupID, day string) (*webhookResultRow, error) {
	if s == nil || s.client == nil || strings.TrimSpace(s.tableURL) == "" {
		return nil, nil
	}
	gid := strings.TrimSpace(groupID)
	if gid == "" {
		return nil, nil
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	if field := strings.TrimSpace(s.fields.BizType); field != "" && strings.TrimSpace(bizType) != "" {
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(field, "is", strings.TrimSpace(bizType)))
	}
	if field := strings.TrimSpace(s.fields.GroupID); field != "" {
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(field, "is", gid))
	}
	trimmedDay := strings.TrimSpace(day)
	if field := strings.TrimSpace(s.fields.CreateAt); field != "" && trimmedDay != "" {
		if dayTime, err := time.ParseInLocation("2006-01-02", trimmedDay, time.Local); err == nil {
			tsMs := strconv.FormatInt(dayTime.UnixMilli(), 10)
			filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(field, "is", "ExactDate", tsMs))
		}
	}
	rows, err := s.client.FetchBitableRows(ctx, s.tableURL, &taskagent.FeishuTaskQueryOptions{
		Filter:     filter,
		Limit:      1,
		IgnoreView: true,
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	decoded := decodeWebhookResultRow(rows[0], s.fields)
	return &decoded, nil
}

func (s *webhookResultStore) createPending(ctx context.Context, input webhookResultCreateInput) (string, error) {
	if s == nil || s.client == nil {
		return "", errors.New("webhook result store is nil")
	}
	biz := strings.TrimSpace(input.BizType)
	if biz == "" && strings.TrimSpace(s.fields.BizType) != "" {
		return "", errors.New("biz type is required")
	}
	statusField := strings.TrimSpace(s.fields.Status)
	if statusField == "" {
		return "", errors.New("webhook result table status field is not configured")
	}
	fields := map[string]any{statusField: WebhookResultPending}
	if field := strings.TrimSpace(s.fields.BizType); field != "" && biz != "" {
		fields[field] = biz
	}
	if field := strings.TrimSpace(s.fields.GroupID); field != "" {
		if gid := strings.TrimSpace(input.GroupID); gid != "" {
			fields[field] = gid
		}
	}
	if field := strings.TrimSpace(s.fields.DramaInfo); field != "" {
		fields[field] = strings.TrimSpace(input.DramaInfo)
	}
	if field := strings.TrimSpace(s.fields.UserInfo); field != "" {
		fields[field] = "{}"
	}
	if field := strings.TrimSpace(s.fields.Records); field != "" {
		fields[field] = "[]"
	}
	if field := strings.TrimSpace(s.fields.CreateAt); field != "" {
		now := input.CreateAtMs
		if now <= 0 {
			now = time.Now().UTC().UnixMilli()
		}
		fields[field] = now
	}
	if field := strings.TrimSpace(s.fields.RetryCount); field != "" {
		fields[field] = 0
	}
	if field := strings.TrimSpace(s.fields.LastError); field != "" {
		fields[field] = ""
	}
	if trimmed := strings.TrimSpace(s.fields.TaskIDs); trimmed != "" {
		fields[trimmed] = encodeTaskIDsForFeishu(input.TaskIDs)
	}
	return s.client.CreateBitableRecord(ctx, s.tableURL, fields)
}

func (s *webhookResultStore) listCandidates(ctx context.Context, batchLimit int) ([]webhookResultRow, error) {
	if s == nil || s.client == nil {
		return nil, errors.New("webhook result store is nil")
	}
	if batchLimit <= 0 {
		batchLimit = 20
	}
	statusField := strings.TrimSpace(s.fields.Status)
	if statusField == "" {
		return nil, errors.New("webhook result table status field is not configured")
	}
	filter := listCandidatesFilter(statusField)
	rows, err := s.client.FetchBitableRows(ctx, s.tableURL, &taskagent.FeishuTaskQueryOptions{
		Filter:     filter,
		Limit:      batchLimit,
		IgnoreView: true,
	})
	if err != nil {
		return nil, err
	}
	out := make([]webhookResultRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, decodeWebhookResultRow(row, s.fields))
	}
	return out, nil
}

func listCandidatesFilter(statusField string) *taskagent.FeishuFilterInfo {
	filter := taskagent.NewFeishuFilterInfo("or")
	filter.Children = append(filter.Children,
		taskagent.NewFeishuChildrenFilter("and", taskagent.NewFeishuCondition(statusField, "is", WebhookResultPending)),
		taskagent.NewFeishuChildrenFilter("and", taskagent.NewFeishuCondition(statusField, "is", WebhookResultFailed)),
	)
	return filter
}

type webhookResultUpdate struct {
	Status     *string
	TaskIDs    *[]int64
	DramaInfo  *string
	StartAtMs  *int64
	EndAtMs    *int64
	RetryCount *int
	LastError  *string
	UserInfo   *string
	Records    *string
}

func (s *webhookResultStore) update(ctx context.Context, recordID string, upd webhookResultUpdate) error {
	if s == nil || s.client == nil {
		return errors.New("webhook result store is nil")
	}
	if strings.TrimSpace(recordID) == "" {
		return errors.New("webhook result record id is empty")
	}
	fields := map[string]any{}
	if upd.Status != nil && strings.TrimSpace(s.fields.Status) != "" {
		fields[s.fields.Status] = strings.TrimSpace(*upd.Status)
	}
	if upd.TaskIDs != nil && strings.TrimSpace(s.fields.TaskIDs) != "" {
		fields[s.fields.TaskIDs] = encodeTaskIDsForFeishu(*upd.TaskIDs)
	}
	if upd.DramaInfo != nil && strings.TrimSpace(s.fields.DramaInfo) != "" {
		fields[s.fields.DramaInfo] = strings.TrimSpace(*upd.DramaInfo)
	}
	if upd.StartAtMs != nil && strings.TrimSpace(s.fields.StartAt) != "" {
		fields[s.fields.StartAt] = *upd.StartAtMs
	}
	if upd.EndAtMs != nil && strings.TrimSpace(s.fields.EndAt) != "" {
		fields[s.fields.EndAt] = *upd.EndAtMs
	}
	if upd.RetryCount != nil && strings.TrimSpace(s.fields.RetryCount) != "" {
		fields[s.fields.RetryCount] = *upd.RetryCount
	}
	if upd.LastError != nil && strings.TrimSpace(s.fields.LastError) != "" {
		fields[s.fields.LastError] = strings.TrimSpace(*upd.LastError)
	}
	if upd.UserInfo != nil && strings.TrimSpace(s.fields.UserInfo) != "" {
		fields[s.fields.UserInfo] = strings.TrimSpace(*upd.UserInfo)
	}
	if upd.Records != nil && strings.TrimSpace(s.fields.Records) != "" {
		fields[s.fields.Records] = strings.TrimSpace(*upd.Records)
	}
	if len(fields) == 0 {
		return nil
	}
	return s.client.UpdateBitableRecord(ctx, s.tableURL, strings.TrimSpace(recordID), fields)
}

func decodeWebhookResultRow(row taskagent.BitableRow, fields webhookResultFields) webhookResultRow {
	out := webhookResultRow{RecordID: strings.TrimSpace(row.RecordID)}
	if row.Fields == nil {
		return out
	}
	out.BizType = strings.TrimSpace(toString(row.Fields[fields.BizType]))
	out.GroupID = strings.TrimSpace(toString(row.Fields[fields.GroupID]))
	out.Status = strings.ToLower(strings.TrimSpace(toString(row.Fields[fields.Status])))
	out.TaskIDs = parseTaskIDs(row.Fields[fields.TaskIDs])
	out.DramaInfo = strings.TrimSpace(toJSONString(row.Fields[fields.DramaInfo]))
	out.UserInfo = strings.TrimSpace(toJSONString(row.Fields[fields.UserInfo]))
	out.Records = strings.TrimSpace(toJSONString(row.Fields[fields.Records]))
	out.CreateAtMs = toInt64(row.Fields[fields.CreateAt])
	out.StartAtMs = toInt64(row.Fields[fields.StartAt])
	out.EndAtMs = toInt64(row.Fields[fields.EndAt])
	out.RetryCount = int(toInt64(row.Fields[fields.RetryCount]))
	out.LastError = strings.TrimSpace(toString(row.Fields[fields.LastError]))
	return out
}

func parseTaskIDs(raw any) []int64 {
	switch v := raw.(type) {
	case nil:
		return nil
	case string:
		return parseTaskIDsFromString(v)
	case []any:
		ids := make([]int64, 0, len(v))
		for _, item := range v {
			ids = append(ids, parseTaskIDsFromString(toString(item))...)
		}
		return uniqueInt64(ids)
	case []string:
		ids := make([]int64, 0, len(v))
		for _, item := range v {
			ids = append(ids, parseTaskIDsFromString(item)...)
		}
		return uniqueInt64(ids)
	default:
		return parseTaskIDsFromString(toString(v))
	}
}

func parseTaskIDsFromString(v string) []int64 {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return nil
	}
	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == ',' || r == '|' || r == ' ' || r == '\n' || r == '\t'
	})
	ids := make([]int64, 0, len(parts))
	for _, part := range parts {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		if n, err := strconv.ParseInt(p, 10, 64); err == nil && n > 0 {
			ids = append(ids, n)
		}
	}
	return uniqueInt64(ids)
}

func uniqueInt64(v []int64) []int64 {
	if len(v) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(v))
	out := make([]int64, 0, len(v))
	for _, n := range v {
		if n <= 0 {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

func encodeTaskIDsForFeishu(taskIDs []int64) string {
	if len(taskIDs) == 0 {
		return ""
	}
	seen := make(map[int64]struct{}, len(taskIDs))
	out := make([]string, 0, len(taskIDs))
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, fmt.Sprintf("%d", id))
	}
	return strings.Join(out, ",")
}

func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case json.Number:
		return x.String()
	case map[string]any:
		// Feishu multi-select and rich-text cells are often encoded as
		// objects like {"text": "123"} or {"value": "123"}; prefer these
		// well-known keys before falling back to fmt.Sprint.
		if raw, ok := x["text"]; ok {
			if s := toString(raw); strings.TrimSpace(s) != "" {
				return s
			}
		}
		if raw, ok := x["value"]; ok {
			if s := toString(raw); strings.TrimSpace(s) != "" {
				return s
			}
		}
		return fmt.Sprint(x)
	case []any:
		// For arrays, return the first non-empty string representation.
		for _, item := range x {
			if s := toString(item); strings.TrimSpace(s) != "" {
				return s
			}
		}
		return ""
	default:
		return fmt.Sprint(x)
	}
}

func toJSONString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case json.Number:
		return x.String()
	case []any:
		// Prefer decoding Feishu rich-text arrays to their plain text form.
		if text := extractTextArray(x); strings.TrimSpace(text) != "" {
			return text
		}
		if b, err := json.Marshal(x); err == nil {
			return string(b)
		}
		return fmt.Sprint(x)
	default:
		if b, err := json.Marshal(x); err == nil {
			return string(b)
		}
		return fmt.Sprint(x)
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		if f, err := x.Float64(); err == nil {
			return int64(f)
		}
		return 0
	case string:
		trimmed := strings.TrimSpace(x)
		if trimmed == "" {
			return 0
		}
		if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return int64(f)
		}
		return 0
	default:
		trimmed := strings.TrimSpace(fmt.Sprint(x))
		if trimmed == "" {
			return 0
		}
		if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return n
		}
		return 0
	}
}
