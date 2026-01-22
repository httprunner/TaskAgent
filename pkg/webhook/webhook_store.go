package webhook

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/pkg/errors"
)

type webhookResultRow struct {
	RecordID   string
	BizType    string
	GroupID    string
	Status     string
	DramaInfo  string
	UserInfo   string
	Records    string
	DateMs     int64
	CreateAtMs int64
	StartAtMs  int64
	EndAtMs    int64
	RetryCount int
	LastError  string

	TaskIDs []int64
	// TaskIDsByStatus stores a decoded view of TaskIDs field when it is encoded as
	// a JSON object mapping status -> task id list.
	TaskIDsByStatus map[string][]int64
}

type webhookResultStore struct {
	client   *taskagent.FeishuClient
	tableURL string
	fields   webhookResultFields
}

type webhookResultCreateInput struct {
	BizType   string
	GroupID   string
	TaskIDs   []int64
	DramaInfo string
	// DateMs stores the logical task day in unix ms (UTC),
	// typically derived from the task Datetime field (ExactDate).
	DateMs int64
	// CreateAtMs stores the physical creation timestamp for the webhook
	// result row in unix ms (UTC). When zero, it is filled with time.Now().
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
// the given biz type, group id and logical task day (Date field).
// This is the preferred uniqueness check for piracy/SingleURL webhook groups
// where <BizType, GroupID, Date> should correspond to a single webhook plan.
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
	// Date is the canonical logical day field for webhook rows.
	if field := strings.TrimSpace(s.fields.Date); field != "" && trimmedDay != "" {
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
	if field := strings.TrimSpace(s.fields.Date); field != "" && input.DateMs > 0 {
		fields[field] = input.DateMs
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
		fields[trimmed] = encodeTaskIDsByStatusForFeishu(map[string][]int64{
			taskIDsDefaultStatus: input.TaskIDs,
		})
	}
	return s.client.CreateBitableRecord(ctx, s.tableURL, fields)
}

type listCandidatesOptions struct {
	BatchLimit  int
	DatePresets []string
}

func (s *webhookResultStore) listCandidates(ctx context.Context, opts listCandidatesOptions) ([]webhookResultRow, error) {
	if s == nil || s.client == nil {
		return nil, errors.New("webhook result store is nil")
	}
	batchLimit := opts.BatchLimit
	if batchLimit <= 0 {
		batchLimit = 20
	}
	statusField := strings.TrimSpace(s.fields.Status)
	if statusField == "" {
		return nil, errors.New("webhook result table status field is not configured")
	}
	dateField := strings.TrimSpace(s.fields.Date)
	filter := listCandidatesFilter(statusField, dateField, opts.DatePresets)
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

func listCandidatesFilter(statusField, dateField string, datePresets []string) *taskagent.FeishuFilterInfo {
	filter := taskagent.NewFeishuFilterInfo("and")
	statusConds := []*taskagent.FeishuCondition{
		taskagent.NewFeishuCondition(statusField, "is", WebhookResultPending),
		taskagent.NewFeishuCondition(statusField, "is", WebhookResultFailed),
	}
	filter.Children = append(filter.Children, taskagent.NewFeishuChildrenFilter("or", statusConds...))

	if strings.TrimSpace(dateField) == "" {
		return filter
	}
	dateConds := make([]*taskagent.FeishuCondition, 0, len(datePresets))
	for _, preset := range datePresets {
		trimmed := strings.TrimSpace(preset)
		if trimmed == "" {
			continue
		}
		dateConds = append(dateConds, taskagent.NewFeishuCondition(dateField, "is", trimmed))
	}
	if len(dateConds) == 0 {
		return filter
	}
	filter.Children = append(filter.Children, taskagent.NewFeishuChildrenFilter("or", dateConds...))
	return filter
}

type webhookResultUpdate struct {
	Status     *string
	DramaInfo  *string
	StartAtMs  *int64
	EndAtMs    *int64
	RetryCount *int
	LastError  *string
	UserInfo   *string
	Records    *string

	TaskIDs *[]int64
	// TaskIDsByStatus, when provided, takes precedence over TaskIDs and is stored
	// as a JSON object mapping status -> task ids.
	TaskIDsByStatus *map[string][]int64
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
	if strings.TrimSpace(s.fields.TaskIDs) != "" {
		if upd.TaskIDsByStatus != nil {
			fields[s.fields.TaskIDs] = encodeTaskIDsByStatusForFeishu(*upd.TaskIDsByStatus)
		} else if upd.TaskIDs != nil {
			fields[s.fields.TaskIDs] = encodeTaskIDsByStatusForFeishu(map[string][]int64{
				taskIDsDefaultStatus: *upd.TaskIDs,
			})
		}
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
	out.BizType = toString(row.Fields[fields.BizType])
	out.GroupID = toString(row.Fields[fields.GroupID])
	out.Status = strings.ToLower(toString(row.Fields[fields.Status]))
	out.TaskIDs = parseTaskIDs(row.Fields[fields.TaskIDs])
	out.TaskIDsByStatus = parseTaskIDsStatusMap(row.Fields[fields.TaskIDs])
	out.DramaInfo = feishusdk.BitableValueToJSONString(row.Fields[fields.DramaInfo])
	out.UserInfo = feishusdk.BitableValueToJSONString(row.Fields[fields.UserInfo])
	out.Records = feishusdk.BitableValueToJSONString(row.Fields[fields.Records])
	out.DateMs = feishusdk.BitableValueToInt64(row.Fields[fields.Date])
	out.CreateAtMs = feishusdk.BitableValueToInt64(row.Fields[fields.CreateAt])
	out.StartAtMs = feishusdk.BitableValueToInt64(row.Fields[fields.StartAt])
	out.EndAtMs = feishusdk.BitableValueToInt64(row.Fields[fields.EndAt])
	out.RetryCount = int(feishusdk.BitableValueToInt64(row.Fields[fields.RetryCount]))
	out.LastError = toString(row.Fields[fields.LastError])
	return out
}

func parseTaskIDs(raw any) []int64 {
	switch v := raw.(type) {
	case nil:
		return nil
	case string:
		if m := parseTaskIDsStatusMap(v); len(m) > 0 {
			ids := uniqueInt64(flattenTaskIDsStatusMap(m))
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			return ids
		}
		if n := feishusdk.BitableValueToInt64(v); n > 0 {
			return []int64{n}
		}
		return nil
	case []any:
		ids := make([]int64, 0, len(v))
		for _, item := range v {
			ids = append(ids, parseTaskIDs(item)...)
		}
		return uniqueInt64(ids)
	case []string:
		ids := make([]int64, 0, len(v))
		for _, item := range v {
			ids = append(ids, parseTaskIDs(item)...)
		}
		return uniqueInt64(ids)
	case map[string]any:
		if m := parseTaskIDsStatusMap(v); len(m) > 0 {
			ids := uniqueInt64(flattenTaskIDsStatusMap(m))
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			return ids
		}
		return nil
	default:
		if n := feishusdk.BitableValueToInt64(v); n > 0 {
			return []int64{n}
		}
		return nil
	}
}

const (
	taskIDsUnknownStatus = "unknown"
	// taskIDsDefaultStatus is used for newly created webhook rows. It represents
	// the initial state before the worker refreshes task statuses.
	taskIDsDefaultStatus = WebhookResultPending
)

func parseTaskIDsStatusMap(raw any) map[string][]int64 {
	switch v := raw.(type) {
	case nil:
		return nil
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil
		}
		if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
			return nil
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
			return nil
		}
		return normalizeTaskIDsStatusMap(decoded)
	case map[string]any:
		return normalizeTaskIDsStatusMap(v)
	default:
		return nil
	}
}

func normalizeTaskIDsStatusMap(raw map[string]any) map[string][]int64 {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string][]int64, len(raw))
	for k, v := range raw {
		status := normalizeTaskIDsStatusKey(k)
		ids := parseTaskIDs(v)
		if len(ids) == 0 {
			continue
		}
		out[status] = append(out[status], ids...)
	}
	return normalizeTaskIDsByStatus(out)
}

func normalizeTaskIDsStatusKey(status string) string {
	trimmed := strings.ToLower(strings.TrimSpace(status))
	if trimmed == "" {
		return taskIDsUnknownStatus
	}
	return trimmed
}

func normalizeTaskIDsByStatus(in map[string][]int64) map[string][]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]int64, len(in))
	for k, ids := range in {
		status := normalizeTaskIDsStatusKey(k)
		if len(ids) == 0 {
			continue
		}
		seen := make(map[int64]struct{}, len(ids))
		uniq := make([]int64, 0, len(ids))
		for _, id := range ids {
			if id <= 0 {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			uniq = append(uniq, id)
		}
		if len(uniq) == 0 {
			continue
		}
		sort.Slice(uniq, func(i, j int) bool { return uniq[i] < uniq[j] })
		out[status] = uniq
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func equalTaskIDsByStatus(a, b map[string][]int64) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
	}
	return true
}

func flattenTaskIDsStatusMap(m map[string][]int64) []int64 {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]int64, 0, len(m)*4)
	for _, k := range keys {
		ids := m[k]
		out = append(out, ids...)
	}
	return out
}

func encodeTaskIDsByStatusForFeishu(m map[string][]int64) string {
	normalized := normalizeTaskIDsByStatus(m)
	if len(normalized) == 0 {
		return ""
	}
	b, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(b)
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

func toString(v any) string {
	return feishusdk.BitableValueToString(v)
}
