package taskagent

import (
	"strconv"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

// FeishuClient is a low-level client for interacting with Feishu Bitables.
// It is intended for advanced use cases (such as custom task creation)
// where the higher-level FeishuTaskClient is not sufficient.
type FeishuClient = feishusdk.Client

// TaskRecordInput describes the payload needed to create a new task record
// in a Feishu task table. It mirrors the schema expected by TaskAgent's
// default task status table.
type TaskRecordInput = feishusdk.TaskRecordInput

// SheetMeta describes spreadsheet metadata and header rows for sheet-based workflows.
type SheetMeta = feishusdk.SheetMeta

// SheetCellUpdate represents a single-cell update in a spreadsheet.
type SheetCellUpdate = feishusdk.SheetCellUpdate

// NewFeishuClientFromEnv constructs a FeishuClient using environment
// variables (FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_TENANT_KEY /
// FEISHU_BASE_URL). It is a thin wrapper around the internal Feishu SDK.
func NewFeishuClientFromEnv() (*FeishuClient, error) {
	return feishusdk.NewClientFromEnv()
}

// BitableRow aliases the generic Feishu bitable row type used in helper
// packages (e.g. webhook summary, result storage).
type BitableRow = feishusdk.BitableRow

// FeishuFilterInfo aliases the filter struct used when querying bitables.
type FeishuFilterInfo = feishusdk.FilterInfo

// FeishuCondition aliases a single filter condition.
type FeishuCondition = feishusdk.Condition

// FeishuChildrenFilter aliases a nested filter group.
type FeishuChildrenFilter = feishusdk.ChildrenFilter

// FeishuTaskQueryOptions aliases the query options used for bitable searches.
type FeishuTaskQueryOptions = feishusdk.QueryOptions

// FeishuTaskFields aliases the task-table field mapping.
type FeishuTaskFields = feishusdk.TaskFields

// FeishuResultFields aliases the result-table field mapping.
type FeishuResultFields = feishusdk.ResultFields

// FeishuSourceFields aliases the source-table field mapping.
type FeishuSourceFields = feishusdk.SourceFields

// FeishuTaskTable aliases the decoded task-table payload.
type FeishuTaskTable = feishusdk.TaskTable

// FeishuTaskRow aliases a single task row.
type FeishuTaskRow = feishusdk.TaskRow

// DefaultTaskFields returns the current default task field mapping.
func DefaultTaskFields() FeishuTaskFields {
	return feishusdk.DefaultTaskFields
}

// DefaultResultFields returns the current default result field mapping.
func DefaultResultFields() FeishuResultFields {
	return feishusdk.DefaultResultFields
}

// DefaultSourceFields returns the current default drama field mapping.
func DefaultSourceFields() FeishuSourceFields {
	return feishusdk.DefaultSourceFields
}

// NewFeishuFilterInfo constructs a new filter with the given conjunction
// ("and"/"or").
func NewFeishuFilterInfo(conjunction string) *FeishuFilterInfo {
	return feishusdk.NewFilterInfo(conjunction)
}

// NewFeishuCondition constructs a single condition.
func NewFeishuCondition(field, operator string, values ...string) *FeishuCondition {
	return feishusdk.NewCondition(field, operator, values...)
}

// ExactDateCondition builds a Feishu "ExactDate" filter condition for a datetime field.
// day must be in "2006-01-02" format (local time).
func ExactDateCondition(fieldName, day string) *FeishuCondition {
	trimmed := strings.TrimSpace(day)
	if trimmed == "" || strings.TrimSpace(fieldName) == "" {
		return nil
	}
	dayTime, err := time.ParseInLocation("2006-01-02", trimmed, time.Local)
	if err != nil {
		return nil
	}
	tsMs := strconv.FormatInt(dayTime.UnixMilli(), 10)
	return NewFeishuCondition(fieldName, "is", "ExactDate", tsMs)
}

// NewFeishuChildrenFilter constructs a nested filter group with the given
// conjunction and conditions.
func NewFeishuChildrenFilter(conjunction string, conds ...*FeishuCondition) *FeishuChildrenFilter {
	return feishusdk.NewChildrenFilter(conjunction, conds...)
}

// MergeFeishuFiltersAND merges multiple filters under a single AND root.
func MergeFeishuFiltersAND(filters ...*FeishuFilterInfo) *FeishuFilterInfo {
	return feishusdk.MergeFiltersAND(filters...)
}

// CloneFeishuFilter performs a deep copy of a FilterInfo so callers
// can safely mutate filters.
func CloneFeishuFilter(filter *FeishuFilterInfo) *FeishuFilterInfo {
	return feishusdk.CloneFilter(filter)
}

// MapAppValue normalizes an app identifier for downstream task fields.
func MapAppValue(app string) string {
	return feishusdk.MapAppValue(app)
}

// StructFieldMap extracts string fields from a struct into a mapping from
// Go field name to Feishu column name. It is commonly used for field
// mapping in summary/webhook helpers.
func StructFieldMap(schema any) map[string]string {
	return feishusdk.StructFieldMap(schema)
}
