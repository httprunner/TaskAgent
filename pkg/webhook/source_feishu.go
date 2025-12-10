package webhook

import (
	"context"
	"strings"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func newFeishuSummarySource(fields summaryFieldConfig, opts Options) (summaryDataSource, error) {
	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		return nil, err
	}

	dramaURL := strings.TrimSpace(opts.DramaTableURL)
	resultURL := strings.TrimSpace(opts.ResultTableURL)
	if dramaURL == "" || resultURL == "" {
		return nil, ErrNoCaptureRecords
	}

	return &feishuSummarySource{
		client:      client,
		fields:      fields,
		dramaTable:  dramaURL,
		resultTable: resultURL,
	}, nil
}

type feishuSummarySource struct {
	client      *taskagent.FeishuClient
	fields      summaryFieldConfig
	dramaTable  string
	resultTable string
}

func (s *feishuSummarySource) FetchDrama(ctx context.Context, params string) (*dramaInfo, error) {
	filter := BuildParamsFilter([]string{params}, s.fields.Drama.DramaName)
	opts := &taskagent.FeishuTaskQueryOptions{
		Filter: filter,
		Limit:  1,
	}
	rows, err := s.client.FetchBitableRows(ctx, s.dramaTable, opts)
	if err != nil {
		return nil, err
	}
	taskagent.MirrorDramaRowsIfNeeded(s.dramaTable, rows)
	if len(rows) == 0 {
		return nil, ErrNoCaptureRecords
	}
	return s.rowToDrama(rows[0], params), nil
}

func (s *feishuSummarySource) FetchRecords(ctx context.Context, query recordQuery) ([]CaptureRecordPayload, error) {
	filter := s.buildRecordFilter(query)
	opts := &taskagent.FeishuTaskQueryOptions{
		Filter: filter,
		Limit:  query.Limit,
	}
	rows, err := s.client.FetchBitableRows(ctx, s.resultTable, opts)
	if err != nil {
		return nil, err
	}
	records := make([]CaptureRecordPayload, 0, len(rows))
	for _, row := range rows {
		records = append(records, CaptureRecordPayload{
			RecordID: row.RecordID,
			Fields:   cloneFields(row.Fields),
		})
	}
	if query.PreferLatest {
		records = PickLatestRecords(records)
	}
	return records, nil
}

func (s *feishuSummarySource) Close() error { return nil }

func (s *feishuSummarySource) rowToDrama(row Row, fallbackName string) *dramaInfo {
	name := strings.TrimSpace(getString(row.Fields, s.fields.Drama.DramaName))
	info := &dramaInfo{
		ID:             strings.TrimSpace(getString(row.Fields, s.fields.Drama.DramaID)),
		Name:           name,
		Priority:       strings.TrimSpace(getString(row.Fields, s.fields.Drama.Priority)),
		RightsScenario: strings.TrimSpace(getString(row.Fields, s.fields.Drama.RightsProtectionScenario)),
		RawFields:      cloneFields(row.Fields),
	}
	if info.Name == "" {
		info.Name = fallbackName
	}
	return info
}

func (s *feishuSummarySource) buildRecordFilter(query recordQuery) *taskagent.FeishuFilterInfo {
	filters := make([]*taskagent.FeishuFilterInfo, 0, 5)
	if query.ExtraFilter != nil {
		filters = append(filters, query.ExtraFilter)
	}
	if trimmed := strings.TrimSpace(query.App); trimmed != "" {
		filters = append(filters, EqFilter(s.fields.Result.App, trimmed))
	}
	if trimmed := strings.TrimSpace(query.Scene); trimmed != "" {
		filters = append(filters, EqFilter(s.fields.Result.Scene, trimmed))
	}
	if trimmed := strings.TrimSpace(query.Params); trimmed != "" {
		filters = append(filters, BuildParamsFilter([]string{trimmed}, s.fields.Result.Params))
	}
	if trimmed := strings.TrimSpace(query.UserID); trimmed != "" {
		filters = append(filters, EqFilter(s.fields.Result.UserID, trimmed))
	}
	if trimmed := strings.TrimSpace(query.UserName); trimmed != "" {
		filters = append(filters, EqFilter(s.fields.Result.UserName, trimmed))
	}
	if trimmed := strings.TrimSpace(query.ItemID); trimmed != "" {
		filters = append(filters, EqFilter(s.fields.Result.ItemID, trimmed))
	}
	return feishusdk.MergeFiltersAND(filters...)
}

// PickLatestRecords returns the last record when PreferLatest is set.
func PickLatestRecords(records []CaptureRecordPayload) []CaptureRecordPayload {
	if len(records) <= 1 {
		return records
	}
	return []CaptureRecordPayload{records[len(records)-1]}
}

func cloneFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(fields))
	for k, v := range fields {
		cloned[k] = v
	}
	return cloned
}
