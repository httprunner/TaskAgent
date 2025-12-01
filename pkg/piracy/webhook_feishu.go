package piracy

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func newFeishuSummarySource(fields summaryFieldConfig, opts WebhookOptions) (summaryDataSource, error) {
	dramaURL := strings.TrimSpace(opts.DramaTableURL)
	if dramaURL == "" {
		dramaURL = strings.TrimSpace(os.Getenv("DRAMA_BITABLE_URL"))
	}
	if dramaURL == "" {
		return nil, fmt.Errorf("drama table url is required for feishu source")
	}

	resultURL := strings.TrimSpace(opts.ResultTableURL)
	if resultURL == "" {
		resultURL = strings.TrimSpace(os.Getenv(feishu.EnvResultBitableURL))
	}
	if resultURL == "" {
		return nil, fmt.Errorf("result table url is required for feishu source")
	}

	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, err
	}

	return &feishuSummarySource{
		client:      client,
		fields:      fields,
		dramaTable:  dramaURL,
		resultTable: resultURL,
	}, nil
}

type feishuSummarySource struct {
	client      *feishu.Client
	fields      summaryFieldConfig
	dramaTable  string
	resultTable string
}

func (s *feishuSummarySource) FetchDrama(ctx context.Context, params string) (*dramaInfo, error) {
	filter := BuildParamsFilter([]string{params}, s.fields.Drama.DramaName)
	rows, err := fetchRows(ctx, s.client, TableConfig{URL: s.dramaTable, Filter: filter, Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("drama %s not found in feishu table", params)
	}
	return s.rowToDrama(rows[0], params), nil
}

func (s *feishuSummarySource) FetchRecords(ctx context.Context, query recordQuery) ([]CaptureRecordPayload, error) {
	filter := s.buildRecordFilter(query)
	rows, err := fetchRows(ctx, s.client, TableConfig{URL: s.resultTable, Filter: filter, Limit: query.Limit})
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

func (s *feishuSummarySource) buildRecordFilter(query recordQuery) *feishu.FilterInfo {
	filters := make([]*feishu.FilterInfo, 0, 5)
	if query.ExtraFilter != nil {
		filters = append(filters, query.ExtraFilter)
	}
	if trimmed := strings.TrimSpace(query.Params); trimmed != "" {
		filters = append(filters, BuildParamsFilter([]string{trimmed}, s.fields.Result.Params))
	}
	if trimmed := strings.TrimSpace(query.UserID); trimmed != "" {
		filters = append(filters, EqFilter(s.fields.Result.UserID, trimmed))
	}
	if trimmed := strings.TrimSpace(query.ItemID); trimmed != "" {
		filters = append(filters, EqFilter(s.fields.Result.ItemID, trimmed))
	}
	return CombineFiltersAND(filters...)
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
