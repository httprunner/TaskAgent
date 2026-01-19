package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
)

type fakeDramaTaskClient struct {
	rows        []taskagent.BitableRow
	createCalls [][]taskagent.TaskRecordInput
	createErr   error
}

func (f *fakeDramaTaskClient) FetchBitableRows(ctx context.Context, rawURL string, opts *taskagent.FeishuTaskQueryOptions) ([]taskagent.BitableRow, error) {
	return f.rows, nil
}

func (f *fakeDramaTaskClient) CreateTaskRecords(ctx context.Context, rawURL string, records []taskagent.TaskRecordInput, override *taskagent.FeishuTaskFields) ([]string, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	copyChunk := append([]taskagent.TaskRecordInput(nil), records...)
	f.createCalls = append(f.createCalls, copyChunk)
	ids := make([]string, len(records))
	for i := range ids {
		ids[i] = fmt.Sprintf("rec-%d-%d", len(f.createCalls), i)
	}
	return ids, nil
}

func TestCreateSearchTasksDramaCreatesRecords(t *testing.T) {
	ts := time.Date(2025, time.December, 5, 0, 0, 0, 0, time.Local).Unix()
	client := &fakeDramaTaskClient{
		rows: []taskagent.BitableRow{
			{Fields: map[string]any{
				taskagent.DefaultSourceFields().DramaName:      "DramaA",
				taskagent.DefaultSourceFields().DramaID:        "B1",
				taskagent.DefaultSourceFields().SearchKeywords: "AliasA|AliasB|DramaA",
				taskagent.DefaultSourceFields().CaptureDate:    "2025-12-05",
			}},
			{Fields: map[string]any{
				taskagent.DefaultSourceFields().DramaName:      "DramaB",
				taskagent.DefaultSourceFields().DramaID:        "B2",
				taskagent.DefaultSourceFields().SearchKeywords: "AliasC",
				taskagent.DefaultSourceFields().CaptureDate:    fmt.Sprintf("%d", ts),
			}},
			{Fields: map[string]any{
				taskagent.DefaultSourceFields().DramaName:      "DramaSkip",
				taskagent.DefaultSourceFields().DramaID:        "B3",
				taskagent.DefaultSourceFields().SearchKeywords: "Other",
				taskagent.DefaultSourceFields().CaptureDate:    "2025-12-04",
			}},
		},
	}

	cfg := TaskConfig{
		Date:           "2025-12-05",
		TaskTableURL:   "task",
		SourceTableURL: "drama",
		BatchSize:      2,
		client:         client,
	}

	res, err := CreateSearchTasks(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CreateSearchTasks returned error: %v", err)
	}
	if res.CreatedCount != 5 {
		t.Fatalf("expected 5 tasks created, got %d", res.CreatedCount)
	}
	if res.TotalParams != 5 {
		t.Fatalf("expected param count 5, got %d", res.TotalParams)
	}
	if len(res.Details) != 2 {
		t.Fatalf("expected 2 drama details, got %d", len(res.Details))
	}
	if len(client.createCalls) != 3 {
		t.Fatalf("expected 3 batch calls, got %d", len(client.createCalls))
	}
	firstBatch := client.createCalls[0]
	if firstBatch[0].Params != "DramaA" || firstBatch[1].Params != "AliasA" {
		t.Fatalf("unexpected first batch params: %#v", firstBatch)
	}
	if client.createCalls[2][0].Params != "AliasC" {
		t.Fatalf("expected AliasC in last batch, got %#v", client.createCalls[2][0].Params)
	}
}

func TestCreateSearchTasksProfileCreatesRecords(t *testing.T) {
	client := &fakeDramaTaskClient{
		rows: []taskagent.BitableRow{
			{Fields: map[string]any{
				taskagent.DefaultSourceFields().BizTaskID:      "TASK-1",
				taskagent.DefaultSourceFields().DramaID:        "B1",
				taskagent.DefaultSourceFields().AccountID:      "U1",
				taskagent.DefaultSourceFields().SearchKeywords: "Alice|Bob|Alice",
				taskagent.DefaultSourceFields().Platform:       "快手",
				taskagent.DefaultSourceFields().CaptureDate:    "2025-12-05",
			}},
			{Fields: map[string]any{
				taskagent.DefaultSourceFields().BizTaskID:      "TASK-2",
				taskagent.DefaultSourceFields().DramaID:        "B2",
				taskagent.DefaultSourceFields().AccountID:      "U2",
				taskagent.DefaultSourceFields().SearchKeywords: "SkipMe",
				taskagent.DefaultSourceFields().Platform:       "快手",
				taskagent.DefaultSourceFields().CaptureDate:    "2025-12-04",
			}},
		},
	}

	cfg := TaskConfig{
		Date:           "2025-12-05",
		TaskTableURL:   "task",
		SourceTableURL: "account",
		BatchSize:      2,
		client:         client,
	}

	res, err := CreateSearchTasks(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CreateSearchTasks returned error: %v", err)
	}
	if res.CreatedCount != 2 {
		t.Fatalf("expected 2 tasks created, got %d", res.CreatedCount)
	}
	if res.TotalParams != 2 {
		t.Fatalf("expected param count 2, got %d", res.TotalParams)
	}
	if len(res.Details) != 1 {
		t.Fatalf("expected 1 account detail, got %d", len(res.Details))
	}
	if len(client.createCalls) != 1 {
		t.Fatalf("expected 1 batch call, got %d", len(client.createCalls))
	}
	firstBatch := client.createCalls[0]
	if len(firstBatch) != 2 {
		t.Fatalf("unexpected batch size: %#v", firstBatch)
	}
	if firstBatch[0].Params != "Alice" || firstBatch[1].Params != "Bob" {
		t.Fatalf("unexpected params: %#v", firstBatch)
	}
	if firstBatch[0].BizTaskID != "TASK-1" || firstBatch[0].UserID != "U1" || firstBatch[0].BookID != "B1" {
		t.Fatalf("unexpected task metadata: %#v", firstBatch[0])
	}
	if firstBatch[0].App != "com.smile.gifmaker" || firstBatch[0].Scene != taskagent.SceneProfileSearch {
		t.Fatalf("unexpected app/scene: %#v", firstBatch[0])
	}
	if firstBatch[0].GroupID != "快手_B1_U1" {
		t.Fatalf("unexpected group id: %#v", firstBatch[0].GroupID)
	}
}

func TestCreateSearchTasksProfileErrors(t *testing.T) {
	cfg := TaskConfig{}
	if _, err := CreateSearchTasks(context.Background(), cfg); err == nil {
		t.Fatal("expected error for missing date")
	}

	client := &fakeDramaTaskClient{}
	cfg = TaskConfig{
		Date:           "2025-12-05",
		TaskTableURL:   "task",
		SourceTableURL: "account",
		client:         client,
	}
	client.createErr = errors.New("boom")
	cfg.BatchSize = 1
	cfg.client = client
	client.rows = []taskagent.BitableRow{
		{Fields: map[string]any{
			taskagent.DefaultSourceFields().BizTaskID:      "TASK-1",
			taskagent.DefaultSourceFields().DramaID:        "B1",
			taskagent.DefaultSourceFields().AccountID:      "U1",
			taskagent.DefaultSourceFields().SearchKeywords: "Alice",
			taskagent.DefaultSourceFields().CaptureDate:    "2025-12-05",
		}},
	}
	if _, err := CreateSearchTasks(context.Background(), cfg); err == nil {
		t.Fatal("expected error when CreateTaskRecords fails")
	}
}

func TestCreateSearchTasksDramaErrors(t *testing.T) {
	cfg := TaskConfig{}
	if _, err := CreateSearchTasks(context.Background(), cfg); err == nil {
		t.Fatal("expected error for missing date")
	}

	client := &fakeDramaTaskClient{}
	cfg = TaskConfig{
		Date:           "2025-12-05",
		TaskTableURL:   "task",
		SourceTableURL: "drama",
		client:         client,
	}
	client.createErr = errors.New("boom")
	cfg.BatchSize = 1
	cfg.client = client
	client.rows = []taskagent.BitableRow{
		{Fields: map[string]any{
			taskagent.DefaultSourceFields().DramaName:      "DramaA",
			taskagent.DefaultSourceFields().DramaID:        "B1",
			taskagent.DefaultSourceFields().SearchKeywords: "AliasA",
			taskagent.DefaultSourceFields().CaptureDate:    "2025-12-05",
		}},
	}
	if _, err := CreateSearchTasks(context.Background(), cfg); err == nil {
		t.Fatal("expected error when CreateTaskRecords fails")
	}
}

func TestBuildAliasParamsDeduplicates(t *testing.T) {
	got := buildKeywordsParams("Primary", "AliasA|AliasB|AliasA|Primary", []string{"|"})
	want := []string{"Primary", "AliasA", "AliasB"}
	if len(got) != len(want) {
		t.Fatalf("unexpected param count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected param order: got %v want %v", got, want)
		}
	}
}

func TestBuildAliasParamsSupportsFullWidthSeparator(t *testing.T) {
	tests := []struct {
		name     string
		aliasRaw string
		seps     []string
		want     []string
	}{
		{
			name:     "defaultSeparators",
			aliasRaw: "AliasA｜AliasB|AliasC",
			seps:     nil,
			want:     []string{"Primary", "AliasA", "AliasB", "AliasC"},
		},
		{
			name:     "explicitList",
			aliasRaw: "AliasA|AliasB｜AliasC",
			seps:     []string{"|", "｜"},
			want:     []string{"Primary", "AliasA", "AliasB", "AliasC"},
		},
		{
			name:     "mixedList",
			aliasRaw: "AliasA/AliasB｜AliasC",
			seps:     []string{"|", "｜", "/"},
			want:     []string{"Primary", "AliasA", "AliasB", "AliasC"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildKeywordsParams("Primary", tc.aliasRaw, tc.seps)
			if len(got) != len(tc.want) {
				t.Fatalf("unexpected param count: got %d want %d", len(got), len(tc.want))
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("unexpected param order: got %v want %v", got, tc.want)
				}
			}
		})
	}
}
