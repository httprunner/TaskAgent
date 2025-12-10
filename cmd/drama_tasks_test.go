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

func TestCreateDramaSearchTasksCreatesRecords(t *testing.T) {
	ts := time.Date(2025, time.December, 5, 0, 0, 0, 0, time.Local).Unix()
	client := &fakeDramaTaskClient{
		rows: []taskagent.BitableRow{
			{Fields: map[string]any{
				taskagent.DefaultDramaFields().DramaName:   "DramaA",
				taskagent.DefaultDramaFields().DramaID:     "B1",
				taskagent.DefaultDramaFields().SearchAlias: "AliasA|AliasB|DramaA",
				taskagent.DefaultDramaFields().CaptureDate: "2025-12-05",
			}},
			{Fields: map[string]any{
				taskagent.DefaultDramaFields().DramaName:   "DramaB",
				taskagent.DefaultDramaFields().DramaID:     "B2",
				taskagent.DefaultDramaFields().SearchAlias: "AliasC",
				taskagent.DefaultDramaFields().CaptureDate: fmt.Sprintf("%d", ts),
			}},
			{Fields: map[string]any{
				taskagent.DefaultDramaFields().DramaName:   "DramaSkip",
				taskagent.DefaultDramaFields().DramaID:     "B3",
				taskagent.DefaultDramaFields().SearchAlias: "Other",
				taskagent.DefaultDramaFields().CaptureDate: "2025-12-04",
			}},
		},
	}

	cfg := DramaTaskConfig{
		Date:          "2025-12-05",
		TaskTableURL:  "task",
		DramaTableURL: "drama",
		BatchSize:     2,
		client:        client,
	}

	res, err := CreateDramaSearchTasks(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CreateDramaSearchTasks returned error: %v", err)
	}
	if res.CreatedCount != 5 {
		t.Fatalf("expected 5 tasks created, got %d", res.CreatedCount)
	}
	if res.ParamCount != 5 {
		t.Fatalf("expected param count 5, got %d", res.ParamCount)
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

func TestCreateDramaSearchTasksErrors(t *testing.T) {
	cfg := DramaTaskConfig{}
	if _, err := CreateDramaSearchTasks(context.Background(), cfg); err == nil {
		t.Fatal("expected error for missing date")
	}

	client := &fakeDramaTaskClient{}
	cfg = DramaTaskConfig{
		Date:          "2025-12-05",
		TaskTableURL:  "task",
		DramaTableURL: "drama",
		client:        client,
	}
	client.createErr = errors.New("boom")
	cfg.BatchSize = 1
	cfg.client = client
	client.rows = []taskagent.BitableRow{
		{Fields: map[string]any{
			taskagent.DefaultDramaFields().DramaName:   "DramaA",
			taskagent.DefaultDramaFields().DramaID:     "B1",
			taskagent.DefaultDramaFields().SearchAlias: "AliasA",
			taskagent.DefaultDramaFields().CaptureDate: "2025-12-05",
		}},
	}
	if _, err := CreateDramaSearchTasks(context.Background(), cfg); err == nil {
		t.Fatal("expected error when CreateTaskRecords fails")
	}
}

func TestBuildAliasParamsDeduplicates(t *testing.T) {
	got := buildAliasParams("Primary", "AliasA|AliasB|AliasA|Primary", []string{"|"})
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
			got := buildAliasParams("Primary", tc.aliasRaw, tc.seps)
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

