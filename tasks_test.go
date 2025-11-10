package pool

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	feishusvc "github.com/httprunner/TaskAgent/feishu"
)

func TestFilterFeishuTasksByDate(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*3600)
	dayStart := time.Date(2025, 11, 9, 0, 0, 0, 0, loc)
	dayEnd := dayStart.Add(24 * time.Hour)
	now := time.Date(2025, 11, 9, 12, 0, 0, 0, loc)

	makeTime := func(h int) *time.Time {
		v := time.Date(2025, 11, 9, h, 0, 0, 0, loc)
		return &v
	}

	tasks := []*FeishuTask{
		{TaskID: 1, Datetime: makeTime(9)},  // valid
		{TaskID: 2, Datetime: makeTime(12)}, // valid edge (equal now)
		{TaskID: 3, Datetime: makeTime(13)}, // future today
		{TaskID: 4, Datetime: timePtr(time.Date(2025, 11, 8, 23, 0, 0, 0, loc))},
		{TaskID: 5}, // missing datetime
	}

	filtered := filterFeishuTasksByDate(tasks, dayStart, dayEnd, now)
	expected := []int64{1, 2, 5}
	if len(filtered) != len(expected) {
		bt := make([]int64, 0, len(filtered))
		for _, task := range filtered {
			bt = append(bt, task.TaskID)
		}
		t.Fatalf("expected %d tasks, got %d: %#v", len(expected), len(filtered), bt)
	}
	for i, id := range expected {
		if filtered[i].TaskID != id {
			t.Fatalf("unexpected task order: got %d at position %d, want %d", filtered[i].TaskID, i, id)
		}
	}
}

func TestFetchFeishuTasksWithStrategyFiltersInvalidTasks(t *testing.T) {
	ctx := context.Background()
	loc := time.FixedZone("UTC+8", 8*3600)
	dayStart := time.Date(2025, 11, 9, 0, 0, 0, 0, loc)
	dayEnd := dayStart.Add(24 * time.Hour)
	now := time.Date(2025, 11, 9, 10, 0, 0, 0, loc)

	valid := timePtr(time.Date(2025, 11, 9, 9, 30, 0, 0, loc))
	future := timePtr(time.Date(2025, 11, 9, 21, 0, 0, 0, loc))
	past := timePtr(time.Date(2025, 11, 8, 23, 0, 0, 0, loc))

	client := &stubTargetClient{
		tables: []*feishusvc.TargetTable{
			{
				Rows: []feishusvc.TargetRow{
					{TaskID: 1, Params: "foo", App: "com.app", Datetime: valid},
					{TaskID: 2, Params: "bar", App: "com.app", Datetime: future},
					{TaskID: 3, Params: "baz", App: "com.app", Datetime: past},
					{TaskID: 4, Params: "qux", App: "com.app"},
				},
			},
		},
	}

	tasks, err := fetchFeishuTasksWithStrategy(ctx, client, "https://example.com/bitable/abc", feishusvc.DefaultTargetFields, "com.app", dayStart, dayEnd, now, []string{""}, 5, true)
	if err != nil {
		t.Fatalf("fetchFeishuTasksWithStrategy returned error: %v", err)
	}
	expectedIDs := []int64{1, 4}
	if len(tasks) != len(expectedIDs) {
		t.Fatalf("expected %d tasks, got %d", len(expectedIDs), len(tasks))
	}
	for i, id := range expectedIDs {
		if tasks[i].TaskID != id {
			t.Fatalf("unexpected task order: got %d at position %d, want %d", tasks[i].TaskID, i, id)
		}
	}
}

func TestFetchFeishuTasksWithStrategyFallbackFiltersFutureTasks(t *testing.T) {
	ctx := context.Background()
	loc := time.FixedZone("UTC+8", 8*3600)
	dayStart := time.Date(2025, 11, 9, 0, 0, 0, 0, loc)
	dayEnd := dayStart.Add(24 * time.Hour)
	now := time.Date(2025, 11, 9, 11, 0, 0, 0, loc)

	valid := timePtr(time.Date(2025, 11, 9, 10, 0, 0, 0, loc))
	future := timePtr(time.Date(2025, 11, 9, 15, 0, 0, 0, loc))

	client := &stubTargetClient{
		tables: []*feishusvc.TargetTable{
			{
				Rows: []feishusvc.TargetRow{
					{TaskID: 10, Params: "foo", App: "com.app", Datetime: valid},
					{TaskID: 20, Params: "bar", App: "com.app", Datetime: future},
				},
			},
		},
	}

	tasks, err := fetchFeishuTasksWithStrategy(ctx, client, "https://example.com/bitable/def", feishusvc.DefaultTargetFields, "com.app", dayStart, dayEnd, now, []string{""}, 2, false)
	if err != nil {
		t.Fatalf("fetchFeishuTasksWithStrategy fallback returned error: %v", err)
	}
	if len(tasks) != 1 || tasks[0].TaskID != 10 {
		t.Fatalf("unexpected fallback result: %#v", tasks)
	}
}

type stubTargetClient struct {
	tables []*feishusvc.TargetTable
	index  int
}

func (s *stubTargetClient) FetchTargetTableWithOptions(ctx context.Context, rawURL string, override *feishusvc.TargetFields, opts *feishusvc.TargetQueryOptions) (*feishusvc.TargetTable, error) {
	if s.index >= len(s.tables) {
		return &feishusvc.TargetTable{}, nil
	}
	tbl := s.tables[s.index]
	s.index++
	return tbl, nil
}

func (s *stubTargetClient) UpdateTargetStatus(ctx context.Context, table *feishusvc.TargetTable, taskID int64, newStatus string) error {
	return nil
}

func (s *stubTargetClient) UpdateTargetFields(ctx context.Context, table *feishusvc.TargetTable, taskID int64, fields map[string]any) error {
	return nil
}

func timePtr(t time.Time) *time.Time {
	v := t
	return &v
}

func TestFetchTodayPendingFeishuTasksFillsWithFallback(t *testing.T) {
	ctx := context.Background()
	loc := time.FixedZone("UTC+8", 8*3600)
	now := time.Date(2025, 11, 9, 10, 0, 0, 0, loc)
	client := &filterAwareTargetClient{
		withDatetimeRows: []feishusvc.TargetRow{
			{TaskID: 1, Params: "A", App: "com.app", Datetime: timePtr(time.Date(2025, 11, 9, 8, 0, 0, 0, loc))},
			{TaskID: 2, Params: "B", App: "com.app", Datetime: timePtr(time.Date(2025, 11, 9, 9, 0, 0, 0, loc))},
		},
		fallbackRows: []feishusvc.TargetRow{
			{TaskID: 2, Params: "B-duplicate", App: "com.app"},
			{TaskID: 3, Params: "C", App: "com.app"},
			{TaskID: 4, Params: "D", App: "com.app"},
			{TaskID: 5, Params: "E", App: "com.app"},
			{TaskID: 6, Params: "F", App: "com.app"},
		},
	}
	tasks, err := fetchTodayPendingFeishuTasks(ctx, client, "https://example.com/bitable/foo", "com.app", 5, now)
	if err != nil {
		t.Fatalf("fetchTodayPendingFeishuTasks returned error: %v", err)
	}
	ids := collectTaskIDs(tasks)
	expected := []int64{1, 2, 3, 4, 5}
	if !equalIDs(ids, expected) {
		t.Fatalf("unexpected task ids: got %v, want %v", ids, expected)
	}
}

func TestFetchTodayPendingFeishuTasksSkipsFallbackWhenSufficient(t *testing.T) {
	ctx := context.Background()
	loc := time.FixedZone("UTC+8", 8*3600)
	now := time.Date(2025, 11, 9, 10, 0, 0, 0, loc)
	withDatetime := make([]feishusvc.TargetRow, 0, 6)
	for i := 0; i < 6; i++ {
		hour := 4 + i
		withDatetime = append(withDatetime, feishusvc.TargetRow{
			TaskID:   int64(10 + i),
			Params:   fmt.Sprintf("task-%d", 10+i),
			App:      "com.app",
			Datetime: timePtr(time.Date(2025, 11, 9, hour, 0, 0, 0, loc)),
		})
	}
	client := &filterAwareTargetClient{
		withDatetimeRows: withDatetime,
		fallbackRows: []feishusvc.TargetRow{
			{TaskID: 99, Params: "Z", App: "com.app"},
		},
	}
	tasks, err := fetchTodayPendingFeishuTasks(ctx, client, "https://example.com/bitable/bar", "com.app", 5, now)
	if err != nil {
		t.Fatalf("fetchTodayPendingFeishuTasks returned error: %v", err)
	}
	ids := collectTaskIDs(tasks)
	expected := []int64{10, 11, 12, 13, 14}
	if !equalIDs(ids, expected) {
		t.Fatalf("unexpected ids: got %v, want %v", ids, expected)
	}
}

type filterAwareTargetClient struct {
	withDatetimeRows []feishusvc.TargetRow
	fallbackRows     []feishusvc.TargetRow
}

func (f *filterAwareTargetClient) FetchTargetTableWithOptions(ctx context.Context, rawURL string, override *feishusvc.TargetFields, opts *feishusvc.TargetQueryOptions) (*feishusvc.TargetTable, error) {
	rows := f.fallbackRows
	if strings.Contains(opts.Filter, "[Datetime]") {
		rows = f.withDatetimeRows
	}
	clone := make([]feishusvc.TargetRow, len(rows))
	copy(clone, rows)
	return &feishusvc.TargetTable{
		Rows:   clone,
		Fields: feishusvc.DefaultTargetFields,
	}, nil
}

func (f *filterAwareTargetClient) UpdateTargetStatus(ctx context.Context, table *feishusvc.TargetTable, taskID int64, newStatus string) error {
	return nil
}

func (f *filterAwareTargetClient) UpdateTargetFields(ctx context.Context, table *feishusvc.TargetTable, taskID int64, fields map[string]any) error {
	return nil
}

func collectTaskIDs(tasks []*FeishuTask) []int64 {
	result := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		result = append(result, task.TaskID)
	}
	return result
}

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type recordingTargetClient struct {
	updates []map[string]any
}

func (r *recordingTargetClient) FetchTargetTableWithOptions(ctx context.Context, rawURL string, override *feishusvc.TargetFields, opts *feishusvc.TargetQueryOptions) (*feishusvc.TargetTable, error) {
	return nil, nil
}

func (r *recordingTargetClient) UpdateTargetStatus(ctx context.Context, table *feishusvc.TargetTable, taskID int64, newStatus string) error {
	return nil
}

func (r *recordingTargetClient) UpdateTargetFields(ctx context.Context, table *feishusvc.TargetTable, taskID int64, fields map[string]any) error {
	cp := make(map[string]any, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	r.updates = append(r.updates, cp)
	return nil
}

func TestUpdateFeishuTaskStatusesAssignsDispatchedDevice(t *testing.T) {
	client := &recordingTargetClient{}
	table := &feishusvc.TargetTable{
		Ref:    feishusvc.BitableRef{AppToken: "app", TableID: "tbl"},
		Fields: feishusvc.DefaultTargetFields,
	}
	task := &FeishuTask{
		TaskID: 1,
		source: &feishuTaskSource{
			client: client,
			table:  table,
		},
	}

	if err := updateFeishuTaskStatuses(context.Background(), []*FeishuTask{task}, "dispatched", "device-xyz"); err != nil {
		t.Fatalf("updateFeishuTaskStatuses returned error: %v", err)
	}
	if len(client.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(client.updates))
	}
	statusField := feishusvc.DefaultTargetFields.Status
	serialField := feishusvc.DefaultTargetFields.DispatchedDevice
	update := client.updates[0]
	if got := update[statusField]; got != "dispatched" {
		t.Fatalf("expected status field=%s got=%v", statusField, got)
	}
	if got := update[serialField]; got != "device-xyz" {
		t.Fatalf("expected device serial field=%s got=%v", serialField, got)
	}
}
