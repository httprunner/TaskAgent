package pool

import (
	"context"
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
	if len(filtered) != 2 {
		bt := make([]int64, 0, len(filtered))
		for _, task := range filtered {
			bt = append(bt, task.TaskID)
		}
		t.Fatalf("expected 2 tasks, got %d: %#v", len(filtered), bt)
	}
	if filtered[0].TaskID != 1 || filtered[1].TaskID != 2 {
		ids := []int64{filtered[0].TaskID, filtered[1].TaskID}
		t.Fatalf("unexpected task order: %#v", ids)
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
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].TaskID != 1 {
		t.Fatalf("expected task 1, got %d", tasks[0].TaskID)
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

func timePtr(t time.Time) *time.Time {
	v := t
	return &v
}
