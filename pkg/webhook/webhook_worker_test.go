package webhook

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
)

func TestParseTaskIDs(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want []int64
	}{
		{name: "empty", in: "", want: nil},
		{name: "string_csv", in: "123,456", want: []int64{123, 456}},
		{name: "string_mixed", in: " 123 | 456  123 ", want: []int64{123, 456}},
		{name: "slice_any", in: []any{"123", "456", "123"}, want: []int64{123, 456}},
		{name: "slice_string", in: []string{"123", "456", "123"}, want: []int64{123, 456}},
		{
			name: "feishu_multi_select_objects",
			in: []any{
				map[string]any{"text": "123"},
				map[string]any{"value": "456"},
				map[string]any{"text": "123"},
			},
			want: []int64{123, 456},
		},
		{
			name: "json_status_map_string",
			in:   `{"success":[123],"failed":["456","123"],"unknown":[0]}`,
			want: []int64{123, 456},
		},
		{
			name: "json_status_map_object",
			in: map[string]any{
				"success": []any{json.Number("123")},
				"failed":  []any{"456", "123"},
			},
			want: []int64{123, 456},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := parseTaskIDs(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len=%d want=%d got=%v", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("idx=%d got=%d want=%d all=%v", i, got[i], tc.want[i], got)
				}
			}
		})
	}
}

func TestAllTasksReady(t *testing.T) {
	now := time.Date(2025, 12, 16, 10, 0, 0, 0, time.Local)
	yesterday := now.Add(-24 * time.Hour)

	tasks := []taskagent.FeishuTaskRow{
		{TaskID: 1, Status: taskagent.StatusSuccess},
		{TaskID: 2, Status: taskagent.StatusError},
		{TaskID: 3, Status: ""},
		{TaskID: 4, Status: taskagent.StatusFailed, Datetime: &yesterday},
		{TaskID: 5, Status: taskagent.StatusFailed, Datetime: &now},
	}

	if !allTasksReady(tasks[:2], []int64{1, 2}, now, taskReadyPolicy{AllowError: true}) {
		t.Fatalf("expected ready for success/error")
	}
	if allTasksReady(tasks, []int64{1, 2, 3}, now, taskReadyPolicy{AllowError: true}) {
		t.Fatalf("expected not ready when empty status disallowed")
	}
	if !allTasksReady(tasks, []int64{1, 2, 3}, now, taskReadyPolicy{AllowEmpty: true, AllowError: true}) {
		t.Fatalf("expected ready when empty status allowed")
	}
	if allTasksReady(tasks, []int64{1, 2, 4}, now, taskReadyPolicy{AllowEmpty: true, AllowError: true}) {
		t.Fatalf("expected not ready when failed not allowed")
	}
	if !allTasksReady(tasks, []int64{1, 2, 4}, now, taskReadyPolicy{AllowEmpty: true, AllowError: true, AllowFailedBeforeToday: true}) {
		t.Fatalf("expected ready when failed is before today and allowed")
	}
	if allTasksReady(tasks, []int64{1, 2, 5}, now, taskReadyPolicy{AllowEmpty: true, AllowError: true, AllowFailedBeforeToday: true}) {
		t.Fatalf("expected not ready when failed is today")
	}
	if allTasksReady(tasks[:2], []int64{1, 2}, now, taskReadyPolicy{AllowError: false}) {
		t.Fatalf("expected not ready when error is disallowed")
	}
	if allTasksReady(tasks[:2], []int64{1, 999}, now, taskReadyPolicy{AllowError: true}) {
		t.Fatalf("expected not ready when task missing")
	}
}

func TestBuildTaskIDsByStatus(t *testing.T) {
	tasks := []taskagent.FeishuTaskRow{
		{TaskID: 1, Status: "Running"},
		{TaskID: 2, Status: ""},
		{TaskID: 3, Status: taskagent.StatusSuccess},
	}
	got := buildTaskIDsByStatus([]int64{1, 2, 3, 999}, tasks)
	want := map[string][]int64{
		"running": []int64{1},
		"success": []int64{3},
		"unknown": []int64{2, 999},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}

func TestBuildTaskItemsByTaskID(t *testing.T) {
	records := []CaptureRecordPayload{
		{Fields: map[string]any{"TaskID": "1001", "ItemID": "b"}},
		{Fields: map[string]any{"TaskID": "1001", "ItemID": "a"}},
		{Fields: map[string]any{"TaskID": "1002", "ItemID": "c"}},
		{Fields: map[string]any{"TaskID": "1002", "ItemID": "c"}}, // duplicate
		{Fields: map[string]any{"TaskID": "9999", "ItemID": "x"}}, // task id not in hint list
		{Fields: map[string]any{"ItemID": "missing_task_id"}},
		{Fields: map[string]any{"TaskID": "1001"}},
	}

	got := buildTaskItemsByTaskID(records, []int64{1001, 1002, 1003})

	assertGroup := func(taskID string, total int, items []string) {
		t.Helper()
		group, ok := got[taskID]
		if !ok {
			t.Fatalf("missing taskID %s", taskID)
		}
		if group.Total != total {
			t.Fatalf("taskID %s total mismatch: got=%d want=%d", taskID, group.Total, total)
		}
		if len(group.Items) != len(items) {
			t.Fatalf("taskID %s items length mismatch: got=%v want=%v", taskID, group.Items, items)
		}
		for i := range items {
			if group.Items[i] != items[i] {
				t.Fatalf("taskID %s items mismatch: got=%v want=%v", taskID, group.Items, items)
			}
		}
	}

	assertGroup("1001", 2, []string{"a", "b"})
	assertGroup("1002", 1, []string{"c"})
	assertGroup("1003", 0, []string{})
	assertGroup("9999", 1, []string{"x"})
}

func TestBuildSingleURLGroupSummary(t *testing.T) {
	groupID := "app_B001_U001"
	row := webhookResultRow{GroupID: groupID}
	tasks := []taskagent.FeishuTaskRow{
		{TaskID: 1, Status: taskagent.StatusSuccess, BookID: "B001", UserID: "U001"},
		{TaskID: 2, Status: taskagent.StatusFailed, BookID: "B001", UserID: "U001"},
		{TaskID: 3, Status: taskagent.StatusSuccess, BookID: "B001", UserID: "U001"},
		{TaskID: 4, Status: taskagent.StatusSuccess, BookID: "", UserID: "U002"},
	}
	taskIDs := []int64{1, 2, 3, 4}

	summary := buildSingleURLGroupSummary(row, tasks, taskIDs)
	if summary == nil {
		t.Fatalf("expected non-nil summary")
	}
	if summary.TaskID != groupID {
		t.Fatalf("task_id mismatch: got=%q want=%q", summary.TaskID, groupID)
	}
	if summary.Total != 4 {
		t.Fatalf("total mismatch: got=%d want=%d", summary.Total, 4)
	}
	if summary.Done != 3 {
		t.Fatalf("done mismatch: got=%d want=%d", summary.Done, 3)
	}
	if len(summary.UniqueCombinations) != 1 {
		t.Fatalf("unique_combinations length mismatch: got=%d want=%d", len(summary.UniqueCombinations), 1)
	}
	if combo := summary.UniqueCombinations[0]; combo.Bid != "B001" || combo.AccountID != "U001" {
		t.Fatalf("unexpected combination: %#v", combo)
	}
}

func TestFilterRecordsByTaskAndUser(t *testing.T) {
	type args struct {
		records []CaptureRecordPayload
		taskIDs []int64
		userID  string
	}
	cases := []struct {
		name string
		args args
		want int
	}{
		{
			name: "match_task_and_user",
			args: args{
				records: []CaptureRecordPayload{
					{RecordID: "1", Fields: map[string]any{"TaskID": "179", "UserID": "u1"}},
					{RecordID: "2", Fields: map[string]any{"TaskID": "180", "UserID": "u1"}},
				},
				taskIDs: []int64{179},
				userID:  "u1",
			},
			want: 1,
		},
		{
			name: "task_match_user_mismatch",
			args: args{
				records: []CaptureRecordPayload{
					{RecordID: "1", Fields: map[string]any{"TaskID": "179", "UserID": "u2"}},
				},
				taskIDs: []int64{179},
				userID:  "u1",
			},
			want: 0,
		},
		{
			name: "user_empty_filters_by_task_only",
			args: args{
				records: []CaptureRecordPayload{
					{RecordID: "1", Fields: map[string]any{"TaskID": "179", "UserID": "u1"}},
					{RecordID: "2", Fields: map[string]any{"TaskID": "180", "UserID": "u1"}},
				},
				taskIDs: []int64{179},
				userID:  "",
			},
			want: 1,
		},
		{
			name: "deduplicates_by_record_id",
			args: args{
				records: []CaptureRecordPayload{
					{RecordID: "1", Fields: map[string]any{"TaskID": "179", "UserID": "u1"}},
					{RecordID: "1", Fields: map[string]any{"TaskID": "179", "UserID": "u1"}},
				},
				taskIDs: []int64{179},
				userID:  "u1",
			},
			want: 1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := filterRecordsByTaskAndUser(tc.args.records, tc.args.taskIDs, tc.args.userID)
			if len(got) != tc.want {
				t.Fatalf("len(got)=%d want=%d, got=%v", len(got), tc.want, got)
			}
		})
	}
}

func TestPickFirstNonEmptyCaptureFieldByTaskIDs(t *testing.T) {
	cases := []struct {
		name      string
		records   []CaptureRecordPayload
		taskIDs   []int64
		taskIDRaw string
		fieldEng  string
		fieldRaw  string
		want      string
	}{
		{
			name: "sqlite_taskid_eng_field",
			records: []CaptureRecordPayload{
				{Fields: map[string]any{"TaskID": "1", "UserAlias": ""}},
				{Fields: map[string]any{"TaskID": "1", "UserAlias": "alias-1"}},
				{Fields: map[string]any{"TaskID": "2", "UserAlias": "alias-2"}},
			},
			taskIDs:  []int64{1, 2},
			fieldEng: "UserAlias",
			want:     "alias-1",
		},
		{
			name: "prefers_first_taskid_in_order",
			records: []CaptureRecordPayload{
				{Fields: map[string]any{"TaskID": "1", "UserAlias": "alias-1"}},
				{Fields: map[string]any{"TaskID": "2", "UserAlias": "alias-2"}},
			},
			taskIDs:  []int64{2, 1},
			fieldEng: "UserAlias",
			want:     "alias-2",
		},
		{
			name: "feishu_raw_field_fallback",
			records: []CaptureRecordPayload{
				{Fields: map[string]any{"任务ID": "1", "用户别名": ""}},
				{Fields: map[string]any{"任务ID": "1", "用户别名": "alias-raw"}},
			},
			taskIDs:   []int64{1},
			taskIDRaw: "任务ID",
			fieldEng:  "UserAlias",
			fieldRaw:  "用户别名",
			want:      "alias-raw",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := pickFirstNonEmptyCaptureFieldByTaskIDs(tc.records, tc.taskIDs, tc.taskIDRaw, tc.fieldEng, tc.fieldRaw)
			if got != tc.want {
				t.Fatalf("got=%q want=%q", got, tc.want)
			}
		})
	}
}
