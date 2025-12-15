package webhook

import (
	"testing"

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
	tasks := []taskagent.FeishuTaskRow{
		{TaskID: 1, Status: taskagent.StatusSuccess},
		{TaskID: 2, Status: taskagent.StatusError},
		{TaskID: 3, Status: taskagent.StatusFailed},
	}
	if !allTasksReady(tasks[:2], []int64{1, 2}) {
		t.Fatalf("expected ready for success/error")
	}
	if allTasksReady(tasks, []int64{1, 2, 3}) {
		t.Fatalf("expected not ready when failed exists")
	}
	if allTasksReady(tasks[:2], []int64{1, 999}) {
		t.Fatalf("expected not ready when task missing")
	}
}
