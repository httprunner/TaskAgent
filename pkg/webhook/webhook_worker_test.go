package webhook

import (
	"strings"
	"testing"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
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

func TestEligibleForWebhook(t *testing.T) {
	cases := []struct {
		name string
		row  feishusdk.TaskRow
		want bool
	}{
		{
			name: "success pending",
			row:  feishusdk.TaskRow{Status: feishusdk.StatusSuccess, Webhook: feishusdk.WebhookPending},
			want: true,
		},
		{
			name: "success failed",
			row:  feishusdk.TaskRow{Status: " success ", Webhook: "FAILED"},
			want: true,
		},
		{
			name: "success already delivered",
			row:  feishusdk.TaskRow{Status: feishusdk.StatusSuccess, Webhook: feishusdk.WebhookSuccess},
			want: false,
		},
		{
			name: "running pending",
			row:  feishusdk.TaskRow{Status: feishusdk.StatusRunning, Webhook: feishusdk.WebhookPending},
			want: false,
		},
		{
			name: "failed pending",
			row:  feishusdk.TaskRow{Status: feishusdk.StatusFailed, Webhook: feishusdk.WebhookPending},
			want: false,
		},
		{
			name: "empty fields",
			row:  feishusdk.TaskRow{},
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := eligibleForWebhook(tc.row); got != tc.want {
				t.Fatalf("eligibleForWebhook() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilterTasksWithStatus(t *testing.T) {
	rows := []feishusdk.TaskRow{
		{TaskID: 1, Status: ""},
		{TaskID: 2, Status: feishusdk.StatusSuccess, Webhook: feishusdk.WebhookPending},
		{TaskID: 3, Status: " success ", Webhook: feishusdk.WebhookFailed},
		{TaskID: 4, Status: " error ", Webhook: feishusdk.WebhookPending},
	}
	filtered := filterTasksWithStatus(rows)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 filtered rows, got %d", len(filtered))
	}
	if filtered[0].TaskID != 2 || filtered[1].TaskID != 3 {
		t.Fatalf("unexpected filtered task ids: %+v", filtered)
	}
}

func TestFilterTasksWithStatusSkipsError(t *testing.T) {
	rows := []feishusdk.TaskRow{
		{TaskID: 10, Status: feishusdk.StatusError, Webhook: feishusdk.WebhookPending},
		{TaskID: 11, Status: feishusdk.StatusSuccess, Webhook: feishusdk.WebhookPending},
	}
	filtered := filterTasksWithStatus(rows)
	if len(filtered) != 1 || filtered[0].TaskID != 11 {
		t.Fatalf("expected only success task to remain, got %+v", filtered)
	}
}

func TestFilterTasksWithStatusSkipsRowsWithoutWebhook(t *testing.T) {
	rows := []feishusdk.TaskRow{
		{TaskID: 20, Status: feishusdk.StatusSuccess, Webhook: ""},
		{TaskID: 21, Status: feishusdk.StatusSuccess, Webhook: feishusdk.WebhookPending},
	}
	filtered := filterTasksWithStatus(rows)
	if len(filtered) != 1 || filtered[0].TaskID != 21 {
		t.Fatalf("expected only webhook-ready task to remain, got %+v", filtered)
	}
}

func TestAllGroupTasksSuccessIgnoresEmptyStatus(t *testing.T) {
	cases := []struct {
		name  string
		rows  []feishusdk.TaskRow
		ready bool
	}{
		{
			name:  "all success",
			rows:  []feishusdk.TaskRow{{Status: feishusdk.StatusSuccess}, {Status: " success "}},
			ready: true,
		},
		{
			name:  "includes empty statuses",
			rows:  []feishusdk.TaskRow{{Status: ""}, {Status: feishusdk.StatusSuccess}},
			ready: true,
		},
		{
			name:  "missing evaluated",
			rows:  []feishusdk.TaskRow{{Status: ""}},
			ready: false,
		},
		{
			name:  "contains failure",
			rows:  []feishusdk.TaskRow{{Status: feishusdk.StatusSuccess}, {Status: feishusdk.StatusFailed}},
			ready: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := allGroupTasksSuccess(tc.rows); got != tc.ready {
				t.Fatalf("allGroupTasksSuccess()=%v, want %v", got, tc.ready)
			}
		})
	}
}

func TestFilterTasksByDateMixedDays(t *testing.T) {
	reference := time.Date(2025, 12, 6, 12, 0, 0, 0, time.Local)
	dayBefore := reference.Add(-24 * time.Hour)
	dayAfter := reference.Add(24 * time.Hour)
	rows := []feishusdk.TaskRow{
		{TaskID: 1, Datetime: &reference, Status: feishusdk.StatusSuccess},
		{TaskID: 2, Datetime: &dayBefore, Status: feishusdk.StatusSuccess},
		{TaskID: 3, Datetime: &dayAfter, Status: feishusdk.StatusSuccess},
		{TaskID: 4, Datetime: nil, Status: feishusdk.StatusSuccess},
	}
	filtered := filterTasksByDate(rows, reference)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 task on reference day, got %d", len(filtered))
	}
	if filtered[0].TaskID != 1 {
		t.Fatalf("expected task 1, got %d", filtered[0].TaskID)
	}
}

func TestNormalizeTaskDay(t *testing.T) {
	day := time.Date(2025, 12, 6, 23, 59, 59, 0, time.Local)
	norm, ok := normalizeTaskDay(&day)
	if !ok {
		t.Fatalf("expected normalizeTaskDay to succeed")
	}
	if norm.Hour() != 0 || norm.Minute() != 0 || norm.Second() != 0 {
		t.Fatalf("expected normalized day to be midnight, got %v", norm)
	}
}

func TestGroupCooldownLifecycle(t *testing.T) {
	worker := &WebhookWorker{
		groupCooldown: make(map[string]time.Time),
		cooldownDur:   5 * time.Millisecond,
	}
	groupID := "kwai_book_user"
	if worker.shouldSkipGroup(groupID) {
		t.Fatalf("group should not be skipped prior to marking cooldown")
	}
	worker.markGroupCooldown(groupID, "test")
	if !worker.shouldSkipGroup(groupID) {
		t.Fatalf("group should be skipped immediately after marking cooldown")
	}
	time.Sleep(2 * worker.cooldownDur)
	if worker.shouldSkipGroup(groupID) {
		t.Fatalf("group cooldown should expire after duration")
	}
}

func TestPartitionFailureStatesPromotesToError(t *testing.T) {
	worker := &WebhookWorker{taskFailures: make(map[int64]int)}
	taskID := int64(12345)
	groupID := "kwai_group"
	for attempt := 1; attempt <= maxWebhookFailures; attempt++ {
		failed, errored := worker.partitionFailureStates([]int64{taskID}, groupID)
		if attempt < maxWebhookFailures {
			if len(failed) != 1 || len(errored) != 0 {
				t.Fatalf("attempt %d expected failed slice, got failed=%v error=%v", attempt, failed, errored)
			}
			continue
		}
		if len(errored) != 1 || errored[0] != taskID {
			t.Fatalf("expected task to escalate to error on attempt %d, got %v", attempt, errored)
		}
	}
}

func TestResetFailureCounts(t *testing.T) {
	worker := &WebhookWorker{taskFailures: map[int64]int{1: 2, 2: 1}}
	worker.resetFailureCounts([]int64{1})
	if _, ok := worker.taskFailures[1]; ok {
		t.Fatalf("task failure counter should be cleared for task 1")
	}
	if _, ok := worker.taskFailures[2]; !ok {
		t.Fatalf("task failure counter for unrelated task should remain")
	}
}

func TestBuildFilterInfoAddsDatetimeCondition(t *testing.T) {
	worker := &WebhookWorker{app: "com.smile.gifmaker"}
	filter := worker.buildFilterInfo(taskagent.SceneProfileSearch, feishusdk.WebhookPending)
	if filter == nil {
		t.Fatalf("expected non-nil filter")
	}
	datetimeField := strings.TrimSpace(feishusdk.DefaultTaskFields.Datetime)
	if datetimeField == "" {
		t.Fatalf("test setup missing datetime field")
	}
	if !conditionExists(filter.Conditions, datetimeField, "Today") {
		t.Fatalf("expected datetime condition for Today, got %+v", filter.Conditions)
	}
}

func conditionExists(conditions []*feishusdk.Condition, field, value string) bool {
	for _, cond := range conditions {
		if cond == nil {
			continue
		}
		name := ""
		if cond.FieldName != nil {
			name = strings.TrimSpace(*cond.FieldName)
		}
		if name != field {
			continue
		}
		for _, v := range cond.Value {
			if strings.TrimSpace(v) == value {
				return true
			}
		}
	}
	return false
}
