package piracy

import (
	"strings"
	"testing"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestEligibleForWebhook(t *testing.T) {
	cases := []struct {
		name string
		row  feishu.TaskRow
		want bool
	}{
		{
			name: "success pending",
			row:  feishu.TaskRow{Status: feishu.StatusSuccess, Webhook: feishu.WebhookPending},
			want: true,
		},
		{
			name: "success failed",
			row:  feishu.TaskRow{Status: " success ", Webhook: "FAILED"},
			want: true,
		},
		{
			name: "success already delivered",
			row:  feishu.TaskRow{Status: feishu.StatusSuccess, Webhook: feishu.WebhookSuccess},
			want: false,
		},
		{
			name: "running pending",
			row:  feishu.TaskRow{Status: feishu.StatusRunning, Webhook: feishu.WebhookPending},
			want: false,
		},
		{
			name: "failed pending",
			row:  feishu.TaskRow{Status: feishu.StatusFailed, Webhook: feishu.WebhookPending},
			want: false,
		},
		{
			name: "empty fields",
			row:  feishu.TaskRow{},
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
	rows := []feishu.TaskRow{
		{TaskID: 1, Status: ""},
		{TaskID: 2, Status: feishu.StatusSuccess, Webhook: feishu.WebhookPending},
		{TaskID: 3, Status: " success ", Webhook: feishu.WebhookFailed},
		{TaskID: 4, Status: " error ", Webhook: feishu.WebhookPending},
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
	rows := []feishu.TaskRow{
		{TaskID: 10, Status: feishu.StatusError, Webhook: feishu.WebhookPending},
		{TaskID: 11, Status: feishu.StatusSuccess, Webhook: feishu.WebhookPending},
	}
	filtered := filterTasksWithStatus(rows)
	if len(filtered) != 1 || filtered[0].TaskID != 11 {
		t.Fatalf("expected only success task to remain, got %+v", filtered)
	}
}

func TestFilterTasksWithStatusSkipsRowsWithoutWebhook(t *testing.T) {
	rows := []feishu.TaskRow{
		{TaskID: 20, Status: feishu.StatusSuccess, Webhook: ""},
		{TaskID: 21, Status: feishu.StatusSuccess, Webhook: feishu.WebhookPending},
	}
	filtered := filterTasksWithStatus(rows)
	if len(filtered) != 1 || filtered[0].TaskID != 21 {
		t.Fatalf("expected only webhook-ready task to remain, got %+v", filtered)
	}
}

func TestAllGroupTasksSuccessIgnoresEmptyStatus(t *testing.T) {
	cases := []struct {
		name  string
		rows  []feishu.TaskRow
		ready bool
	}{
		{
			name:  "all success",
			rows:  []feishu.TaskRow{{Status: feishu.StatusSuccess}, {Status: " success "}},
			ready: true,
		},
		{
			name:  "includes empty statuses",
			rows:  []feishu.TaskRow{{Status: ""}, {Status: feishu.StatusSuccess}},
			ready: true,
		},
		{
			name:  "missing evaluated",
			rows:  []feishu.TaskRow{{Status: ""}},
			ready: false,
		},
		{
			name:  "contains failure",
			rows:  []feishu.TaskRow{{Status: feishu.StatusSuccess}, {Status: feishu.StatusFailed}},
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
	rows := []feishu.TaskRow{
		{TaskID: 1, Datetime: &reference, Status: feishu.StatusSuccess},
		{TaskID: 2, Datetime: &dayBefore, Status: feishu.StatusSuccess},
		{TaskID: 3, Datetime: &dayAfter, Status: feishu.StatusSuccess},
		{TaskID: 4, Datetime: nil, Status: feishu.StatusSuccess},
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
		t.Fatalf("group should not be skipped prior to marking")
	}
	worker.markGroupCooldown(groupID, "test")
	if !worker.shouldSkipGroup(groupID) {
		t.Fatalf("group should be skipped immediately after marking")
	}
	worker.clearGroupCooldown(groupID)
	if worker.shouldSkipGroup(groupID) {
		t.Fatalf("group should not be skipped after clearing")
	}
	worker.markGroupCooldown(groupID, "test2")
	time.Sleep(10 * time.Millisecond)
	if worker.shouldSkipGroup(groupID) {
		t.Fatalf("group cooldown should expire automatically")
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
	filter := worker.buildFilterInfo(pool.SceneProfileSearch, feishu.WebhookPending)
	if filter == nil {
		t.Fatalf("expected non-nil filter")
	}
	datetimeField := strings.TrimSpace(feishu.DefaultTaskFields.Datetime)
	if datetimeField == "" {
		t.Fatalf("test setup missing datetime field")
	}
	if !conditionExists(filter.Conditions, datetimeField, "Today") {
		t.Fatalf("expected datetime condition for Today, got %+v", filter.Conditions)
	}
}

func conditionExists(conditions []*feishu.Condition, field, value string) bool {
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
