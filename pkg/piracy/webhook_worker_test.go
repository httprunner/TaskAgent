package piracy

import (
	"testing"
	"time"

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
		{TaskID: 2, Status: feishu.StatusSuccess},
		{TaskID: 3, Status: " success "},
	}
	filtered := filterTasksWithStatus(rows)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 filtered rows, got %d", len(filtered))
	}
	if filtered[0].TaskID != 2 || filtered[1].TaskID != 3 {
		t.Fatalf("unexpected filtered task ids: %+v", filtered)
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
