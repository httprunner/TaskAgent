package piracy

import (
	"testing"

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
