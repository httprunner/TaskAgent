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
