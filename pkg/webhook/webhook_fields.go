package webhook

import (
	"strings"

	taskagent "github.com/httprunner/TaskAgent"
)

const (
	// WebhookResultPending means the row is waiting for all tasks to finish.
	WebhookResultPending = "pending"
	// WebhookResultSuccess means the webhook has been delivered successfully.
	WebhookResultSuccess = "success"
	// WebhookResultFailed means the delivery failed and should be retried.
	WebhookResultFailed = "failed"
	// WebhookResultError means the row is permanently errored and skipped.
	WebhookResultError = "error"
)

type webhookResultFields struct {
	BizType      string
	ParentTaskID string
	GroupID      string
	Status       string
	TaskIDs      string
	DramaInfo    string
	UserInfo     string
	Records      string
	// Date stores the logical task date used for deduplication
	// and day-based filtering (derived from task Datetime).
	Date       string
	CreateAt   string
	StartAt    string
	EndAt      string
	RetryCount string
	LastError  string
}

func defaultWebhookResultFields() webhookResultFields {
	fields := webhookResultFields{
		BizType:      "BizType",
		ParentTaskID: "ParentTaskID",
		GroupID:      "GroupID",
		Status:       "Status",
		TaskIDs:      "TaskIDs",
		DramaInfo:    "DramaInfo",
		UserInfo:     "UserInfo",
		Records:      "Records",
		Date:         "Date",
		CreateAt:     "CreateAt",
		StartAt:      "StartAt",
		EndAt:        "EndAt",
		RetryCount:   "RetryCount",
		LastError:    "LastError",
	}

	// Prefer WEBHOOK_FIELD_* overrides.
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_BIZTYPE"}, &fields.BizType)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_PARENT_TASK_ID"}, &fields.ParentTaskID)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_GROUPID"}, &fields.GroupID)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_STATUS"}, &fields.Status)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_TASKIDS"}, &fields.TaskIDs)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_DRAMAINFO"}, &fields.DramaInfo)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_USERINFO"}, &fields.UserInfo)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_RECORDS"}, &fields.Records)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_DATE"}, &fields.Date)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_CREATEAT"}, &fields.CreateAt)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_STARTAT"}, &fields.StartAt)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_ENDAT"}, &fields.EndAt)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_RETRYCOUNT"}, &fields.RetryCount)
	overrideFieldFromEnvs([]string{"WEBHOOK_FIELD_LASTERROR"}, &fields.LastError)
	return fields
}

func overrideFieldFromEnvs(keys []string, dst *string) {
	if dst == nil || len(keys) == 0 {
		return
	}
	for _, key := range keys {
		if val := strings.TrimSpace(taskagent.EnvString(key, "")); val != "" {
			*dst = val
			return
		}
	}
}
