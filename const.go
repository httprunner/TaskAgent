package taskagent

import "github.com/httprunner/TaskAgent/internal/feishusdk"

// Shared environment variable names for Feishu bitable integrations.
// Downstream agents should prefer these root-level constants when wiring
// TaskAgent into their environments.
const (
	// EnvTaskBitableURL indicates where to pull Feishu target tasks from
	// when running against a bitable-backed queue.
	EnvTaskBitableURL = feishusdk.EnvTaskBitableURL
	// EnvResultBitableURL indicates where to push Feishu result rows.
	EnvResultBitableURL = feishusdk.EnvResultBitableURL
	// EnvSourceSheetURL indicates where to pull raw task rows from Feishu Sheets.
	EnvSourceSheetURL = feishusdk.EnvSourceSheetURL
	// EnvWebhookBitableURL points to the dedicated webhook result table used by the
	// group-based webhook flow.
	EnvWebhookBitableURL = feishusdk.EnvWebhookBitableURL
	// EnvDeviceBitableURL indicates where to pull/push Feishu device status rows.
	EnvDeviceBitableURL = feishusdk.EnvDeviceBitableURL
	// EnvCookieBitableURL points to the dedicated cookies table for SingleURLWorker.
	EnvCookieBitableURL = feishusdk.EnvCookieBitableURL
	// EnvAccountBitableURL indicates where to pull account registration rows.
	EnvAccountBitableURL = feishusdk.EnvAccountBitableURL
	EnvDramaBitableURL   = feishusdk.EnvDramaBitableURL

	// EnvTaskGroupPriorityEnable enables group-aware task prioritization in the default
	// DevicePoolAgent wiring (when enabled, tasks are re-ordered by remaining group size).
	EnvTaskGroupPriorityEnable = "TASK_GROUP_PRIORITY_ENABLE"
	// EnvTaskGroupPriorityOversample controls how many candidate tasks are fetched before re-ordering.
	EnvTaskGroupPriorityOversample = "TASK_GROUP_PRIORITY_OVERSAMPLE"
	// EnvTaskGroupPriorityTTL controls the in-memory remaining-count cache TTL.
	EnvTaskGroupPriorityTTL = "TASK_GROUP_PRIORITY_TTL"
	// EnvTaskGroupPriorityMaxGroups caps how many distinct groups are counted per fetch.
	EnvTaskGroupPriorityMaxGroups = "TASK_GROUP_PRIORITY_MAX_GROUPS"
	// EnvTaskGroupPriorityCountCap caps how many pending/failed rows are scanned per group.
	EnvTaskGroupPriorityCountCap = "TASK_GROUP_PRIORITY_COUNT_CAP"
	// EnvTaskGroupPriorityFocusGroups caps how many top-ranked groups can be selected per fetch.
	// When >0, TaskAgent prefers tasks from the remaining-minimum K groups, then fills from the rest.
	EnvTaskGroupPriorityFocusGroups = "TASK_GROUP_PRIORITY_FOCUS_GROUPS"

	// EnvDeviceJobTimeout controls the per-device job timeout.
	// Use duration strings like "30m" or "1h". Set to "0" to disable.
	EnvDeviceJobTimeout = "DEVICE_JOB_TIMEOUT"
)

// Shared status values for Feishu task rows and related resources.
// These are re-exported from the internal Feishu SDK so callers can
// depend on the root taskagent package only.
const (
	// StatusError marks a task row as permanently errored and skipped.
	StatusError          = feishusdk.StatusError
	StatusPending        = feishusdk.StatusPending
	StatusSuccess        = feishusdk.StatusSuccess
	StatusReady          = feishusdk.StatusReady
	StatusDownloadQueued = feishusdk.StatusDownloaderQueued
	StatusProcessing     = feishusdk.StatusDownloaderProcessing
	StatusFailed         = feishusdk.StatusFailed
	StatusDownloadFailed = feishusdk.StatusDownloaderFailed
	StatusDispatched     = feishusdk.StatusDispatched
	StatusRunning        = feishusdk.StatusRunning

	CookieStatusValid   = feishusdk.CookieStatusValid
	CookieStatusInvalid = feishusdk.CookieStatusInvalid

	WebhookPending = feishusdk.WebhookPending
	WebhookSuccess = feishusdk.WebhookSuccess
	WebhookFailed  = feishusdk.WebhookFailed
	WebhookError   = feishusdk.WebhookError
)
