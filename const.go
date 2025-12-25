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
	// EnvWebhookBitableURL points to the dedicated webhook result table used by the
	// group-based webhook flow.
	EnvWebhookBitableURL = feishusdk.EnvWebhookBitableURL
	// EnvDeviceBitableURL indicates where to pull/push Feishu device status rows.
	EnvDeviceBitableURL = feishusdk.EnvDeviceBitableURL
	// EnvCookieBitableURL points to the dedicated cookies table for SingleURLWorker.
	EnvCookieBitableURL = feishusdk.EnvCookieBitableURL
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
