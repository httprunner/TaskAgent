package feishu

// Shared constants for Feishu bitable integrations. Keep these in TaskAgent so
// downstream agents can reference a single source of truth when wiring Feishu task workflows.
const (
	// EnvTargetBitableURL indicates where to pull Feishu target tasks from
	// when running against a bitable-backed queue.
	EnvTargetBitableURL = "TARGET_BITABLE_URL"
	// EnvResultBitableURL indicates where to push Feishu result rows.
	EnvResultBitableURL = "RESULT_BITABLE_URL"

	// StatusSuccess marks a task row as completed successfully.
	StatusSuccess = "success"
	// StatusFailed marks a task row as failed.
	StatusFailed = "failed"
	// StatusDispatched marks a task row as dispatched.
	StatusDispatched = "dispatched"
)
