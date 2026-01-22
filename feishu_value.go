package taskagent

import "github.com/httprunner/TaskAgent/internal/feishusdk"

// BitableValueToString normalizes a Feishu bitable cell value into a plain string.
func BitableValueToString(value any) string {
	return feishusdk.BitableValueToString(value)
}

// BitableValueToFloat64 converts a Feishu bitable cell value into float64 with best-effort parsing.
func BitableValueToFloat64(value any) (float64, bool) {
	return feishusdk.BitableValueToFloat64(value)
}
