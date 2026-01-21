package webhook

import (
	"testing"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestGetString_NullIsEmpty(t *testing.T) {
	fields := map[string]any{
		"nil":   nil,
		"ptr":   (*int)(nil),
		"slice": ([]string)(nil),
	}
	for key := range fields {
		if got := feishusdk.BitableFieldString(fields, key); got != "" {
			t.Fatalf("key=%s got=%q want empty", key, got)
		}
	}
}
