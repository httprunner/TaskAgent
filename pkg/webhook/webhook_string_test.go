package webhook

import "testing"

func TestGetString_NullIsEmpty(t *testing.T) {
	fields := map[string]any{
		"nil":   nil,
		"ptr":   (*int)(nil),
		"slice": ([]string)(nil),
	}
	for key := range fields {
		if got := getString(fields, key); got != "" {
			t.Fatalf("key=%s got=%q want empty", key, got)
		}
	}
}
