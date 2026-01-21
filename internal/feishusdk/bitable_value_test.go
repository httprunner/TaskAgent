package feishusdk

import "testing"

func TestBitableValueToString(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"text", "hello", "hello"},
		{"number", 123.45, "123.45"},
		{"int", 123, "123"},
		{"bool", true, "true"},
		{"multi-select", []any{"A", "B"}, "A,B"},
		{"rich-text", []any{map[string]any{"text": "foo"}, map[string]any{"text": "bar"}}, "foo bar"},
		{"person", map[string]any{"name": "Alice", "id": "u1"}, "Alice"},
		{"link", map[string]any{"text": "Doc", "link": "https://example.com"}, "Doc"},
		{"attachment", map[string]any{"name": "a.png", "file_token": "t1"}, "a.png"},
		{"attachment-list", []any{map[string]any{"name": "a.png"}, map[string]any{"name": "b.png"}}, "a.png,b.png"},
		{"wrapper", map[string]any{"type": 1, "value": []any{map[string]any{"text": "wrapped"}}}, "wrapped"},
		{"location", map[string]any{"address": "Street 1", "cityname": "City"}, "Street 1"},
	}
	for _, tt := range tests {
		if got := BitableValueToString(tt.input); got != tt.want {
			t.Fatalf("%s: got %q want %q", tt.name, got, tt.want)
		}
	}
}
