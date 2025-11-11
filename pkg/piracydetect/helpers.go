package piracydetect

import (
	"encoding/json"
	"strconv"
	"strings"
)

// getString reads a field value from a Feishu bitable row fields map as string.
func getString(fields map[string]any, name string) string {
	if fields == nil || strings.TrimSpace(name) == "" {
		return ""
	}
	if val, ok := fields[name]; ok {
		switch v := val.(type) {
		case string:
			return strings.TrimSpace(v)
		case []byte:
			return strings.TrimSpace(string(v))
		case json.Number:
			return strings.TrimSpace(v.String())
		default:
			if b, err := json.Marshal(v); err == nil {
				return strings.TrimSpace(string(b))
			}
			return ""
		}
	}
	return ""
}

// getFloat reads a field value as float64 if possible.
func getFloat(fields map[string]any, name string) (float64, bool) {
	if fields == nil || strings.TrimSpace(name) == "" {
		return 0, false
	}
	val, ok := fields[name]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f, true
		}
	case []byte:
		s := strings.TrimSpace(string(v))
		if s == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}
