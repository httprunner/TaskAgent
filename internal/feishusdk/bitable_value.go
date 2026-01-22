package feishusdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// BitableValueToString normalizes a Feishu bitable cell value into a plain string.
// It follows the official bitable value structures (text/number/boolean/list/object)
// and strips schema metadata such as type wrappers.
// Reference: https://feishu.apifox.cn/doc-436428.md
func BitableValueToString(value any) string {
	return strings.TrimSpace(normalizeBitableValue(value))
}

// BitableFieldString reads a field value from a Feishu bitable row fields map as string.
func BitableFieldString(fields map[string]any, name string) string {
	if fields == nil || strings.TrimSpace(name) == "" {
		return ""
	}
	val, ok := fields[name]
	if !ok {
		return ""
	}
	return BitableValueToString(val)
}

// BitableValueToJSONString converts a Feishu bitable cell value into a JSON-friendly string.
func BitableValueToJSONString(value any) string {
	if value == nil {
		return ""
	}
	if s := BitableValueToString(value); s != "" {
		return s
	}
	if b, err := json.Marshal(value); err == nil {
		return string(b)
	}
	return fmt.Sprint(value)
}

// BitableValueToInt64 converts a Feishu bitable cell value into int64 with best-effort parsing.
func BitableValueToInt64(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case uint:
		return int64(v)
	case uint64:
		return int64(v)
	case uint32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n
		}
		if f, err := v.Float64(); err == nil {
			return int64(f)
		}
		return 0
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0
		}
		if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return int64(f)
		}
		return 0
	default:
		trimmed := strings.TrimSpace(fmt.Sprint(v))
		if trimmed == "" {
			return 0
		}
		if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return int64(f)
		}
		return 0
	}
}

// BitableValueToFloat64 converts a Feishu bitable cell value into float64 with best-effort parsing.
func BitableValueToFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case nil:
		return 0, false
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
		return 0, false
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return f, true
		}
		return 0, false
	case []byte:
		trimmed := strings.TrimSpace(string(v))
		if trimmed == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		trimmed := strings.TrimSpace(fmt.Sprint(v))
		if trimmed == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return f, true
		}
		return 0, false
	}
}

// BitableValueToInt64Strict converts a Feishu bitable cell value into int64 with strict integer parsing.
func BitableValueToInt64Strict(value any) (int64, error) {
	switch v := value.(type) {
	case float64:
		if math.Mod(v, 1) != 0 {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		return int64(v), nil
	case float32:
		if math.Mod(float64(v), 1) != 0 {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case json.Number:
		return v.Int64()
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, errors.New("empty numeric string")
		}
		return strconv.ParseInt(trimmed, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}

func normalizeBitableValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	case json.Number:
		return strings.TrimSpace(v.String())
	case float64:
		return formatFloat(v)
	case float32:
		return formatFloat(float64(v))
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []any:
		return normalizeBitableArray(v)
	case map[string]any:
		return normalizeBitableObject(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func normalizeBitableArray(items []any) string {
	if len(items) == 0 {
		return ""
	}
	if isRichTextArray(items) {
		return joinRichText(items)
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		part := normalizeBitableValue(item)
		if part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",")
}

func normalizeBitableObject(obj map[string]any) string {
	if obj == nil {
		return ""
	}
	if nested, ok := obj["value"]; ok {
		if text := normalizeBitableValue(nested); text != "" {
			return text
		}
	}
	if nested, ok := obj["values"]; ok {
		if text := normalizeBitableValue(nested); text != "" {
			return text
		}
	}
	if nested, ok := obj["elements"]; ok {
		if text := normalizeBitableValue(nested); text != "" {
			return text
		}
	}
	if nested, ok := obj["content"]; ok {
		if text := normalizeBitableValue(nested); text != "" {
			return text
		}
	}
	if text, ok := obj["text"].(string); ok {
		if trimmed := strings.TrimSpace(text); trimmed != "" {
			return trimmed
		}
	}
	if link, ok := obj["link"].(string); ok {
		if trimmed := strings.TrimSpace(link); trimmed != "" {
			return trimmed
		}
	}
	if name := pickFirstString(obj, "name", "en_name", "email", "id", "user_id"); name != "" {
		return name
	}
	if attachment := pickFirstString(obj, "name", "url", "tmp_url", "file_token"); attachment != "" {
		return attachment
	}
	if location := pickFirstString(obj, "address", "name"); location != "" {
		return location
	}
	locationParts := []string{
		pickFirstString(obj, "location"),
		pickFirstString(obj, "pname"),
		pickFirstString(obj, "cityname"),
		pickFirstString(obj, "adname"),
	}
	locationParts = compactStrings(locationParts)
	if len(locationParts) > 0 {
		return strings.Join(locationParts, ",")
	}
	if b, err := json.Marshal(obj); err == nil {
		return strings.TrimSpace(string(b))
	}
	return ""
}

func isRichTextArray(items []any) bool {
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			if _, hasText := m["text"]; hasText {
				return true
			}
		}
	}
	return false
}

func joinRichText(items []any) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			if text, ok := m["text"].(string); ok {
				if trimmed := strings.TrimSpace(text); trimmed != "" {
					parts = append(parts, trimmed)
				}
				continue
			}
			if nested, ok := m["value"]; ok {
				if nestedText := normalizeBitableValue(nested); nestedText != "" {
					parts = append(parts, nestedText)
				}
			}
		} else if text := normalizeBitableValue(item); text != "" {
			parts = append(parts, text)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

func pickFirstString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if raw, ok := obj[key]; ok {
			if text := normalizeBitableValue(raw); text != "" {
				return text
			}
		}
	}
	return ""
}

func compactStrings(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func formatFloat(v float64) string {
	if math.Mod(v, 1) == 0 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
