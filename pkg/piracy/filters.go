package piracy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func stringPtr(v string) *string { return &v }

// ParseFilterJSON decodes a JSON FilterInfo payload and enforces an AND root conjunction.
func ParseFilterJSON(raw string) (*feishu.FilterInfo, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	var info feishu.FilterInfo
	if err := json.Unmarshal([]byte(trimmed), &info); err != nil {
		return nil, err
	}
	cloned := feishu.CloneFilter(&info)
	if cloned == nil {
		return nil, nil
	}
	if cloned.Conjunction == nil || strings.TrimSpace(*cloned.Conjunction) == "" {
		cloned.Conjunction = stringPtr("and")
	} else if !strings.EqualFold(*cloned.Conjunction, "and") {
		return nil, fmt.Errorf("filter conjunction must be 'and'")
	} else {
		cloned.Conjunction = stringPtr("and")
	}
	return cloned, nil
}

// FilterToJSON marshals a FilterInfo for logging/debugging.
func FilterToJSON(filter *feishu.FilterInfo) string {
	if filter == nil {
		return ""
	}
	raw, err := json.Marshal(filter)
	if err != nil {
		return ""
	}
	return string(raw)
}

// CombineFiltersAND merges multiple filters under a single AND conjunction.
func CombineFiltersAND(filters ...*feishu.FilterInfo) *feishu.FilterInfo {
	return feishu.MergeFiltersAND(filters...)
}

// BuildParamsFilter generates an OR group over Params field values.
func BuildParamsFilter(values []string, fieldName string) *feishu.FilterInfo {
	field := strings.TrimSpace(fieldName)
	if field == "" {
		field = "Params"
	}
	conds := make([]*feishu.Condition, 0, len(values))
	for _, raw := range values {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		conds = append(conds, feishu.NewCondition(field, "is", trimmed))
	}
	if len(conds) == 0 {
		return nil
	}
	filter := feishu.NewFilterInfo("and")
	filter.Children = append(filter.Children, feishu.NewChildrenFilter("or", conds...))
	return filter
}

// BuildBookIDFilter generates an OR group over BookID field values.
func BuildBookIDFilter(values []string, fieldName string) *feishu.FilterInfo {
	field := strings.TrimSpace(fieldName)
	if field == "" {
		field = "BookID"
	}
	conds := make([]*feishu.Condition, 0, len(values))
	for _, raw := range values {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		if cond := feishu.NewCondition(field, "is", trimmed); cond != nil {
			conds = append(conds, cond)
		}
	}
	if len(conds) == 0 {
		return nil
	}
	child := feishu.NewChildrenFilter("or")
	child.Conditions = conds
	filter := feishu.NewFilterInfo("and")
	filter.Children = append(filter.Children, child)
	return filter
}

// EqFilter creates a filter that matches a single "field is value" condition.
func EqFilter(fieldName, value string) *feishu.FilterInfo {
	if strings.TrimSpace(fieldName) == "" || strings.TrimSpace(value) == "" {
		return nil
	}
	filter := feishu.NewFilterInfo("and")
	filter.Conditions = append(filter.Conditions, feishu.NewCondition(fieldName, "is", strings.TrimSpace(value)))
	return filter
}

// EmptyFilter returns true if the filter has no conditions or child groups.
func EmptyFilter(filter *feishu.FilterInfo) bool {
	if filter == nil {
		return true
	}
	return len(filter.Conditions) == 0 && len(filter.Children) == 0
}
