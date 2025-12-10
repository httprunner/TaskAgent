package webhook

import (
	"strings"

	taskagent "github.com/httprunner/TaskAgent"
)

// BuildParamsFilter generates an OR group over Params field values.
func BuildParamsFilter(values []string, fieldName string) *taskagent.FeishuFilterInfo {
	field := strings.TrimSpace(fieldName)
	if field == "" {
		field = "Params"
	}
	conds := make([]*taskagent.FeishuCondition, 0, len(values))
	for _, raw := range values {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		conds = append(conds, taskagent.NewFeishuCondition(field, "is", trimmed))
	}
	if len(conds) == 0 {
		return nil
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Children = append(filter.Children, taskagent.NewFeishuChildrenFilter("or", conds...))
	return filter
}

// EqFilter creates a filter that matches a single "field is value" condition.
func EqFilter(fieldName, value string) *taskagent.FeishuFilterInfo {
	if strings.TrimSpace(fieldName) == "" || strings.TrimSpace(value) == "" {
		return nil
	}
	filter := taskagent.NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(fieldName, "is", strings.TrimSpace(value)))
	return filter
}
