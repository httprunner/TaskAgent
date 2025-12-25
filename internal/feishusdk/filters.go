package feishusdk

import (
	"strings"

	bitablev1 "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
)

// FilterInfo aliases the Feishu SDK filter structure so callers don't have to import the SDK directly.
type (
	FilterInfo     = bitablev1.FilterInfo
	Condition      = bitablev1.Condition
	ChildrenFilter = bitablev1.ChildrenFilter
)

func newStringPtr(val string) *string {
	v := val
	return &v
}

// NewFilterInfo constructs a filter with the provided conjunction (defaults to "and").
func NewFilterInfo(conjunction string) *FilterInfo {
	if strings.TrimSpace(conjunction) == "" {
		conjunction = "and"
	}
	conj := strings.ToLower(conjunction)
	return &FilterInfo{Conjunction: newStringPtr(conj)}
}

// NewCondition creates a Condition node with the provided operator and values.
func NewCondition(field, operator string, values ...string) *Condition {
	fieldName := strings.TrimSpace(field)
	if fieldName == "" {
		return nil
	}
	op := strings.TrimSpace(operator)
	if op == "" {
		op = "is"
	}
	cond := &Condition{FieldName: newStringPtr(fieldName), Operator: newStringPtr(op)}
	if len(values) > 0 {
		cond.Value = append([]string(nil), values...)
		return cond
	}
	// Feishu Bitable filter conditions require a Value field even for certain
	// unary operators (e.g. isNotEmpty/isEmpty). Provide an explicit empty
	// value to avoid 400 responses like "Missing required parameter: Value".
	switch strings.ToLower(op) {
	case "isnotempty", "isempty":
		cond.Value = []string{""}
	}
	return cond
}

// NewChildrenFilter creates a grouping node with its own conjunction.
func NewChildrenFilter(conjunction string, conds ...*Condition) *ChildrenFilter {
	if strings.TrimSpace(conjunction) == "" {
		conjunction = "and"
	}
	child := &ChildrenFilter{Conjunction: newStringPtr(strings.ToLower(conjunction))}
	for _, cond := range conds {
		if cond == nil {
			continue
		}
		child.Conditions = append(child.Conditions, cond)
	}
	return child
}

// CloneFilter performs a deep copy so callers can safely mutate filters.
func CloneFilter(filter *FilterInfo) *FilterInfo {
	if filter == nil {
		return nil
	}
	cloned := &FilterInfo{}
	if filter.Conjunction != nil {
		cloned.Conjunction = newStringPtr(strings.ToLower(strings.TrimSpace(*filter.Conjunction)))
	}
	for _, cond := range filter.Conditions {
		if cond == nil {
			continue
		}
		copy := *cond
		if cond.FieldName != nil {
			copy.FieldName = newStringPtr(*cond.FieldName)
		}
		if cond.Operator != nil {
			copy.Operator = newStringPtr(*cond.Operator)
		}
		if len(cond.Value) > 0 {
			copy.Value = append([]string(nil), cond.Value...)
		}
		cloned.Conditions = append(cloned.Conditions, &copy)
	}
	for _, child := range filter.Children {
		if child == nil {
			continue
		}
		copy := &ChildrenFilter{}
		if child.Conjunction != nil {
			copy.Conjunction = newStringPtr(strings.ToLower(strings.TrimSpace(*child.Conjunction)))
		}
		for _, cond := range child.Conditions {
			if cond == nil {
				continue
			}
			cc := *cond
			if cond.FieldName != nil {
				cc.FieldName = newStringPtr(*cond.FieldName)
			}
			if cond.Operator != nil {
				cc.Operator = newStringPtr(*cond.Operator)
			}
			if len(cond.Value) > 0 {
				cc.Value = append([]string(nil), cond.Value...)
			}
			copy.Conditions = append(copy.Conditions, &cc)
		}
		cloned.Children = append(cloned.Children, copy)
	}
	return cloned
}

// MergeFiltersAND merges multiple filters under a single AND conjunction.
func MergeFiltersAND(filters ...*FilterInfo) *FilterInfo {
	var merged *FilterInfo
	for _, f := range filters {
		if f == nil {
			continue
		}
		if merged == nil {
			merged = CloneFilter(f)
			if merged != nil && merged.Conjunction == nil {
				merged.Conjunction = newStringPtr("and")
			}
			continue
		}
		merged = appendFilterAND(merged, f)
	}
	return merged
}

func appendFilterAND(base, extra *FilterInfo) *FilterInfo {
	if base == nil {
		return CloneFilter(extra)
	}
	out := CloneFilter(base)
	if extra == nil {
		return out
	}
	ex := CloneFilter(extra)
	if out.Conjunction == nil {
		out.Conjunction = newStringPtr("and")
	}
	if ex.Conjunction == nil {
		ex.Conjunction = newStringPtr("and")
	}
	if strings.EqualFold(*ex.Conjunction, "and") {
		out.Conditions = append(out.Conditions, ex.Conditions...)
		out.Children = append(out.Children, ex.Children...)
		return out
	}
	child := NewChildrenFilter(*ex.Conjunction)
	child.Conditions = append(child.Conditions, ex.Conditions...)
	out.Children = append(out.Children, child)
	out.Children = append(out.Children, ex.Children...)
	return out
}
