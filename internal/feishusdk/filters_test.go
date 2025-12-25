package feishusdk

import "testing"

func TestNewCondition_IsNotEmptyRequiresValue(t *testing.T) {
	cond := NewCondition("Status", "isNotEmpty")
	if cond == nil {
		t.Fatalf("expected condition")
	}
	if cond.Value == nil || len(cond.Value) != 1 || cond.Value[0] != "" {
		t.Fatalf("expected Value to be [\"\"], got %#v", cond.Value)
	}
}

func TestNewCondition_IsEmptyRequiresValue(t *testing.T) {
	cond := NewCondition("Status", "isEmpty")
	if cond == nil {
		t.Fatalf("expected condition")
	}
	if cond.Value == nil || len(cond.Value) != 1 || cond.Value[0] != "" {
		t.Fatalf("expected Value to be [\"\"], got %#v", cond.Value)
	}
}

func TestNewCondition_NoValuesKeepsNilForOtherOperators(t *testing.T) {
	cond := NewCondition("Status", "contains")
	if cond == nil {
		t.Fatalf("expected condition")
	}
	if cond.Value != nil {
		t.Fatalf("expected Value to be nil, got %#v", cond.Value)
	}
}
