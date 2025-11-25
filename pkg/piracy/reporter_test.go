package piracy

import "testing"

func TestBuildParamsFilter(t *testing.T) {
	filter := BuildParamsFilter([]string{"DramaA", "DramaB"}, "Params")
	if filter == nil {
		t.Fatalf("expected filter")
	}
	if filter.Conjunction == nil || *filter.Conjunction != "and" {
		t.Fatalf("unexpected conjunction %+v", filter.Conjunction)
	}
	if len(filter.Children) != 1 {
		t.Fatalf("expected single child group, got %d", len(filter.Children))
	}
	child := filter.Children[0]
	if child.Conjunction == nil || *child.Conjunction != "or" {
		t.Fatalf("unexpected child conjunction %+v", child.Conjunction)
	}
	if len(child.Conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(child.Conditions))
	}
}

func TestCombineFiltersAND(t *testing.T) {
	left := EqFilter("Params", "DramaA")
	right := EqFilter("UserID", "123")
	merged := CombineFiltersAND(left, right)
	if merged == nil {
		t.Fatalf("expected merged filter")
	}
	if len(merged.Conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(merged.Conditions))
	}
}
