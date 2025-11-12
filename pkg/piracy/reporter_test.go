package piracy

import "testing"

func TestBuildParamsFilter(t *testing.T) {
	filter := buildParamsFilter([]string{"DramaA", "DramaB"}, "Params")
	expected := "OR(CurrentValue.[Params]=\"DramaA\", CurrentValue.[Params]=\"DramaB\")"
	if filter != expected {
		t.Fatalf("unexpected filter %s", filter)
	}

	single := buildParamsFilter([]string{"DramaC"}, "Custom Field")
	expectedSingle := "CurrentValue.[Custom Field]=\"DramaC\""
	if single != expectedSingle {
		t.Fatalf("unexpected single filter %s", single)
	}

	existing := buildParamsFilter([]string{"DramaD"}, "CurrentValue.[Other]")
	expectedExisting := "CurrentValue.[Other]=\"DramaD\""
	if existing != expectedExisting {
		t.Fatalf("unexpected existing filter %s", existing)
	}
}

func TestCombineFilters(t *testing.T) {
	if got := combineFilters("", ""); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
	if got := combineFilters("A", ""); got != "A" {
		t.Fatalf("expected base, got %s", got)
	}
	if got := combineFilters("", "B"); got != "B" {
		t.Fatalf("expected addition, got %s", got)
	}
	if got := combineFilters("A", "B"); got != "AND(A, B)" {
		t.Fatalf("unexpected combined filter %s", got)
	}
}
