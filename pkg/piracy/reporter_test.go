package piracy

import (
	"testing"
	"time"
)

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

func TestInheritDatetimeRawPrefersParentRaw(t *testing.T) {
	now := time.Now()
	got := inheritDatetimeRaw("2024-11-28 12:34:56", &now)
	if got != "2024-11-28 12:34:56" {
		t.Fatalf("expected parent raw to be preserved, got %q", got)
	}
}

func TestInheritDatetimeRawFormatsTime(t *testing.T) {
	loc := time.Date(2024, time.November, 28, 7, 8, 9, 0, time.FixedZone("UTC+8", 8*3600))
	got := inheritDatetimeRaw("", &loc)
	if got != "1732748889000" {
		t.Fatalf("expected fallback unix millis, got %q", got)
	}
}

func TestInheritDatetimeRawEmptyWhenUnset(t *testing.T) {
	got := inheritDatetimeRaw("", nil)
	if got != "" {
		t.Fatalf("expected empty string when no datetime available, got %q", got)
	}
}

func TestBuildPiracyGroupTaskRecordsCopiesBookID(t *testing.T) {
	now := time.Now()
	details := []MatchDetail{
		{
			Match: Match{
				Params:   "ParamA",
				UserID:   "User123",
				UserName: "昵称",
				Ratio:    0.75,
			},
			Videos: []VideoDetail{
				{ItemID: "video-collection", Tags: "合集"},
				{ItemID: "video-anchor-1", AnchorPoint: `{"appLink":"kwai://video-anchor-1"}`},
				{ItemID: "video-anchor-2", AnchorPoint: `{"appLink":"kwai://video-anchor-1"}`},
			},
		},
	}
	records := buildPiracyGroupTaskRecords("com.app.test", 321, &now, "", "  book-xyz  ", details)
	if len(records) != 3 {
		t.Fatalf("expected 3 records (profile, collection, anchor), got %d", len(records))
	}
	for idx, rec := range records {
		if rec.BookID != "book-xyz" {
			t.Fatalf("record %d expected bookID book-xyz, got %q", idx, rec.BookID)
		}
		if rec.GroupID != "321_1" {
			t.Fatalf("record %d expected group id 321_1, got %q", idx, rec.GroupID)
		}
	}
}
