package piracy

import "testing"

func TestPickLatestRecords(t *testing.T) {
	records := []CaptureRecordPayload{
		{RecordID: "1", Fields: map[string]any{"Datetime": "1700000000000"}},
		{RecordID: "2", Fields: map[string]any{"Datetime": "1700000005000"}},
		{RecordID: "3", Fields: map[string]any{"Datetime": "1699999999000"}},
	}
	latest := pickLatestRecords(records, "Datetime")
	if len(latest) != 1 {
		to := len(latest)
		t.Fatalf("expected 1 record, got %d", to)
	}
	if latest[0].RecordID != "3" {
		t.Fatalf("expected last record, got %s", latest[0].RecordID)
	}
}

func TestPickLatestRecordsFallback(t *testing.T) {
	records := []CaptureRecordPayload{
		{RecordID: "a", Fields: map[string]any{"Datetime": ""}},
		{RecordID: "b", Fields: map[string]any{"Datetime": ""}},
	}
	latest := pickLatestRecords(records, "")
	if len(latest) != 1 {
		t.Fatalf("expected 1 record, got %d", len(latest))
	}
	if latest[0].RecordID != "b" {
		t.Fatalf("expected last record fallback, got %s", latest[0].RecordID)
	}
}
