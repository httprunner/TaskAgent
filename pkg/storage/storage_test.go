package storage

import "testing"

func TestBuildJSONLRow(t *testing.T) {
	duration := 1.23
	record := ResultRecord{
		DBRecord: Record{
			Params:       "hot query",
			DeviceSerial: "serial-001",
			App:          "com.example.app",
			ItemID:       "item-123",
			VideoID:      "video-456",
			ItemCaption:  "caption",
			ItemDuration: &duration,
			UserName:     "author",
			UserID:       "user-007",
			Tags:         "tag1,tag2",
			TaskID:       99,
			Datetime:     1731489600,
			Extra: map[string]any{
				"key": "value",
			},
		},
	}

	row, err := buildJSONLRow(record)
	if err != nil {
		t.Fatalf("buildJSONLRow returned error: %v", err)
	}

	if got, want := row["Params"], "hot query"; got != want {
		t.Fatalf("Params mismatch, want %q got %q", want, got)
	}
	if got := row["ItemDuration"]; got != duration {
		t.Fatalf("ItemDuration mismatch, want %v got %v", duration, got)
	}
	if got, want := row["Extra"], "{\"key\":\"value\"}"; got != want {
		t.Fatalf("Extra mismatch, want %q got %q", want, got)
	}
}

func TestBuildJSONLRowNilDuration(t *testing.T) {
	record := ResultRecord{DBRecord: Record{Extra: "{}"}}
	row, err := buildJSONLRow(record)
	if err != nil {
		t.Fatalf("buildJSONLRow returned error: %v", err)
	}
	if row["ItemDuration"] != nil {
		t.Fatalf("expected nil ItemDuration, got %v", row["ItemDuration"])
	}
}
