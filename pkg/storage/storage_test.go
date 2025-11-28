package storage

import (
	"os"
	"testing"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestBuildJSONLRow(t *testing.T) {
	duration := 1.23
	record := ResultRecord{
		DBRecord: Record{
			Params:         "hot query",
			DeviceSerial:   "serial-001",
			App:            "com.example.app",
			Scene:          "feed",
			ItemID:         "item-123",
			ItemCaption:    "caption",
			ItemCDNURL:     "https://cdn",
			ItemURL:        "https://share",
			ItemDuration:   &duration,
			UserName:       "author",
			UserID:         "user-007",
			UserAuthEntity: "corp",
			Tags:           "tag1,tag2",
			TaskID:         99,
			Datetime:       1731489600,
			LikeCount:      5,
			ViewCount:      9,
			AnchorPoint:    "anchor",
			CommentCount:   3,
			CollectCount:   2,
			ForwardCount:   1,
			ShareCount:     4,
			PayMode:        "free",
			Collection:     "合集",
			Episode:        "{\"total\":10}",
			PublishTime:    "2025-11-25T08:00:00Z",
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

func TestEnsureResultReporterSkipsWithoutEnv(t *testing.T) {
	origFactory := feishuStorageFactory
	origReporterFactory := resultReporterFactory
	origReporter := globalReporter
	t.Cleanup(func() {
		feishuStorageFactory = origFactory
		resultReporterFactory = origReporterFactory
		globalReporter = origReporter
		os.Unsetenv(feishu.EnvResultBitableURL)
	})

	os.Unsetenv(feishu.EnvResultBitableURL)
	called := 0
	feishuStorageFactory = func() (*feishu.ResultStorage, error) {
		called++
		return &feishu.ResultStorage{}, nil
	}
	resultReporterFactory = func(storage *feishu.ResultStorage) (*resultReporter, error) {
		return &resultReporter{}, nil
	}

	if err := EnsureResultReporter(); err != nil {
		t.Fatalf("EnsureResultReporter returned error: %v", err)
	}
	if called != 0 {
		t.Fatalf("expected storage factory not called, got %d", called)
	}
}

func TestEnsureResultReporterStartsOnce(t *testing.T) {
	origFactory := feishuStorageFactory
	origReporterFactory := resultReporterFactory
	origReporter := globalReporter
	t.Cleanup(func() {
		feishuStorageFactory = origFactory
		resultReporterFactory = origReporterFactory
		globalReporter = origReporter
		os.Unsetenv(feishu.EnvResultBitableURL)
	})

	os.Setenv(feishu.EnvResultBitableURL, "https://bitable")
	storageCalls := 0
	feishuStorageFactory = func() (*feishu.ResultStorage, error) {
		storageCalls++
		return &feishu.ResultStorage{}, nil
	}
	resultReporterFactory = func(storage *feishu.ResultStorage) (*resultReporter, error) {
		return &resultReporter{}, nil
	}
	globalReporter = nil

	if err := EnsureResultReporter(); err != nil {
		t.Fatalf("EnsureResultReporter returned error: %v", err)
	}
	if globalReporter == nil {
		t.Fatalf("expected global reporter to be initialized")
	}
	if storageCalls != 1 {
		t.Fatalf("expected storage factory called once, got %d", storageCalls)
	}
	if err := EnsureResultReporter(); err != nil {
		t.Fatalf("EnsureResultReporter second call returned error: %v", err)
	}
	if storageCalls != 1 {
		t.Fatalf("expected no extra storage factory call, got %d", storageCalls)
	}
}
