package storage

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSQLiteUniqueIndex(t *testing.T) {
	// Setup temporary DB
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_records.sqlite")
	os.Setenv("TRACKING_STORAGE_DB_PATH", dbPath)
	defer os.Unsetenv("TRACKING_STORAGE_DB_PATH")

	sink, err := newSQLiteWriter()
	if err != nil {
		t.Fatalf("newSQLiteWriter failed: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()

	// 1. Insert base record
	baseRecord := ResultRecord{
		DBRecord: Record{
			ItemID:       "item-123",
			Scene:        "scene-A",
			Params:       "query-A",
			DeviceSerial: "device-1",
			TaskID:       100,
			Extra:        "{}",
		},
	}
	if err := sink.Write(ctx, baseRecord); err != nil {
		t.Fatalf("Base write failed: %v", err)
	}

	// 2. Insert duplicate (Same Device, Params, ItemID) -> Should be ignored
	duplicateRecord := baseRecord
	duplicateRecord.DBRecord.TaskID = 200 // Different TaskID shouldn't matter
	if err := sink.Write(ctx, duplicateRecord); err != nil {
		t.Fatalf("Duplicate write failed: %v", err)
	}

	// 3. Insert different Device (Same Params, ItemID) -> Should be accepted
	diffDeviceRecord := baseRecord
	diffDeviceRecord.DBRecord.DeviceSerial = "device-2"
	if err := sink.Write(ctx, diffDeviceRecord); err != nil {
		t.Fatalf("Diff device write failed: %v", err)
	}

	// 4. Insert different Params (Same Device, ItemID) -> Should be accepted
	diffParamsRecord := baseRecord
	diffParamsRecord.DBRecord.Params = "query-B"
	if err := sink.Write(ctx, diffParamsRecord); err != nil {
		t.Fatalf("Diff params write failed: %v", err)
	}

	// Verify count
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("Failed to open db for verification: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM capture_results").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}

	// Expected: 1 (base) + 0 (duplicate) + 1 (diff device) + 1 (diff params) = 3
	if count != 3 {
		t.Errorf("Expected 3 records, got %d. Unique index behavior incorrect.", count)
	}
}
