package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestSQLiteUpsertResetsReportedState(t *testing.T) {
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
	record := ResultRecord{
		DBRecord: Record{
			ItemID:       "video-dup",
			Scene:        "视频录屏采集",
			Params:       "{\"aid\":\"123\",\"eid\":\"abc\",\"type\":\"capture\"}",
			DeviceSerial: "device-upsert",
			TaskID:       42,
			ItemURL:      "https://old.example/video.mp4",
			LikeCount:    3,
			Extra:        map[string]any{"aid": "123"},
		},
	}
	if err := sink.Write(ctx, record); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()

	table := resolveResultTableName()
	markStmt := fmt.Sprintf("UPDATE %s SET %s=1, %s=?, %s='stale error' WHERE ItemID=?",
		quoteIdent(table), quoteIdent(reportedColumn), quoteIdent(reportedAtColumn), quoteIdent(reportErrorColumn))
	if _, err := db.Exec(markStmt, int64(1234567890), record.DBRecord.ItemID); err != nil {
		t.Fatalf("mark reported failed: %v", err)
	}

	updated := record
	updated.DBRecord.ItemURL = "https://new.example/video.mp4"
	updated.DBRecord.LikeCount = 99
	updated.DBRecord.CommentCount = 7
	updated.DBRecord.Extra = map[string]any{"aid": "123", "run": "new"}
	if err := sink.Write(ctx, updated); err != nil {
		t.Fatalf("duplicate write failed: %v", err)
	}

	var (
		itemURL      string
		likeCount    int64
		commentCount int64
		reported     int
		reportedAt   sql.NullInt64
		reportError  sql.NullString
		extraPayload string
	)
	query := fmt.Sprintf("SELECT ItemURL, LikeCount, CommentCount, %s, %s, %s, Extra FROM %s WHERE ItemID=?",
		quoteIdent(reportedColumn), quoteIdent(reportedAtColumn), quoteIdent(reportErrorColumn), quoteIdent(table))
	row := db.QueryRow(query, record.DBRecord.ItemID)
	if err := row.Scan(&itemURL, &likeCount, &commentCount, &reported, &reportedAt, &reportError, &extraPayload); err != nil {
		t.Fatalf("scan row failed: %v", err)
	}

	if itemURL != updated.DBRecord.ItemURL {
		t.Fatalf("expected itemURL %s, got %s", updated.DBRecord.ItemURL, itemURL)
	}
	if likeCount != updated.DBRecord.LikeCount {
		t.Fatalf("expected likeCount %d, got %d", updated.DBRecord.LikeCount, likeCount)
	}
	if commentCount != updated.DBRecord.CommentCount {
		t.Fatalf("expected commentCount %d, got %d", updated.DBRecord.CommentCount, commentCount)
	}
	if reported != reportStatusPending {
		t.Fatalf("expected reported=%d, got %d", reportStatusPending, reported)
	}
	if reportedAt.Valid {
		t.Fatalf("expected reported_at NULL, got %v", reportedAt.Int64)
	}
	if reportError.Valid {
		t.Fatalf("expected report_error NULL, got %s", reportError.String)
	}
	if !strings.Contains(extraPayload, "\"run\"") {
		t.Fatalf("expected extra payload to contain run flag, got %s", extraPayload)
	}
}
