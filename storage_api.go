package taskagent

import (
	"database/sql"

	"github.com/httprunner/TaskAgent/internal/storage"
)

// ResultStorageConfig controls which sinks are enabled for capture result storage.
// It is a thin alias to the internal storage configuration so downstream
// callers can integrate without importing TaskAgent subpackages directly.
type ResultStorageConfig = storage.Config

// ResultRecord represents a single capture result to be written to the
// configured sinks (SQLite / Feishu / JSONL).
// It aliases the underlying storage type for compatibility.
type ResultRecord = storage.ResultRecord

// CaptureRecord mirrors the SQLite schema for capture_results table.
// It is exposed so callers can construct ResultRecord.DBRecord values.
type CaptureRecord = storage.Record

// ResultStorageManager fan-outs capture results to the configured sinks and
// exposes helper methods such as LookupCaptureParamsByItemID.
// It aliases the underlying storage manager implementation.
type ResultStorageManager = storage.Manager

// NewResultStorageManager builds a storage manager based on the provided
// configuration. Callers should Close the returned manager when done.
func NewResultStorageManager(cfg ResultStorageConfig) (*ResultStorageManager, error) {
	return storage.NewManager(cfg)
}

// EnsureResultReporter starts the shared Feishu reporter so queued SQLite rows
// continue to flush even if no TrackingRunner is active.
// It is safe to call multiple times; subsequent calls are no-ops.
func EnsureResultReporter() error {
	return storage.EnsureResultReporter()
}

// OpenCaptureResultsDB opens the shared SQLite database that backs the
// capture_results table. Callers are responsible for closing the returned DB.
func OpenCaptureResultsDB() (*sql.DB, error) {
	return storage.OpenCaptureResultsDB()
}

// ResolveResultDBPath returns the absolute path to the tracking SQLite
// database used for capture results and task mirrors.
func ResolveResultDBPath() (string, error) {
	return storage.ResolveDatabasePath()
}

// MirrorDramaRowsIfNeeded mirrors drama rows into the local SQLite cache when
// the provided table URL matches DRAMA_BITABLE_URL.
func MirrorDramaRowsIfNeeded(tableURL string, rows []BitableRow) {
	storage.MirrorDramaRowsIfNeeded(tableURL, rows)
}
