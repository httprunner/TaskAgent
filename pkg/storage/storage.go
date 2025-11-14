package storage

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	pkgerrors "github.com/pkg/errors"
	_ "modernc.org/sqlite"
)

const (
	envDisableJSONL   = "TRACKING_STORAGE_DISABLE_JSONL"
	envEnableSQLite   = "TRACKING_STORAGE_ENABLE_SQLITE"
	envEnableFeishu   = "RESULT_STORAGE_ENABLE_FEISHU"
	envTrackingDBPath = "TRACKING_STORAGE_DB_PATH"
	defaultDBDirName  = ".eval"
	defaultDBFileName = "records.sqlite"
)

// Config controls enabled sinks.
type Config struct {
	JSONLPath    string
	EnableSQLite bool
	EnableFeishu bool
}

// Record captures the SQLite schema payload.
type Record struct {
	Params       string
	DeviceSerial string
	App          string
	ItemID       string
	VideoID      string
	ItemCaption  string
	ItemDuration *float64
	UserName     string
	UserID       string
	Tags         string
	TaskID       int64
	Datetime     int64
	Extra        interface{}
}

// ResultRecord holds payloads for each sink.
type ResultRecord struct {
	JSONPayload interface{}
	DBRecord    Record
	FeishuInput *feishu.ResultRecordInput
}

// Sink defines the contract for each storage implementation.
type Sink interface {
	Write(ctx context.Context, record ResultRecord) error
	Close() error
	Name() string
}

// Manager fan-outs records to configured sinks.
type Manager struct {
	sinks []Sink
	name  string
}

// NewManager builds a storage manager based on cfg.
func NewManager(cfg Config) (*Manager, error) {
	sinks, err := buildSinks(cfg)
	if err != nil {
		return nil, err
	}
	if len(sinks) == 0 {
		return nil, pkgerrors.New("storage: no sinks enabled")
	}
	names := make([]string, 0, len(sinks))
	for _, s := range sinks {
		names = append(names, s.Name())
	}
	return &Manager{sinks: sinks, name: strings.Join(names, ",")}, nil
}

func buildSinks(cfg Config) ([]Sink, error) {
	sinks := make([]Sink, 0, 3)
	if shouldEnableJSONL(cfg) {
		jsonl, err := newJSONLWriter(cfg.JSONLPath)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, jsonl)
	}
	if shouldEnableSQLite(cfg) {
		sqliteSink, err := newSQLiteWriter()
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, sqliteSink)
	}
	if shouldEnableFeishu(cfg) {
		feishuSink, err := newFeishuWriter()
		if err != nil {
			return nil, err
		}
		if feishuSink != nil {
			sinks = append(sinks, feishuSink)
		}
	}
	return sinks, nil
}

func shouldEnableJSONL(cfg Config) bool {
	if strings.TrimSpace(cfg.JSONLPath) == "" {
		return false
	}
	if strings.TrimSpace(os.Getenv(envDisableJSONL)) != "" {
		return false
	}
	return true
}

func shouldEnableSQLite(cfg Config) bool {
	if cfg.EnableSQLite {
		return true
	}
	val := strings.TrimSpace(os.Getenv(envEnableSQLite))
	return strings.EqualFold(val, "1") || strings.EqualFold(val, "true")
}

func shouldEnableFeishu(cfg Config) bool {
	if cfg.EnableFeishu {
		return true
	}
	val := strings.TrimSpace(os.Getenv(envEnableFeishu))
	return strings.EqualFold(val, "1") || strings.EqualFold(val, "true")
}

func newJSONLWriter(path string) (Sink, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, pkgerrors.New("storage: jsonl path is empty")
	}
	if err := ensureDir(filepath.Dir(trimmed)); err != nil {
		return nil, err
	}
	file, err := os.Create(trimmed)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: create jsonl file failed")
	}
	return &jsonlWriter{path: trimmed, file: file, writer: bufio.NewWriter(file)}, nil
}

func ensureDir(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return pkgerrors.Wrapf(err, "storage: create dir %s failed", dir)
	}
	return nil
}

func newSQLiteWriter() (Sink, error) {
	dbPath, err := resolveDatabasePath()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: open sqlite database failed")
	}
	if err := configureSQLite(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := prepareSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	stmt, err := db.Prepare(`INSERT INTO capture_results
		(Params, DeviceSerial, App, ItemID, VideoID, ItemCaption, ItemDuration, UserName, UserID, Tags, TaskID, Datetime, Extra)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		db.Close()
		return nil, pkgerrors.Wrap(err, "storage: prepare sqlite insert failed")
	}
	return &sqliteWriter{db: db, stmt: stmt, path: dbPath}, nil
}

func newFeishuWriter() (Sink, error) {
	storage, err := feishu.NewResultStorageFromEnv()
	if err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, nil
	}
	return &feishuWriter{storage: storage}, nil
}

func (m *Manager) Write(ctx context.Context, record ResultRecord) error {
	var errs []error
	for _, sink := range m.sinks {
		if err := sink.Write(ctx, record); err != nil {
			errs = append(errs, pkgerrors.Wrap(err, fmt.Sprintf("%s write failed", sink.Name())))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) Close() error {
	var errs []error
	for _, sink := range m.sinks {
		if err := sink.Close(); err != nil {
			errs = append(errs, pkgerrors.Wrap(err, fmt.Sprintf("%s close failed", sink.Name())))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) Name() string {
	if m == nil {
		return "storage"
	}
	if m.name == "" {
		return "storage"
	}
	return m.name
}

type jsonlWriter struct {
	path   string
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
}

func (j *jsonlWriter) Write(_ context.Context, record ResultRecord) error {
	if j == nil || j.writer == nil {
		return pkgerrors.New("storage: jsonl writer nil")
	}
	row, err := buildJSONLRow(record)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return pkgerrors.Wrap(err, "storage: marshal json payload failed")
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if _, err := j.writer.Write(payload); err != nil {
		return pkgerrors.Wrap(err, "storage: write json payload failed")
	}
	if err := j.writer.WriteByte('\n'); err != nil {
		return pkgerrors.Wrap(err, "storage: write newline failed")
	}
	if err := j.writer.Flush(); err != nil {
		return pkgerrors.Wrap(err, "storage: flush json writer failed")
	}
	return nil
}

func (j *jsonlWriter) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer != nil {
		if err := j.writer.Flush(); err != nil {
			return pkgerrors.Wrap(err, "storage: flush on close failed")
		}
	}
	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return pkgerrors.Wrap(err, "storage: close json file failed")
		}
	}
	return nil
}

func (j *jsonlWriter) Name() string {
	if j == nil || j.path == "" {
		return "jsonl"
	}
	return j.path
}

func buildJSONLRow(record ResultRecord) (map[string]any, error) {
	var itemDuration any
	if record.DBRecord.ItemDuration != nil {
		itemDuration = *record.DBRecord.ItemDuration
	}
	payload, err := formatExtra(record.DBRecord.Extra)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"Params":       record.DBRecord.Params,
		"DeviceSerial": record.DBRecord.DeviceSerial,
		"App":          record.DBRecord.App,
		"ItemID":       record.DBRecord.ItemID,
		"VideoID":      record.DBRecord.VideoID,
		"ItemCaption":  record.DBRecord.ItemCaption,
		"ItemDuration": itemDuration,
		"UserName":     record.DBRecord.UserName,
		"UserID":       record.DBRecord.UserID,
		"Tags":         record.DBRecord.Tags,
		"TaskID":       record.DBRecord.TaskID,
		"Datetime":     record.DBRecord.Datetime,
		"Extra":        payload,
	}, nil
}

type sqliteWriter struct {
	db   *sql.DB
	stmt *sql.Stmt
	path string
}

func (s *sqliteWriter) Write(ctx context.Context, record ResultRecord) error {
	if s == nil || s.db == nil || s.stmt == nil {
		return pkgerrors.New("storage: sqlite storage nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := formatExtra(record.DBRecord.Extra)
	if err != nil {
		return err
	}
	_, err = s.stmt.ExecContext(ctx,
		record.DBRecord.Params,
		record.DBRecord.DeviceSerial,
		record.DBRecord.App,
		record.DBRecord.ItemID,
		record.DBRecord.VideoID,
		record.DBRecord.ItemCaption,
		record.DBRecord.ItemDuration,
		record.DBRecord.UserName,
		record.DBRecord.UserID,
		record.DBRecord.Tags,
		record.DBRecord.TaskID,
		record.DBRecord.Datetime,
		payload,
	)
	if err != nil {
		return pkgerrors.Wrap(err, "storage: sqlite insert failed")
	}
	return nil
}

func (s *sqliteWriter) Close() error {
	if s == nil {
		return nil
	}
	if s.stmt != nil {
		s.stmt.Close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *sqliteWriter) Name() string {
	if s == nil || s.path == "" {
		return "sqlite"
	}
	return s.path
}

func resolveDatabasePath() (string, error) {
	if custom := strings.TrimSpace(os.Getenv(envTrackingDBPath)); custom != "" {
		if err := ensureDirExists(filepath.Dir(custom)); err != nil {
			return "", err
		}
		return custom, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", pkgerrors.Wrap(err, "storage: locate user home failed")
	}
	dir := filepath.Join(home, defaultDBDirName)
	if err := ensureDirExists(dir); err != nil {
		return "", err
	}
	return filepath.Join(dir, defaultDBFileName), nil
}

// ResolveDatabasePath returns the absolute path to the tracking SQLite
// database, creating the parent directory if necessary. Callers outside the
// storage package can reuse this helper to open read-only connections without
// duplicating environment resolution logic.
func ResolveDatabasePath() (string, error) {
	return resolveDatabasePath()
}

func ensureDirExists(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return pkgerrors.Wrapf(err, "storage: create dir %s failed", path)
	}
	return nil
}

func configureSQLite(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return pkgerrors.Wrapf(err, "storage: execute %s failed", pragma)
		}
	}
	db.SetMaxOpenConns(1)
	return nil
}

func prepareSchema(db *sql.DB) error {
	createTable := `CREATE TABLE IF NOT EXISTS capture_results (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			Params TEXT,
			DeviceSerial TEXT NOT NULL,
			App TEXT NOT NULL,
			ItemID TEXT,
			VideoID TEXT,
			ItemCaption TEXT,
			ItemDuration REAL,
			UserName TEXT,
			UserID TEXT,
			Tags TEXT,
			TaskID INTEGER,
			Datetime INTEGER,
			Extra TEXT NOT NULL
		);`
	if _, err := db.Exec(createTable); err != nil {
		return pkgerrors.Wrap(err, "storage: init sqlite schema failed")
	}
	if err := ensureSQLiteColumn(db, "capture_results", "ItemDuration", "REAL"); err != nil {
		return err
	}
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_capture_results_params_datetime ON capture_results(Params, Datetime DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_capture_results_itemid ON capture_results(ItemID);`,
	}
	for _, stmt := range indexes {
		if _, err := db.Exec(stmt); err != nil {
			return pkgerrors.Wrap(err, "storage: init sqlite indexes failed")
		}
	}
	return nil
}

func ensureSQLiteColumn(db *sql.DB, table, column, columnType string) error {
	query := fmt.Sprintf("PRAGMA table_info(%s);", table)
	rows, err := db.Query(query)
	if err != nil {
		return pkgerrors.Wrapf(err, "storage: describe %s schema failed", table)
	}
	defer rows.Close()
	exists := false
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return pkgerrors.Wrap(err, "storage: scan sqlite table info failed")
		}
		if strings.EqualFold(name, column) {
			exists = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		return pkgerrors.Wrap(err, "storage: iterate sqlite table info failed")
	}
	if exists {
		return nil
	}
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, columnType)
	if _, err := db.Exec(stmt); err != nil {
		return pkgerrors.Wrapf(err, "storage: add column %s to %s failed", column, table)
	}
	return nil
}

func formatExtra(extra interface{}) (string, error) {
	switch v := extra.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", pkgerrors.Wrap(err, "storage: marshal extra payload")
		}
		return string(b), nil
	}
}

type feishuWriter struct {
	storage *feishu.ResultStorage
}

func (f *feishuWriter) Write(ctx context.Context, record ResultRecord) error {
	if f == nil || f.storage == nil || record.FeishuInput == nil {
		return nil
	}
	return f.storage.Write(ctx, *record.FeishuInput)
}

func (f *feishuWriter) Close() error {
	return nil
}

func (f *feishuWriter) Name() string {
	if f == nil || f.storage == nil {
		return "feishu"
	}
	return f.storage.TableURL()
}
