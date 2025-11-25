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
	envEnableFeishu   = "RESULT_STORAGE_ENABLE_FEISHU"
	envTrackingDBPath = "TRACKING_STORAGE_DB_PATH"
	defaultDBDirName  = ".eval"
	defaultDBFileName = "records.sqlite"
	resultTableName   = "capture_results"
	reportedColumn    = "reported"
	reportedAtColumn  = "reported_at"
	reportErrorColumn = "report_error"
)

var captureResultColumns = []string{
	"Params",
	"DeviceSerial",
	"App",
	"Scene",
	"ItemID",
	"ItemCaption",
	"ItemCDNURL",
	"ItemURL",
	"ItemDuration",
	"UserName",
	"UserID",
	"UserAuthEntity",
	"Tags",
	"TaskID",
	"Datetime",
	"LikeCount",
	"ViewCount",
	"AnchorPoint",
	"CommentCount",
	"CollectCount",
	"ForwardCount",
	"ShareCount",
	"PayMode",
	"Collection",
	"Episode",
	"PublishTime",
	"Extra",
}

func resolveResultTableName() string {
	if val := strings.TrimSpace(os.Getenv("RESULT_SQLITE_TABLE")); val != "" {
		return val
	}
	return resultTableName
}

// Config controls enabled sinks.
type Config struct {
	JSONLPath    string
	EnableFeishu bool
}

// Record captures the SQLite schema payload.
type Record struct {
	Params         string
	DeviceSerial   string
	App            string
	Scene          string
	ItemID         string
	ItemCaption    string
	ItemCDNURL     string
	ItemURL        string
	ItemDuration   *float64
	UserName       string
	UserID         string
	UserAuthEntity string
	Tags           string
	TaskID         int64
	Datetime       int64
	LikeCount      int64
	ViewCount      int
	AnchorPoint    string
	CommentCount   int64
	CollectCount   int64
	ForwardCount   int64
	ShareCount     int64
	PayMode        string
	Collection     string
	Episode        string
	PublishTime    string
	Extra          interface{}
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
	sinks    []Sink
	name     string
	reporter *resultReporter
}

// NewManager builds a storage manager based on cfg.
func NewManager(cfg Config) (*Manager, error) {
	sinks, feishuStorage, err := buildSinks(cfg)
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
	manager := &Manager{sinks: sinks, name: strings.Join(names, ",")}
	if feishuStorage != nil {
		reporter, err := newResultReporter(feishuStorage)
		if err != nil {
			for _, sink := range sinks {
				sink.Close()
			}
			return nil, err
		}
		manager.reporter = reporter
		names = append(names, "feishu-reporter")
		manager.name = strings.Join(names, ",")
	}
	return manager, nil
}

func buildSinks(cfg Config) ([]Sink, *feishu.ResultStorage, error) {
	sinks := make([]Sink, 0, 3)
	enableFeishu := shouldEnableFeishu(cfg)
	if shouldEnableJSONL(cfg) {
		jsonl, err := newJSONLWriter(cfg.JSONLPath)
		if err != nil {
			return nil, nil, err
		}
		sinks = append(sinks, jsonl)
	}
	sqliteSink, err := newSQLiteWriter()
	if err != nil {
		return nil, nil, err
	}
	sinks = append(sinks, sqliteSink)
	var feishuStorage *feishu.ResultStorage
	if enableFeishu {
		var err error
		feishuStorage, err = newFeishuStorage()
		if err != nil {
			return nil, nil, err
		}
	}
	return sinks, feishuStorage, nil
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
	stmt, err := db.Prepare(buildResultInsertStatement())
	if err != nil {
		db.Close()
		return nil, pkgerrors.Wrap(err, "storage: prepare sqlite insert failed")
	}
	return &sqliteWriter{db: db, stmt: stmt, path: dbPath}, nil
}

func buildResultInsertStatement() string {
	table := resolveResultTableName()
	columns := strings.Join(captureResultColumns, ", ")
	placeholders := strings.Repeat("?,", len(captureResultColumns))
	placeholders = strings.TrimRight(placeholders, ",")
	return fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, table, columns, placeholders)
}

func newFeishuStorage() (*feishu.ResultStorage, error) {
	storage, err := feishu.NewResultStorageFromEnv()
	if err != nil {
		return nil, err
	}
	return storage, nil
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
	if m.reporter != nil {
		if err := m.reporter.Close(); err != nil {
			errs = append(errs, pkgerrors.Wrap(err, "feishu reporter close failed"))
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
	row := map[string]any{
		"Scene":          record.DBRecord.Scene,
		"Params":         record.DBRecord.Params,
		"DeviceSerial":   record.DBRecord.DeviceSerial,
		"App":            record.DBRecord.App,
		"ItemID":         record.DBRecord.ItemID,
		"ItemCaption":    record.DBRecord.ItemCaption,
		"ItemCDNURL":     record.DBRecord.ItemCDNURL,
		"ItemURL":        record.DBRecord.ItemURL,
		"ItemDuration":   itemDuration,
		"UserName":       record.DBRecord.UserName,
		"UserID":         record.DBRecord.UserID,
		"UserAuthEntity": record.DBRecord.UserAuthEntity,
		"Tags":           record.DBRecord.Tags,
		"TaskID":         record.DBRecord.TaskID,
		"Datetime":       record.DBRecord.Datetime,
		"LikeCount":      record.DBRecord.LikeCount,
		"ViewCount":      record.DBRecord.ViewCount,
		"AnchorPoint":    record.DBRecord.AnchorPoint,
		"CommentCount":   record.DBRecord.CommentCount,
		"CollectCount":   record.DBRecord.CollectCount,
		"ForwardCount":   record.DBRecord.ForwardCount,
		"ShareCount":     record.DBRecord.ShareCount,
		"PayMode":        record.DBRecord.PayMode,
		"Collection":     record.DBRecord.Collection,
		"Episode":        record.DBRecord.Episode,
		"PublishTime":    record.DBRecord.PublishTime,
		"Extra":          payload,
	}
	return row, nil
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
		record.DBRecord.Scene,
		record.DBRecord.ItemID,
		record.DBRecord.ItemCaption,
		record.DBRecord.ItemCDNURL,
		record.DBRecord.ItemURL,
		record.DBRecord.ItemDuration,
		record.DBRecord.UserName,
		record.DBRecord.UserID,
		record.DBRecord.UserAuthEntity,
		record.DBRecord.Tags,
		record.DBRecord.TaskID,
		record.DBRecord.Datetime,
		record.DBRecord.LikeCount,
		record.DBRecord.ViewCount,
		record.DBRecord.AnchorPoint,
		record.DBRecord.CommentCount,
		record.DBRecord.CollectCount,
		record.DBRecord.ForwardCount,
		record.DBRecord.ShareCount,
		record.DBRecord.PayMode,
		record.DBRecord.Collection,
		record.DBRecord.Episode,
		record.DBRecord.PublishTime,
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
	table := resolveResultTableName()
	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			Params TEXT,
			DeviceSerial TEXT NOT NULL,
			App TEXT NOT NULL,
			Scene TEXT,
			ItemID TEXT,
			ItemCaption TEXT,
			ItemCDNURL TEXT,
			ItemURL TEXT,
			ItemDuration REAL,
			UserName TEXT,
			UserID TEXT,
			UserAuthEntity TEXT,
			Tags TEXT,
			TaskID INTEGER,
			Datetime INTEGER,
			LikeCount INTEGER,
			ViewCount INTEGER,
			AnchorPoint TEXT,
			CommentCount INTEGER,
			CollectCount INTEGER,
			ForwardCount INTEGER,
			ShareCount INTEGER,
			PayMode TEXT,
			Collection TEXT,
			Episode TEXT,
			PublishTime TEXT,
			Extra TEXT NOT NULL,
			%s INTEGER NOT NULL DEFAULT 0,
			%s INTEGER,
			%s TEXT
		);`, table, reportedColumn, reportedAtColumn, reportErrorColumn)
	if _, err := db.Exec(createTable); err != nil {
		return pkgerrors.Wrap(err, "storage: init sqlite schema failed")
	}
	for _, col := range []struct {
		name string
		typ  string
	}{
		{"Scene", "TEXT"},
		{"ItemCDNURL", "TEXT"},
		{"ItemURL", "TEXT"},
		{"ItemDuration", "REAL"},
		{"UserAuthEntity", "TEXT"},
		{"LikeCount", "INTEGER"},
		{"ViewCount", "INTEGER"},
		{"AnchorPoint", "TEXT"},
		{"CommentCount", "INTEGER"},
		{"CollectCount", "INTEGER"},
		{"ForwardCount", "INTEGER"},
		{"ShareCount", "INTEGER"},
		{"PayMode", "TEXT"},
		{"Collection", "TEXT"},
		{"Episode", "TEXT"},
		{"PublishTime", "TEXT"},
		{reportedColumn, "INTEGER NOT NULL DEFAULT 0"},
		{reportedAtColumn, "INTEGER"},
		{reportErrorColumn, "TEXT"},
	} {
		if err := ensureSQLiteColumn(db, table, col.name, col.typ); err != nil {
			return err
		}
	}
	indexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_params_datetime ON %s(Params, Datetime DESC);`, table, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_itemid ON %s(ItemID);`, table, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_reported ON %s(%s);`, table, table, reportedColumn),
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
