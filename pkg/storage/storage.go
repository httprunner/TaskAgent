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
	"github.com/rs/zerolog/log"
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

var (
	globalReporter        *resultReporter
	globalReporterMu      sync.Mutex
	feishuStorageFactory  = newFeishuStorage
	resultReporterFactory = newResultReporter
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

	readerOnce sync.Once
	reader     *captureResultReader
	readerErr  error
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
		// Use process-level singleton reporter to avoid concurrency competition
		_, err := getOrStartGlobalReporter(feishuStorage)
		if err != nil {
			for _, sink := range sinks {
				sink.Close()
			}
			return nil, err
		}
		// NOTE: We do NOT assign manager.reporter = reporter here.
		// The global reporter runs as a daemon and should not be closed by individual managers.
		// manager.reporter = reporter
		names = append(names, "feishu-reporter")
		manager.name = strings.Join(names, ",")
	}
	return manager, nil
}

func getOrStartGlobalReporter(storage *feishu.ResultStorage) (*resultReporter, error) {
	globalReporterMu.Lock()
	defer globalReporterMu.Unlock()

	if globalReporter != nil {
		return globalReporter, nil
	}

	reporter, err := resultReporterFactory(storage)
	if err != nil {
		return nil, err
	}
	globalReporter = reporter
	return globalReporter, nil
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
	quotedCols := make([]string, len(captureResultColumns))
	placeholders := make([]string, len(captureResultColumns))
	for i, col := range captureResultColumns {
		quotedCols[i] = quoteIdent(col)
		placeholders[i] = "?"
	}
	conflictCols := []string{"DeviceSerial", "Params", "ItemID"}
	conflictSet := make(map[string]struct{}, len(conflictCols))
	for _, col := range conflictCols {
		conflictSet[col] = struct{}{}
	}
	updateAssignments := make([]string, 0, len(captureResultColumns)+3)
	for _, col := range captureResultColumns {
		if _, skip := conflictSet[col]; skip {
			continue
		}
		updateAssignments = append(updateAssignments,
			fmt.Sprintf("%s=excluded.%s", quoteIdent(col), quoteIdent(col)))
	}
	updateAssignments = append(updateAssignments,
		fmt.Sprintf("%s=%d", quoteIdent(reportedColumn), reportStatusPending),
		fmt.Sprintf("%s=NULL", quoteIdent(reportedAtColumn)),
		fmt.Sprintf("%s=NULL", quoteIdent(reportErrorColumn)),
	)
	conflictClause := make([]string, len(conflictCols))
	for i, col := range conflictCols {
		conflictClause[i] = quoteIdent(col)
	}
	return fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s`,
		quoteIdent(table),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictClause, ", "),
		strings.Join(updateAssignments, ", "))
}

func newFeishuStorage() (*feishu.ResultStorage, error) {
	storage, err := feishu.NewResultStorageFromEnv()
	if err != nil {
		return nil, err
	}
	return storage, nil
}

// EnsureResultReporter starts the shared Feishu reporter so queued SQLite rows
// continue to flush even if no TrackingRunner is active.
func EnsureResultReporter() error {
	if strings.TrimSpace(os.Getenv(feishu.EnvResultBitableURL)) == "" {
		log.Warn().Msg("storage: RESULT_BITABLE_URL not set, skip reporter bootstrap")
		return nil
	}

	globalReporterMu.Lock()
	if globalReporter != nil {
		globalReporterMu.Unlock()
		log.Debug().Msg("storage: result reporter already running")
		return nil
	}
	globalReporterMu.Unlock()

	storage, err := feishuStorageFactory()
	if err != nil {
		return err
	}
	if storage == nil {
		log.Warn().Msg("storage: feishu storage factory returned nil, reporter not started")
		return nil
	}

	if _, err := getOrStartGlobalReporter(storage); err != nil {
		return err
	}
	log.Info().Msg("storage: result reporter initialized")
	return nil
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
	if m.reader != nil {
		if err := m.reader.Close(); err != nil {
			errs = append(errs, pkgerrors.Wrap(err, "capture result reader close failed"))
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

// LookupCaptureParamsByItemID returns the Params field of the latest capture_result row for the given ItemID.
func (m *Manager) LookupCaptureParamsByItemID(ctx context.Context, itemID string) (string, error) {
	if strings.TrimSpace(itemID) == "" {
		log.Warn().Msg("storage: empty item ID provided to LookupCaptureParamsByItemID")
		return "", nil
	}
	reader, err := m.ensureCaptureResultReader()
	if err != nil {
		log.Error().Err(err).Str("item_id", itemID).Msg("storage: ensure capture result reader failed")
		return "", err
	}
	return reader.lookupParamsByItemID(ctx, itemID)
}

func (m *Manager) ensureCaptureResultReader() (*captureResultReader, error) {
	if m == nil {
		return nil, pkgerrors.New("storage: manager nil")
	}
	m.readerOnce.Do(func() {
		m.reader, m.readerErr = newCaptureResultReader()
	})
	return m.reader, m.readerErr
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
		// 5s 过短，Feishu 网络抖动或上报堆积时易触发 SQLITE_BUSY，延长至 60s。
		"PRAGMA busy_timeout=60000;",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return pkgerrors.Wrapf(err, "storage: execute %s failed", pragma)
		}
	}
	db.SetMaxOpenConns(1)
	// 限制空闲连接，避免旧连接持锁。
	db.SetMaxIdleConns(1)
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

	// Cleanup historical duplicates so the unique index creation will not fail.
	if err := dedupeCaptureResults(db, table); err != nil {
		return err
	}
	for _, stmt := range indexes {
		if _, err := db.Exec(stmt); err != nil {
			return pkgerrors.Wrap(err, "storage: init sqlite indexes failed")
		}
	}

	if err := ensureDeviceTaskItemUniqueIndex(db, table); err != nil {
		return err
	}
	return nil
}

// dedupeCaptureResults removes duplicate rows (keeping the earliest rowid) for
// the composite key used by the unique index. This allows the index creation to
// succeed even if older builds inserted duplicates.
func dedupeCaptureResults(db *sql.DB, table string) error {
	stmt := fmt.Sprintf(`DELETE FROM %s WHERE rowid NOT IN (
		SELECT MIN(rowid) FROM %s GROUP BY DeviceSerial, Params, ItemID
	);`, quoteIdent(table), quoteIdent(table))
	if _, err := db.Exec(stmt); err != nil {
		return pkgerrors.Wrap(err, "storage: dedupe capture_results failed")
	}
	return nil
}

// ensureDeviceTaskItemUniqueIndex guarantees the desired unique index exists
// even if an older build created idx_*_dedup with different columns. It drops
// the stale index when the column set mismatches, then recreates the correct one.
func ensureDeviceTaskItemUniqueIndex(db *sql.DB, table string) error {
	indexName := fmt.Sprintf("idx_%s_dedup", table)
	columns, err := inspectIndexColumns(db, indexName)
	if err != nil {
		return err
	}
	desired := []string{"DeviceSerial", "Params", "ItemID"}
	if slicesEqualFold(columns, desired) {
		return nil // already correct
	}
	if len(columns) > 0 { // exists but wrong definition
		dropStmt := fmt.Sprintf("DROP INDEX IF EXISTS %s;", quoteIdent(indexName))
		if _, err := db.Exec(dropStmt); err != nil {
			return pkgerrors.Wrap(err, "storage: drop stale dedup index failed")
		}
	}
	createStmt := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(DeviceSerial, Params, ItemID);`, quoteIdent(indexName), quoteIdent(table))
	if _, err := db.Exec(createStmt); err != nil {
		return pkgerrors.Wrap(err, "storage: create task/app/item unique index failed")
	}
	return nil
}

// inspectIndexColumns returns the column list (in order) for the given index
// name, or nil if the index does not exist.
func inspectIndexColumns(db *sql.DB, indexName string) ([]string, error) {
	if strings.TrimSpace(indexName) == "" {
		return nil, nil
	}
	query := fmt.Sprintf("PRAGMA index_info(%s);", quoteIdent(indexName))
	rows, err := db.Query(query)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: inspect index info failed")
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var (
			seqno int
			cid   int
			name  string
		)
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, pkgerrors.Wrap(err, "storage: scan index info failed")
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, pkgerrors.Wrap(err, "storage: iterate index info failed")
	}
	return cols, nil
}

// slicesEqualFold compares two string slices case-insensitively and order-sensitively.
func slicesEqualFold(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
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
