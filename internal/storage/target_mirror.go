package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/pkg/errors"
)

const (
	captureTasksTable     = "capture_tasks"
	targetUpdatedAtColumn = "updated_at"
)

// TaskStatus contains the columns mirrored into the local capture_tasks table.
type TaskStatus struct {
	TaskID           int64
	Params           string
	ItemID           string
	BookID           string
	URL              string
	App              string
	Scene            string
	StartAt          *time.Time
	StartAtRaw       string
	EndAt            *time.Time
	EndAtRaw         string
	Datetime         *time.Time
	DatetimeRaw      string
	Status           string
	Webhook          string
	UserID           string
	UserName         string
	Extra            string
	Logs             string
	DeviceSerial     string
	DispatchedDevice string
	DispatchedAt     *time.Time
	DispatchedAtRaw  string
	ElapsedSeconds   int64
	ItemsCollected   int64
}

// TaskMirror keeps Feishu task rows synchronized inside SQLite.
type TaskMirror struct {
	db      *sql.DB
	stmt    *sql.Stmt
	fields  feishusdk.TaskFields
	columns []string
}

// NewTaskMirror opens the shared SQLite database (ResolveDatabasePath) and
// ensures the capture_tasks table exists.
func NewTaskMirror() (*TaskMirror, error) {
	path, err := ResolveDatabasePath()
	if err != nil {
		return nil, errors.Wrap(err, "resolve sqlite path for capture targets failed")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, errors.Wrap(err, "open sqlite database for capture targets failed")
	}
	if err := configureTaskSQLite(db); err != nil {
		db.Close()
		return nil, err
	}
	fields := feishusdk.DefaultTaskFields
	columns := buildTaskColumnOrder(fields)
	if err := ensureTaskSchema(db, captureTasksTable, fields, columns); err != nil {
		db.Close()
		return nil, err
	}
	stmt, err := prepareTaskUpsert(db, captureTasksTable, fields, columns)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &TaskMirror{db: db, stmt: stmt, fields: fields, columns: columns}, nil
}

func configureTaskSQLite(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, stmt := range pragmas {
		if _, err := db.Exec(stmt); err != nil {
			return errors.Wrapf(err, "execute sqlite pragma %s failed", stmt)
		}
	}
	db.SetMaxOpenConns(1)
	return nil
}

func buildTaskColumnOrder(fields feishusdk.TaskFields) []string {
	return []string{
		fields.TaskID,
		fields.Params,
		fields.ItemID,
		fields.BookID,
		fields.URL,
		fields.App,
		fields.Scene,
		fields.StartAt,
		fields.EndAt,
		fields.Datetime,
		fields.Status,
		fields.Webhook,
		fields.UserID,
		fields.UserName,
		fields.Extra,
		fields.Logs,
		fields.DeviceSerial,
		fields.DispatchedDevice,
		fields.DispatchedAt,
		fields.ElapsedSeconds,
		fields.ItemsCollected,
	}
}

func ensureTaskSchema(db *sql.DB, table string, fields feishusdk.TaskFields, columns []string) error {
	defs := []string{
		fmt.Sprintf("%s INTEGER PRIMARY KEY", quoteIdent(fields.TaskID)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.Params)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.ItemID)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.BookID)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.URL)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.App)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.Scene)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.StartAt)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.EndAt)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.Datetime)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.Status)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.Webhook)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.UserID)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.UserName)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.Extra)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.Logs)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.DeviceSerial)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.DispatchedDevice)),
		fmt.Sprintf("%s TEXT", quoteIdent(fields.DispatchedAt)),
		fmt.Sprintf("%s INTEGER", quoteIdent(fields.ElapsedSeconds)),
		fmt.Sprintf("%s INTEGER", quoteIdent(fields.ItemsCollected)),
		fmt.Sprintf("%s TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP", quoteIdent(targetUpdatedAtColumn)),
	}
	createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
%s
);`, quoteIdent(table), strings.Join(defs, ",\n"))
	if _, err := db.Exec(createStmt); err != nil {
		return errors.Wrap(err, "create capture targets table failed")
	}
	if err := ensureColumnExists(db, table, fields.ItemID, "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, table, fields.BookID, "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, table, fields.URL, "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, table, fields.StartAt, "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, table, fields.EndAt, "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, table, fields.Logs, "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, table, fields.Webhook, "TEXT"); err != nil {
		return err
	}
	if err := ensureColumnExists(db, table, fields.ItemsCollected, "INTEGER"); err != nil {
		return err
	}
	indexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s(%s);`, quoteIdent("idx_"+table+"_status"), quoteIdent(table), quoteIdent(fields.Status)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s(%s);`, quoteIdent("idx_"+table+"_updated_at"), quoteIdent(table), quoteIdent(targetUpdatedAtColumn)),
	}
	for _, stmt := range indexes {
		if _, err := db.Exec(stmt); err != nil {
			return errors.Wrap(err, "create capture targets index failed")
		}
	}
	return nil
}

func prepareTaskUpsert(db *sql.DB, table string, fields feishusdk.TaskFields, columns []string) (*sql.Stmt, error) {
	quotedCols := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdent(col)
		placeholders[i] = "?"
	}
	columnList := strings.Join(append(quotedCols, quoteIdent(targetUpdatedAtColumn)), ", ")
	valuesList := strings.Join(placeholders, ", ") + ", CURRENT_TIMESTAMP"

	builder := &strings.Builder{}
	fmt.Fprintf(builder, "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET ",
		quoteIdent(table), columnList, valuesList, quoteIdent(fields.TaskID))

	sets := make([]string, 0, len(columns))
	for _, col := range columns {
		if col == fields.TaskID {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s=excluded.%s", quoteIdent(col), quoteIdent(col)))
	}
	sets = append(sets, fmt.Sprintf("%s=CURRENT_TIMESTAMP", quoteIdent(targetUpdatedAtColumn)))
	builder.WriteString(strings.Join(sets, ", "))

	stmt, err := db.Prepare(builder.String())
	if err != nil {
		return nil, errors.Wrap(err, "prepare capture targets upsert statement failed")
	}
	return stmt, nil
}

// Close releases sqlite resources.
func (m *TaskMirror) Close() error {
	if m == nil {
		return nil
	}
	if m.stmt != nil {
		m.stmt.Close()
	}
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// UpsertTasks mirrors the provided target tasks.
func (m *TaskMirror) UpsertTasks(tasks []*TaskStatus) error {
	if m == nil || len(tasks) == 0 {
		return nil
	}
	for _, task := range tasks {
		if task == nil || task.TaskID == 0 {
			continue
		}
		if err := m.upsert(task); err != nil {
			return err
		}
	}
	return nil
}

func (m *TaskMirror) upsert(task *TaskStatus) error {
	elapsed := sql.NullInt64{}
	if task.ElapsedSeconds > 0 {
		elapsed = sql.NullInt64{Int64: task.ElapsedSeconds, Valid: true}
	}
	itemsCollected := sql.NullInt64{}
	// Preserve historical rows where ItemsCollected was NULL, but for new
	// mirrored data treat zero as an explicit value so analytics can
	// distinguish "missing" from "zero collected".
	itemsCollected = sql.NullInt64{Int64: task.ItemsCollected, Valid: true}
	_, err := m.stmt.Exec(
		task.TaskID,
		nullableString(task.Params),
		nullableString(task.ItemID),
		nullableString(task.BookID),
		nullableString(task.URL),
		nullableString(task.App),
		nullableString(task.Scene),
		formatDatetime(task.StartAtRaw, task.StartAt),
		formatDatetime(task.EndAtRaw, task.EndAt),
		formatDatetime(task.DatetimeRaw, task.Datetime),
		nullableString(task.Status),
		nullableString(task.Webhook),
		nullableString(task.UserID),
		nullableString(task.UserName),
		nullableString(task.Extra),
		nullableString(task.Logs),
		nullableString(task.DeviceSerial),
		nullableString(task.DispatchedDevice),
		formatDatetime(task.DispatchedAtRaw, task.DispatchedAt),
		elapsed,
		itemsCollected,
	)
	if err != nil {
		return errors.Wrapf(err, "upsert capture target %d failed", task.TaskID)
	}
	return nil
}

func nullableString(value string) sql.NullString {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}

func formatDatetime(raw string, ts *time.Time) sql.NullString {
	if trimmed := strings.TrimSpace(raw); trimmed != "" {
		return sql.NullString{String: trimmed, Valid: true}
	}
	if ts != nil && !ts.IsZero() {
		return sql.NullString{String: ts.UTC().Format(time.RFC3339), Valid: true}
	}
	return sql.NullString{}
}

func ensureColumnExists(db *sql.DB, table, column, columnType string) error {
	columnName := strings.TrimSpace(column)
	if columnName == "" {
		return nil
	}
	exists, err := columnExists(db, table, columnName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", quoteIdent(table), quoteIdent(columnName), columnType)
	if _, err := db.Exec(stmt); err != nil {
		return errors.Wrapf(err, "add column %s to table %s failed", columnName, table)
	}
	return nil
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s);", quoteIdent(table))
	rows, err := db.Query(query)
	if err != nil {
		return false, errors.Wrap(err, "query capture targets schema failed")
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid    int
			name   string
			ctype  string
			notnul int
			dflt   sql.NullString
			pk     int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnul, &dflt, &pk); err != nil {
			return false, errors.Wrap(err, "scan capture targets schema failed")
		}
		_ = cid
		_ = ctype
		_ = notnul
		_ = dflt
		_ = pk
		if strings.EqualFold(strings.TrimSpace(name), column) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, errors.Wrap(err, "iterate capture targets schema failed")
	}
	return false, nil
}

func quoteIdent(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	escaped := strings.ReplaceAll(trimmed, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", escaped)
}
