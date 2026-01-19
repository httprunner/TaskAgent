package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const dramaUpdatedAtColumn = "updated_at"

var (
	dramaMirrorOnce sync.Once
	dramaMirrorInst *dramaDetailStore
	dramaMirrorErr  error
)

// MirrorDramaRowsIfNeeded caches drama metadata locally when the fetched table
// matches DRAMA_BITABLE_URL.
func MirrorDramaRowsIfNeeded(tableURL string, rows []feishusdk.BitableRow) {
	if len(rows) == 0 || !isDramaCatalogURL(tableURL) {
		return
	}
	store, err := acquireDramaDetailStore()
	if err != nil {
		log.Error().Err(err).Msg("init drama detail store failed")
		return
	}
	if err := store.UpsertRows(rows); err != nil {
		log.Error().Err(err).Msg("sync drama details to sqlite failed")
	}
}

func isDramaCatalogURL(raw string) bool {
	envURL := env.String(feishusdk.EnvDramaBitableURL, "")
	if envURL == "" || strings.TrimSpace(raw) == "" {
		return false
	}
	envRef, envErr := feishusdk.ParseBitableURL(envURL)
	rawRef, rawErr := feishusdk.ParseBitableURL(raw)
	if envErr != nil || rawErr != nil {
		return strings.TrimSpace(raw) == envURL
	}
	if envRef.AppToken == "" || rawRef.AppToken == "" {
		return strings.TrimSpace(raw) == envURL
	}
	return strings.EqualFold(envRef.AppToken, rawRef.AppToken) && strings.EqualFold(envRef.TableID, rawRef.TableID)
}

func acquireDramaDetailStore() (*dramaDetailStore, error) {
	dramaMirrorOnce.Do(func() {
		dramaMirrorInst, dramaMirrorErr = newDramaDetailStore()
	})
	return dramaMirrorInst, dramaMirrorErr
}

type dramaDetailStore struct {
	db      *sql.DB
	stmt    *sql.Stmt
	table   string
	fields  feishusdk.SourceFields
	columns []string
}

func newDramaDetailStore() (*dramaDetailStore, error) {
	table := env.String("DRAMA_SQLITE_TABLE", "drama_catalog")
	fields := resolveDramaFields()
	if strings.TrimSpace(fields.DramaName) == "" {
		return nil, errors.New("drama name field cannot be empty")
	}

	path, err := ResolveDatabasePath()
	if err != nil {
		return nil, errors.Wrap(err, "resolve sqlite path for drama catalog failed")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, errors.Wrap(err, "open sqlite database for drama catalog failed")
	}
	if err := configureTaskSQLite(db); err != nil {
		db.Close()
		return nil, err
	}
	columns := buildDramaColumnOrder(fields)
	if err := ensureDramaSchema(db, table, fields, columns); err != nil {
		db.Close()
		return nil, err
	}
	stmt, err := prepareDramaUpsert(db, table, fields, columns)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &dramaDetailStore{db: db, stmt: stmt, table: table, fields: fields, columns: columns}, nil
}

func buildDramaColumnOrder(fields feishusdk.SourceFields) []string {
	cols := []string{fields.DramaName}
	if trimmed := strings.TrimSpace(fields.DramaID); trimmed != "" {
		cols = append(cols, trimmed)
	}
	if trimmed := strings.TrimSpace(fields.TotalDuration); trimmed != "" {
		cols = append(cols, trimmed)
	}
	if trimmed := strings.TrimSpace(fields.EpisodeCount); trimmed != "" {
		cols = append(cols, trimmed)
	}
	if trimmed := strings.TrimSpace(fields.Priority); trimmed != "" {
		cols = append(cols, trimmed)
	}
	if trimmed := strings.TrimSpace(fields.RightsProtectionScenario); trimmed != "" {
		cols = append(cols, trimmed)
	}
	return cols
}

func ensureDramaSchema(db *sql.DB, table string, fields feishusdk.SourceFields, columns []string) error {
	defs := make([]string, 0, len(columns)+1)
	defs = append(defs, fmt.Sprintf("%s TEXT PRIMARY KEY", quoteIdent(fields.DramaName)))
	for _, col := range columns[1:] {
		defs = append(defs, fmt.Sprintf("%s %s", quoteIdent(col), pickDramaColumnType(col, fields)))
	}
	defs = append(defs, fmt.Sprintf("%s TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP", quoteIdent(dramaUpdatedAtColumn)))
	createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
%s
);`, quoteIdent(table), strings.Join(defs, ",\n"))
	if _, err := db.Exec(createStmt); err != nil {
		return errors.Wrap(err, "create drama catalog table failed")
	}
	if trimmed := strings.TrimSpace(fields.DramaID); trimmed != "" {
		idxStmt := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s(%s);`,
			quoteIdent("idx_"+table+"_drama_id"), quoteIdent(table), quoteIdent(trimmed))
		if _, err := db.Exec(idxStmt); err != nil {
			return errors.Wrap(err, "create drama catalog index failed")
		}
	}
	return nil
}

func pickDramaColumnType(name string, fields feishusdk.SourceFields) string {
	switch name {
	case fields.TotalDuration, fields.EpisodeCount:
		return "REAL"
	default:
		return "TEXT"
	}
}

func prepareDramaUpsert(db *sql.DB, table string, fields feishusdk.SourceFields, columns []string) (*sql.Stmt, error) {
	quoted := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quoteIdent(col)
		placeholders[i] = "?"
	}
	columnList := strings.Join(append(quoted, quoteIdent(dramaUpdatedAtColumn)), ", ")
	valuesList := strings.Join(placeholders, ", ") + ", CURRENT_TIMESTAMP"

	builder := &strings.Builder{}
	fmt.Fprintf(builder, "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET ",
		quoteIdent(table), columnList, valuesList, quoteIdent(fields.DramaName))

	sets := make([]string, 0, len(columns))
	for _, col := range columns {
		if col == fields.DramaName {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s=excluded.%s", quoteIdent(col), quoteIdent(col)))
	}
	sets = append(sets, fmt.Sprintf("%s=CURRENT_TIMESTAMP", quoteIdent(dramaUpdatedAtColumn)))
	builder.WriteString(strings.Join(sets, ", "))

	stmt, err := db.Prepare(builder.String())
	if err != nil {
		return nil, errors.Wrap(err, "prepare drama catalog upsert statement failed")
	}
	return stmt, nil
}

func (s *dramaDetailStore) Close() error {
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

func (s *dramaDetailStore) UpsertRows(rows []feishusdk.BitableRow) error {
	if s == nil || len(rows) == 0 {
		return nil
	}
	for _, row := range rows {
		if err := s.upsert(row); err != nil {
			return err
		}
	}
	return nil
}

func (s *dramaDetailStore) upsert(row feishusdk.BitableRow) error {
	if row.Fields == nil {
		return nil
	}
	values := make([]any, 0, len(s.columns))
	var primary sql.NullString
	for _, col := range s.columns {
		switch col {
		case s.fields.DramaName:
			name := strings.TrimSpace(getFieldString(row.Fields, col))
			if name == "" {
				return nil
			}
			primary = sql.NullString{String: name, Valid: true}
			values = append(values, primary)
		case s.fields.TotalDuration:
			values = append(values, nullableFloat(row.Fields, col))
		case s.fields.EpisodeCount:
			values = append(values, nullableFloat(row.Fields, col))
		default:
			values = append(values, nullableString(getFieldString(row.Fields, col)))
		}
	}
	_, err := s.stmt.Exec(values...)
	if err != nil {
		return errors.Wrapf(err, "upsert drama %s failed", primary.String)
	}
	return nil
}

func nullableFloat(fields map[string]any, name string) sql.NullFloat64 {
	val, ok := getFieldFloat(fields, name)
	if !ok {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: val, Valid: true}
}

func getFieldString(fields map[string]any, name string) string {
	if fields == nil || strings.TrimSpace(name) == "" {
		return ""
	}
	if val, ok := fields[name]; ok {
		switch v := val.(type) {
		case string:
			return strings.TrimSpace(v)
		case []byte:
			return strings.TrimSpace(string(v))
		case json.Number:
			return strings.TrimSpace(v.String())
		default:
			if b, err := json.Marshal(v); err == nil {
				return strings.TrimSpace(string(b))
			}
		}
	}
	return ""
}

func getFieldFloat(fields map[string]any, name string) (float64, bool) {
	if fields == nil || strings.TrimSpace(name) == "" {
		return 0, false
	}
	val, ok := fields[name]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return f, true
		}
	case []byte:
		trimmed := strings.TrimSpace(string(v))
		if trimmed == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func resolveDramaFields() feishusdk.SourceFields {
	return feishusdk.DefaultSourceFields
}

// Exposed only for tests to reset cached state.
func ResetDramaMirrorForTest() {
	if dramaMirrorInst != nil {
		dramaMirrorInst.Close()
	}
	dramaMirrorInst = nil
	dramaMirrorErr = nil
	dramaMirrorOnce = sync.Once{}
}
