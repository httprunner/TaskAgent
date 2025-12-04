package piracy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/storage"
)

func newSQLiteSummarySource(fields summaryFieldConfig, opts WebhookOptions) (summaryDataSource, error) {
	dbPath := strings.TrimSpace(opts.SQLitePath)
	if dbPath == "" {
		var err error
		dbPath, err = storage.ResolveDatabasePath()
		if err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database failed: %w", err)
	}

	dramaTable := pickTableEnv("DRAMA_SQLITE_TABLE", "drama_catalog")
	resultTable := pickTableEnv("RESULT_SQLITE_TABLE", "capture_results")
	if strings.TrimSpace(dramaTable) == "" {
		return nil, fmt.Errorf("drama sqlite table name is empty")
	}
	if strings.TrimSpace(resultTable) == "" {
		return nil, fmt.Errorf("result sqlite table name is empty")
	}

	return &sqliteSummarySource{
		db:          db,
		fields:      fields,
		dramaTable:  dramaTable,
		resultTable: resultTable,
	}, nil
}

func pickTableEnv(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

type sqliteSummarySource struct {
	db          *sql.DB
	fields      summaryFieldConfig
	dramaTable  string
	resultTable string
}

func (s *sqliteSummarySource) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *sqliteSummarySource) FetchDrama(ctx context.Context, params string) (*dramaInfo, error) {
	if strings.TrimSpace(params) == "" {
		return nil, fmt.Errorf("params is empty")
	}
	// Use LIKE to handle JSON-formatted drama names: [{"text":"许你一世峥嵘","type":"text"}]
	query := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s LIKE ? LIMIT 1",
		s.column(s.fields.Drama.DramaID),
		s.column(s.fields.Drama.DramaName),
		s.column(s.fields.Drama.Priority),
		s.column(s.fields.Drama.RightsProtectionScenario),
		quoteIdentifier(s.dramaTable),
		s.column(s.fields.Drama.DramaName),
	)

	row := s.db.QueryRowContext(ctx, query, "%"+params+"%")
	var (
		id, name, priority, rights sql.NullString
	)
	if err := row.Scan(&id, &name, &priority, &rights); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("drama %s not found in sqlite table", params)
		}
		return nil, fmt.Errorf("query drama row failed: %w", err)
	}

	// Parse JSON-formatted fields if needed
	dramaName := strings.TrimSpace(name.String)
	dramaID := strings.TrimSpace(id.String)
	priorityVal := strings.TrimSpace(priority.String)
	rightsVal := strings.TrimSpace(rights.String)

	// Try to extract text from JSON format: [{"text":"value","type":"text"}]
	dramaName = extractTextFromJSON(dramaName)
	dramaID = extractTextFromJSON(dramaID)
	priorityVal = extractTextFromJSON(priorityVal)
	rightsVal = extractTextFromJSON(rightsVal)

	info := &dramaInfo{
		ID:             dramaID,
		Name:           dramaName,
		Priority:       priorityVal,
		RightsScenario: rightsVal,
		RawFields: map[string]any{
			s.fields.Drama.DramaID:                  dramaID,
			s.fields.Drama.DramaName:                dramaName,
			s.fields.Drama.Priority:                 priorityVal,
			s.fields.Drama.RightsProtectionScenario: rightsVal,
		},
	}
	if info.Name == "" {
		info.Name = params
	}
	return info, nil
}

func (s *sqliteSummarySource) FetchRecords(ctx context.Context, query recordQuery) ([]CaptureRecordPayload, error) {
	builder := strings.Builder{}
	builder.WriteString("SELECT id, ")
	builder.WriteString(s.column(s.fields.Result.Params))
	builder.WriteString(", DeviceSerial, ")
	builder.WriteString(s.column(s.fields.Result.App))
	builder.WriteString(", ItemID, ItemCaption, ItemDuration, ")
	builder.WriteString(s.column(s.fields.Result.UserName))
	builder.WriteString(", ")
	builder.WriteString(s.column(s.fields.Result.UserID))
	builder.WriteString(", Tags, TaskID, Datetime, Extra FROM ")
	builder.WriteString(quoteIdentifier(s.resultTable))
	builder.WriteString(" WHERE 1=1")
	args := make([]any, 0, 5)

	if params := strings.TrimSpace(query.Params); params != "" {
		builder.WriteString(" AND ")
		builder.WriteString(quoteIdentifier(s.fields.Result.Params))
		builder.WriteString(" = ?")
		args = append(args, params)
	}
	if itemID := strings.TrimSpace(query.ItemID); itemID != "" {
		builder.WriteString(" AND ")
		builder.WriteString(s.column(s.fields.Result.ItemID))
		builder.WriteString(" = ?")
		args = append(args, itemID)
	}
	if app := strings.TrimSpace(query.App); app != "" {
		builder.WriteString(" AND ")
		builder.WriteString(quoteIdentifier(s.fields.Result.App))
		builder.WriteString(" = ?")
		args = append(args, app)
	}
	if scene := strings.TrimSpace(query.Scene); scene != "" {
		builder.WriteString(" AND ")
		builder.WriteString(s.column(s.fields.Result.Scene))
		builder.WriteString(" = ?")
		args = append(args, scene)
	}
	if userID := strings.TrimSpace(query.UserID); userID != "" {
		builder.WriteString(" AND ")
		builder.WriteString(quoteIdentifier(s.fields.Result.UserID))
		builder.WriteString(" = ?")
		args = append(args, userID)
	}
	if userName := strings.TrimSpace(query.UserName); userName != "" {
		builder.WriteString(" AND ")
		builder.WriteString(quoteIdentifier(s.fields.Result.UserName))
		builder.WriteString(" = ?")
		args = append(args, userName)
	}

	builder.WriteString(" ORDER BY Datetime DESC")
	limit := query.Limit
	if query.PreferLatest && (limit <= 0 || limit > 1) {
		limit = 1
	}
	if limit > 0 {
		builder.WriteString(" LIMIT ?")
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query capture_results failed: %w", err)
	}
	defer rows.Close()

	records := make([]CaptureRecordPayload, 0)
	for rows.Next() {
		var (
			id                                                                     int64
			params, deviceSerial, app, itemID, itemCaption, userName, userID, tags sql.NullString
			itemDuration                                                           sql.NullFloat64
			taskID                                                                 sql.NullInt64
			datetime                                                               sql.NullInt64
			extra                                                                  sql.NullString
		)
		if err := rows.Scan(&id, &params, &deviceSerial, &app, &itemID, &itemCaption, &itemDuration, &userName, &userID, &tags, &taskID, &datetime, &extra); err != nil {
			return nil, fmt.Errorf("scan capture_results row failed: %w", err)
		}
		recordFields := map[string]any{}
		addStringField(recordFields, s.fields.Result.Params, params.String)
		addStringField(recordFields, "DeviceSerial", deviceSerial.String)
		addStringField(recordFields, s.fields.Result.App, app.String)
		addStringField(recordFields, "ItemID", itemID.String)
		addStringField(recordFields, "ItemCaption", itemCaption.String)
		if itemDuration.Valid {
			recordFields["ItemDuration"] = itemDuration.Float64
		}
		addStringField(recordFields, s.fields.Result.UserName, userName.String)
		addStringField(recordFields, s.fields.Result.UserID, userID.String)
		addStringField(recordFields, "Tags", tags.String)
		if taskID.Valid {
			recordFields["TaskID"] = taskID.Int64
		}
		if datetime.Valid {
			recordFields["Datetime"] = datetime.Int64
			recordFields["DatetimeISO"] = time.UnixMilli(datetime.Int64).UTC().Format(time.RFC3339)
		}
		if val := decodeExtra(extra.String); val != nil {
			recordFields["Extra"] = val
		}

		records = append(records, CaptureRecordPayload{
			RecordID: fmt.Sprintf("%d", id),
			Fields:   recordFields,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate capture_results failed: %w", err)
	}
	return records, nil
}

func quoteIdentifier(name string) string {
	trimmed := strings.TrimSpace(name)
	trimmed = strings.ReplaceAll(trimmed, "\"", "")
	if trimmed == "" {
		return "\"\""
	}
	return fmt.Sprintf("\"%s\"", trimmed)
}

func (s *sqliteSummarySource) column(name string) string {
	return quoteIdentifier(strings.TrimSpace(name))
}

func addStringField(fields map[string]any, key, value string) {
	name := strings.TrimSpace(key)
	if strings.TrimSpace(name) == "" {
		return
	}
	if strings.TrimSpace(value) == "" {
		return
	}
	fields[name] = strings.TrimSpace(value)
}

func decodeExtra(raw string) any {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	var data any
	if err := json.Unmarshal([]byte(trimmed), &data); err == nil {
		return data
	}
	return trimmed
}

// extractTextFromJSON attempts to parse JSON format: [{"text":"value","type":"text"}]
// Returns the original string if parsing fails or if it's not in the expected format.
func extractTextFromJSON(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.HasPrefix(trimmed, "[") {
		return trimmed
	}
	var items []map[string]any
	if err := json.Unmarshal([]byte(trimmed), &items); err != nil {
		return trimmed
	}
	if len(items) == 0 {
		return trimmed
	}
	if text, ok := items[0]["text"].(string); ok {
		return strings.TrimSpace(text)
	}
	return trimmed
}
