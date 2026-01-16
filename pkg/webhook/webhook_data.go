package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
)

func fetchCaptureRecordsByTaskIDs(ctx context.Context, taskIDs []int64, userID string) ([]CaptureRecordPayload, error) {
	if len(taskIDs) == 0 {
		return nil, nil
	}
	sqliteRecords, err := fetchSQLiteCaptureRecordsByTaskIDs(ctx, taskIDs)
	if err == nil && len(sqliteRecords) > 0 {
		return filterRecordsByTaskAndUser(sqliteRecords, taskIDs, userID), nil
	}

	feishuRecords, feishuErr := fetchFeishuCaptureRecordsByTaskIDs(ctx, taskIDs)
	if feishuErr != nil {
		if err != nil {
			return nil, errors.Wrapf(feishuErr, "sqlite error: %v", err)
		}
		return nil, feishuErr
	}
	return filterRecordsByTaskAndUser(feishuRecords, taskIDs, userID), nil
}

func fetchSQLiteCaptureRecordsByTaskIDs(ctx context.Context, taskIDs []int64) ([]CaptureRecordPayload, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := taskagent.OpenCaptureResultsDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	table := strings.TrimSpace(taskagent.EnvString("RESULT_SQLITE_TABLE", "capture_results"))
	if table == "" {
		return nil, errors.New("sqlite result table name is empty")
	}
	// SQLite schema uses a stable TaskID column and should not rely on
	// Feishu field mappings (RESULT_FIELD_*), which may be localized.
	taskIDCol := strings.TrimSpace(taskagent.EnvString("RESULT_SQLITE_FIELD_TASKID", "TaskID"))
	if taskIDCol == "" {
		return nil, errors.New("sqlite TaskID column name is empty")
	}
	placeholders := make([]string, 0, len(taskIDs))
	args := make([]any, 0, len(taskIDs))
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	if len(placeholders) == 0 {
		return nil, nil
	}

	builder := strings.Builder{}
	builder.WriteString("SELECT * FROM ")
	builder.WriteString(quoteIdentifier(table))
	builder.WriteString(" WHERE ")
	builder.WriteString(quoteIdentifier(taskIDCol))
	builder.WriteString(" IN (")
	builder.WriteString(strings.Join(placeholders, ","))
	builder.WriteString(")")
	query := builder.String()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "query sqlite capture_results failed")
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "read sqlite columns failed")
	}
	if len(cols) == 0 {
		return nil, nil
	}

	var results []CaptureRecordPayload
	for rows.Next() {
		dest := make([]any, len(cols))
		holder := make([]any, len(cols))
		for i := range dest {
			dest[i] = &holder[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Wrap(err, "scan sqlite capture row failed")
		}
		fields := make(map[string]any, len(cols))
		for i, col := range cols {
			fields[col] = normalizeSQLiteValue(holder[i])
		}
		recordID := strings.TrimSpace(fmt.Sprint(fields["id"]))
		if recordID == "" {
			recordID = strings.TrimSpace(fmt.Sprint(fields["rowid"]))
		}
		results = append(results, CaptureRecordPayload{RecordID: recordID, Fields: fields})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate sqlite capture rows failed")
	}
	return results, nil
}

func normalizeSQLiteValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	default:
		return x
	}
}

func quoteIdentifier(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return `""`
	}
	return `"` + strings.ReplaceAll(trimmed, `"`, `""`) + `"`
}

func fetchFeishuCaptureRecordsByTaskIDs(ctx context.Context, taskIDs []int64) ([]CaptureRecordPayload, error) {
	rawURL := strings.TrimSpace(taskagent.EnvString(taskagent.EnvResultBitableURL, ""))
	if rawURL == "" {
		return nil, nil
	}
	client, err := taskagent.NewFeishuClientFromEnv()
	if err != nil {
		return nil, err
	}
	field := strings.TrimSpace(taskagent.DefaultResultFields().TaskID)
	if field == "" {
		return nil, errors.New("result TaskID field mapping is empty")
	}
	filter := taskagent.NewFeishuFilterInfo("or")
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		filter.Conditions = append(filter.Conditions, taskagent.NewFeishuCondition(field, "is", fmt.Sprintf("%d", id)))
	}
	if len(filter.Conditions) == 0 {
		return nil, nil
	}
	limit := len(filter.Conditions) * 200
	if limit < 200 {
		limit = 200
	}
	if limit > 5000 {
		limit = 5000
	}
	rows, err := client.FetchBitableRows(ctx, rawURL, &taskagent.FeishuTaskQueryOptions{Filter: filter, Limit: limit})
	if err != nil {
		return nil, err
	}
	out := make([]CaptureRecordPayload, 0, len(rows))
	for _, row := range rows {
		out = append(out, CaptureRecordPayload{RecordID: row.RecordID, Fields: row.Fields})
	}
	return out, nil
}

func decodeDramaInfo(raw string) (map[string]any, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		// Backward/compat: some writers may store DramaInfo as a JSON-encoded string
		// (e.g. "\"{\\\"DramaID\\\":...}\""). Try decoding the wrapper string first.
		var wrapped string
		if err2 := json.Unmarshal([]byte(trimmed), &wrapped); err2 != nil {
			return nil, err
		}
		wrapped = strings.TrimSpace(wrapped)
		if wrapped == "" {
			return nil, nil
		}
		if err3 := json.Unmarshal([]byte(wrapped), &decoded); err3 != nil {
			return nil, err
		}
	}
	return normalizeDramaInfoValue(decoded)
}

func normalizeDramaInfoValue(decoded any) (map[string]any, error) {
	switch v := decoded.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		return v, nil
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil, nil
		}
		var nested any
		if err := json.Unmarshal([]byte(trimmed), &nested); err != nil {
			return nil, err
		}
		return normalizeDramaInfoValue(nested)
	case []any:
		// Feishu rich-text fields may be returned as an array of {text,type} objects.
		// Attempt to flatten it to a JSON string, then parse again.
		if text := extractTextArray(v); strings.TrimSpace(text) != "" {
			var nested any
			if err := json.Unmarshal([]byte(strings.TrimSpace(text)), &nested); err != nil {
				return nil, err
			}
			return normalizeDramaInfoValue(nested)
		}
		if len(v) == 1 {
			if only, ok := v[0].(map[string]any); ok {
				return only, nil
			}
		}
		return nil, fmt.Errorf("unexpected drama info json array")
	default:
		return nil, fmt.Errorf("unexpected drama info json type %T", decoded)
	}
}
