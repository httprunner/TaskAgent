package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
)

func fetchCaptureRecordsByTaskIDs(ctx context.Context, taskIDs []int64) ([]CaptureRecordPayload, error) {
	if len(taskIDs) == 0 {
		return nil, nil
	}
	records, err := fetchSQLiteCaptureRecordsByTaskIDs(ctx, taskIDs)
	if err == nil && len(records) > 0 {
		return records, nil
	}
	if err != nil {
		feishuRecords, feishuErr := fetchFeishuCaptureRecordsByTaskIDs(ctx, taskIDs)
		if feishuErr != nil {
			return nil, errors.Wrapf(feishuErr, "sqlite error: %v", err)
		}
		return feishuRecords, nil
	}
	return fetchFeishuCaptureRecordsByTaskIDs(ctx, taskIDs)
}

func fetchCaptureRecordsByQuery(ctx context.Context, query recordQuery) ([]CaptureRecordPayload, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	fields := loadSummaryFieldConfig()

	var sqliteErr error
	sqliteDS, err := newSummaryDataSource(SourceSQLite, fields, Options{})
	if err == nil && sqliteDS != nil {
		defer sqliteDS.Close()
		recs, err := sqliteDS.FetchRecords(ctx, query)
		if err == nil && len(recs) > 0 {
			return recs, nil
		}
		if err != nil {
			sqliteErr = err
		}
	}

	feishuDS, ferr := newSummaryDataSource(SourceFeishu, fields, Options{})
	if ferr != nil {
		if sqliteErr != nil {
			return nil, errors.Wrapf(ferr, "sqlite error: %v", sqliteErr)
		}
		return nil, ferr
	}
	defer feishuDS.Close()
	recs, ferr2 := feishuDS.FetchRecords(ctx, query)
	if ferr2 != nil {
		if sqliteErr != nil {
			return nil, errors.Wrapf(ferr2, "sqlite error: %v", sqliteErr)
		}
		return nil, ferr2
	}
	return recs, nil
}

func fetchSQLiteCaptureRecordsByTaskIDs(ctx context.Context, taskIDs []int64) ([]CaptureRecordPayload, error) {
	db, err := taskagent.OpenCaptureResultsDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	table := strings.TrimSpace(taskagent.EnvString("RESULT_SQLITE_TABLE", "capture_results"))
	if table == "" {
		return nil, errors.New("sqlite result table name is empty")
	}
	taskIDCol := strings.TrimSpace(taskagent.DefaultResultFields().TaskID)
	if taskIDCol == "" {
		return nil, errors.New("result TaskID field mapping is empty")
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

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
		quoteIdentifier(table),
		quoteIdentifier(taskIDCol),
		strings.Join(placeholders, ","),
	)
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
	var fields map[string]any
	if err := json.Unmarshal([]byte(trimmed), &fields); err != nil {
		return nil, err
	}
	return fields, nil
}
