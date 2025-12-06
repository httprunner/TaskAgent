package piracy

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/storage"
)

type captureTaskRow struct {
	TaskID      int64
	Params      string
	Status      string
	Datetime    *time.Time
	DatetimeRaw string
}

func fetchBookCaptureTasks(ctx context.Context, bookID, app, scene string, targetDate *time.Time) ([]captureTaskRow, error) {
	trimmedBook := strings.TrimSpace(bookID)
	if trimmedBook == "" {
		return nil, nil
	}
	path, err := storage.ResolveDatabasePath()
	if err != nil {
		return nil, fmt.Errorf("resolve sqlite path failed: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite failed: %w", err)
	}
	defer db.Close()

	fields := feishu.DefaultTaskFields
	columns := []string{fields.TaskID, fields.Params, fields.Status, fields.Datetime}
	quoted := make([]string, 0, len(columns))
	for _, col := range columns {
		quoted = append(quoted, quoteIdentifier(col))
	}

	query := fmt.Sprintf(`SELECT %s, %s, %s, %s FROM capture_tasks WHERE TRIM(%s)=?`,
		quoted[0], quoted[1], quoted[2], quoted[3], quoteIdentifier(fields.BookID))
	args := []any{trimmedBook}
	if trimmedScene := strings.TrimSpace(scene); trimmedScene != "" {
		query += fmt.Sprintf(" AND TRIM(%s)=?", quoteIdentifier(fields.Scene))
		args = append(args, trimmedScene)
	}
	if trimmedApp := strings.TrimSpace(app); trimmedApp != "" {
		query += fmt.Sprintf(" AND TRIM(%s)=?", quoteIdentifier(fields.App))
		args = append(args, trimmedApp)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query capture_tasks failed: %w", err)
	}
	defer rows.Close()

	result := make([]captureTaskRow, 0)
	for rows.Next() {
		var (
			id       int64
			param    sql.NullString
			status   sql.NullString
			datetime sql.NullString
		)
		if err := rows.Scan(&id, &param, &status, &datetime); err != nil {
			return nil, fmt.Errorf("scan capture_tasks row failed: %w", err)
		}
		row := captureTaskRow{
			TaskID:      id,
			Params:      strings.TrimSpace(param.String),
			Status:      strings.TrimSpace(status.String),
			DatetimeRaw: strings.TrimSpace(datetime.String),
		}
		if parsed, ok := parseSQLiteDatetime(row.DatetimeRaw); ok {
			row.Datetime = parsed
		}
		if targetDate != nil && row.Datetime != nil {
			if !sameLocalDay(*row.Datetime, *targetDate) {
				continue
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate capture_tasks rows failed: %w", err)
	}
	return result, nil
}

func sameLocalDay(a, b time.Time) bool {
	la := a.In(time.Local)
	lb := b.In(time.Local)
	return la.Year() == lb.Year() && la.YearDay() == lb.YearDay()
}

func captureTasksAllSuccess(rows []captureTaskRow) bool {
	for _, row := range rows {
		if strings.TrimSpace(row.Status) != feishu.StatusSuccess {
			return false
		}
	}
	return len(rows) > 0
}

func paramsFromCaptureTasks(rows []captureTaskRow) []string {
	seen := make(map[string]struct{}, len(rows))
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		trimmed := strings.TrimSpace(row.Params)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}
