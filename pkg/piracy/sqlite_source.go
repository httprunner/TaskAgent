package piracy

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/storage"
	"github.com/pkg/errors"
)

type sqliteResultSource struct {
	db    *sql.DB
	table string
}

func newSQLiteResultSource() (*sqliteResultSource, error) {
	path, err := storage.ResolveDatabasePath()
	if err != nil {
		return nil, errors.Wrap(err, "piracy: resolve sqlite path failed")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, errors.Wrap(err, "piracy: open sqlite database failed")
	}
	table := strings.TrimSpace(os.Getenv("RESULT_SQLITE_TABLE"))
	if table == "" {
		table = "capture_results"
	}
	return &sqliteResultSource{db: db, table: table}, nil
}

func (s *sqliteResultSource) FetchRows(ctx context.Context, cfg Config, params []string) ([]Row, error) {
	if s == nil || len(params) == 0 {
		return nil, nil
	}
	paramCol := strings.TrimSpace(cfg.ParamsField)
	if paramCol == "" {
		paramCol = feishu.DefaultResultFields.Params
	}
	userIDCol := strings.TrimSpace(cfg.UserIDField)
	if userIDCol == "" {
		userIDCol = feishu.DefaultResultFields.UserID
	}
	userNameCol := strings.TrimSpace(feishu.DefaultResultFields.UserName)
	durationCol := strings.TrimSpace(cfg.DurationField)
	if durationCol == "" {
		durationCol = feishu.DefaultResultFields.ItemDuration
	}

	placeholders := make([]string, len(params))
	args := make([]any, len(params))
	for i, param := range params {
		placeholders[i] = "?"
		args[i] = strings.TrimSpace(param)
	}

	query := fmt.Sprintf(`SELECT id, %s, %s, %s, %s FROM %s WHERE %s IN (%s)`,
		quoteIdentifier(paramCol),
		quoteIdentifier(userIDCol),
		quoteIdentifier(userNameCol),
		quoteIdentifier(durationCol),
		quoteIdentifier(s.table),
		quoteIdentifier(paramCol),
		strings.Join(placeholders, ","),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "piracy: query sqlite capture results failed")
	}
	defer rows.Close()

	resultRows := make([]Row, 0, len(params))
	for rows.Next() {
		var (
			id       int64
			param    sql.NullString
			userID   sql.NullString
			userName sql.NullString
			duration sql.NullFloat64
		)
		if err := rows.Scan(&id, &param, &userID, &userName, &duration); err != nil {
			return nil, errors.Wrap(err, "piracy: scan sqlite capture row failed")
		}
		if strings.TrimSpace(param.String) == "" || strings.TrimSpace(userID.String) == "" || !duration.Valid || duration.Float64 <= 0 {
			continue
		}
		fields := map[string]any{
			cfg.ParamsField:   strings.TrimSpace(param.String),
			cfg.UserIDField:   strings.TrimSpace(userID.String),
			"UserName":        strings.TrimSpace(userName.String),
			cfg.DurationField: duration.Float64,
		}
		resultRows = append(resultRows, Row{
			RecordID: fmt.Sprintf("sqlite-%d", id),
			Fields:   fields,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "piracy: iterate sqlite capture rows failed")
	}
	return resultRows, nil
}

func (s *sqliteResultSource) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
