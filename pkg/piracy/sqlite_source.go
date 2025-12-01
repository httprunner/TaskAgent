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
	db, err := storage.OpenCaptureResultsDB()
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
	itemIDCol := strings.TrimSpace(cfg.ItemIDField)
	if itemIDCol == "" {
		itemIDCol = feishu.DefaultResultFields.ItemID
	}

	placeholders := make([]string, len(params))
	args := make([]any, len(params))
	for i, param := range params {
		placeholders[i] = "?"
		args[i] = strings.TrimSpace(param)
	}

	query := fmt.Sprintf(`SELECT id, %s, %s, %s, %s, %s FROM %s WHERE %s IN (%s)`,
		quoteIdentifier(paramCol),
		quoteIdentifier(userIDCol),
		quoteIdentifier(userNameCol),
		quoteIdentifier(durationCol),
		quoteIdentifier(itemIDCol),
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
			itemID   sql.NullString
		)
		if err := rows.Scan(&id, &param, &userID, &userName, &duration, &itemID); err != nil {
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
		if trimmed := strings.TrimSpace(itemID.String); trimmed != "" {
			fields[cfg.ItemIDField] = trimmed
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

// FetchVideoDetails retrieves video details (ItemID, Tags, AnchorPoint) for a specific params + userID combination.
func (s *sqliteResultSource) FetchVideoDetails(ctx context.Context, params, userID string) ([]VideoDetail, error) {
	if s == nil {
		return nil, nil
	}
	params = strings.TrimSpace(params)
	userID = strings.TrimSpace(userID)
	if params == "" || userID == "" {
		return nil, nil
	}

	fields := feishu.DefaultResultFields
	query := fmt.Sprintf(`SELECT %s, %s, %s FROM %s WHERE %s = ? AND %s = ?`,
		quoteIdentifier(fields.ItemID),
		quoteIdentifier(fields.Tags),
		quoteIdentifier(fields.AnchorPoint),
		quoteIdentifier(s.table),
		quoteIdentifier(fields.Params),
		quoteIdentifier(fields.UserID),
	)

	rows, err := s.db.QueryContext(ctx, query, params, userID)
	if err != nil {
		return nil, errors.Wrap(err, "piracy: query sqlite video details failed")
	}
	defer rows.Close()

	videos := make([]VideoDetail, 0)
	seenItemIDs := make(map[string]struct{})
	for rows.Next() {
		var (
			itemID      sql.NullString
			tags        sql.NullString
			anchorPoint sql.NullString
		)
		if err := rows.Scan(&itemID, &tags, &anchorPoint); err != nil {
			return nil, errors.Wrap(err, "piracy: scan sqlite video detail row failed")
		}
		trimmedItemID := strings.TrimSpace(itemID.String)
		if trimmedItemID == "" {
			continue
		}
		// Deduplicate by ItemID
		if _, seen := seenItemIDs[trimmedItemID]; seen {
			continue
		}
		seenItemIDs[trimmedItemID] = struct{}{}

		videos = append(videos, VideoDetail{
			ItemID:      trimmedItemID,
			Tags:        strings.TrimSpace(tags.String),
			AnchorPoint: strings.TrimSpace(anchorPoint.String),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "piracy: iterate sqlite video detail rows failed")
	}
	return videos, nil
}

func (s *sqliteResultSource) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
