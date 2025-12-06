package piracy

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	pool "github.com/httprunner/TaskAgent"
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
	cfg.ApplyDefaults()
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
	appCol := strings.TrimSpace(cfg.ResultAppField)
	if appCol == "" {
		appCol = feishu.DefaultResultFields.App
	}

	taskMappings, err := s.fetchTaskMappings(ctx, params, cfg)
	if err != nil {
		return nil, err
	}

	placeholders := make([]string, len(params))
	args := make([]any, len(params))
	for i, param := range params {
		placeholders[i] = "?"
		args[i] = strings.TrimSpace(param)
	}

	query := fmt.Sprintf(`SELECT id, %s, %s, %s, %s, %s, %s FROM %s WHERE %s IN (%s)`,
		quoteIdentifier(paramCol),
		quoteIdentifier(userIDCol),
		quoteIdentifier(userNameCol),
		quoteIdentifier(durationCol),
		quoteIdentifier(itemIDCol),
		quoteIdentifier(appCol),
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
			app      sql.NullString
		)
		if err := rows.Scan(&id, &param, &userID, &userName, &duration, &itemID, &app); err != nil {
			return nil, errors.Wrap(err, "piracy: scan sqlite capture row failed")
		}
		if strings.TrimSpace(param.String) == "" || strings.TrimSpace(userID.String) == "" || !duration.Valid || duration.Float64 <= 0 {
			continue
		}
		paramVal := strings.TrimSpace(param.String)
		appVal := strings.TrimSpace(app.String)
		info, ok := lookupParamTaskInfo(taskMappings, paramVal, appVal)
		bookID := ""
		if ok {
			bookID = strings.TrimSpace(info.BookID)
			if appVal == "" {
				appVal = strings.TrimSpace(info.App)
			}
		}
		if bookID == "" || appVal == "" {
			continue
		}
		fields := map[string]any{
			cfg.ParamsField:   paramVal,
			cfg.UserIDField:   strings.TrimSpace(userID.String),
			"UserName":        strings.TrimSpace(userName.String),
			cfg.DurationField: duration.Float64,
		}
		if trimmed := strings.TrimSpace(cfg.ResultAppField); trimmed != "" {
			fields[trimmed] = appVal
		}
		if trimmed := strings.TrimSpace(cfg.TaskBookIDField); trimmed != "" {
			fields[trimmed] = bookID
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

func (s *sqliteResultSource) fetchTaskMappings(ctx context.Context, params []string, cfg Config) (map[paramTaskKey]paramTaskInfo, error) {
	if s == nil || len(params) == 0 {
		return nil, nil
	}
	paramCol := strings.TrimSpace(cfg.TaskParamsField)
	bookIDCol := strings.TrimSpace(cfg.TaskBookIDField)
	appCol := strings.TrimSpace(cfg.TaskAppField)
	statusCol := strings.TrimSpace(cfg.TaskStatusField)
	sceneCol := strings.TrimSpace(cfg.TaskSceneField)
	if paramCol == "" || bookIDCol == "" || statusCol == "" || sceneCol == "" {
		return nil, errors.New("piracy: task field mappings are incomplete for sqlite")
	}
	placeholders := make([]string, len(params))
	args := make([]any, 0, len(params)+2)
	for i, param := range params {
		placeholders[i] = "?"
		args = append(args, strings.TrimSpace(param))
	}
	args = append(args, feishu.StatusSuccess, pool.SceneGeneralSearch)
	query := fmt.Sprintf(`SELECT %s, %s, %s FROM %s WHERE %s IN (%s) AND TRIM(%s)=? AND TRIM(%s)=?`,
		quoteIdentifier(paramCol),
		quoteIdentifier(bookIDCol),
		quoteIdentifier(appCol),
		quoteIdentifier("capture_tasks"),
		quoteIdentifier(paramCol),
		strings.Join(placeholders, ","),
		quoteIdentifier(statusCol),
		quoteIdentifier(sceneCol),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "piracy: query capture_tasks for mappings failed")
	}
	defer rows.Close()
	mapping := make(map[paramTaskKey]paramTaskInfo)
	for rows.Next() {
		var (
			param  sql.NullString
			bookID sql.NullString
			app    sql.NullString
		)
		if err := rows.Scan(&param, &bookID, &app); err != nil {
			return nil, errors.Wrap(err, "piracy: scan capture_tasks mapping row failed")
		}
		p := strings.TrimSpace(param.String)
		b := strings.TrimSpace(bookID.String)
		if p == "" || b == "" {
			continue
		}
		a := strings.TrimSpace(app.String)
		info := paramTaskInfo{BookID: b, App: a, Param: p}
		key := makeParamTaskKey(p, a)
		if _, exists := mapping[key]; !exists {
			mapping[key] = info
		}
		emptyKey := makeParamTaskKey(p, "")
		if _, exists := mapping[emptyKey]; !exists {
			mapping[emptyKey] = info
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "piracy: iterate capture_tasks mappings failed")
	}
	return mapping, nil
}

// FetchVideoDetails retrieves video details (ItemID, Tags, AnchorPoint) for a specific params + userID (+ app) combination.
func (s *sqliteResultSource) FetchVideoDetails(ctx context.Context, params, userID, app string) ([]VideoDetail, error) {
	if s == nil {
		return nil, nil
	}
	params = strings.TrimSpace(params)
	userID = strings.TrimSpace(userID)
	if params == "" || userID == "" {
		return nil, nil
	}

	fields := feishu.DefaultResultFields
	clauses := []string{
		fmt.Sprintf("%s = ?", quoteIdentifier(fields.Params)),
		fmt.Sprintf("%s = ?", quoteIdentifier(fields.UserID)),
	}
	args := []any{params, userID}
	if trimmedApp := strings.TrimSpace(app); trimmedApp != "" {
		clauses = append(clauses, fmt.Sprintf("%s = ?", quoteIdentifier(fields.App)))
		args = append(args, trimmedApp)
	}
	query := fmt.Sprintf(`SELECT %s, %s, %s FROM %s WHERE %s`,
		quoteIdentifier(fields.ItemID),
		quoteIdentifier(fields.Tags),
		quoteIdentifier(fields.AnchorPoint),
		quoteIdentifier(s.table),
		strings.Join(clauses, " AND "),
	)

	rows, err := s.db.QueryContext(ctx, query, args...)
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
