package piracy

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/storage"
	"github.com/rs/zerolog/log"
)

// ReplayConfig describes the inputs required to replay piracy detection for a cached task.
type ReplayConfig struct {
	TaskID int64
	DBPath string
	App    string
}

// ReplayTask re-runs piracy detection for a single task stored inside the local sqlite cache
// and recreates the corresponding child tasks inside the Feishu task table.
func ReplayTask(ctx context.Context, cfg ReplayConfig) error {
	if cfg.TaskID <= 0 {
		return fmt.Errorf("task id must be greater than 0")
	}

	dbPath := strings.TrimSpace(cfg.DBPath)
	if dbPath == "" {
		resolved, err := storage.ResolveDatabasePath()
		if err != nil {
			return fmt.Errorf("resolve sqlite path failed: %w", err)
		}
		dbPath = resolved
	}

	row, err := loadReplayTask(ctx, dbPath, cfg.TaskID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(row.Params) == "" {
		return fmt.Errorf("task %d has empty params", row.TaskID)
	}

	reporter := NewReporter()
	if reporter == nil || !reporter.IsConfigured() {
		return fmt.Errorf("piracy reporter is not fully configured; ensure RESULT_BITABLE_URL/DRAMA_BITABLE_URL/TASK_BITABLE_URL are set")
	}

	log.Info().
		Str("db_path", dbPath).
		Int64("task_id", row.TaskID).
		Str("params", row.Params).
		Msg("replaying piracy workflow from sqlite")

	details, err := reporter.DetectMatchesWithDetails(ctx, []string{row.Params})
	if err != nil {
		return fmt.Errorf("detect matches failed: %w", err)
	}
	if len(details) == 0 {
		log.Warn().Int64("task_id", row.TaskID).Msg("no piracy matches detected; nothing to report")
		return nil
	}

	appName := strings.TrimSpace(cfg.App)
	if appName == "" {
		appName = strings.TrimSpace(row.App)
	}
	if appName == "" {
		return fmt.Errorf("task %d missing app; pass --app or ensure App column is populated", row.TaskID)
	}

	if err := reporter.ReportMatchesWithChildTasks(ctx, appName, row.TaskID, row.Datetime, row.DatetimeRaw, details); err != nil {
		return fmt.Errorf("create child tasks failed: %w", err)
	}

	log.Info().
		Int("match_groups", len(details)).
		Int64("task_id", row.TaskID).
		Msg("piracy child tasks replay completed")
	return nil
}

type replayTaskRow struct {
	TaskID      int64
	Params      string
	App         string
	Scene       string
	Datetime    *time.Time
	DatetimeRaw string
}

func loadReplayTask(ctx context.Context, path string, taskID int64) (*replayTaskRow, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sqlite path is empty")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db failed: %w", err)
	}
	defer db.Close()

	query := `SELECT TaskID, Params, App, Scene, Datetime FROM capture_tasks WHERE TaskID=?`
	row := replayTaskRow{}
	var rawDatetime sql.NullString
	if err := db.QueryRowContext(ctx, query, taskID).Scan(&row.TaskID, &row.Params, &row.App, &row.Scene, &rawDatetime); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("task %d not found in capture_tasks", taskID)
		}
		return nil, fmt.Errorf("query sqlite failed: %w", err)
	}
	if rawDatetime.Valid {
		row.DatetimeRaw = strings.TrimSpace(rawDatetime.String)
		if ts, ok := parseSQLiteDatetime(row.DatetimeRaw); ok {
			row.Datetime = ts
		}
	}
	return &row, nil
}

func parseSQLiteDatetime(raw string) (*time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, false
	}
	if num, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		var ts time.Time
		switch {
		case len(trimmed) >= 13 || num > 1e12:
			ts = time.UnixMilli(num)
		default:
			ts = time.Unix(num, 0)
		}
		val := ts.In(time.Local)
		return &val, true
	}
	layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"}
	for _, layout := range layouts {
		if parsed, err := time.ParseInLocation(layout, trimmed, time.Local); err == nil {
			return &parsed, true
		}
	}
	log.Warn().Str("datetime", raw).Msg("unable to parse sqlite datetime; using raw string only")
	return nil, false
}
