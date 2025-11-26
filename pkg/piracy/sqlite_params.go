package piracy

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/storage"
	"github.com/pkg/errors"
)

// FetchSQLiteParams returns the distinct Params values stored in the sqlite
// capture result table. The params column name follows Config resolution
// (RESULT_FIELD_PARAMS override or default Feishu schema). Results are sorted
// alphabetically for deterministic processing. If dbPath is empty, the default
// storage.ResolveDatabasePath is used.
func FetchSQLiteParams(ctx context.Context, cfg Config, dbPath string) ([]string, error) {
	cfg.ApplyDefaults()

	path := strings.TrimSpace(dbPath)
	if path == "" {
		var err error
		path, err = storage.ResolveDatabasePath()
		if err != nil {
			return nil, errors.Wrap(err, "piracy: resolve sqlite path failed")
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, errors.Wrap(err, "piracy: open sqlite database failed")
	}
	defer db.Close()

	table := strings.TrimSpace(os.Getenv("RESULT_SQLITE_TABLE"))
	if table == "" {
		table = "capture_results"
	}
	column := strings.TrimSpace(cfg.ParamsField)
	if column == "" {
		column = "Params"
	}

	query := fmt.Sprintf(`SELECT DISTINCT %s FROM %s WHERE TRIM(%s) != ''`,
		quoteIdentifier(column), quoteIdentifier(table), quoteIdentifier(column))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "piracy: query distinct params from sqlite failed")
	}
	defer rows.Close()

	params := make([]string, 0)
	for rows.Next() {
		var val sql.NullString
		if err := rows.Scan(&val); err != nil {
			return nil, errors.Wrap(err, "piracy: scan params from sqlite failed")
		}
		if trimmed := strings.TrimSpace(val.String); trimmed != "" {
			params = append(params, trimmed)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "piracy: iterate params from sqlite failed")
	}

	sort.Strings(params)
	return params, nil
}
