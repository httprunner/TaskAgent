package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	pkgerrors "github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type captureResultReader struct {
	db    *sql.DB
	table string
}

func newCaptureResultReader() (*captureResultReader, error) {
	db, err := OpenCaptureResultsDB()
	if err != nil {
		return nil, err
	}
	return &captureResultReader{db: db, table: resolveResultTableName()}, nil
}

func configureSQLiteReader(db *sql.DB) error {
	if db == nil {
		return pkgerrors.New("storage: sqlite reader db nil")
	}
	pragmas := []string{
		"PRAGMA busy_timeout=60000;",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return pkgerrors.Wrapf(err, "storage: execute %s for reader failed", pragma)
		}
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return nil
}

func (r *captureResultReader) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// OpenCaptureResultsDB returns a SQLite connection configured for read operations on capture_results.
func OpenCaptureResultsDB() (*sql.DB, error) {
	dbPath, err := resolveDatabasePath()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: open capture results sqlite failed")
	}
	if err := configureSQLiteReader(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (r *captureResultReader) lookupParamsByItemID(ctx context.Context, itemID string) (string, error) {
	if r == nil || r.db == nil {
		return "", pkgerrors.New("storage: capture result reader nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	query := fmt.Sprintf("SELECT Params FROM %s WHERE ItemID=? ORDER BY Datetime DESC LIMIT 1", quoteIdent(r.table))
	var params sql.NullString
	err := r.db.QueryRowContext(ctx, query, itemID).Scan(&params)
	if err == sql.ErrNoRows {
		log.Warn().Str("item_id", itemID).Msg("storage: no capture result found for item ID")
		return "", nil
	}
	if err != nil {
		log.Error().Err(err).Str("item_id", itemID).Msg("storage: query capture params by item id failed")
		return "", pkgerrors.Wrap(err, "storage: query capture params by item id failed")
	}
	itemParams := strings.TrimSpace(params.String)
	log.Debug().Str("item_id", itemID).Str("params", itemParams).Msg("storage: found capture params for item ID")
	return itemParams, nil
}
