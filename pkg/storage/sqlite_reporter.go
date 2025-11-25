package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	pkgerrors "github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	envResultReportPollInterval = "RESULT_REPORT_POLL_INTERVAL"
	envResultReportBatchSize    = "RESULT_REPORT_BATCH"
	envResultReportHTTPTimeout  = "RESULT_REPORT_HTTP_TIMEOUT"
)

type resultReporter struct {
	storage       *feishu.ResultStorage
	db            *sql.DB
	table         string
	pollInterval  time.Duration
	batchSize     int
	feishuTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newResultReporter(storage *feishu.ResultStorage) (*resultReporter, error) {
	if storage == nil {
		return nil, nil
	}
	dbPath, err := resolveDatabasePath()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: open sqlite reporter db failed")
	}
	reporter := &resultReporter{
		storage:       storage,
		db:            db,
		table:         resolveResultTableName(),
		pollInterval:  parsePollInterval(),
		batchSize:     parseBatchSize(),
		feishuTimeout: parseReportTimeout(),
	}
	reporter.ctx, reporter.cancel = context.WithCancel(context.Background())
	reporter.wg.Add(1)
	go reporter.loop()
	return reporter, nil
}

func (r *resultReporter) loop() {
	defer r.wg.Done()
	r.flushOnce()
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.flushOnce()
		}
	}
}

func (r *resultReporter) flushOnce() {
	timeout := r.feishuTimeout + 5*time.Second
	if timeout < 30*time.Second {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	rows, err := r.fetchPending(ctx)
	if err != nil {
		log.Error().Err(err).Msg("result reporter fetch pending rows failed")
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		if err := r.dispatchRow(row); err != nil {
			log.Error().Err(err).Int64("row_id", row.ID).Msg("result reporter dispatch row failed")
			if markErr := r.markFailure(row.ID, err); markErr != nil {
				log.Error().Err(markErr).Int64("row_id", row.ID).Msg("result reporter mark failure failed")
			}
			continue
		}
		if err := r.markSuccess(row.ID); err != nil {
			log.Error().Err(err).Int64("row_id", row.ID).Msg("result reporter mark success failed")
			continue
		}
	}
}

func (r *resultReporter) fetchPending(ctx context.Context) ([]pendingResultRow, error) {
	query := fmt.Sprintf(`SELECT id, Datetime, DeviceSerial, App, Scene, Params, ItemID, ItemCaption,
		ItemCDNURL, ItemURL, ItemDuration, UserName, UserID, UserAuthEntity, Tags, TaskID,
		Extra, LikeCount, ViewCount, AnchorPoint, CommentCount, CollectCount, ForwardCount,
		ShareCount, PayMode, Collection, Episode, PublishTime
		FROM %s WHERE %s IN (0, -1) ORDER BY id ASC LIMIT ?`, quoteIdent(r.table), quoteIdent(reportedColumn))
	rows, err := r.db.QueryContext(ctx, query, r.batchSize)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: query pending capture rows failed")
	}
	defer rows.Close()

	results := make([]pendingResultRow, 0, r.batchSize)
	for rows.Next() {
		var row pendingResultRow
		if err := rows.Scan(
			&row.ID,
			&row.Datetime,
			&row.DeviceSerial,
			&row.App,
			&row.Scene,
			&row.Params,
			&row.ItemID,
			&row.ItemCaption,
			&row.ItemCDNURL,
			&row.ItemURL,
			&row.ItemDuration,
			&row.UserName,
			&row.UserID,
			&row.UserAuthEntity,
			&row.Tags,
			&row.TaskID,
			&row.Extra,
			&row.LikeCount,
			&row.ViewCount,
			&row.AnchorPoint,
			&row.CommentCount,
			&row.CollectCount,
			&row.ForwardCount,
			&row.ShareCount,
			&row.PayMode,
			&row.Collection,
			&row.Episode,
			&row.PublishTime,
		); err != nil {
			return nil, pkgerrors.Wrap(err, "storage: scan pending capture row failed")
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, pkgerrors.Wrap(err, "storage: iterate pending capture rows failed")
	}
	return results, nil
}

func (r *resultReporter) dispatchRow(row pendingResultRow) error {
	if r.storage == nil {
		return pkgerrors.New("storage: feishu reporter nil")
	}
	payload := row.toResultRecord()
	ctx, cancel := context.WithTimeout(r.ctx, r.feishuTimeout)
	defer cancel()
	return r.storage.Write(ctx, payload)
}

func (r *resultReporter) markSuccess(id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stmt := fmt.Sprintf(`UPDATE %s SET %s=1, %s=?, %s=NULL WHERE id=?`,
		quoteIdent(r.table), quoteIdent(reportedColumn), quoteIdent(reportedAtColumn), quoteIdent(reportErrorColumn))
	return pkgerrors.Wrap(execWithRetry(ctx, r.db, stmt, time.Now().UnixMilli(), id), "storage: mark capture row reported")
}

func (r *resultReporter) markFailure(id int64, err error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stmt := fmt.Sprintf(`UPDATE %s SET %s=-1, %s=?, %s=? WHERE id=?`,
		quoteIdent(r.table), quoteIdent(reportedColumn), quoteIdent(reportedAtColumn), quoteIdent(reportErrorColumn))
	return pkgerrors.Wrap(execWithRetry(ctx, r.db, stmt, time.Now().UnixMilli(), truncateError(err), id), "storage: mark capture row failed")
}

func (r *resultReporter) Close() error {
	if r == nil {
		return nil
	}
	r.cancel()
	r.wg.Wait()
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

type pendingResultRow struct {
	ID             int64
	Datetime       sql.NullInt64
	DeviceSerial   sql.NullString
	App            sql.NullString
	Scene          sql.NullString
	Params         sql.NullString
	ItemID         sql.NullString
	ItemCaption    sql.NullString
	ItemCDNURL     sql.NullString
	ItemURL        sql.NullString
	ItemDuration   sql.NullFloat64
	UserName       sql.NullString
	UserID         sql.NullString
	UserAuthEntity sql.NullString
	Tags           sql.NullString
	TaskID         sql.NullInt64
	Extra          sql.NullString
	LikeCount      sql.NullInt64
	ViewCount      sql.NullInt64
	AnchorPoint    sql.NullString
	CommentCount   sql.NullInt64
	CollectCount   sql.NullInt64
	ForwardCount   sql.NullInt64
	ShareCount     sql.NullInt64
	PayMode        sql.NullString
	Collection     sql.NullString
	Episode        sql.NullString
	PublishTime    sql.NullString
}

func (r pendingResultRow) toResultRecord() feishu.ResultRecordInput {
	var durationPtr *float64
	if r.ItemDuration.Valid {
		val := r.ItemDuration.Float64
		durationPtr = &val
	}
	var dtRaw string
	if r.Datetime.Valid {
		dtRaw = strconv.FormatInt(r.Datetime.Int64, 10)
	}
	return feishu.ResultRecordInput{
		DatetimeRaw:         dtRaw,
		DeviceSerial:        trimNull(r.DeviceSerial),
		App:                 trimNull(r.App),
		Scene:               trimNull(r.Scene),
		Params:              trimNull(r.Params),
		ItemID:              trimNull(r.ItemID),
		ItemCaption:         trimNull(r.ItemCaption),
		ItemCDNURL:          trimNull(r.ItemCDNURL),
		ItemURL:             trimNull(r.ItemURL),
		ItemDurationSeconds: durationPtr,
		UserName:            trimNull(r.UserName),
		UserID:              trimNull(r.UserID),
		UserAuthEntity:      trimNull(r.UserAuthEntity),
		Tags:                trimNull(r.Tags),
		TaskID:              r.TaskID.Int64,
		Extra:               decodeExtraPayload(trimNull(r.Extra)),
		LikeCount:           r.LikeCount.Int64,
		ViewCount:           int(r.ViewCount.Int64),
		AnchorPoint:         trimNull(r.AnchorPoint),
		CommentCount:        r.CommentCount.Int64,
		CollectCount:        r.CollectCount.Int64,
		ForwardCount:        r.ForwardCount.Int64,
		ShareCount:          r.ShareCount.Int64,
		PayMode:             trimNull(r.PayMode),
		Collection:          trimNull(r.Collection),
		Episode:             trimNull(r.Episode),
		PublishTime:         trimNull(r.PublishTime),
	}
}

func trimNull(val sql.NullString) string {
	return strings.TrimSpace(val.String)
}

func decodeExtraPayload(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var payload any
	if err := json.Unmarshal([]byte(raw), &payload); err == nil {
		return payload
	}
	return raw
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) <= 512 {
		return msg
	}
	return msg[:512]
}

func parsePollInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envResultReportPollInterval))
	if raw == "" {
		return 5 * time.Second
	}
	if dur, err := time.ParseDuration(raw); err == nil && dur > 0 {
		return dur
	}
	return 5 * time.Second
}

func parseBatchSize() int {
	raw := strings.TrimSpace(os.Getenv(envResultReportBatchSize))
	if raw == "" {
		return 30
	}
	if size, err := strconv.Atoi(raw); err == nil && size > 0 {
		return size
	}
	return 30
}

func parseReportTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envResultReportHTTPTimeout))
	if raw == "" {
		return 30 * time.Second
	}
	if dur, err := time.ParseDuration(raw); err == nil && dur > 0 {
		return dur
	}
	return 30 * time.Second
}

func execWithRetry(ctx context.Context, db *sql.DB, stmt string, args ...any) error {
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		_, err := db.ExecContext(ctx, stmt, args...)
		if err == nil {
			return nil
		}
		if !isSQLiteBusy(err) || attempt == maxAttempts-1 {
			return err
		}
		backoff := time.Duration(attempt+1) * 200 * time.Millisecond
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "sqlite_busy")
}
