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

	reportStatusFailed   = -1
	reportStatusPending  = 0
	reportStatusSuccess  = 1
	reportStatusInflight = 2

	// minClaimTTL defines the minimum amount of time a claimed row must stay
	// in the inflight state before it can be re-queued. This keeps the reporter
	// crash-tolerant without letting stuck rows accumulate indefinitely.
	minClaimTTL = 5 * time.Minute
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
	db, err := OpenCaptureResultsDB()
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: open sqlite reporter db failed")
	}
	if err := configureSQLite(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := prepareSchema(db); err != nil {
		db.Close()
		return nil, err
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
	// Ensure timeout is long enough for the entire batch
	// 3 minutes should be enough for 10-30 items even with retries
	timeout := 3 * time.Minute
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	rows, err := r.fetchPending(ctx)
	if err != nil {
		log.Error().Err(err).Msg("result reporter fetch pending rows failed")
		return
	}
	log.Debug().
		Int("pending_rows", len(rows)).
		Dur("timeout", timeout).
		Msg("result reporter flush fetched pending sqlite rows")
	if len(rows) == 0 {
		return
	}

	batchErr := r.dispatchBatch(rows)
	if batchErr == nil {
		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		if err := r.markSuccessBatch(ids); err != nil {
			log.Error().Err(err).Int("count", len(ids)).Msg("result reporter mark success batch failed")
			for _, id := range ids {
				if markErr := r.markSuccess(id); markErr != nil {
					log.Error().Err(markErr).Int64("row_id", id).Msg("result reporter mark success fallback failed")
				}
			}
		} else {
			log.Info().Int("count", len(ids)).Msg("result reporter marked batch reported")
		}
		return
	}

	log.Warn().
		Err(batchErr).
		Int("pending_rows", len(rows)).
		Msg("result reporter batch dispatch failed, falling back to single uploads")

	// Collect successful and failed row IDs for batch updates
	var successIDs []int64
	failedRows := make(map[int64]error)

	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		log.Trace().
			Int64("row_id", row.ID).
			Str("app", trimNull(row.App)).
			Str("item_id", trimNull(row.ItemID)).
			Msg("result reporter dispatching capture row to feishu")
		if err := r.dispatchRow(row); err != nil {
			log.Error().Err(err).Int64("row_id", row.ID).Msg("result reporter dispatch row failed")
			failedRows[row.ID] = err
			continue
		}
		successIDs = append(successIDs, row.ID)
	}

	// Batch update successful rows
	if len(successIDs) > 0 {
		if err := r.markSuccessBatch(successIDs); err != nil {
			log.Error().Err(err).Int("count", len(successIDs)).Msg("result reporter mark success batch failed")
			// Fallback to individual updates
			for _, id := range successIDs {
				if err := r.markSuccess(id); err != nil {
					log.Error().Err(err).Int64("row_id", id).Msg("result reporter mark success fallback failed")
				}
			}
		} else {
			log.Info().Int("count", len(successIDs)).Msg("result reporter marked batch reported")
		}
	}

	// Mark failed rows
	for id, dispatchErr := range failedRows {
		if markErr := r.markFailure(id, dispatchErr); markErr != nil {
			log.Error().Err(markErr).Int64("row_id", id).Msg("result reporter mark failure failed")
		}
	}
}

func (r *resultReporter) fetchPending(ctx context.Context) ([]pendingResultRow, error) {
	// 1) requeue stale inflight rows (e.g. reporter crashed previously)
	if err := r.requeueStaleInflight(ctx); err != nil {
		log.Error().Err(err).Msg("result reporter requeue stale inflight rows failed")
	}

	// 2) claim a batch of pending rows so concurrent reporters don't double-send
	claimedAt := time.Now().UnixMilli()
	rows, err := r.claimPending(ctx, claimedAt)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// claimPending atomically marks up to batchSize pending rows as inflight and
// returns their full payloads. Using a single transaction prevents multiple
// reporters from fetching and dispatching the same rows concurrently.
func (r *resultReporter) claimPending(ctx context.Context, claimedAt int64) ([]pendingResultRow, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "storage: begin tx for claiming rows failed")
	}

	claimStmt := fmt.Sprintf(`UPDATE %s SET %s=?, %s=? WHERE id IN (
		SELECT id FROM %s WHERE %s IN (%d, %d) ORDER BY id ASC LIMIT ?
	);`,
		quoteIdent(r.table), quoteIdent(reportedColumn), quoteIdent(reportedAtColumn),
		quoteIdent(r.table), quoteIdent(reportedColumn), reportStatusPending, reportStatusFailed)
	if _, err := tx.ExecContext(ctx, claimStmt, reportStatusInflight, claimedAt, r.batchSize); err != nil {
		tx.Rollback()
		return nil, pkgerrors.Wrap(err, "storage: claim pending capture rows failed")
	}

	query := fmt.Sprintf(`SELECT id, Datetime, DeviceSerial, App, Scene, Params, ItemID, ItemCaption,
		ItemCDNURL, ItemURL, ItemDuration, UserName, UserID, UserAuthEntity, Tags, TaskID,
		Extra, LikeCount, ViewCount, AnchorPoint, CommentCount, CollectCount, ForwardCount,
		ShareCount, PayMode, Collection, Episode, PublishTime
		FROM %s WHERE %s=? AND %s=? ORDER BY id ASC`, quoteIdent(r.table), quoteIdent(reportedColumn), quoteIdent(reportedAtColumn))
	rows, err := tx.QueryContext(ctx, query, reportStatusInflight, claimedAt)
	if err != nil {
		tx.Rollback()
		return nil, pkgerrors.Wrap(err, "storage: query claimed capture rows failed")
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
			rows.Close()
			tx.Rollback()
			return nil, pkgerrors.Wrap(err, "storage: scan claimed capture row failed")
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		tx.Rollback()
		return nil, pkgerrors.Wrap(err, "storage: iterate claimed capture rows failed")
	}
	if err := tx.Commit(); err != nil {
		return nil, pkgerrors.Wrap(err, "storage: commit claim transaction failed")
	}
	return results, nil
}

// requeueStaleInflight resets rows that were claimed but never finished
// (e.g. reporter crashed) back to pending so they can be retried.
func (r *resultReporter) requeueStaleInflight(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// derive a TTL based on poll interval to keep it responsive but safe
	ttl := 3 * r.pollInterval
	if ttl < minClaimTTL {
		ttl = minClaimTTL
	}
	cutoff := time.Now().Add(-ttl).UnixMilli()
	stmt := fmt.Sprintf(`UPDATE %s SET %s=%d WHERE %s=%d AND %s<?`,
		quoteIdent(r.table), quoteIdent(reportedColumn), reportStatusPending,
		quoteIdent(reportedColumn), reportStatusInflight, quoteIdent(reportedAtColumn))
	if _, err := r.db.ExecContext(ctx, stmt, cutoff); err != nil {
		return pkgerrors.Wrap(err, "storage: requeue stale inflight rows failed")
	}
	return nil
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

func (r *resultReporter) dispatchBatch(rows []pendingResultRow) error {
	if r.storage == nil {
		return pkgerrors.New("storage: feishu reporter nil")
	}
	if len(rows) == 0 {
		return nil
	}
	records := make([]feishu.ResultRecordInput, 0, len(rows))
	for _, row := range rows {
		records = append(records, row.toResultRecord())
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.feishuTimeout)
	defer cancel()
	return r.storage.WriteBatch(ctx, records)
}

func (r *resultReporter) markSuccess(id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stmt := fmt.Sprintf(`UPDATE %s SET %s=1, %s=?, %s=NULL WHERE id=?`,
		quoteIdent(r.table), quoteIdent(reportedColumn), quoteIdent(reportedAtColumn), quoteIdent(reportErrorColumn))
	return pkgerrors.Wrap(execWithRetry(ctx, r.db, stmt, time.Now().UnixMilli(), id), "storage: mark capture row reported")
}

func (r *resultReporter) markSuccessBatch(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build placeholders for IN clause
	placeholders := make([]string, len(ids))
	for i := range ids {
		placeholders[i] = "?"
	}
	placeholderStr := strings.Join(placeholders, ",")

	stmt := fmt.Sprintf(`UPDATE %s SET %s=1, %s=?, %s=NULL WHERE id IN (%s)`,
		quoteIdent(r.table), quoteIdent(reportedColumn), quoteIdent(reportedAtColumn),
		quoteIdent(reportErrorColumn), placeholderStr)

	// Build args: reported_at timestamp + all IDs
	args := make([]any, 0, len(ids)+1)
	args = append(args, time.Now().UnixMilli())
	for _, id := range ids {
		args = append(args, id)
	}

	return pkgerrors.Wrap(execWithRetry(ctx, r.db, stmt, args...), "storage: mark capture rows reported batch")
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
		return 10
	}
	if size, err := strconv.Atoi(raw); err == nil && size > 0 {
		return size
	}
	return 10
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
	// Increased backoff intervals to handle longer lock contention
	backoffs := []time.Duration{500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		_, err := db.ExecContext(ctx, stmt, args...)
		if err == nil {
			return nil
		}
		if !isSQLiteBusy(err) || attempt == maxAttempts-1 {
			return err
		}
		backoff := backoffs[attempt]
		log.Warn().Err(err).
			Int("attempt", attempt+1).
			Dur("backoff", backoff).
			Msg("sqlite busy, retrying after backoff")
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
