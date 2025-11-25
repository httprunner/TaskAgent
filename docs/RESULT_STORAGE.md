# Search Result Storage

This document describes the steady-state design for storing capture results, uploading them to Feishu, and feeding piracy detection.

## Architecture

```
Tracking runner → storage.Manager → [JSONL sink]* + SQLite sink
                                      ↓
                           capture_results (reported=0)
                                      ↓ async
                              resultReporter goroutine
                                      ↓
                             Feishu ResultStorage
```

\* JSONL output is optional and controlled via `TRACKING_STORAGE_DISABLE_JSONL`. Every capture always lands in SQLite.

### SQLite schema (`capture_results`)

- Mirrors the Feishu结果表 columns (Scene, Params, DeviceSerial, App, ItemID, CDN URL, Share URL, duration, user fields, metrics, publish time, Extra JSON).
- Bookkeeping columns:
  - `reported` — 0 pending, 1 success, -1 failed.
  - `reported_at` — unix ms of the last upload attempt.
  - `report_error` — last Feishu error message (truncated to 512 chars).
- Table name is configurable via `RESULT_SQLITE_TABLE`.
- Existing databases auto-migrate via `ALTER TABLE` when TaskAgent starts.

### storage.Manager

- Always enables SQLite sink. Feishu uploads are now offloaded to the reporter and no longer run inline with the capture loop.
- `ResultRecord.DBRecord` carries the rich set of columns so SQLite rows match Feishu payloads one-to-one.

## Async Feishu Reporter (`pkg/storage/sqlite_reporter.go`)

- Starts automatically when Feishu reporting is enabled (`RESULT_STORAGE_ENABLE_FEISHU=1` or `tracking.WithBitableStorage(true)`).
- Scans `reported IN (0, -1)` on each tick and uploads up to `RESULT_REPORT_BATCH` rows (default 30).
- Poll interval defaults to 5 s (`RESULT_REPORT_POLL_INTERVAL`).
- Each Feishu call has its own timeout (`RESULT_REPORT_HTTP_TIMEOUT`, default 30 s) so a slow API does not stall the entire cycle.
- SQLite updates (`reported` flips, error text) use independent 5 s contexts to ensure bookkeeping always completes.

#### Failure handling

- Success → `reported=1`, `report_error=NULL`, `reported_at` stamped.
- Failure → `reported=-1`, `report_error` contains the Feishu error, record retried on the next tick.
- Manual retry → `UPDATE capture_results SET reported=0 WHERE reported=-1;`

## Piracy Detection

- `pkg/piracy/sqlite_source.go` fetches result rows for the requested Params directly from SQLite.
- `Reporter.detectWithFiltersInternal` prefers the SQLite source; if no rows are available it falls back to `feishu.FetchBitableRows`.
- Drama metadata still comes from Feishu (and is mirrored by `storage.MirrorDramaRowsIfNeeded`), so ratio calculations remain consistent.

## Configuration Summary

| Variable | Default | Description |
| --- | --- | --- |
| `RESULT_STORAGE_ENABLE_FEISHU` | unset | When truthy, starts the async reporter and enables Feishu uploads. |
| `TRACKING_STORAGE_DB_PATH` | `$HOME/.eval/records.sqlite` | Location of the shared SQLite database. |
| `RESULT_SQLITE_TABLE` | `capture_results` | Table storing capture rows + `reported` fields. |
| `RESULT_REPORT_POLL_INTERVAL` | `5s` | Reporter scan interval (`time.ParseDuration` format). |
| `RESULT_REPORT_BATCH` | `30` | Maximum rows processed per tick. |
| `RESULT_REPORT_HTTP_TIMEOUT` | `30s` | Per-row Feishu upload timeout. |
| `FEISHU_REPORT_RPS` | `1` | Global Feishu limiter inside `pkg/feishu/storage.go`. |

## Operational Tips

1. Query `reported=-1` regularly to watch for stuck rows; inspect `report_error` for the root cause (timeouts, `99991400`, etc.).
2. Tune `RESULT_REPORT_*` and `FEISHU_REPORT_RPS` together when scaling device counts.
3. Piracy workflows should pass the Params of interest so the SQLite source can filter efficiently.
4. Tests: `go test ./pkg/storage` validates the storage pipeline; `go test ./pkg/piracy` exercises detection logic (requires Feishu tables under the hard row cap).
