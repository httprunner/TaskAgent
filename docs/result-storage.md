# Search Result Storage

This document describes the steady-state design for storing capture results, uploading them to Feishu, and feeding downstream detection/reporting workflows.

## Architecture

```
Tracking runner → storage.Manager → [JSONL sink]* + SQLite sink
                                      ↓
                           capture_results (reported=0)
                                      ↓ async
                              resultReporter goroutine
                                      ↓           ↘ (FEISHU_REPORT_RPS limiter)
                             Feishu ResultStorage   ← RESULT_STORAGE_ENABLE_FEISHU
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

## Async Feishu Reporter (`internal/storage/sqlite_reporter.go`)

- Starts automatically when Feishu reporting is enabled (`RESULT_STORAGE_ENABLE_FEISHU=1` or `tracking.WithBitableStorage(true)`).
- Scans `reported IN (0, -1)` on each tick and uploads up to `RESULT_REPORT_BATCH` rows (default 30).
- Poll interval defaults to 5 s (`RESULT_REPORT_POLL_INTERVAL`).
- Each Feishu call has its own timeout (`RESULT_REPORT_HTTP_TIMEOUT`, default 30 s) so a slow API does not stall the entire cycle.
- Uploads flow through the `FEISHU_REPORT_RPS` limiter (default 1 row/sec) to avoid 99991400 responses; raise this value only when Feishu确认更高频控。
- SQLite updates (`reported` flips, error text) use independent 5 s contexts to ensure bookkeeping always completes。

#### Failure handling

- Success → `reported=1`, `report_error=NULL`, `reported_at` stamped.
- Failure → `reported=-1`, `report_error` contains the Feishu error, record retried on the next tick.
- Manual retry → `UPDATE capture_results SET reported=0 WHERE reported=-1;`

### 常用排查 SQL
```sql
-- 查看最新失败的记录及错误原因
SELECT TaskID, Params, report_error, reported_at
FROM capture_results
WHERE reported = -1
ORDER BY reported_at DESC
LIMIT 20;

-- 强制重试所有失败记录
UPDATE capture_results SET reported = 0 WHERE reported = -1;
```
调整 `RESULT_REPORT_BATCH`/`RESULT_REPORT_POLL_INTERVAL` 前，先观察 `report_error` 中的错误码，频控异常通常为 `99991400`。

## Webhook Helpers & Downstream Workflows

- 下游业务（例如 fox search agent）可以基于本仓库提供的结果表 / SQLite 数据实现各类检测与统计工作流。
- webhook worker 统一从任务表/结果表聚合 payload，并交由上层流程触发回调。
- 剧单元数据仍然来自 Feishu（并可通过 `storage.MirrorDramaRowsIfNeeded` 镜像到本地 SQLite），以保证比值统计和 webhook payload 一致性。

## Configuration Summary

| Variable | Default | Description |
| --- | --- | --- |
| `RESULT_STORAGE_ENABLE_FEISHU` | unset | When truthy, starts the async reporter and enables Feishu uploads. |
| `TRACKING_STORAGE_DB_PATH` | `$HOME/.eval/records.sqlite` | Location of the shared SQLite database. |
| `RESULT_SQLITE_TABLE` | `capture_results` | Table storing capture rows + `reported` fields. |
| `RESULT_REPORT_POLL_INTERVAL` | `5s` | Reporter scan interval (`time.ParseDuration` format). |
| `RESULT_REPORT_BATCH` | `30` | Maximum rows processed per tick. |
| `RESULT_REPORT_HTTP_TIMEOUT` | `30s` | Per-row Feishu upload timeout. |
| `FEISHU_REPORT_RPS` | `1` | Global Feishu limiter inside `internal/feishusdk/storage.go`. |

## Operational Tips

1. Query `reported=-1` regularly to watch for stuck rows (示例 SQL 见上文)。
2. 扩容时同步调整 `RESULT_REPORT_*` 与 `FEISHU_REPORT_RPS`，否则 reporter 可能在速率限制器前排队。
3. 当 result 表暂不可用时，可仅依赖 SQLite（不要设置 `RESULT_STORAGE_ENABLE_FEISHU`），稍后再批量重投。
4. Piracy 工作流（Group/Webhook）优先查询 SQLite，因此保持 `TRACKING_STORAGE_DB_PATH` 持久化，必要时用 `sqlite3` 导出 CSV 供调试。
5. Tests: `go test ./internal/storage` 验证 pipeline；Webhook 相关逻辑可通过 `go test ./pkg/webhook` 做基础覆盖。
