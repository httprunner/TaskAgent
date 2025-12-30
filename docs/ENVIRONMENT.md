# Environment Variables

`internal/env.Ensure` automatically loads the first `.env` file it finds above the current working directory, so place all secrets/configuration in that file rather than exporting them manually.

For test runs (`go test`), TaskAgent keeps the environment hermetic by default and will **not** auto-load `.env` unless you explicitly set `GOTEST_LOAD_DOTENV=1`.

The tables below enumerate every environment variable TaskAgent reads via `os.Getenv`/`LookupEnv` along with defaults and consumers.

## Feishu credentials & API endpoints

| Variable | Required | Default | Used by | Notes |
| --- | --- | --- | --- | --- |
| `FEISHU_APP_ID` | Yes | – | `internal/feishusdk`, `cmd`, `internal/storage`, `internal/devrecorder`, `examples/create_task_with_http` | App ID for the Feishu Open Platform. |
| `FEISHU_APP_SECRET` | Yes | – | Same as above | App secret paired with the App ID. |
| `FEISHU_TENANT_KEY` | Optional | empty | `internal/feishusdk` | Needed only for tenant-scoped self-built apps. |
| `FEISHU_BASE_URL` | Optional | `https://open.feishu.cn` | `internal/feishusdk`, `examples/create_task_with_http` | Override for sandbox domains. |
| `FEISHU_REPORT_RPS` | Optional | `1` | `internal/feishusdk/storage.go` | Global limiter for result-table writes (floating-point, rows/sec). |

## Table URLs (Feishu Bitables)

| Variable | Required | Default | Used by | Description |
| --- | --- | --- | --- | --- |
| `TASK_BITABLE_URL` | Yes for Feishu-backed schedulers | – | `taskagent.Config`, `cmd`, `examples/create_task_with_http`, `examples/create_task_with_sdk` | Source of pending tasks (个人页搜索 / 综合页搜索等). |
| `RESULT_BITABLE_URL` | Yes when uploading captures to Feishu | – | `internal/storage`, `cmd` | Result table receiving capture rows + webhook summaries. |
| `DRAMA_BITABLE_URL` | Required when fetching drama metadata from Feishu | – | `pkg/webhook`, `cmd` | Drama catalog table for ratio/metadata lookups. |
| `WEBHOOK_BITABLE_URL` | Optional | empty | `pkg/webhook` | Dedicated webhook result table for group-based flows (aggregates TaskIDs + delivery status). |
| `DEVICE_BITABLE_URL` | Optional | empty | `internal/devrecorder` | Device heartbeat table; leave blank to disable recorder writes. |
| `DEVICE_TASK_BITABLE_URL` | Optional | empty | `internal/devrecorder`, `internal/storage` | Device-dispatch history table (one row per job). |

## Device pool selection

| Variable | Required | Default | Used by | Description |
| --- | --- | --- | --- | --- |
| `DEVICE_ALLOWLIST` | Optional | empty | `taskagent.DevicePoolAgent` | Restrict scheduling to a subset of locally connected device serials. Must be a comma-separated list (e.g. `device-A,device-B`). When empty, all connected devices are eligible. |

## 资源下载服务

| Variable | Required | Default | Used by | Description |
| --- | --- | --- | --- | --- |
| `CRAWLER_SERVICE_BASE_URL` | Optional | `http://localhost:8080` | `pkg/singleurl`, `pkg/webhook`（single_url_capture 汇总）, `cmd singleurl` | Base URL for `content_web_crawler` 的 `/download/tasks` API（创建 task + 轮询 `/download/tasks/<task_id>` 状态 + `/download/tasks/finish` 汇总上报）。 |
| `SINGLE_URL_CONCURRENCY` | Optional | `1` | `pkg/singleurl`, `cmd singleurl` | Max parallel crawler API calls per `ProcessOnce` (create via `POST /download/tasks` and polling via `GET /download/tasks/<task_id>`). Feishu status updates remain serial to stay under rate limits. |
| `COOKIE_BITABLE_URL` | Optional | – | – | (Deprecated) SingleURLWorker no longer reads/forwards cookies. |
| `ENABLE_COOKIE_VALIDATION` | Optional | unset (`false`) | – | (Deprecated) Kept for backward compatibility; no longer used by SingleURLWorker. |

Cookies 表字段要求（Deprecated）：
- `Cookies`：账号 Web 登录态字符串；
- `Platform`：平台名称；
- `Status`：`valid` / `invalid`。

## Field overrides

TaskAgent exposes per-table override knobs so you can align with custom schemas without recompiling. Leave them unset to keep the defaults defined in `internal/feishusdk/constants.go`.

### Task table (`TASK_FIELD_*`)
| Variable | Default | Purpose |
| --- | --- | --- |
| `TASK_FIELD_TASKID` | `TaskID` | Primary identifier column. |
| `TASK_FIELD_PARENT_TASK_ID` | `ParentTaskID` | Parent task ID (综合页搜索 TaskID). |
| `TASK_FIELD_APP` | `App` | App/platform name used for filtering by `agent.Start(ctx, app)`. |
| `TASK_FIELD_SCENE` | `Scene` | Scene name (个人页搜索、综合页搜索、视频录屏采集、单个链接采集等). |
| `TASK_FIELD_PARAMS` | `Params` | Parameters/payload column consumed by runners. |
| `TASK_FIELD_ITEMID` | `ItemID` | Single-resource identifier for specialized scenes. |
| `TASK_FIELD_BOOKID` | `BookID` | Drama identifier required by “单个链接采集”任务。|
| `TASK_FIELD_URL` | `URL` | 视频分享链接地址，单个链接采集 worker 会直接使用。|
| `TASK_FIELD_USERID` | `UserID` | User identifier. |
| `TASK_FIELD_USERNAME` | `UserName` | User display name. |
| `TASK_FIELD_DATE` | `Date` | Optional scheduling field. |
| `TASK_FIELD_STATUS` | `Status` | Task lifecycle status (pending/queued/dispatched/running/success/failed/error). |
| `TASK_FIELD_GROUPID` | `GroupID` | Piracy/SingleURL group-task identifier. |
| `TASK_FIELD_DEVICE_SERIAL` | `DeviceSerial` | Target device serial (optional pre-allocation). |
| `TASK_FIELD_DISPATCHED_DEVICE` | `DispatchedDevice` | Actual device used. |
| `TASK_FIELD_DISPATCHED_AT` | `DispatchedAt` | Dispatch timestamp. |

### Webhook result table (`WEBHOOK_FIELD_*`)
| Variable | Default | Purpose |
| --- | --- | --- |
| `WEBHOOK_FIELD_BIZTYPE` | `BizType` | Business type (e.g. `piracy_general_search`, `video_screen_capture`). |
| `WEBHOOK_FIELD_PARENT_TASK_ID` | `ParentTaskID` | Parent task ID (综合页搜索 TaskID). |
| `WEBHOOK_FIELD_GROUPID` | `GroupID` | Group identifier (`{App}_{BookID}_{UserID}`). |
| `WEBHOOK_FIELD_STATUS` | `Status` | Webhook delivery state (pending/success/failed/error). |
| `WEBHOOK_FIELD_TASKIDS` | `TaskIDs` | Text TaskID list (comma-separated numeric IDs like `123,456`). |
| `WEBHOOK_FIELD_DRAMAINFO` | `DramaInfo` | Raw drama row fields JSON (text). |
| `WEBHOOK_FIELD_USERINFO` | `UserInfo` | Reserved user info JSON (text). |
| `WEBHOOK_FIELD_RECORDS` | `Records` | Flattened capture records JSON (text). |
| `WEBHOOK_FIELD_DATE` | `Date` | Logical task date (ExactDate from task `Date`, used for dedup/filtering). |
| `WEBHOOK_FIELD_CREATEAT` | `CreateAt` | Creation time (date). |
| `WEBHOOK_FIELD_STARTAT` | `StartAt` | First processing time (date). |
| `WEBHOOK_FIELD_ENDAT` | `EndAt` | Last processing time (date). |
| `WEBHOOK_FIELD_RETRYCOUNT` | `RetryCount` | Retry counter (number). |
| `WEBHOOK_FIELD_LASTERROR` | `LastError` | Last error message (text). |
| `TASK_FIELD_START_AT` | `StartAt` | Execution start timestamp. |
| `TASK_FIELD_END_AT` | `EndAt` | Execution end timestamp. |
| `TASK_FIELD_ELAPSED_SECONDS` | `ElapsedSeconds` | Duration of the run in seconds. |
| `TASK_FIELD_ITEMS_COLLECTED` | `ItemsCollected` | Number of items collected during the run. |
| `TASK_FIELD_RETRYCOUNT` | `RetryCount` | Task retry counter (failed→running transitions). |
| `TASK_FIELD_EXTRA` | `Extra` | Free-form JSON for additional metadata. |

### Result table (`RESULT_FIELD_*`)
| Variable | Default | Purpose |
| --- | --- | --- |
| `RESULT_FIELD_DATETIME` | `Datetime` | Capture timestamp. |
| `RESULT_FIELD_APP` / `RESULT_FIELD_SCENE` | `App` / `Scene` | Origin app & scene. |
| `RESULT_FIELD_PARAMS` | `Params` | Capture params (drama name, etc.). |
| `RESULT_FIELD_ITEMID` | `ItemID` | Video/record identifier. |
| `RESULT_FIELD_ITEMCAPTION` | `ItemCaption` | Title or caption. |
| `RESULT_FIELD_ITEMCDNURL` | `ItemCDNURL` | CDN link for the asset. |
| `RESULT_FIELD_ITEMURL` | `ItemURL` | Share URL. |
| `RESULT_FIELD_DURATION` | `ItemDuration` | Duration in seconds. |
| `RESULT_FIELD_USERNAME` / `RESULT_FIELD_USERID` | `UserName` / `UserID` | Uploader info. |
| `RESULT_FIELD_USERAUTHENTITY` | `UserAuthEntity` | Verification badge text. |
| `RESULT_FIELD_TAGS` | `Tags` | Content tags (合集/短剧/...). |
| `RESULT_FIELD_TASKID` | `TaskID` | Link back to the originating task. |
| `RESULT_FIELD_DEVICE_SERIAL` | `DeviceSerial` | Device that captured the record. |
| `RESULT_FIELD_EXTRA` | `Extra` | JSON payload. |
| `RESULT_FIELD_LIKECOUNT`, `RESULT_FIELD_VIEWCOUNT`, `RESULT_FIELD_COMMENTCOUNT`, `RESULT_FIELD_COLLECTCOUNT`, `RESULT_FIELD_FORWARDCOUNT`, `RESULT_FIELD_SHARECOUNT` | Social metrics columns. |
| `RESULT_FIELD_ANCHORPOINT` | `AnchorPoint` | Anchor metadata JSON (used for appLink extraction). |
| `RESULT_FIELD_PAYMODE` | `PayMode` | Monetization info. |
| `RESULT_FIELD_COLLECTION` / `RESULT_FIELD_EPISODE` | `Collection` / `Episode` | Aggregation metadata. |
| `RESULT_FIELD_PUBLISHTIME` | `PublishTime` | Publish timestamp. |

### Drama table (`DRAMA_FIELD_*`)
| Variable | Default | Purpose |
| --- | --- | --- |
| `DRAMA_FIELD_ID` | `短剧 ID` | Unique drama identifier. |
| `DRAMA_FIELD_NAME` | `短剧名称` | Name used for Params matching. |
| `DRAMA_FIELD_DURATION` | `全剧时长（秒）` | Total duration (seconds). |
| `DRAMA_FIELD_EPISODE_COUNT` | `全剧集数` | Episode count in the template schema. |
| `DRAMA_FIELD_PRIORITY` | `优先级` | Internal priority. |
| `DRAMA_FIELD_RIGHTS_SCENARIO` | `维权场景` | Rights-protection scenario label. |

### Device tables (`DEVICE_FIELD_*` and `DEVICE_TASK_FIELD_*`)
| Variable | Default | Purpose |
| --- | --- | --- |
| `DEVICE_FIELD_SERIAL` | `DeviceSerial` | Unique device ID written by the recorder. |
| `DEVICE_FIELD_OSTYPE` / `DEVICE_FIELD_OSVERSION` | `OSType` / `OSVersion` | OS metadata. |
| `DEVICE_FIELD_IP_LOCATION` | `IPLocation` | Optional region text. |
| `DEVICE_FIELD_ISROOT` | `IsRoot` | Rooted flag. |
| `DEVICE_FIELD_PROVIDERUUID` | `ProviderUUID` | Provider identifier when multiple pools share a table. |
| `DEVICE_FIELD_AGENT_VERSION` | `AgentVersion` | Agent build string propagated via `Config.AgentVersion`. |
| `DEVICE_FIELD_STATUS` | `Status` | idle/dispatched/running/offline. |
| `DEVICE_FIELD_LAST_SEEN_AT` / `DEVICE_FIELD_LAST_ERROR` | `LastSeenAt` / `LastError` | Health data. |
| `DEVICE_FIELD_TAGS` | `Tags` | Optional labels. |
| `DEVICE_FIELD_RUNNING_TASK` / `DEVICE_FIELD_PENDING_TASKS` | `RunningTask` / `PendingTasks` | Live task snapshot. `RunningTask` is stored as plain text; `PendingTasks` is stored as a comma-separated string in a text column (for example: `"44007,44008,44009"`).

Device-task history tables follow the same pattern; set `DEVICE_TASK_FIELD_*` (e.g., `DEVICE_TASK_FIELD_JOBID`, `DEVICE_TASK_FIELD_DEVICE_SERIAL`, `DEVICE_TASK_FIELD_ASSIGNED_TASKS`, `DEVICE_TASK_FIELD_STATE`, `DEVICE_TASK_FIELD_ERROR_MESSAGE`, `DEVICE_TASK_FIELD_STARTED_AT`, `DEVICE_TASK_FIELD_FINISHED_AT`) to match your schema when writing dispatch rows.

## Storage, recorder, and reporter settings

| Variable | Default | Purpose |
| --- | --- | --- |
| `TRACKING_STORAGE_DISABLE_JSONL` | unset | Disable JSONL sink for capture history (SQLite always stays on). |
| `TRACKING_STORAGE_DB_PATH` | `$HOME/.eval/records.sqlite` | Location of the shared SQLite database used by storage + webhook helpers. |
| `RESULT_STORAGE_ENABLE_FEISHU` | unset (`false`) | Force Feishu uploads even if the agent would normally run offline. |
| `DRAMA_SQLITE_TABLE` | `drama_catalog` | SQLite table for drama metadata. |
| `RESULT_SQLITE_TABLE` | `capture_results` | SQLite table for capture records (`reported` bookkeeping columns included). |
| `RESULT_REPORT_POLL_INTERVAL` | `5s` | Async reporter scan interval (Go duration string). |
| `RESULT_REPORT_BATCH` | `30` | Max pending rows uploaded per reporter tick. |
| `RESULT_REPORT_HTTP_TIMEOUT` | `30s` | Per-row Feishu upload timeout. |

## Piracy & detection knobs

| Variable | Default | Purpose |
| --- | --- | --- |
| `THRESHOLD` | `0.5` | detection threshold（0–1）. |

## Testing & tooling toggles

| Variable | Default | Purpose |
| --- | --- | --- |
| `FEISHU_LIVE_TEST` | unset (`0`) | When set to `1`, enables live Feishu integration tests（`go test ./internal/feishusdk -run Live`、`go test ./pkg/webhook -run Live` 等）。Keep unset in CI to avoid touching production tables. |

## Tips
- Keep every secret/URL in `.env`; never commit the file. `godotenv` + `envload` ensure both CLI tools and Go binaries see the same configuration.
- Field override vars accept any non-empty string; call `feishusdk.RefreshFieldMappings()` in tests when you mutate env vars mid-run.
- When enabling result uploads, configure both `RESULT_BITABLE_URL` and `RESULT_STORAGE_ENABLE_FEISHU=1`; tune the reporter via `RESULT_REPORT_*` + `FEISHU_REPORT_RPS` to stay under Feishu’s rate limits.
