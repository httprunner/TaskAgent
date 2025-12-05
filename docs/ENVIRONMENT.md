# Environment Variables

`internal/envload.Ensure` automatically loads the first `.env` file it finds above the current working directory, so place all secrets/configuration in that file rather than exporting them manually. The tables below enumerate every environment variable TaskAgent reads via `os.Getenv`/`LookupEnv` along with defaults and consumers.

## Feishu credentials & API endpoints

| Variable | Required | Default | Used by | Notes |
| --- | --- | --- | --- | --- |
| `FEISHU_APP_ID` | Yes | – | `pkg/feishu`, `cmd/piracy`, `pkg/storage`, `pkg/devrecorder` | App ID for the Feishu Open Platform. |
| `FEISHU_APP_SECRET` | Yes | – | Same as above | App secret paired with the App ID. |
| `FEISHU_TENANT_KEY` | Optional | empty | `pkg/feishu` | Needed only for tenant-scoped self-built apps. |
| `FEISHU_BASE_URL` | Optional | `https://open.feishu.cn` | `pkg/feishu`, `examples/create_task_with_http` | Override for sandbox domains. |
| `FEISHU_REPORT_RPS` | Optional | `1` | `pkg/feishu/storage.go` | Global limiter for result-table writes (floating-point, rows/sec). |

## Table URLs (Feishu Bitables)

| Variable | Required | Default | Used by | Description |
| --- | --- | --- | --- | --- |
| `TASK_BITABLE_URL` | Yes for Feishu-backed schedulers | – | `pool.Config`, `pkg/piracy`, `cmd/piracy` | Source of pending tasks (个人页搜索 / 综合页搜索等). |
| `RESULT_BITABLE_URL` | Yes when uploading captures to Feishu | – | `pkg/storage`, `pkg/piracy`, `cmd/piracy` | Result table receiving capture rows + webhook summaries. |
| `DRAMA_BITABLE_URL` | Required when fetching drama metadata from Feishu | – | `pkg/piracy`, `cmd/piracy` | Drama catalog table for ratio/metadata lookups. |
| `DEVICE_BITABLE_URL` | Optional | empty | `pkg/devrecorder` | Device heartbeat table; leave blank to disable recorder writes. |
| `DEVICE_TASK_BITABLE_URL` | Optional | empty | `pkg/devrecorder`, `pkg/storage` | Device-dispatch history table (one row per job). |

## 资源下载服务

| Variable | Required | Default | Used by | Description |
| --- | --- | --- | --- | --- |
| `CRAWLER_SERVICE_BASE_URL` | Optional | `http://localhost:8080` | `pool/single_url_worker` | Base URL for `content_web_crawler` 的 `/download/tasks` API，SingleURLWorker 始终携带 `sync_to_hive=true` 并轮询 Redis 持久化的 job 状态。 |
| `COOKIE_BITABLE_URL` | Optional | – | `pool/single_url_worker` | Feishu bitable storing account cookies（字段：`Cookies`、`Platform`、`Status`），worker 会轮询 `Status=valid` 的 cookies 并随机/轮流使用。 |
| `ENABLE_COOKIE_VALIDATION` | Optional | unset (`false`) | `pool/single_url_worker` | 设为 `true` 时才会对快手 cookies 发送首页请求校验登录态；默认跳过校验直接使用表里记录。 |

Cookies 表字段要求：
- `Cookies`：账号 Web 登录态字符串；
- `Platform`：平台名称；
- `Status`：`valid` / `invalid`，SingleURLWorker 仅挑选 `valid` 行，后续会在验证失败时写回 `invalid`（预留能力）。

## Field overrides

TaskAgent exposes per-table override knobs so you can align with custom schemas without recompiling. Leave them unset to keep the defaults defined in `pkg/feishu/constants.go`.

### Task table (`TASK_FIELD_*`)
| Variable | Default | Purpose |
| --- | --- | --- |
| `TASK_FIELD_TASKID` | `TaskID` | Primary identifier column. |
| `TASK_FIELD_APP` | `App` | App/platform name used for filtering by `agent.Start(ctx, app)`. |
| `TASK_FIELD_SCENE` | `Scene` | Scene name (个人页搜索、综合页搜索、视频录屏采集、单个链接采集等). |
| `TASK_FIELD_PARAMS` | `Params` | Parameters/payload column consumed by runners. |
| `TASK_FIELD_ITEMID` | `ItemID` | Single-resource identifier for specialized scenes. |
| `TASK_FIELD_BOOKID` | `BookID` | Drama identifier required by “单个链接采集”任务。|
| `TASK_FIELD_URL` | `URL` | 视频分享链接地址，单个链接采集 worker 会直接使用。|
| `TASK_FIELD_USERID` | `UserID` | User identifier. |
| `TASK_FIELD_USERNAME` | `UserName` | User display name. |
| `TASK_FIELD_DATETIME` | `Datetime` | Optional scheduling field. |
| `TASK_FIELD_STATUS` | `Status` | Task lifecycle status (pending/queued/dispatched/running/success/failed). |
| `TASK_FIELD_WEBHOOK` | `Webhook` | Webhook dispatch state (pending/success/failed/error). |
| `TASK_FIELD_GROUPID` | `GroupID` | Piracy group-task identifier. |
| `TASK_FIELD_DEVICE_SERIAL` | `DeviceSerial` | Target device serial (optional pre-allocation). |
| `TASK_FIELD_DISPATCHED_DEVICE` | `DispatchedDevice` | Actual device used. |
| `TASK_FIELD_DISPATCHED_AT` | `DispatchedAt` | Dispatch timestamp. |
| `TASK_FIELD_START_AT` | `StartAt` | Execution start timestamp. |
| `TASK_FIELD_END_AT` | `EndAt` | Execution end timestamp. |
| `TASK_FIELD_ELAPSED_SECONDS` | `ElapsedSeconds` | Duration of the run in seconds. |
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
| `DEVICE_FIELD_STATUS` | `Status` | idle/running/offline. |
| `DEVICE_FIELD_LAST_SEEN_AT` / `DEVICE_FIELD_LAST_ERROR` | `LastSeenAt` / `LastError` | Health data. |
| `DEVICE_FIELD_TAGS` | `Tags` | Optional labels. |
| `DEVICE_FIELD_RUNNING_TASK` / `DEVICE_FIELD_PENDING_TASKS` | `RunningTask` / `PendingTasks` | Live task snapshot.

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
| `THRESHOLD` | `0.5` | Suspicion threshold for piracy detector (0–1). Used by `cmd/piracy detect/auto`. |

## Testing & tooling toggles

| Variable | Default | Purpose |
| --- | --- | --- |
| `FEISHU_LIVE_TEST` | unset (`0`) | When set to `1`, enables live Feishu integration tests (`go test ./feishu -run Live`, `pkg/piracy` live tests). Keep unset in CI to avoid touching production tables. |

## Tips
- Keep every secret/URL in `.env`; never commit the file. `godotenv` + `envload` ensure both CLI tools and Go binaries see the same configuration.
- Field override vars accept any non-empty string; call `feishu.RefreshFieldMappings()` in tests when you mutate env vars mid-run.
- When enabling result uploads, configure both `RESULT_BITABLE_URL` and `RESULT_STORAGE_ENABLE_FEISHU=1`; tune the reporter via `RESULT_REPORT_*` + `FEISHU_REPORT_RPS` to stay under Feishu’s rate limits.
