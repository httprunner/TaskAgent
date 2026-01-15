# TaskAgent

TaskAgent is a Go module that:

- Polls Feishu Bitable task tables
- Schedules Android capture devices
- Persists capture results to SQLite + Feishu
- Provides summary webhook helpers

It is designed as a Feishu Bitable–based task scheduler you can embed into your own agents: you implement device/job execution, TaskAgent handles task fetching, device pooling and result storage.

## Overview & Architecture

```
Feishu Task Table ──> FeishuTaskClient (task)
        │                      │
        ▼                      ▼
   DeviceProvider ──> DevicePoolAgent (task) ──> JobRunner (your code)
        │                      │               │
        ▼                      ▼               ▼
  Device recorder     lifecycle callbacks      Storage + webhook packages
```

### Core modules

- **Root package `taskagent`**
  - Scheduling core: `Task`, `TaskManager`, `JobRunner`, `DeviceProvider`, `DeviceRecorder`.
  - Device pool: `DevicePoolAgent` + internal `deviceManager` for device discovery and status tracking.
  - Feishu task source: `FeishuTaskClient` + `FeishuTask` and helpers for querying/updating task tables and mirroring task status into SQLite.
  - Public APIs for external callers:
    - Device pool: `NewDevicePoolAgent`, `Config`.
    - Task source: `NewFeishuTaskClient`, `FeishuTaskClientOptions`.
    - Result storage: `ResultStorageConfig`, `ResultStorageManager`, `NewResultStorageManager`, `EnsureResultReporter`, `OpenCaptureResultsDB`, `ResolveResultDBPath`.
    - Feishu SDK aliases: `FeishuClient`, `TaskRecordInput`, `BitableRow`, field mappings (`DefaultTaskFields`, `DefaultResultFields`, `DefaultDramaFields`), filter helpers (`FeishuFilterInfo`, `NewFeishuFilterInfo`, `NewFeishuCondition`, etc.).
    - Env + constants: `EnvString` and env/status constants (`EnvTaskBitableURL`, `EnvResultBitableURL`, `EnvDeviceBitableURL`, `EnvCookieBitableURL`, `StatusPending/Success/...`).
  - Multi-scenario helpers (optional):
    - `DispatchPlanner` hooks: `Config.DispatchPlanner`.
    - Reference implementations: `MultiDispatchPlanner`, `MultiTaskManager`, `MultiJobRunner`.

- **`internal/feishusdk`**
  - Low-level Feishu Bitable SDK: HTTP client, auth, schema structs (`TaskFields`, `ResultFields`, `DramaFields`, `DeviceFields`), filter builders, and rate-limited Feishu result writer.
  - Not imported directly by external code; instead use the aliases and helpers exported by `taskagent`.

- **`internal/storage`**
  - SQLite-first storage for capture results and task mirrors.
  - Builds sinks from `storage.Config` and drives an async Feishu result reporter.
  - Accessed from outside only via:
    - `taskagent.NewResultStorageManager`
    - `taskagent.EnsureResultReporter`
    - `taskagent.OpenCaptureResultsDB`
    - `taskagent.ResolveResultDBPath`
    - `taskagent.MirrorDramaRowsIfNeeded`

- **`internal/devrecorder` + `taskagent` device recorder**
  - Device heartbeats recorder that writes device info into a Feishu Bitable.
  - External entry point: `taskagent.NewDeviceRecorderFromEnv`.

- **`providers/adb`**
  - Default Android device provider built on `github.com/httprunner/httprunner/v5/pkg/gadb`.
  - Plugged into the pool via `DeviceProvider`; you can swap in your own provider without changing scheduling code.

- **`pkg/singleurl`**
  - Single-URL capture worker that reuses `FeishuTaskClient` and the result storage APIs.
  - Illustrates how to implement a dedicated JobRunner around the scheduling core.

- **`pkg/webhook`**
  - Summary webhook module used by multiple scenarios:
    - `webhook.go`: builds and sends summary webhooks by aggregating drama metadata + capture records from Feishu or SQLite.
    - `source_feishu.go` / `source_sqlite.go`: Feishu/SQLite data sources for summary payloads.
    - `webhook_worker.go`: processes webhook result rows (WEBHOOK_BITABLE_URL) and delivers summary payloads.
    - `webhook_create.go`: creates webhook result rows (group creation + external-task backfill).

- **`cmd`**
  - CLI entrypoints built on `pkg/webhook` and `pkg/singleurl`:
    - `webhook-worker`: process webhook result rows using `webhook.NewWebhookResultWorker`.
    - `webhook-creator`: create webhook result rows for external tasks (video screen capture).
    - `singleurl`: single-URL capture helper built on TaskAgent.
    - `create-tasks`: create task rows from Feishu spreadsheet inputs.
    - `drama-tasks`: generate 综合页搜索 tasks from a drama catalog table.

- **`docs/`**
  - Deep dives into environment configuration, Feishu API usage, result storage and the webhook worker.

### Module overview table

| Package | 职责概述 | 典型调用方 |
| --- | --- | --- |
| `taskagent` | Feishu 多维表格任务调度核心：设备池（`DevicePoolAgent`）、任务源（`FeishuTaskClient`）、结果存储与环境常量封装。 | fox search agent、内部调度服务、示例程序 |
| `internal/feishusdk` | 封装 Feishu Bitable HTTP API、字段映射与速率限制，为根包提供底层 SDK 能力。 | 仅被 `taskagent`、`internal/storage`、`pkg/webhook` 等内部模块调用 |
| `internal/storage` | 负责 SQLite `capture_results`/任务镜像表以及异步 Feishu 结果上报与短剧元数据镜像。 | 通过 `taskagent.NewResultStorageManager`、`EnsureResultReporter` 间接使用 |
| `pkg/singleurl` | 基于 TaskAgent 的“单个链接采集” worker，不依赖设备池，直接调用下载服务完成采集。 | fox search agent、`cmd` 单链采集子命令 |
| `pkg/webhook` | Summary webhook 能力：从 Feishu/SQLite 汇总数据构造 payload，并通过 webhook 结果表驱动重试与状态机。 | fox search agent Webhook 下游、`cmd webhook-*` |

## Getting Started
1. **Clone & download modules**
   ```bash
   git clone git@github.com:httprunner/TaskAgent.git
   cd TaskAgent
   go mod download
   ```
2. **Provide credentials** – Create a `.env` (automatically loaded by `internal/env.Ensure`) with at least `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `TASK_BITABLE_URL`, and any recorder/storage URLs. See [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) for the full matrix.
   - Note: `go test` does **not** auto-load `.env` by default. Set `GOTEST_LOAD_DOTENV=1` if you want tests to opt-in to `.env` loading.
3. **Validate toolchain** – Run the standard gate before making changes:
   ```bash
   go fmt ./...
   go vet ./...
   go test ./...
   # Optional: live API validation (requires production tables)
   FEISHU_LIVE_TEST=1 go test ./internal/feishusdk -run Live
   ```
4. **Implement a JobRunner** – supply how tasks execute once TaskAgent hands you a device serial + payload:
   ```go
   package main

   import (
       "context"
       "log"
       "time"

       taskagent "github.com/httprunner/TaskAgent"
   )

   type CaptureRunner struct{}

   func (CaptureRunner) RunJob(ctx context.Context, req taskagent.JobRequest) error {
       for _, task := range req.Tasks {
          if req.Notifier != nil {
              _ = req.Notifier.OnTaskStarted(ctx, req.DeviceSerial, task)
          }
           // TODO: execute capture logic with task.Payload / req.DeviceSerial
          if req.Notifier != nil {
              _ = req.Notifier.OnTaskResult(ctx, req.DeviceSerial, task, nil)
          }
       }
       return nil
   }

   func main() {
       cfg := taskagent.Config{
           PollInterval:   30 * time.Second,
           MaxTasksPerJob: 2,
           BitableURL:     "", // Fill with TASK_BITABLE_URL from your environment.
           AgentVersion:   "capture-agent",
       }
       runner := CaptureRunner{}
       agent, err := taskagent.NewDevicePoolAgent(cfg, runner)
       if err != nil {
           log.Fatal(err)
       }
       ctx, cancel := context.WithCancel(context.Background())
       defer cancel()
       if err := agent.Start(ctx, "capture-app"); err != nil {
           log.Fatal(err)
       }
   }
   ```
   Add `DeviceRecorder` (Feishu tables) or custom providers as needed; leave URLs empty to disable recorder writes.

## Usage Patterns

### Embed TaskAgent in your scheduler
1. Implement `task.DeviceProvider` if you are not using the bundled ADB provider.
2. Implement `task.JobRunner` to translate Feishu payloads into device-specific actions.
3. Optionally wire a `task.TaskManager` alternative if tasks are not Feishu-backed.
4. Configure device & task recorders (`DEVICE_BITABLE_URL`, `DEVICE_TASK_BITABLE_URL`) to observe fleet health and dispatch history.

### Examples
- **Create a `pending` task via SDK**: `go run ./examples/create_task_with_sdk -bid <bid> -uid <uid> -eid <eid>` (see `examples/create_task_with_sdk/README.md`)
- **Create a `pending` task via pure HTTP**: `go run ./examples/create_task_with_http -bid <bid> -uid <uid> -eid <eid>` (see `examples/create_task_with_http/README.md`)

### Infra CLI (`cmd`)
- **Webhook worker**: `go run ./cmd webhook-worker --task-url "$TASK_BITABLE_URL" --webhook-bitable-url "$WEBHOOK_BITABLE_URL" --webhook-url "$SUMMARY_WEBHOOK_URL"`
  - 支持定向调试：`--group-id "<GroupID>"`、`--date "2025-12-17"`（可组合）。任一参数非空时仅执行单次扫描，并只处理匹配 `GroupID` / 逻辑日期（Date 字段）的结果行。
- **Webhook creator (video screen capture)**: one-shot `go run ./cmd webhook-creator --task-url "$TASK_BITABLE_URL" --webhook-bitable-url "$WEBHOOK_BITABLE_URL" --app kwai`, or polling with `--poll-interval 30s`
- **Single URL worker**: `go run ./cmd singleurl --task-url "$TASK_BITABLE_URL" --crawler-base-url "$CRAWLER_SERVICE_BASE_URL"`
- **Sheet task creator**: `go run ./cmd create-tasks --source-url "$SOURCE_SHEET_URL" --task-url "$TASK_BITABLE_URL" --limit 2000` (add `--poll-interval 2m` for continuous polling)
- **Drama tasks generator**: `go run ./cmd drama-tasks --date 2025-12-01 --drama-url "$DRAMA_BITABLE_URL" --task-url "$TASK_BITABLE_URL"`

  The CLI is focused on infrastructure helpers (webhook retries、单链采集、剧单转任务)，与具体检测/搜索等业务逻辑解耦。更多细节见 [`docs/webhook-worker.md`](docs/webhook-worker.md) 和 [`docs/single-url-capture.md`](docs/single-url-capture.md)。

### Storage & observability
- `taskagent.NewResultStorageManager` + `taskagent.ResultStorageConfig` 写入 SQLite (`TRACKING_STORAGE_DB_PATH`) 并可选上报 Feishu (`RESULT_STORAGE_ENABLE_FEISHU=1`)。
- Async reporter knobs (`RESULT_REPORT_*`, `FEISHU_REPORT_RPS`) keep uploads within Feishu rate-limits（内部由 `internal/storage` 驱动）。
- `taskagent.NewDeviceRecorderFromEnv` 封装了设备状态上报（`DeviceSerial`, `Status`, `RunningTask`, `PendingTasks` 等）到 Feishu 表；字段名可通过 `DEVICE_FIELD_*` env 覆盖，其中 `RunningTask` 建议使用文本列，`PendingTasks` 需配置为文本列并按逗号分隔的任务 ID 展示（例如 `44007,44008,44009`）。
- Consult [`docs/result-storage.md`](docs/result-storage.md) for schema diagrams and failure playbooks.

## Configuration quick reference
- Credentials & Feishu endpoints: `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `FEISHU_TENANT_KEY`, `FEISHU_BASE_URL`.
- Task/result/device tables: `TASK_BITABLE_URL`, `RESULT_BITABLE_URL`, `DEVICE_BITABLE_URL`, `DEVICE_TASK_BITABLE_URL`.
- Field overrides: `TASK_FIELD_*`, `RESULT_FIELD_*`, `DRAMA_FIELD_*`, `DEVICE_FIELD_*`, `DEVICE_TASK_FIELD_*`.
- Storage knobs: `TRACKING_STORAGE_DISABLE_JSONL`, `TRACKING_STORAGE_DB_PATH`, `RESULT_STORAGE_ENABLE_FEISHU`, `RESULT_SQLITE_TABLE`, `DRAMA_SQLITE_TABLE`.
- Reporter throttles: `RESULT_REPORT_POLL_INTERVAL`, `RESULT_REPORT_BATCH`, `RESULT_REPORT_HTTP_TIMEOUT`, `FEISHU_REPORT_RPS`.
- App binding: `Config.App` and `NewFeishuTaskClient` require a non-empty app value.
Refer to [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) for the authoritative table describing defaults, requirements, and consuming packages.

## Task lifecycle & priority

```
pending/failed (Feishu filter)
        │ FetchAvailableTasks
        ▼
dispatched ──> TaskManager.OnTaskStarted (Status=running, recorder updates)
        │
        ▼
running ──> TaskManager.OnTaskResult (success/failed)
        │
        ▼
OnTasksCompleted → Feishu updates + recorder cleanup
```

`FeishuTaskClient` does not apply default prioritization. Callers must pass explicit `TaskFetchFilter` lists to control scene/status/date order.

When `TASK_GROUP_PRIORITY_ENABLE=1`, `DevicePoolAgent` additionally re-orders fetched tasks by remaining group size (ascending) per `<BizType, GroupID, DateDay>` so "almost done" groups finish earlier and downstream webhook pushes can be triggered sooner. `GroupTaskPrioritizer` requires an `App` binding.

## Multi-scenario scheduling (Multi*)

When you need to run multiple task "scenes" in a single process (e.g. search + single-url) but still keep **one** device pool, you can combine:

- `MultiTaskManager`: fetches tasks from two task managers and multiplexes lifecycle callbacks by task scene.
- `MultiDispatchPlanner`: assigns tasks to idle devices with per-scene caps (search vs `SceneSingleURLCapture`) and a 50/50 device preference.
- `MultiJobRunner`: runs the two scene groups sequentially on the same device (no concurrency on one device).

Minimal wiring:

```go
multiTM := &taskagent.MultiTaskManager{
  Search: someSearchTM, SingleURL: someSingleURLTM,
  SearchFetchLimit: 0, SingleURLFetchLimit: 0,
}
planner := &taskagent.MultiDispatchPlanner{SearchMaxTasksPerDevice: 5, SingleURLMaxTasksPerDevice: 10}
runner := &taskagent.MultiJobRunner{SearchRunner: searchRunner, SingleURLRunner: singleURLRunner}

agent, _ := taskagent.NewDevicePoolAgent(taskagent.Config{
  PollInterval: 30 * time.Second,
  MaxTasksPerJob: 10,
  App: "com.smile.gifmaker",
  TaskManager: multiTM,
  FetchTaskFilters: []taskagent.TaskFetchFilter{
    {App: "com.smile.gifmaker", Scene: taskagent.SceneGeneralSearch, Status: taskagent.StatusPending, Date: taskagent.TaskDateToday},
  },
  DispatchPlanner: planner,
}, runner)
_ = agent.Start(ctx, "com.smile.gifmaker")
```

Notes:
- `MultiDispatchPlanner` currently recognizes `SceneSingleURLCapture` as the "single-url" scene and treats everything else as "search".
- `FetchTaskFilters` are required; TaskAgent does not provide default fetch filters.
- If you need different sharding keys, per-scene priorities, or a different mixing policy, implement your own `DispatchPlanner`.

## Troubleshooting
- **Missing tasks** – verify `TASK_BITABLE_URL` points to a view with Status=`pending/failed`, App matches the `Start` argument, and the service account has permission to read.
- **Recorder errors** – leave `DEVICE_BITABLE_URL`/`DEVICE_TASK_BITABLE_URL` empty to disable, or double-check field overrides align with your schema.
- **Result uploads throttled** – increase `RESULT_REPORT_BATCH`, relax `RESULT_REPORT_POLL_INTERVAL`, or scale `FEISHU_REPORT_RPS` to avoid 99991400 responses.
- **Webhook retries** – inspect `WEBHOOK_BITABLE_URL` rows (pending/failed/error) and run `go run ./cmd webhook-worker`; see [`docs/webhook-worker.md`](docs/webhook-worker.md).

## Further reading
- [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) – comprehensive list of configuration keys.
- [`docs/result-storage.md`](docs/result-storage.md) – SQLite + async reporter internals.
- [`docs/webhook-worker.md`](docs/webhook-worker.md) – webhook lifecycle, creator/worker responsibilities.
- [`docs/feishu-api-summary.md`](docs/feishu-api-summary.md) – Bitable API reference for this repo.
- [`AGENTS.md`](AGENTS.md) – contributor workflow and coding standards.
