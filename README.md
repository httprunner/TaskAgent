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
    - Env + constants: `EnvString` and env/status constants (`EnvTaskBitableURL`, `EnvResultBitableURL`, `EnvDeviceBitableURL`, `EnvCookieBitableURL`, `StatusPending/Success/...`, `WebhookPending/Success/...`).

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
    - `webhook_worker.go`: standalone webhook worker that scans task tables and retries failed/pending webhooks.

- **`cmd`**
  - CLI entrypoints built on `pkg/webhook` and `pkg/singleurl`:
    - `webhook-worker`: standalone summary webhook worker using `webhook.NewWebhookWorker`.
    - `singleurl`: single-URL capture helper built on TaskAgent.
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
| `pkg/webhook` | Summary webhook 能力：从 Feishu/SQLite 汇总数据构造 payload，并通过 worker 扫描任务表重试 Webhook。 | fox search agent Webhook 下游、`cmd webhook-worker` |

## Getting Started
1. **Clone & download modules**
   ```bash
   git clone git@github.com:httprunner/TaskAgent.git
   cd TaskAgent
   go mod download
   ```
2. **Provide credentials** – Create a `.env` (automatically loaded by `internal/env.Ensure`) with at least `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `TASK_BITABLE_URL`, and any recorder/storage URLs. See [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) for the full matrix.
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
           if req.Lifecycle != nil && req.Lifecycle.OnTaskStarted != nil {
               req.Lifecycle.OnTaskStarted(task)
           }
           // TODO: execute capture logic with task.Payload / req.DeviceSerial
           if req.Lifecycle != nil && req.Lifecycle.OnTaskResult != nil {
               req.Lifecycle.OnTaskResult(task, nil)
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

### Infra CLI (`cmd`)
- **Webhook worker**: `go run ./cmd webhook-worker --app kwai --task-url "$TASK_BITABLE_URL"`
- **Single URL worker**: `go run ./cmd singleurl --task-url "$TASK_BITABLE_URL" --crawler-base-url "$CRAWLER_SERVICE_BASE_URL"`
- **Drama tasks generator**: `go run ./cmd drama-tasks --date 2025-12-01 --drama-url "$DRAMA_BITABLE_URL" --task-url "$TASK_BITABLE_URL"`

  The CLI is focused on infrastructure helpers (webhook retries、单链采集、剧单转任务)，与具体检测/搜索等业务逻辑解耦。更多细节见 [`docs/webhook-worker.md`](docs/webhook-worker.md) 和 [`docs/single-url-capture.md`](docs/single-url-capture.md)。

### Storage & observability
- `taskagent.NewResultStorageManager` + `taskagent.ResultStorageConfig` 写入 SQLite (`TRACKING_STORAGE_DB_PATH`) 并可选上报 Feishu (`RESULT_STORAGE_ENABLE_FEISHU=1`)。
- Async reporter knobs (`RESULT_REPORT_*`, `FEISHU_REPORT_RPS`) keep uploads within Feishu rate-limits（内部由 `internal/storage` 驱动）。
- `taskagent.NewDeviceRecorderFromEnv` 封装了设备状态上报（`DeviceSerial`, `Status`, `RunningTask`, 等）到 Feishu 表；字段名可通过 `DEVICE_FIELD_*` env 覆盖。
- Consult [`docs/result-storage.md`](docs/result-storage.md) for schema diagrams and failure playbooks.

## Configuration quick reference
- Credentials & Feishu endpoints: `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `FEISHU_TENANT_KEY`, `FEISHU_BASE_URL`.
- Task/result/device tables: `TASK_BITABLE_URL`, `RESULT_BITABLE_URL`, `DEVICE_BITABLE_URL`, `DEVICE_TASK_BITABLE_URL`.
- Field overrides: `TASK_FIELD_*`, `RESULT_FIELD_*`, `DRAMA_FIELD_*`, `DEVICE_FIELD_*`, `DEVICE_TASK_FIELD_*`.
- Storage knobs: `TRACKING_STORAGE_DISABLE_JSONL`, `TRACKING_STORAGE_DB_PATH`, `RESULT_STORAGE_ENABLE_FEISHU`, `RESULT_SQLITE_TABLE`, `DRAMA_SQLITE_TABLE`.
- Reporter throttles: `RESULT_REPORT_POLL_INTERVAL`, `RESULT_REPORT_BATCH`, `RESULT_REPORT_HTTP_TIMEOUT`, `FEISHU_REPORT_RPS`.
Refer to [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) for the authoritative table describing defaults, requirements, and consuming packages.

## Task lifecycle & priority

```
pending/failed (Feishu filter)
        │ FetchAvailableTasks
        ▼
dispatched ──> TaskLifecycle.OnTaskStarted (Status=running, recorder updates)
        │
        ▼
running ──> TaskLifecycle.OnTaskResult (success/failed)
        │
        ▼
OnTasksCompleted → Feishu updates + recorder cleanup
```

`FeishuTaskClient` fetches tasks in prioritized bands (个人页搜索 before 综合页搜索, same-day before backlog, failed before untouched) and only fills the shortfall to `MaxTasksPerJob`. See [`client.go`](client.go) for the full prioritization table.

## Troubleshooting
- **Missing tasks** – verify `TASK_BITABLE_URL` points to a view with Status=`pending/failed`, App matches the `Start` argument, and the service account has permission to read.
- **Recorder errors** – leave `DEVICE_BITABLE_URL`/`DEVICE_TASK_BITABLE_URL` empty to disable, or double-check field overrides align with your schema.
- **Result uploads throttled** – increase `RESULT_REPORT_BATCH`, relax `RESULT_REPORT_POLL_INTERVAL`, or scale `FEISHU_REPORT_RPS` to avoid 99991400 responses.
- **Webhook retries** – inspect `Webhook` column (pending/failed/error) and run `go run ./cmd webhook-worker` with the same App filter; see [`docs/webhook-worker.md`](docs/webhook-worker.md) for retry semantics.

## Further reading
- [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) – comprehensive list of configuration keys.
- [`docs/result-storage.md`](docs/result-storage.md) – SQLite + async reporter internals.
- [`docs/webhook-worker.md`](docs/webhook-worker.md) – webhook lifecycle and CLI usage.
- [`docs/feishu-api-summary.md`](docs/feishu-api-summary.md) – Bitable API reference for this repo.
- [`AGENTS.md`](AGENTS.md) – contributor workflow and coding standards.
