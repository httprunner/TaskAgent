# TaskAgent

TaskAgent is a Go 1.24 module that polls Feishu Bitable queues, keeps Android capture devices busy, persists outcomes to Feishu/SQLite, and triggers piracy-specific workflows. It is designed to be embedded inside existing schedulers/agents so you only need to provide device implementations plus business-side runners.

## Overview & Architecture

```
Feishu Task Table ──> TaskManager (tasks.go)
        │                  │
        ▼                  ▼
 DeviceProvider ──> DevicePoolAgent (pool.go) ──> JobRunner (your code)
        │                  │                          │
        ▼                  ▼                          ▼
 Device recorder   TaskLifecycle hooks        Storage + webhook/piracy packages
```

### Core building blocks
- **DevicePoolAgent** & **TaskLifecycle**: orchestrate polling, dispatching, retrying, and lifecycle callbacks so Feishu status rows & device snapshots stay consistent.
- **feishu package**: wraps Bitable APIs, exposes schema structs (`TaskFields`, `ResultFields`, `DeviceFields`) plus rate-limited result writers (`FEISHU_REPORT_RPS`).
- **providers/adb**: default ADB-backed device provider; swap in custom providers without touching the pool.
- **pkg/devrecorder** & **pkg/storage**: optional Feishu device heartbeats and SQLite-first capture storage with async reporting.
- **cmd/piracy / pkg/piracy**: piracy detection CLI, webhook worker, backfill helpers, and group-task automation.

### Repository map
- `pool.go`, `tasks.go`, `pool_test.go`: scheduling core and unit coverage.
- `feishu/`: API client, schema definitions, spreadsheet helpers, rate limiter, and README.
- `pkg/piracy/`, `cmd/piracy/`: CLI entry points plus detection/webhook logic.
- `pkg/storage/`, `pkg/devrecorder/`: SQLite + Feishu sinks and device recorder helpers.
- `docs/`: deep dives (Feishu API usage, piracy group tasks, webhook worker, result storage).

## Getting Started
1. **Clone & download modules**
   ```bash
   git clone git@github.com:httprunner/TaskAgent.git
   cd TaskAgent
   go mod download
   ```
2. **Provide credentials** – Create a `.env` (loaded via `internal/envload`) with at least `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `TASK_BITABLE_URL`, and any recorder/storage URLs. See [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) for the full matrix.
3. **Validate toolchain** – Run the standard gate before making changes:
   ```bash
   go fmt ./...
   go vet ./...
   go test ./...
   # Optional: live API validation (requires production tables)
   FEISHU_LIVE_TEST=1 go test ./feishu -run Live
   ```
4. **Implement a JobRunner** – supply how tasks execute once TaskAgent hands you a device serial + payload:
   ```go
   package main

   import (
       "context"
       "log"
       "os"
       "time"

       pool "github.com/httprunner/TaskAgent"
       "github.com/httprunner/TaskAgent/pkg/feishu"
   )

   type CaptureRunner struct{}

   func (CaptureRunner) RunJob(ctx context.Context, req pool.JobRequest) error {
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
       cfg := pool.Config{
           PollInterval:   30 * time.Second,
           MaxTasksPerJob: 2,
           BitableURL:     os.Getenv(feishu.EnvTaskBitableURL),
           AgentVersion:   "capture-agent",
       }
       runner := CaptureRunner{}
       agent, err := pool.NewDevicePoolAgent(cfg, runner)
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
1. Implement `pool.DeviceProvider` if you are not using the bundled ADB provider.
2. Implement `pool.JobRunner` to translate Feishu payloads into device-specific actions.
3. Optionally wire a `TaskManager` alternative if tasks are not Feishu-backed.
4. Configure device & task recorders (`DEVICE_BITABLE_URL`, `DEVICE_TASK_BITABLE_URL`) to observe fleet health and dispatch history.

### Piracy CLI (`cmd/piracy`)
- **Detect**: `go run ./cmd/piracy detect --result-filter='{"conjunction":"and","conditions":[...]}' --output-csv result.csv`
- **Auto workflow**: `go run ./cmd/piracy auto --task-url "$TASK_BITABLE_URL" --result-url "$RESULT_BITABLE_URL"`
- **Webhook worker**: `go run ./cmd/piracy webhook-worker --app kwai --task-url "$TASK_BITABLE_URL"`
  The CLI reuses the same Feishu/SQLite helpers as the agent; see [`docs/webhook-worker.md`](docs/webhook-worker.md) and [`docs/piracy-group-tasks.md`](docs/piracy-group-tasks.md) for behavior details.

### Storage & observability
- `pkg/storage` writes every capture to SQLite (`TRACKING_STORAGE_DB_PATH`) and optionally Feishu (`RESULT_STORAGE_ENABLE_FEISHU=1`).
- Async reporter knobs (`RESULT_REPORT_*`, `FEISHU_REPORT_RPS`) keep uploads within Feishu rate-limits.
- `pkg/devrecorder` mirrors device state (`DeviceSerial`, `Status`, `RunningTask`, etc.) to Feishu tables; override column names via `DEVICE_FIELD_*` env vars.
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

`FeishuTaskClient` fetches tasks in prioritized bands (个人页搜索 before 综合页搜索, same-day before backlog, failed before untouched) and only fills the shortfall to `MaxTasksPerJob`. See [`tasks.go`](tasks.go) for the full prioritization table.

## Troubleshooting
- **Missing tasks** – verify `TASK_BITABLE_URL` points to a view with Status=`pending/failed`, App matches the `Start` argument, and the service account has permission to read.
- **Recorder errors** – leave `DEVICE_BITABLE_URL`/`DEVICE_TASK_BITABLE_URL` empty to disable, or double-check field overrides align with your schema.
- **Result uploads throttled** – increase `RESULT_REPORT_BATCH`, relax `RESULT_REPORT_POLL_INTERVAL`, or scale `FEISHU_REPORT_RPS` to avoid 99991400 responses.
- **Webhook retries** – inspect `Webhook` column (pending/failed/error) and run `cmd/piracy webhook-worker` with the same App filter; see [`docs/webhook-worker.md`](docs/webhook-worker.md) for retry semantics.

## Further reading
- [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md) – comprehensive list of configuration keys.
- [`docs/result-storage.md`](docs/result-storage.md) – SQLite + async reporter internals.
- [`docs/piracy-group-tasks.md`](docs/piracy-group-tasks.md) – how group child tasks and webhooks are derived.
- [`docs/webhook-worker.md`](docs/webhook-worker.md) – webhook lifecycle and CLI usage.
- [`docs/feishu-api-summary.md`](docs/feishu-api-summary.md) – Bitable API reference for this repo.
- [`AGENTS.md`](AGENTS.md) – contributor workflow and coding standards.
