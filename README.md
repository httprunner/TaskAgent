# TaskAgent

TaskAgent is a Go 1.24 module that keeps Android capture devices busy by polling Feishu Bitable tasks, dispatching them through a pluggable job runner, and feeding status back to Feishu. The library is designed to be embedded in internal agents or schedulers that already know how to execute tasks once a device serial and payload are provided.

## Highlights
- **Feishu-native task source** – `feishu` package parses Bitable links, fetches pending rows, and updates statuses without duplicating schemas in code.
- **Device pool orchestration** – `pool.DevicePoolAgent` refreshes connected devices, batches tasks, and invokes your `JobRunner` implementation with cancelable contexts.
- **Provider & manager interfaces** – swap in custom `DeviceProvider` or `TaskManager` implementations while the defaults cover ADB devices and Feishu queues.
- **Lean dependency stack** – relies on `httprunner/v5`'s `gadb`, `zerolog`, and the official Lark Open Platform SDK only when needed.

## Repository Layout
- `pool.go` / `pool_test.go` – device scheduling loop, job lifecycle hooks, and unit coverage.
- `tasks.go` – Feishu-backed `TaskManager` plus helpers for query filters and status updates.
- `feishu/` – API client, Bitable schema structs, spreadsheet helpers, and dedicated README for module-level details.
- `providers/adb/` – thin wrapper over `gadb` that lists available Android serial numbers.
- `AGENTS.md` – contributor-focused workflows, coding standards, and security guardrails.

## Requirements
- Go 1.24 or newer (`go env GOVERSION` to verify).
- A Feishu custom app with API access plus the following environment variables: `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, optional `FEISHU_TENANT_KEY`, `FEISHU_BASE_URL`, and `FEISHU_LIVE_TEST` (toggles integration tests).
- A Feishu target Bitable that stores pending tasks (surface its link via `TASK_BITABLE_URL` or inject it directly into `pool.Config.BitableURL`).
- Optional observability tables: `DEVICE_INFO_BITABLE_URL` (设备信息表) and `DEVICE_TASK_BITABLE_URL` (设备任务表) for recording device heartbeat/dispatch snapshots. Leave empty to disable recording. Column names are customizable via `DEVICE_INFO_FIELD_*` / `DEVICE_TASK_FIELD_*`; defaults match the sample schemas below.
- Result write throttling: Feishu结果表写入内置全局限速器，默认 1 RPS（可通过 `FEISHU_REPORT_RPS` 配置浮点值），避免多设备并发写表触发频控。
- Access to the result Bitables you plan to push to, if any.
- Android Debug Bridge (ADB) on the PATH when using the default provider.

## Quick Start
1. Clone the repository and install dependencies:
   ```bash
   git clone git@github.com:httprunner/TaskAgent.git
   cd TaskAgent
   go mod download
   ```
2. Create a `.env` file (loaded by `godotenv`) and populate Feishu credentials plus Bitable URLs such as `TASK_BITABLE_URL`.
3. Run tests to validate the setup:
   ```bash
   go test ./...
   # Run Feishu live tests only when you have real tables configured
   FEISHU_LIVE_TEST=1 go test ./feishu -run Live
   ```
4. Embed the agent by implementing `pool.JobRunner` (import the module with an alias to match its package name):
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

   type MyRunner struct{}

   var _ pool.JobRunner = (*MyRunner)(nil)

   func (MyRunner) RunJob(ctx context.Context, req pool.JobRequest) error {
       // Operate on req.DeviceSerial and req.Tasks.
       return nil
   }

   func main() {
       cfg := pool.Config{
           PollInterval:   30 * time.Second,
           MaxTasksPerJob: 2,
           BitableURL:     os.Getenv(feishu.EnvTaskBitableURL),
           DeviceRecorder: mustRecorder, // optional: write device info / job rows to Feishu
           AgentVersion:   "v0.0.0",     // propagate to recorder rows
       }
       if cfg.BitableURL == "" {
           log.Fatal("set TASK_BITABLE_URL before starting the pool agent")
       }

       runner := &MyRunner{}
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
   If you configure `DEVICE_INFO_BITABLE_URL` / `DEVICE_TASK_BITABLE_URL`, the pool will upsert device heartbeats (Status/LastSeenAt) and create one row per dispatch (JobID `${serial}-YYMMDDHHMM`, AssignedTasks, Start/End, State, ErrorMessage). Leave the URLs empty to skip recording.
   `MyRunner` must satisfy `pool.JobRunner` so the agent can call `RunJob` per device batch; decode the Feishu payload from `req.Tasks[n].Payload`. Pass the `app` argument that matches the Feishu target-table `App` column so the built-in `FeishuTaskClient` filters and updates the correct rows (statuses transition through `dispatched`, `success`, and `failed`).

## Development & Testing
- Format/lint: `go fmt ./...` and `go vet ./...` to keep style consistent.
- Unit tests: `go test ./...` covers pool orchestration and Feishu helpers via mocks.
- Live integration: enable `FEISHU_LIVE_TEST=1` before running `go test ./feishu -run Live`; these tests will touch real Bitables, so only run them in disposable environments.

## Documentation & Support
- Contributor guide: see `AGENTS.md` for coding standards, testing expectations, and security notes.
- Feishu integration: `feishu/README.md` details schema expectations, sample payloads, and troubleshooting steps.
- Issues & ideas: open a GitHub issue with logs (scrub secrets) and the command/output you ran (`go test`, `go build`, etc.).
