# Repository Guidelines

## Project Structure & Module Organization
TaskAgent is a Go 1.24 module centered on the `pool` package: `pool.go` and `tasks.go` orchestrate device polling, job dispatch, lifecycle callbacks, and Feishu task sync, while `pool_test.go` covers scheduler behavior. `feishu/` holds the API client plus Bitable helpers, and `providers/adb/` wraps httprunner `gadb` to list Android serials. Refer to `README.md` for a high-level architecture recap, and keep Feishu credentials + table links (`FEISHU_APP_ID`, `FEISHU_APP_SECRET`, optional `FEISHU_TENANT_KEY`, `FEISHU_BASE_URL`, `TASK_BITABLE_URL`, optional `FEISHU_LIVE_TEST`) in `.env` so `godotenv` can load them for local runs.

## Build, Test, and Development Commands
- `go fmt ./...` — canonical formatting before commits.
- `go vet ./...` — static sanity check for scheduler, Feishu, and providers.
- `go test ./...` — unit tests for pool and Feishu helpers. Lifecycle changes (pending → dispatched → running → success/failed) must include unit coverage in `pool_test.go` so regressions are caught.
- `FEISHU_LIVE_TEST=1 go test ./feishu -run Live` — integration run against real Bitable tables; supply env vars and reachable tables.
- `go build ./...` — compile every package prior to publishing.

## Coding Style & Naming Conventions
Follow idiomatic Go: tabs for indentation, `gofmt` spacing, and short lowercase package names (`feishu`, `providers/adb`). Exported APIs use PascalCase (`DevicePoolAgent`, `FetchAvailableTasks`); private helpers stay camelCase. Keep files narrowly scoped—provider logic lives in `providers/<name>`, Feishu contracts under `feishu/`, and test doubles beside their code in `_test.go`.

When building new runners, always honor the `TaskLifecycle` callbacks passed through `pool.JobRequest.Lifecycle`: invoke `OnTaskStarted` before executing each task and `OnTaskResult` afterward. This keeps Feishu task rows and device tables in sync (pending → dispatched → running → success/failed) even for custom integrations.

## Testing Guidelines
Add table-driven tests mirroring `pool_test.go` for each new scheduling branch, and include negative cases for Feishu filters. Use the `Test<Component><Scenario>` naming pattern so failures map to behaviors. Keep CI deterministic by skipping live Feishu tests unless `FEISHU_LIVE_TEST=1` is set. Target ≥80% statement coverage for new packages and call out intentional gaps in the PR description.

## Commit & Pull Request Guidelines
With no existing history, adopt Conventional Commits (`feat: pool`, `fix: feishu`) to keep automation predictable. Each PR should describe the problem, outline the approach (files touched, configs added), and paste `go test ./...` output or screenshots for live Feishu runs. Link to tracking issues when possible and mention required env changes so reviewers can reproduce quickly.

## Security & Configuration Tips
Never commit `.env` or raw Feishu URLs. Use `NewClientFromEnv` so secrets stay outside source control, rotate credentials if live tests fail because of auth errors, and sanitize `zerolog` output before sharing logs because payloads may include user data. When targeting non-Android fleets, create a new provider package rather than editing `providers/adb` so platform-specific permissions stay isolated.
