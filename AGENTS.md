# Repository Guidelines

## 1. Project orientation
- **Core packages**: root package `taskagent`（模块路径 `github.com/httprunner/TaskAgent`）hosts the scheduling core（设备池 + Feishu 任务源 + 生命周期回调，作为外部集成的唯一入口），`internal/feishusdk` wraps every Bitable call（通过 `taskagent` 根包对外暴露常量和高层 API），`internal/storage` handles SQLite + Feishu reporting（对外通过 `taskagent` 暴露结果存储 API），`pkg/webhook` implements summary webhook flows, and `providers/adb` is the default device provider. Read [`README.md`](README.md) for the architecture diagram and [`docs/`](docs) for deep dives.
- **Configuration**: keep Feishu credentials plus table URLs in `.env` (`FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `TASK_BITABLE_URL`, etc.). Use `internal/env` helpers for reading configuration (they auto-call `env.Ensure`), and avoid raw `os.Getenv` in new code.

## 2. Daily development workflow
1. Install deps via `go mod download` and keep Go at 1.24+.
2. Before pushing, always run:
   ```bash
   go fmt ./...
   go vet ./...
   go test ./...
   ```
   Add `FEISHU_LIVE_TEST=1 go test ./internal/feishusdk -run Live` only when you have access to production Bitables.
3. When touching schedulers/webhook workflows, update the relevant doc under `docs/` and cross-link from the README if behavior changes.

## 3. Coding conventions
- Gofmt everything, keep packages short + lowercase, and colocate mocks in `*_test.go` files.
- Use interfaces (`taskagent.DeviceProvider`, `taskagent.TaskManager`, `taskagent.JobRunner`) instead of wiring business logic directly into the pool.
- Honor `TaskNotifier`: invoke `OnTaskStarted` before the device work begins and call `OnTaskResult` exactly once per task so Feishu status + recorder state stay consistent.
- Prefer structured logging via `zerolog` and propagate `context.Context` through every exported API so callers can cancel long-running Feishu calls.

## 4. Feature-specific guidance
- **New JobRunners**: surface clear errors, respect context cancellation, and avoid blocking the scheduler (long runs should stream progress via the lifecycle hooks).
- **New device providers**: add them under `providers/<name>`; never modify ADB-specific code to keep platform concerns isolated. Provide unit fakes if the provider shells out.
- **Storage & recorder**: if you change schema fields, update `internal/feishusdk/constants.go`, `docs/result-storage.md`, and the env matrix. Async reporter tuning knobs (`RESULT_REPORT_*`, `FEISHU_REPORT_RPS`) must be documented whenever defaults shift.
- **Group/webhook flows**: keep semantics aligned with `pkg/webhook`（尤其是 GroupID / Status / BizType / TaskIDs 字段约定），并确保 creator/worker 变更时在 `pkg/webhook` 中补充表驱动测试。

## 5. Testing expectations
- Table-driven tests for every scheduling or webhook branch (`Test<Component><Scenario>` naming).
- Mock Feishu interactions unless the test is explicitly gated by `FEISHU_LIVE_TEST`.
- Cover failure modes (rate limits, missing envs, empty device lists) so TaskAgent remains resilient.
- Target ≥80 % statement coverage for new packages and justify any intentional gaps in the PR description.

## 6. Commits & reviews
- Use Conventional Commit headers (`feat: pool`, `fix: storage`, `docs: webhook`) for clarity.
- Each PR description must include: problem statement, approach summary, configs/envs touched, and output from `go test ./...` (plus live-test output if applicable).
- Keep commits free of generated artifacts and double-check `git status` before pushing.

## 7. Security & secrets
- Never commit `.env`, Bitable URLs, or raw access tokens. Use `NewClientFromEnv` so secrets live outside code.
- Sanitize logs (Task params may contain user data). Avoid copying payload blobs into errors unless necessary for debugging.
- When sharing repro steps, mask device serials and Feishu URLs.

## 8. Documentation touchpoints
- README → high-level overview, `docs/ENVIRONMENT.md` → authoritative env reference, `docs/` → subsystem deep dives. Keep them in sync with code changes.
- If you add a new env var or CLI flag, update `docs/ENVIRONMENT.md` plus the relevant doc (storage, webhook, recorder, etc.).
- Feishu API reference → prioritize official Feishu Apifox docs when working on Feishu-related APIs/parameters: https://feishu.apifox.cn/llms.txt

## 9. Language & submission rules
- Use Chinese for day-to-day discussions in code reviews, PR descriptions, and issues unless stakeholders request otherwise.
- Keep all code comments, inline documentation, and TODO markers in English for consistency with tooling and linters.
- Git commit messages must be written in English and follow the Conventional Commits format (e.g., `feat: ...`, `fix: ...`).
