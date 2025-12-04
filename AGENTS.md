# Repository Guidelines

## 1. Project orientation
- **Core packages**: `pool.go`/`tasks.go` drive scheduling + lifecycle hooks, `pkg/feishu` wraps every Bitable call, `pkg/storage` handles SQLite + Feishu reporting, `pkg/piracy` implements detection/webhook workflows, and `providers/adb` is the default device provider. Read [`README.md`](README.md) for the architecture diagram and [`docs/`](docs) for deep dives.
- **Configuration**: keep Feishu credentials plus table URLs in `.env` (`FEISHU_APP_ID`, `FEISHU_APP_SECRET`, `TASK_BITABLE_URL`, etc.). `internal/envload.Ensure` loads the first `.env` it finds upward from the repo root.

## 2. Daily development workflow
1. Install deps via `go mod download` and keep Go at 1.24+.
2. Before pushing, always run:
   ```bash
   go fmt ./...
   go vet ./...
   go test ./...
   ```
   Add `FEISHU_LIVE_TEST=1 go test ./feishu -run Live` only when you have access to production Bitables.
3. When touching schedulers/piracy workflows, update the relevant doc under `docs/` and cross-link from the README if behavior changes.

## 3. Coding conventions
- Gofmt everything, keep packages short + lowercase, and colocate mocks in `*_test.go` files.
- Use interfaces (`pool.DeviceProvider`, `pool.TaskManager`, `pool.JobRunner`) instead of wiring business logic directly into the pool.
- Honor `TaskLifecycle`: invoke `OnTaskStarted` before the device work begins and call `OnTaskResult` exactly once per task so Feishu status + recorder state stay consistent.
- Prefer structured logging via `zerolog` and propagate `context.Context` through every exported API so callers can cancel long-running Feishu calls.

## 4. Feature-specific guidance
- **New JobRunners**: surface clear errors, respect context cancellation, and avoid blocking the scheduler (long runs should stream progress via the lifecycle hooks).
- **New device providers**: add them under `providers/<name>`; never modify ADB-specific code to keep platform concerns isolated. Provide unit fakes if the provider shells out.
- **Storage & recorder**: if you change schema fields, update `pkg/feishu/constants.go`, `docs/result-storage.md`, and the env matrix. Async reporter tuning knobs (`RESULT_REPORT_*`, `FEISHU_REPORT_RPS`) must be documented whenever defaults shift.
- **Piracy/webhook flows**: keep group-task semantics aligned with [`docs/piracy-group-tasks.md`](docs/piracy-group-tasks.md) and ensure webhook-worker changes include table-driven tests in `pkg/piracy`.

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
- If you add a new env var or CLI flag, update `docs/ENVIRONMENT.md` plus the relevant doc (storage, webhook, piracy, recorder, etc.).

## 9. Language & submission rules
- Use Chinese for day-to-day discussions in code reviews, PR descriptions, and issues unless stakeholders request otherwise.
- Keep all code comments, inline documentation, and TODO markers in English for consistency with tooling and linters.
- Git commit messages must be written in English and follow the Conventional Commits format (e.g., `feat: ...`, `fix: ...`).
